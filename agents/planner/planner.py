"""
Planner Agent —— 将用户任务拆解为子任务图，调用 DeepSeek API 生成规划，
并将子任务写入 TaskManager，通过 EventBus 广播事件。

使用方式:
    from agents.planner import Planner
    from runtime import EventBus, TaskManager

    bus = EventBus()
    tm  = TaskManager()
    planner = Planner(bus, tm, api_key="sk-xxx")

    result = planner.plan("写一个 Python 贪吃蛇游戏", task_id="task-root")
    # result == {
    #     "task_id": "task-root",
    #     "subtasks": [
    #         {"id": "sub-1", "title": "...", "description": "...", "depends_on": []},
    #         ...
    #     ]
    # }
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

import requests

from runtime.event_bus import EventBus
from runtime.task_manager import TaskManager

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
logger = logging.getLogger("planner")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# DeepSeek 规划提示词模板
# ---------------------------------------------------------------------------
PLAN_SYSTEM_PROMPT = """你是一个资深的软件工程架构师，擅长将复杂的开发任务拆解为可独立执行的子任务。

请将用户提供的开发任务拆解为 3-10 个子任务。每个子任务必须满足：
- 单一职责：一个子任务只做一件事
- 可独立执行：描述清晰到另一个开发者可以直接开工
- 依赖明确：如果子任务 B 必须在子任务 A 之后执行，需要在 depends_on 中标明

## 输出格式
你**必须**只输出一个合法的 JSON 数组，不要带任何 markdown 标记或额外文字。数组元素结构如下：

[
  {
    "title": "子任务简短标题",
    "description": "详细描述，包含技术栈、关键文件路径、验收标准",
    "depends_on": []          // 依赖的子任务在前一个数组中的 0-based 索引列表
  },
  ...
]

示例：
用户: 实现一个 REST API 用户登录功能
输出:
[
  {"title": "设计数据库用户表", "description": "创建 users 表，字段包含 id, username, password_hash, email, created_at", "depends_on": []},
  {"title": "实现注册端点", "description": "POST /api/register，校验输入，哈希密码，写入数据库", "depends_on": [0]},
  {"title": "实现登录端点", "description": "POST /api/login，验证凭据，返回 JWT token", "depends_on": [0]},
  {"title": "添加认证中间件", "description": "解析 JWT，注入 request.user，保护需要认证的路由", "depends_on": [2]}
]
"""

PLAN_USER_TEMPLATE = """请将以下任务拆解为子任务：

任务描述：{task}

请直接输出 JSON 数组。"""


# ---------------------------------------------------------------------------
# Planner Agent
# ---------------------------------------------------------------------------
class Planner:
    """Planner Agent —— 将用户任务拆解为子任务图。

    Parameters
    ----------
    bus : EventBus
        项目级事件总线，用于广播 planning 相关事件。
    tm : TaskManager
        项目级任务管理器，用于持久化子任务。
    api_key : str | None
        DeepSeek API Key。若不传则从环境变量 DEEPSEEK_API_KEY 读取。
    model : str
        调用的 DeepSeek 模型名称，默认 deepseek-chat。
    api_base : str
        DeepSeek API 基础地址，默认 https://api.deepseek.com/v1。
    max_retries : int
        API 调用失败时的最大重试次数。
    """

    DEFAULT_MODEL = "deepseek-chat"
    DEFAULT_API_BASE = "https://api.deepseek.com/v1"

    def __init__(
        self,
        bus: EventBus,
        tm: TaskManager,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        api_base: Optional[str] = None,
        max_retries: int = 3,
    ) -> None:
        self.bus = bus
        self.tm = tm
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        self.model = model or self.DEFAULT_MODEL
        self.api_base = (api_base or self.DEFAULT_API_BASE).rstrip("/")
        self.max_retries = max_retries

        if not self.api_key:
            raise ValueError(
                "DEEPSEEK_API_KEY 未设置。请通过构造函数传入 api_key，"
                "或设置环境变量 DEEPSEEK_API_KEY。"
            )

        self._chat_url = f"{self.api_base}/chat/completions"

    # ------------------------------------------------------------------
    # 公开 API
    # ------------------------------------------------------------------

    def plan(self, task: str, task_id: Optional[str] = None) -> Dict[str, Any]:
        """接收用户任务，拆解为子任务图，写入 TaskManager，广播事件。

        Parameters
        ----------
        task : str
            用户的需求描述 / 开发目标。
        task_id : str | None
            可选的任务 ID。若不传则自动生成 UUID。

        Returns
        -------
        dict
            {
                "task_id": str,
                "subtasks": [
                    {"id": "uuid", "title": "...", "description": "...", "depends_on": [...]},
                    ...
                ]
            }

        Raises
        ------
        RuntimeError
            当 DeepSeek API 调用全部重试失败时抛出。
        """
        task_id = task_id or str(uuid.uuid4())

        logger.info("[plan] 开始规划 task_id=%s task='%s'", task_id, task)

        # 1. 调用 DeepSeek 获取子任务原始列表
        raw_subtasks = self._call_deepseek(task)

        # 2. 为每个子任务生成 UUID，同时完成依赖映射
        subtask_ids = [str(uuid.uuid4()) for _ in raw_subtasks]
        id_map: Dict[int, str] = {i: sid for i, sid in enumerate(subtask_ids)}

        subtasks: List[Dict[str, Any]] = []
        for idx, raw in enumerate(raw_subtasks):
            subtask = {
                "id": subtask_ids[idx],
                "title": raw.get("title", f"Subtask-{idx + 1}"),
                "description": raw.get("description", ""),
                "depends_on": [
                    id_map[di] for di in raw.get("depends_on", []) if di in id_map
                ],
            }
            subtasks.append(subtask)

        # 3. 写入 TaskManager —— 每个子任务 agent="coder", status="pending"
        for st in subtasks:
            self.tm.create_task(
                agent="coder",
                payload={
                    "parent_task_id": task_id,
                    "title": st["title"],
                    "description": st["description"],
                    "depends_on": st["depends_on"],
                },
                task_id=st["id"],
            )
            logger.debug("[plan] 创建子任务 %s: %s", st["id"], st["title"])

        # 4. 更新父任务（若存在）
        try:
            self.tm.update_task(
                task_id=task_id,
                payload={"goal": task, "subtask_count": len(subtasks)},
                status="pending",
            )
        except Exception:
            # 父任务可能不在 TaskManager 中，忽略
            pass

        # 5. 广播事件
        plan_payload = {
            "task_id": task_id,
            "goal": task,
            "subtasks": subtasks,
            "subtask_count": len(subtasks),
        }
        self.bus.emit_event(task_id, "planner", "task.planned", plan_payload)
        logger.info("[plan] 事件已发送: task.planned (task_id=%s)", task_id)

        for st in subtasks:
            self.bus.emit_event(
                st["id"],
                "planner",
                "subtask.created",
                {
                    "parent_task_id": task_id,
                    "subtask_id": st["id"],
                    "title": st["title"],
                    "description": st["description"],
                    "depends_on": st["depends_on"],
                },
            )
        logger.info("[plan] 事件已发送: subtask.created x%d", len(subtasks))

        # 6. 返回结构化结果
        result: Dict[str, Any] = {
            "task_id": task_id,
            "subtasks": subtasks,
        }
        logger.info(
            "[plan] 规划完成 task_id=%s subtask_count=%d",
            task_id,
            len(subtasks),
        )
        return result

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _call_deepseek(self, task: str) -> List[Dict[str, Any]]:
        """调用 DeepSeek Chat API，返回子任务列表。

        Returns
        -------
        list[dict]
            原始解析出的子任务列表，每个元素含 title / description / depends_on。
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": PLAN_SYSTEM_PROMPT},
                {"role": "user", "content": PLAN_USER_TEMPLATE.format(task=task)},
            ],
            "temperature": 0.3,
            "max_tokens": 4096,
        }

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug("[plan] DeepSeek API 第 %d/%d 次调用", attempt, self.max_retries)
                resp = requests.post(
                    self._chat_url,
                    headers=headers,
                    json=payload,
                    timeout=60,
                )

                if resp.status_code == 401:
                    raise RuntimeError(
                        "DeepSeek API 认证失败 (401)。请检查 DEEPSEEK_API_KEY 是否正确。"
                    )
                if resp.status_code == 402:
                    raise RuntimeError(
                        "DeepSeek API 账户余额不足 (402)。请充值后重试。"
                    )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    logger.warning(
                        "[plan] 触发速率限制 (429)，%d 秒后重试...", retry_after
                    )
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()

                data = resp.json()
                content = data["choices"][0]["message"]["content"]

                # 清洗可能的 markdown 代码块包裹
                content = self._strip_markdown_fence(content)

                parsed = json.loads(content)
                if not isinstance(parsed, list):
                    raise ValueError(f"API 返回的不是 JSON 数组: {type(parsed)}")

                return parsed

            except (requests.RequestException, json.JSONDecodeError, ValueError, KeyError) as exc:
                last_error = exc
                logger.warning(
                    "[plan] API 调用失败 (第 %d/%d 次): %s",
                    attempt,
                    self.max_retries,
                    exc,
                )
                if attempt < self.max_retries:
                    sleep_s = 2 ** attempt  # 指数退避
                    logger.debug("[plan] %d 秒后重试...", sleep_s)
                    time.sleep(sleep_s)

        # 所有重试均已耗尽
        raise RuntimeError(
            f"DeepSeek API 调用在 {self.max_retries} 次重试后仍然失败。"
            f"最后一次错误: {last_error}"
        )

    @staticmethod
    def _strip_markdown_fence(text: str) -> str:
        """移除可能的 ```json ... ``` 包裹。"""
        t = text.strip()
        if t.startswith("```json"):
            t = t[len("```json"):]
        elif t.startswith("```"):
            t = t[len("```"):]
        if t.endswith("```"):
            t = t[:-3]
        return t.strip()
