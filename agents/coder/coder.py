"""
CoderAgent - DeepSeek API 驱动代码生成 Agent

职责：
  1. 从 TaskManager 拉取 coding 任务
  2. 调用 DeepSeek API（OpenAI 兼容）生成代码
  3. 解析生成结果并写入 workspace/task-<id>/ 目录
  4. 执行 shell 命令（测试、lint 等）
  5. 自动 git commit 每个任务 workspace
  6. 通过 EventBus 发送事件，同步 TaskManager 状态

状态流转：
  pending → running → review   （成功）
                     → failed   （失败）
"""

from __future__ import annotations

import json
import logging
import os
import re
import textwrap
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from runtime.event_bus import EventBus
from runtime.task_manager import TaskManager
from workspace.manager import WorkspaceManager

logger = logging.getLogger("agents.coder")
logger.addHandler(logging.NullHandler())


class CoderAgent:
    """通过 DeepSeek API 生成代码并落地到任务独立 workspace 的 Agent。

    Parameters
    ----------
    task_manager : TaskManager
        任务管理器实例，用于读取 / 更新任务状态。
    event_bus : EventBus
        事件总线实例，用于发送过程事件。
    workspace_mgr : WorkspaceManager, optional
        工作区管理器。不传则自动创建默认实例。
    api_key : str, optional
        DeepSeek API Key。不传则从环境变量 DEEPSEEK_API_KEY 读取。
    model : str, default "deepseek-chat"
        DeepSeek 模型名称。
    max_retries : int, default 2
        API 调用失败时的最大重试次数。
    shell_timeout : int, default 120
        单次 shell 命令执行超时（秒）。
    """

    API_BASE = "https://api.deepseek.com/v1"
    CHAT_ENDPOINT = f"{API_BASE}/chat/completions"

    SYSTEM_PROMPT = textwrap.dedent("""\
        你是一个精通编程的 AI 助手。请严格按照以下格式输出代码：

        1. 每个文件用一个代码块表示，格式为：
           ```语言:文件相对路径
           代码内容
           ```

           示例：
           ```python:src/main.py
           print("hello")
           ```

        2. 如果某个文件需要执行 shell 命令（如安装依赖、运行测试），
           在代码块之后用单独一段标注，格式为：
           !shell
           cd workspace && python -m pytest

        3. 只输出代码，不要输出额外解释。确保代码完整、可直接运行。
        """)

    RE_CODE_BLOCK = re.compile(
        r"```(?P<lang>[^\s:]*?)\s*:\s*(?P<path>[^\n]+?)\s*\n"
        r"(?P<code>.*?)"
        r"```",
        re.DOTALL,
    )
    RE_SHELL_CMD = re.compile(r"^!shell\s*\n(?P<cmd>.+?)$", re.MULTILINE | re.DOTALL)

    def __init__(
        self,
        task_manager: TaskManager,
        event_bus: EventBus,
        workspace_mgr: Optional[WorkspaceManager] = None,
        api_key: Optional[str] = None,
        model: str = "deepseek-chat",
        max_retries: int = 2,
        shell_timeout: int = 120,
    ) -> None:
        self.tm = task_manager
        self.bus = event_bus
        self.wm = workspace_mgr or WorkspaceManager()

        self._api_key = api_key or os.environ.get("DEEPSEEK_API_KEY", "")
        if not self._api_key:
            logger.warning("DEEPSEEK_API_KEY 未配置；API 调用将失败。")

        self.model = model
        self.max_retries = max_retries
        self.shell_timeout = shell_timeout

        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        })

    # ── 公开入口 ────────────────────────────────────────────

    def execute(self, task_id: str) -> bool:
        """执行编码任务的主入口。

        Returns
        -------
        bool
            True 表示成功（任务进入 review），False 表示失败（任务进入 failed）。
        """
        task = self.tm.get_task(task_id)
        if task is None:
            logger.error("任务不存在: %s", task_id)
            return False

        logger.info("CoderAgent 开始执行任务 %s", task_id)

        # 创建 per-task workspace + git init
        ws_path = self.wm.create(task_id)

        try:
            self.tm.update_task(task_id, status="running")
            self.bus.emit_event(task_id, "coder", "task.coding_started", {
                "agent": task.get("agent", ""),
                "workspace": str(ws_path),
            })

            payload = self._parse_payload(task.get("payload", "{}"))
            raw_response = self._call_deepseek(payload)
            self.bus.emit_event(task_id, "coder", "task.code_generated", {
                "model": self.model,
                "response_length": len(raw_response),
            })

            files_written, shell_commands = self._parse_response(task_id, raw_response)
            for fpath in files_written:
                self.bus.emit_event(task_id, "coder", "task.code_written", {
                    "file": fpath,
                })

            # git commit 编码结果
            commit = self.wm.git_commit(task_id, f"coder: generated {len(files_written)} file(s)")
            self.bus.emit_event(task_id, "coder", "task.git_committed", {
                "commit": commit.get("stdout", ""),
                "files": files_written,
            })

            shell_results: List[Dict[str, Any]] = []
            for cmd in shell_commands:
                result = self.wm.run_shell(task_id, cmd, timeout=self.shell_timeout)
                shell_results.append(result)
                self.bus.emit_event(task_id, "coder", "task.shell_executed", {
                    "command": cmd,
                    "exit_code": result["exit_code"],
                    "output_preview": result["stdout"][:500],
                })

            result_payload = {
                "files_written": files_written,
                "shell_results": shell_results,
                "workspace": str(ws_path),
            }
            self.tm.update_task(task_id, status="review", result=result_payload)
            self.bus.emit_event(task_id, "coder", "task.coding_completed", {
                "files_count": len(files_written),
                "shell_count": len(shell_results),
            })
            logger.info("任务 %s 编码完成，进入 review", task_id)
            return True

        except Exception:
            tb = traceback.format_exc()
            logger.error("任务 %s 编码失败:\n%s", task_id, tb)
            self.tm.update_task(task_id, status="failed", result={"error": tb})
            self.bus.emit_event(task_id, "coder", "task.coding_failed", {
                "error": str(tb)[:1000],
            })
            return False

    # ── 内部方法 ────────────────────────────────────────────

    def _parse_payload(self, raw_payload: Any) -> Dict[str, Any]:
        if isinstance(raw_payload, dict):
            return raw_payload
        try:
            return json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError):
            return {"description": str(raw_payload)}

    def _call_deepseek(self, payload: Dict[str, Any]) -> str:
        user_message = payload.get("description", "")
        if not user_message:
            user_message = payload.get("task", payload.get("content", str(payload)))

        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]

        body = {
            "model": self.model,
            "messages": messages,
            "temperature": payload.get("temperature", 0.2),
            "max_tokens": payload.get("max_tokens", 4096),
            "stream": False,
        }

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self._session.post(
                    self.CHAT_ENDPOINT,
                    json=body,
                    timeout=(10, 120),
                )
                resp.raise_for_status()
                data = resp.json()
                choice = data.get("choices", [{}])[0]
                content = choice.get("message", {}).get("content", "")
                if not content:
                    raise ValueError("API 返回空内容")
                return content

            except requests.exceptions.Timeout as e:
                last_error = e
                logger.warning("API 超时 (attempt %d/%d)", attempt + 1, self.max_retries + 1)
            except requests.exceptions.HTTPError as e:
                last_error = e
                status = e.response.status_code if e.response is not None else "?"
                logger.warning("API HTTP %s (attempt %d/%d)", status, attempt + 1, self.max_retries + 1)
                if status == 401:
                    raise RuntimeError(f"DeepSeek API 认证失败: {e}") from e
            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning("API 请求异常 (attempt %d/%d): %s", attempt + 1, self.max_retries + 1, e)

            if attempt < self.max_retries:
                time.sleep(2 ** attempt)

        raise RuntimeError(f"DeepSeek API 调用失败（已重试 {self.max_retries} 次）") from last_error

    def _parse_response(self, task_id: str, raw: str) -> Tuple[List[str], List[str]]:
        files_written: List[str] = []
        shell_commands: List[str] = []

        for match in self.RE_CODE_BLOCK.finditer(raw):
            lang = match.group("lang") or ""
            relpath = match.group("path").strip()
            code = match.group("code")

            if not relpath:
                if lang:
                    relpath = f"generated.{lang}"
                else:
                    relpath = "generated.txt"

            try:
                self.wm.write_file(task_id, relpath, code)
                files_written.append(relpath)
            except (OSError, ValueError) as e:
                logger.error("写入文件失败 %s: %s", relpath, e)
                raise

        for match in self.RE_SHELL_CMD.finditer(raw):
            cmd = match.group("cmd").strip()
            if cmd:
                shell_commands.append(cmd)

        if not files_written:
            logger.warning("未匹配到代码块，将全文写入 generated.txt")
            self.wm.write_file(task_id, "generated.txt", raw)
            files_written.append("generated.txt")

        return files_written, shell_commands
