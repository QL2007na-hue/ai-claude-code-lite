"""
BaseAgent —— Agent SDK 抽象基类。

第三方开发者继承此类，实现自己的 Agent，统一对接 ai-runtime 的
EventBus / TaskManager / WorkspaceManager / AI Provider 等基础设施。

生命周期：
    1. ctx = RuntimeContext(...)
    2. agent = MyAgent(ctx)           # 构造，持有 ctx 引用
    3. agent.subscribe([...])         # 注册关心的事件（可选，在 init 中调用）
    4. await agent.init()             # 异步初始化（启动事件监听线程等）
    5. await agent.run(task_id)       # 执行任务（可多次调用）
    6. await agent.stop()             # 优雅停止

最小实现示例::

    from sdk.agent_sdk.base_agent import BaseAgent

    class MyAgent(BaseAgent):
        @property
        def name(self) -> str:
            return "my-agent"

        async def on_event(self, task_id, agent, event, payload):
            self.logger.info("收到事件: %s 来自 %s", event, agent)
            self.emit_event("event.received", {"original_event": event})

        async def run(self, task_id: str):
            self.logger.info("开始处理任务 %s", task_id)
            self.emit_event("task.started", {})
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import traceback
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from sdk.agent_sdk.context import RuntimeContext


class BaseAgent(ABC):
    """Agent SDK 抽象基类。

    所有第三方 Agent 必须继承此类，并实现：
        - name (property)
        - on_event (async method)

    可选覆盖：
        - init()   —— 自定义初始化逻辑
        - run()    —— 自定义任务执行逻辑
        - stop()   —— 自定义清理逻辑
    """

    # ── 构造 & 属性 ─────────────────────────────────────────

    def __init__(self, ctx: "RuntimeContext") -> None:
        """初始化 Agent。

        Parameters
        ----------
        ctx : RuntimeContext
            运行时上下文，包含 EventBus / TaskManager / Workspace / Provider 等。
        """
        self.ctx = ctx
        """运行时上下文，可访问 event_bus / task_manager / workspace / provider / shared_memory。"""

        self.running: bool = False
        """Agent 是否处于运行状态。由 init() 设为 True，stop() 设为 False。"""

        self._current_task_id: Optional[str] = None
        """当前正在执行的任务 ID（在 run() 中设置）。"""

        self._subscribed_events: Set[str] = set()
        """Agent 关心的事件集合。空集合表示接收所有事件。"""

        self._listen_thread: Optional[threading.Thread] = None
        """事件监听后台线程。"""

        self._lock: threading.Lock = threading.Lock()
        """内部线程锁。"""

        # 预配置日志器
        self.logger: logging.Logger = logging.getLogger(f"agent.{self.name}")
        """预配置日志器，名称格式为 agent.<name>。"""
        if not self.logger.handlers:
            _h = logging.StreamHandler()
            _h.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            ))
            self.logger.addHandler(_h)
            self.logger.setLevel(logging.DEBUG)

    @property
    def name(self) -> str:
        """Agent 唯一名称。

        默认返回类名，子类应覆盖此属性。
        该名称同时用于：
            - agent_registry 的 key
            - EventBus emit_event 的 agent 字段
            - logger 的名称后缀
        """
        return self.__class__.__name__

    # ── 事件发射快捷方法 ────────────────────────────────────

    def emit_event(self, event: str, payload: Any = None) -> Optional[str]:
        """向 EventBus 发射事件的快捷方法。

        等价于：
            self.ctx.event_bus.emit_event(
                task_id=self._current_task_id,
                agent=self.name,
                event=event,
                payload=payload,
            )

        Parameters
        ----------
        event : str
            事件名称（如 "task.started", "analysis.complete"）。
        payload : Any
            事件负载，会通过 json.dumps 序列化。

        Returns
        -------
        str | None
            事件在 Redis Stream 中的消息 ID；若 task_id 未设置则返回 None。

        Note
        ----
        必须在 run(task_id) 被调用后使用，否则 task_id 未设置。
        """
        if self._current_task_id is None:
            self.logger.warning(
                "emit_event('%s') —— 当前无 task_id，事件未发送。"
                "请在 run(task_id) 内部调用此方法。",
                event,
            )
            return None

        return self.ctx.event_bus.emit_event(
            task_id=self._current_task_id,
            agent=self.name,
            event=event,
            payload=payload,
        )

    # ── 事件订阅 ────────────────────────────────────────────

    def subscribe(self, events: List[str]) -> None:
        """注册 Agent 关心的事件列表。

        应在 init() 中调用，例如：
            await super().init()
            self.subscribe(["task.created", "task.planned"])

        不调用 subscribe() 或传入空列表表示接收所有事件。

        Parameters
        ----------
        events : list[str]
            事件名称列表。支持字符串匹配（精确匹配）。
            传入空列表或 ["*"] 可接收全部事件。
        """
        self._subscribed_events = set(events)
        self.logger.debug("订阅事件: %s", self._subscribed_events or "[全部]")

    def _event_matches(self, event: str) -> bool:
        """判断事件是否匹配当前 Agent 的订阅。"""
        if not self._subscribed_events or "*" in self._subscribed_events:
            return True
        return event in self._subscribed_events

    # ── 生命周期 ────────────────────────────────────────────

    async def init(self) -> None:
        """异步初始化 Agent。

        子类覆盖时务必先调用 super().init()：
            async def init(self):
                await super().init()
                # 自定义初始化逻辑...

        默认行为：
            1. 设置 running = True
            2. 启动事件监听后台线程
            3. 将自身注册到 ctx.agent_registry
        """
        self.running = True
        self.ctx.register_agent(self)

        # 启动事件监听后台线程
        self._listen_thread = threading.Thread(
            target=self._event_listen_loop,
            daemon=True,
            name=f"agent-listener-{self.name}",
        )
        self._listen_thread.start()

        self.logger.info("Agent '%s' 已初始化", self.name)

    async def run(self, task_id: str) -> None:
        """执行 Agent 主任务。

        子类应覆盖此方法实现具体任务逻辑：
            async def run(self, task_id: str):
                task = self.ctx.task_manager.get_task(task_id)
                ...
                self.emit_event("task.done", {"result": ...})

        Parameters
        ----------
        task_id : str
            任务 ID，Agent 通过 self.ctx.task_manager 获取任务详情。
        """
        self._current_task_id = task_id
        self.logger.info("Agent '%s' 开始处理任务 %s", self.name, task_id)

    async def stop(self) -> None:
        """优雅停止 Agent。

        默认行为：
            1. 设置 running = False
            2. 等待事件监听线程退出（最多 10 秒）
            3. 从 ctx.agent_registry 中注销
        """
        self.running = False

        if self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=10)
            if self._listen_thread.is_alive():
                self.logger.warning("事件监听线程未能在 10s 内退出")

        self.ctx.unregister_agent(self.name)
        self.logger.info("Agent '%s' 已停止", self.name)

    # ── 事件监听（内部） ────────────────────────────────────

    def _event_listen_loop(self) -> None:
        """后台线程：通过 EventBus.subscribe() 监听事件并分发到 on_event()。

        使用同步回调方式（EventBus.subscribe 是阻塞式 while 循环），
        在回调中通过 asyncio.run_coroutine_threadsafe 将事件分发到异步 on_event。
        """
        bus = self.ctx.event_bus

        def callback(data: Dict[str, str]) -> None:
            if not self.running:
                raise StopIteration()

            task_id = data.get("task_id", "")
            event = data.get("event", "")
            agent = data.get("agent", "")

            # 过滤事件
            if not self._event_matches(event):
                return

            # 解析 payload
            raw_payload = data.get("payload", "{}")
            payload: Any = {}
            try:
                payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
            except (json.JSONDecodeError, TypeError):
                payload = {"raw": raw_payload}

            # 分发到 on_event
            try:
                # 在 event loop 中调度异步 on_event
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        self._safe_on_event(task_id, agent, event, payload),
                        loop,
                    )
                else:
                    # 当前线程没有 running loop，同步执行
                    self.logger.warning(
                        "事件 %s 无法调度（无 running event loop），同步执行",
                        event,
                    )
                    asyncio.run(self._safe_on_event(task_id, agent, event, payload))
            except RuntimeError:
                # asyncio.get_event_loop() 在某些环境中会抛 RuntimeError
                self.logger.warning(
                    "事件 %s 无法获取 event loop，同步执行", event
                )
                try:
                    asyncio.run(self._safe_on_event(task_id, agent, event, payload))
                except Exception:
                    self.logger.exception(
                        "on_event 同步执行异常: task_id=%s event=%s", task_id, event
                    )

        try:
            bus.subscribe(callback)
        except StopIteration:
            self.logger.debug("事件监听线程收到停止信号")
        except Exception:
            if self.running:
                self.logger.exception("Agent '%s' 事件监听异常", self.name)

    async def _safe_on_event(
        self, task_id: str, agent: str, event: str, payload: Any
    ) -> None:
        """带异常保护的 on_event 包装。"""
        try:
            await self.on_event(task_id, agent, event, payload)
        except Exception:
            self.logger.exception(
                "on_event 异常: task_id=%s agent=%s event=%s",
                task_id, agent, event,
            )

    # ── 抽象方法 ────────────────────────────────────────────

    @abstractmethod
    async def on_event(self, task_id: str, agent: str, event: str, payload: Any) -> None:
        """处理接收到的 EventBus 事件。

        子类必须实现此方法，定义 Agent 对各类事件的响应逻辑。

        Parameters
        ----------
        task_id : str
            事件关联的任务 ID。
        agent : str
            发送事件的 Agent 名称。
        event : str
            事件名称（如 "task.planned", "task.coding_completed"）。
        payload : Any
            事件负载（dict 或原始数据）。

        Example
        -------
        async def on_event(self, task_id, agent, event, payload):
            if event == "task.planned":
                self.logger.info("检测到规划完成: %s", payload.get("subtask_count"))
            elif event == "task.coding_failed":
                self.logger.warning("编码失败，建议重试")
        """
        ...

    # ── 上下文快捷属性（文档用） ─────────────────────────────

    # 以下属性均通过 self.ctx 访问，这里仅作为类型提示文档：

    # @property
    # def task_manager(self): return self.ctx.task_manager
    # @property
    # def workspace(self): return self.ctx.workspace
    # @property
    # def provider(self): return self.ctx.provider
    # @property
    # def shared_memory(self): return self.ctx.shared_memory
