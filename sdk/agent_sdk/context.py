"""
RuntimeContext —— Agent SDK 的运行时上下文。

将 EventBus / TaskManager / WorkspaceManager / BaseProvider / 共享内存 / Agent 注册表
统一收敛为单一上下文对象，供所有 BaseAgent 子类通过 self.ctx 访问。

Usage::

    from runtime import EventBus, TaskManager
    from workspace import WorkspaceManager
    from providers import BaseProvider
    from sdk.agent_sdk.context import RuntimeContext

    ctx = RuntimeContext(
        event_bus=EventBus(),
        task_manager=TaskManager(),
        workspace=WorkspaceManager(),
        provider=my_provider,    # 任意 BaseProvider 子类实例
    )

    # 第三方 Agent 使用
    agent = MyAgent(ctx)
    ctx.register_agent(agent)
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from runtime.event_bus import EventBus
    from runtime.task_manager import TaskManager
    from workspace.manager import WorkspaceManager
    from providers.base_provider import BaseProvider
    from sdk.agent_sdk.base_agent import BaseAgent


@dataclass
class RuntimeContext:
    """Agent 运行时上下文，持有所有底层基础设施引用。

    线程安全：
        shared_memory 使用 threading.Lock 保护。
        agent_registry 使用 threading.Lock 保护。
    """

    event_bus: "EventBus"
    """项目级 EventBus 实例，Agent 通过 emit_event() 快捷方法发事件。"""

    task_manager: "TaskManager"
    """项目级 TaskManager 实例，Agent 通过 ctx.task_manager 操作任务。"""

    workspace: "WorkspaceManager"
    """WorkspaceManager 实例，提供 per-task 隔离工作区。"""

    provider: "BaseProvider"
    """AI Provider 实例（BaseProvider 子类），Agent 通过 ctx.provider.chat() 调用模型。"""

    shared_memory: Dict[str, Any] = field(default_factory=dict)
    """跨 Agent 共享内存字典。使用 threading.Lock 保证线程安全。

    用法：
        with ctx.shared_memory_lock:
            ctx.shared_memory["my_key"] = value
    """

    agent_registry: Dict[str, "BaseAgent"] = field(default_factory=dict)
    """Agent 注册表，key 为 agent.name，value 为 BaseAgent 实例。"""

    # ── 内部锁 ──────────────────────────────────────────────

    _shared_memory_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _agent_registry_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # ── 公开 API ────────────────────────────────────────────

    @property
    def shared_memory_lock(self) -> threading.Lock:
        """共享内存的线程锁（供外部 with 语句使用）。"""
        return self._shared_memory_lock

    def get_agent(self, name: str) -> Optional["BaseAgent"]:
        """按名称查找已注册的 Agent。

        Parameters
        ----------
        name : str
            Agent 的 self.name。

        Returns
        -------
        BaseAgent | None
            找到返回 Agent 实例，未找到返回 None。
        """
        with self._agent_registry_lock:
            return self.agent_registry.get(name)

    def register_agent(self, agent: "BaseAgent") -> None:
        """注册 Agent 到运行时上下文。

        幂等：重复注册同名 Agent 会覆盖旧实例。

        Parameters
        ----------
        agent : BaseAgent
            待注册的 Agent 实例。
        """
        with self._agent_registry_lock:
            self.agent_registry[agent.name] = agent

    def unregister_agent(self, name: str) -> bool:
        """注销指定 Agent。

        Returns
        -------
        bool
            True 表示成功移除，False 表示 Agent 不存在。
        """
        with self._agent_registry_lock:
            if name in self.agent_registry:
                del self.agent_registry[name]
                return True
            return False

    def list_agents(self) -> Dict[str, "BaseAgent"]:
        """返回当前所有已注册 Agent 的快照。"""
        with self._agent_registry_lock:
            return dict(self.agent_registry)
