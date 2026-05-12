from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from runtime.event_bus import EventBus
from runtime.task_manager import TaskManager
from workspace.manager import WorkspaceManager


@dataclass
class PluginContext:
    """插件运行时上下文 —— 提供对 Runtime 核心服务的访问。

    每个插件通过此对象与服务层交互，无需自行创建实例。
    """

    event_bus: EventBus
    task_manager: TaskManager
    workspace_mgr: WorkspaceManager

    def emit(self, task_id: str, event: str, payload: Any = None) -> str:
        """便捷方法：发送事件到 EventBus，agent 自动标记为 plugin name。"""
        return self.event_bus.emit_event(
            task_id=task_id,
            agent="plugin",
            event=event,
            payload=payload,
        )


class BasePlugin(ABC):
    """插件基类 —— 所有 Runtime 插件的抽象父类。

    子类只需：
      1. 设置 name / version
      2. 实现 on_event() 或 subscribe 特定事件过滤器
      3. 通过 self.ctx 访问 EventBus / TaskManager / WorkspaceManager

    Usage:
        class MyPlugin(BasePlugin):
            name = "my-plugin"
            version = "1.0"

            def on_event(self, task_id, agent, event, payload):
                if event == "task.code_written":
                    code = self.ctx.workspace_mgr.read_file(task_id, payload["file"])

        # 注册到 PluginLoader:
        loader = PluginLoader(ctx)
        loader.register(MyPlugin())
    """

    name: str = ""
    version: str = "0.1.0"
    description: str = ""

    def __init__(self):
        self._ctx: Optional[PluginContext] = None

    # ── 生命周期 ────────────────────────────────────────────

    def on_load(self) -> None:
        """插件被加载时调用。可在此做初始化。"""
        pass

    def on_unload(self) -> None:
        """插件被卸载时调用。可在此做清理。"""
        pass

    # ── 事件钩子 ────────────────────────────────────────────

    @abstractmethod
    def on_event(
        self,
        task_id: str,
        agent: str,
        event: str,
        payload: Any,
    ) -> None:
        """当 Runtime 中发生任何事件时调用。

        插件通过判断 event 字段来决定是否处理：
            if event == "task.coding_started":
                ...

        Parameters
        ----------
        task_id : str
            事件关联的任务 ID。
        agent : str
            触发事件的 Agent 名称（planner / coder / reviewer）。
        event : str
            事件名称（如 task.planned, task.code_written）。
        payload : Any
            事件载荷，JSON 可序列化的数据。
        """
        ...

    # ── 事件订阅（可选覆写） ─────────────────────────────────

    def subscribe(self) -> List[str]:
        """返回此插件关心的事件名列表。只关心列表中的事件。

        如果返回空列表，则所有事件都会传递给 on_event()。
        如果不覆写，默认接收所有事件。

        Example:
            def subscribe(self):
                return ["task.code_written", "task.review_rejected"]
        """
        return []

    # ── 任务生命周期钩子（可选覆写） ──────────────────────────

    def on_task_created(self, task: Dict[str, Any]) -> None:
        """任务创建时调用。"""
        pass

    def on_task_updated(self, task: Dict[str, Any], old_status: str) -> None:
        """任务状态变化时调用。"""
        pass

    # ── 上下文属性 ──────────────────────────────────────────

    @property
    def ctx(self) -> PluginContext:
        if self._ctx is None:
            raise RuntimeError(
                f"插件 '{self.name}' 尚未绑定 PluginContext。"
                "请通过 PluginLoader.register() 加载插件。"
            )
        return self._ctx
