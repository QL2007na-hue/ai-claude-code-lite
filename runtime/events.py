"""
Runtime Event Protocol —— Pydantic v2 事件模型定义。

本模块定义了 AI Runtime 中所有事件类型的强类型模型，提供：
  - 事件名命名空间校验（task.* / plugin.* / system.* / review.* / subtask.*）
  - EventBus 序列化/反序列化互转（to_eventbus_kwargs / from_eventbus_data）
  - 自动生成 event_id / correlation_id / timestamp

Usage:
    from runtime.events import PlanCompleted, TaskCreated, SystemStarted

    # 创建事件
    evt = PlanCompleted(
        task_id="task-001",
        agent="planner",
        payload={"goal": "写一个贪吃蛇", "subtasks": [...]},
    )
    # 转为 EventBus.emit_event() 的 kwargs
    bus.emit_event(**evt.to_eventbus_kwargs())

    # 从 Redis Stream 原始数据反序列化
    raw = {"task_id": "task-001", "agent": "planner", "event": "task.planned", ...}
    evt = RuntimeEvent.from_eventbus_data(raw)
"""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type, Union

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
)

# ───────────────────────────────────────────────────────────────
# 常量
# ───────────────────────────────────────────────────────────────

# 合法的事件命名空间前缀
_VALID_EVENT_PREFIXES: Tuple[str, ...] = (
    "task.",
    "plugin.",
    "system.",
    "review.",
    "subtask.",
)

# 事件名 → 模型类的注册表（由 __init_subclass__ 自动填充）
_EVENT_MODEL_REGISTRY: Dict[str, Type["RuntimeEvent"]] = {}

# 合法的事件命名空间前缀正则（编译一次复用）
_EVENT_PREFIX_RE = re.compile(
    r"^(task\.|plugin\.|system\.|review\.|subtask\.)"
)

# 保留事件名——即使不匹配前缀也允许
_RESERVED_EVENT_NAMES: frozenset = frozenset({
    "heartbeat",
})


# ───────────────────────────────────────────────────────────────
# Base Model
# ───────────────────────────────────────────────────────────────

class RuntimeEvent(BaseModel):
    """AI Runtime 事件基类。

    所有具体事件类型均继承自此模型。
    提供事件名命名空间校验、EventBus 序列化/反序列化能力。

    Fields
    ------
    task_id : str
        事件关联的任务 ID（系统级事件可为空字符串）。
    agent : str
        触发事件的 Agent 名称（planner / coder / reviewer / orchestrator / system / plugin）。
    event : str
        事件名称，须以合法前缀开头（如 task.planned, plugin.todo_found）。
    payload : dict | str
        事件载荷，JSON 可序列化的数据。
    timestamp : str
        Unix 时间戳字符串。
    event_id : str
        事件唯一 ID（UUID4）。
    correlation_id : str
        关联 ID，用于将多个事件串联为同一条因果关系链。
    """

    task_id: str = Field(
        default="",
        description="事件关联的任务 ID",
        min_length=0,
    )
    agent: str = Field(
        default="system",
        description="触发事件的 Agent 名称",
        min_length=1,
    )
    event: str = Field(
        default="",
        description="事件名称（须以 task./plugin./system./review./subtask. 开头）",
        min_length=1,
    )
    payload: Union[Dict[str, Any], str] = Field(
        default_factory=dict,
        description="事件载荷，JSON 可序列化",
    )
    timestamp: str = Field(
        default_factory=lambda: str(time.time()),
        description="Unix 时间戳字符串",
    )
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="事件唯一 ID（UUID4）",
    )
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="关联 ID，用于串联同一因果链的事件",
    )

    # ── 子类注册机制 ──────────────────────────────────────────

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """自动将子类注册到事件名→模型类的映射表。"""
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "__event_name__") and cls.__event_name__:
            _EVENT_MODEL_REGISTRY[cls.__event_name__] = cls

    # ── 字段级校验 ────────────────────────────────────────────

    @field_validator("event", mode="before")
    @classmethod
    def _strip_event_whitespace(cls, v: str) -> str:
        if isinstance(v, str):
            return v.strip()
        return v

    # ── 模型级校验 ────────────────────────────────────────────

    @model_validator(mode="after")
    def _validate_event_namespace(self) -> "RuntimeEvent":
        """校验 event 字段是否以合法命名空间前缀开头。"""
        event_name = self.event
        if not event_name:
            return self  # 允许空事件的基类实例，子类会覆盖

        # 保留事件名豁免
        if event_name in _RESERVED_EVENT_NAMES:
            return self

        if not _EVENT_PREFIX_RE.match(event_name):
            raise ValueError(
                f"事件名 '{event_name}' 不合法。"
                f"必须以 {_VALID_EVENT_PREFIXES} 之一开头"
            )
        return self

    # ── 序列化 / 反序列化 ─────────────────────────────────────

    def to_eventbus_kwargs(self) -> Dict[str, Any]:
        """转换为 EventBus.emit_event() 关键字参数。

        Returns
        -------
        dict
            {'task_id': ..., 'agent': ..., 'event': ..., 'payload': ...}
            可直接通过 ``bus.emit_event(**evt.to_eventbus_kwargs())`` 发送。
        """
        payload = self.payload
        if not isinstance(payload, str):
            payload = _safe_json_dumps(payload)
        return {
            "task_id": self.task_id,
            "agent": self.agent,
            "event": self.event,
            "payload": payload,
        }

    def to_dict(self) -> Dict[str, Any]:
        """转换为完整字典（含 event_id / correlation_id / timestamp）。"""
        return self.model_dump()

    @classmethod
    def from_eventbus_data(cls, data: Dict[str, str]) -> "RuntimeEvent":
        """从 Redis Stream 原始数据构造事件模型。

        根据 ``data["event"]`` 自动分发到对应子类；
        若事件名未注册子类，则返回基类 RuntimeEvent 实例。

        Parameters
        ----------
        data : dict
            EventBus 通过 Redis Streams 传递的原始字段：
            ``{"task_id": str, "agent": str, "event": str, "payload": str, "timestamp": str}``

        Returns
        -------
        RuntimeEvent
            具体事件子类实例或基类实例。
        """
        event_name = data.get("event", "")
        raw_payload = data.get("payload", "{}")

        # 反序列化 payload
        payload: Any = {}
        if isinstance(raw_payload, str):
            try:
                payload = json.loads(raw_payload)
            except (json.JSONDecodeError, TypeError):
                payload = {"raw": raw_payload}
        elif isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            payload = {"raw": str(raw_payload)}

        # 分发到注册的子类
        model_cls = _EVENT_MODEL_REGISTRY.get(event_name, cls)

        return model_cls(
            task_id=data.get("task_id", ""),
            agent=data.get("agent", ""),
            event=event_name,
            payload=payload,
            timestamp=data.get("timestamp", str(time.time())),
        )

    model_config = {
        "extra": "forbid",            # 禁止额外字段
        "validate_assignment": True,  # 属性赋值时也校验
        "str_strip_whitespace": True,
    }


# ───────────────────────────────────────────────────────────────
# AgentEvent 及子类
# ───────────────────────────────────────────────────────────────

class AgentEvent(RuntimeEvent):
    """由 Agent 触发的事件抽象基类。"""

    agent: str = Field(
        default="",
        description="触发事件的 Agent（planner / coder / reviewer）",
    )


class PlanStarted(AgentEvent):
    """规划开始事件 —— Planner 开始拆解任务。

    EventBus event name: ``task.plan_started``
    """

    __event_name__: ClassVar[str] = "task.plan_started"
    event: str = Field(default="task.plan_started", frozen=True)
    agent: str = Field(default="planner", frozen=True)


class PlanCompleted(AgentEvent):
    """规划完成事件 —— Planner 完成子任务拆解。

    EventBus event name: ``task.planned``

    payload 须包含 subtasks 字段：
        {"goal": str, "subtasks": [{"id": str, "title": str, "description": str, "depends_on": list}, ...]}
    """

    __event_name__: ClassVar[str] = "task.planned"
    event: str = Field(default="task.planned", frozen=True)
    agent: str = Field(default="planner", frozen=True)

    @model_validator(mode="after")
    def _require_subtasks(self) -> "PlanCompleted":
        p = self.payload if isinstance(self.payload, dict) else {}
        if "subtasks" not in p:
            raise ValueError("PlanCompleted.payload 必须包含 'subtasks' 字段")
        return self


class PlanFailed(AgentEvent):
    """规划失败事件 —— Planner 调用失败。

    EventBus event name: ``task.plan_failed``
    """

    __event_name__: ClassVar[str] = "task.plan_failed"
    event: str = Field(default="task.plan_failed", frozen=True)
    agent: str = Field(default="planner", frozen=True)


class CodeGenerated(AgentEvent):
    """代码生成事件 —— Coder 完成 LLM 调用，拿到原始响应。

    EventBus event name: ``task.code_generated``
    """

    __event_name__: ClassVar[str] = "task.code_generated"
    event: str = Field(default="task.code_generated", frozen=True)
    agent: str = Field(default="coder", frozen=True)


class CodeWritten(AgentEvent):
    """代码写入事件 —— Coder 将生成的文件写入 workspace。

    EventBus event name: ``task.code_written``

    payload 须包含 file 字段：``{"file": "src/main.py"}``
    """

    __event_name__: ClassVar[str] = "task.code_written"
    event: str = Field(default="task.code_written", frozen=True)
    agent: str = Field(default="coder", frozen=True)

    @model_validator(mode="after")
    def _require_file(self) -> "CodeWritten":
        p = self.payload if isinstance(self.payload, dict) else {}
        if "file" not in p:
            raise ValueError("CodeWritten.payload 必须包含 'file' 字段")
        return self


class GitCommitted(AgentEvent):
    """Git 提交事件 —— Coder 将 workspace 提交到 git。

    EventBus event name: ``task.git_committed``
    """

    __event_name__: ClassVar[str] = "task.git_committed"
    event: str = Field(default="task.git_committed", frozen=True)
    agent: str = Field(default="coder", frozen=True)


class ShellExecuted(AgentEvent):
    """Shell 执行事件 —— Coder 运行了代码块中的 shell 命令。

    EventBus event name: ``task.shell_executed``
    """

    __event_name__: ClassVar[str] = "task.shell_executed"
    event: str = Field(default="task.shell_executed", frozen=True)
    agent: str = Field(default="coder", frozen=True)


class CodingStarted(AgentEvent):
    """编码开始事件 —— Coder 开始执行任务。

    EventBus event name: ``task.coding_started``
    """

    __event_name__: ClassVar[str] = "task.coding_started"
    event: str = Field(default="task.coding_started", frozen=True)
    agent: str = Field(default="coder", frozen=True)


class CodingCompleted(AgentEvent):
    """编码完成事件 —— Coder 成功完成代码生成与写入，进入 review。

    EventBus event name: ``task.coding_completed``
    """

    __event_name__: ClassVar[str] = "task.coding_completed"
    event: str = Field(default="task.coding_completed", frozen=True)
    agent: str = Field(default="coder", frozen=True)


class CodingFailed(AgentEvent):
    """编码失败事件 —— Coder 执行过程中发生异常。

    EventBus event name: ``task.coding_failed``
    """

    __event_name__: ClassVar[str] = "task.coding_failed"
    event: str = Field(default="task.coding_failed", frozen=True)
    agent: str = Field(default="coder", frozen=True)


class ReviewStarted(AgentEvent):
    """审查开始事件 —— Reviewer 开始对 workspace 进行代码审查。

    EventBus event name: ``task.review_started``
    """

    __event_name__: ClassVar[str] = "task.review_started"
    event: str = Field(default="task.review_started", frozen=True)
    agent: str = Field(default="reviewer", frozen=True)


class ReviewCompleted(AgentEvent):
    """审查完成事件 —— Reviewer 完成审查（通过或不通过）。

    EventBus event name: ``task.review_completed``

    payload 须包含 approved / score / issues_count 字段。
    """

    __event_name__: ClassVar[str] = "task.review_completed"
    event: str = Field(default="task.review_completed", frozen=True)
    agent: str = Field(default="reviewer", frozen=True)

    @model_validator(mode="after")
    def _require_review_fields(self) -> "ReviewCompleted":
        p = self.payload if isinstance(self.payload, dict) else {}
        for key in ("approved", "score", "issues_count"):
            if key not in p:
                raise ValueError(f"ReviewCompleted.payload 必须包含 '{key}' 字段")
        return self


# ───────────────────────────────────────────────────────────────
# TaskEvent 及子类
# ───────────────────────────────────────────────────────────────

class TaskEvent(RuntimeEvent):
    """任务生命周期事件抽象基类。"""

    agent: str = Field(default="orchestrator")


class TaskCreated(TaskEvent):
    """任务创建事件 —— 新任务已写入 TaskManager。

    EventBus event name: ``task.created``
    """

    __event_name__: ClassVar[str] = "task.created"
    event: str = Field(default="task.created", frozen=True)


class TaskStatusChanged(TaskEvent):
    """任务状态变更事件 —— 任务在状态机中流转。

    EventBus event name: ``task.status_changed``

    payload 须包含:
        {"old_status": "pending", "new_status": "running"}
    """

    __event_name__: ClassVar[str] = "task.status_changed"
    event: str = Field(default="task.status_changed", frozen=True)

    @model_validator(mode="after")
    def _require_status_fields(self) -> "TaskStatusChanged":
        p = self.payload if isinstance(self.payload, dict) else {}
        for key in ("old_status", "new_status"):
            if key not in p:
                raise ValueError(f"TaskStatusChanged.payload 必须包含 '{key}' 字段")
        return self


class TaskCompleted(TaskEvent):
    """任务完成事件 —— 任务（含所有子任务）已全部完成。

    EventBus event name: ``task.done``
    """

    __event_name__: ClassVar[str] = "task.done"
    event: str = Field(default="task.done", frozen=True)


class TaskPartiallyFailed(TaskEvent):
    """任务部分失败事件 —— 部分子任务失败但整体 DAG 仍标记完成。

    EventBus event name: ``task.partially_failed``
    """

    __event_name__: ClassVar[str] = "task.partially_failed"
    event: str = Field(default="task.partially_failed", frozen=True)


class TaskFailed(TaskEvent):
    """任务失败事件 —— 任务彻底失败。

    EventBus event name: ``task.failed``
    """

    __event_name__: ClassVar[str] = "task.failed"
    event: str = Field(default="task.failed", frozen=True)


class TaskRetryExhausted(TaskEvent):
    """重试耗尽事件 —— review 不通过且已达到最大重试次数。

    EventBus event name: ``task.retry_exhausted``
    """

    __event_name__: ClassVar[str] = "task.retry_exhausted"
    event: str = Field(default="task.retry_exhausted", frozen=True)


class TaskDoneNoSubtasks(TaskEvent):
    """无子任务直接完成事件 —— 规划未产生子任务时直接标记完成。

    EventBus event name: ``task.done_no_subtasks``
    """

    __event_name__: ClassVar[str] = "task.done_no_subtasks"
    event: str = Field(default="task.done_no_subtasks", frozen=True)


class SubtaskCreated(TaskEvent):
    """子任务创建事件 —— Planner 为父任务创建了一个子任务。

    EventBus event name: ``subtask.created``

    payload 须包含 parent_task_id / subtask_id / title。
    """

    __event_name__: ClassVar[str] = "subtask.created"
    event: str = Field(default="subtask.created", frozen=True)
    agent: str = Field(default="planner", frozen=True)

    @model_validator(mode="after")
    def _require_subtask_fields(self) -> "SubtaskCreated":
        p = self.payload if isinstance(self.payload, dict) else {}
        for key in ("parent_task_id", "subtask_id", "title"):
            if key not in p:
                raise ValueError(f"SubtaskCreated.payload 必须包含 '{key}' 字段")
        return self


# ───────────────────────────────────────────────────────────────
# ReviewEvent 及子类
# ───────────────────────────────────────────────────────────────

class ReviewEvent(RuntimeEvent):
    """审查结果事件抽象基类。

    agent 默认为 reviewer。
    """

    agent: str = Field(default="reviewer", frozen=True)


class ReviewApproved(ReviewEvent):
    """审查通过事件 —— 代码评分 >= 70 且无致命问题。

    EventBus event name: ``review.approved``

    payload 须包含 score / issues_count。
    """

    __event_name__: ClassVar[str] = "review.approved"
    event: str = Field(default="review.approved", frozen=True)

    @model_validator(mode="after")
    def _require_score_and_count(self) -> "ReviewApproved":
        p = self.payload if isinstance(self.payload, dict) else {}
        for key in ("score", "issues_count"):
            if key not in p:
                raise ValueError(f"ReviewApproved.payload 必须包含 '{key}' 字段")
        return self


class ReviewRejected(ReviewEvent):
    """审查不通过事件 —— 代码评分 < 70 或存在致命问题。

    EventBus event name: ``review.rejected``

    payload 须包含 score / issues（问题详情列表）。
    """

    __event_name__: ClassVar[str] = "review.rejected"
    event: str = Field(default="review.rejected", frozen=True)

    @model_validator(mode="after")
    def _require_score_and_issues(self) -> "ReviewRejected":
        p = self.payload if isinstance(self.payload, dict) else {}
        for key in ("score", "issues"):
            if key not in p:
                raise ValueError(f"ReviewRejected.payload 必须包含 '{key}' 字段")
        return self


class ReviewFailed(ReviewEvent):
    """审查异常失败事件 —— Reviewer 抛出异常。

    EventBus event name: ``review.failed``
    """

    __event_name__: ClassVar[str] = "review.failed"
    event: str = Field(default="review.failed", frozen=True)


# ───────────────────────────────────────────────────────────────
# PluginEvent 及子类
# ───────────────────────────────────────────────────────────────

class PluginEvent(RuntimeEvent):
    """插件触发的事件抽象基类。

    agent 默认为 plugin。
    """

    agent: str = Field(default="plugin", frozen=True)


class PluginTodoFound(PluginEvent):
    """插件发现 TODO/FIXME 标记事件。

    EventBus event name: ``plugin.todo_found``

    payload 须包含 file / count / items。
    """

    __event_name__: ClassVar[str] = "plugin.todo_found"
    event: str = Field(default="plugin.todo_found", frozen=True)

    @model_validator(mode="after")
    def _require_todo_fields(self) -> "PluginTodoFound":
        p = self.payload if isinstance(self.payload, dict) else {}
        for key in ("file", "count", "items"):
            if key not in p:
                raise ValueError(f"PluginTodoFound.payload 必须包含 '{key}' 字段")
        return self


class PluginCheckComplete(PluginEvent):
    """插件检查完成事件。

    EventBus event name: ``plugin.check_complete``

    payload 须包含 files_scanned / todos_found。
    """

    __event_name__: ClassVar[str] = "plugin.check_complete"
    event: str = Field(default="plugin.check_complete", frozen=True)

    @model_validator(mode="after")
    def _require_check_fields(self) -> "PluginCheckComplete":
        p = self.payload if isinstance(self.payload, dict) else {}
        for key in ("files_scanned", "todos_found"):
            if key not in p:
                raise ValueError(f"PluginCheckComplete.payload 必须包含 '{key}' 字段")
        return self


# ───────────────────────────────────────────────────────────────
# SystemEvent 及子类
# ───────────────────────────────────────────────────────────────

class SystemEvent(RuntimeEvent):
    """系统级生命周期事件抽象基类。

    agent 默认为 system，task_id 通常为空字符串。
    """

    agent: str = Field(default="system", frozen=True)
    task_id: str = Field(default="")


class SystemStarted(SystemEvent):
    """系统启动事件 —— AI Runtime 完成初始化。

    EventBus event name: ``system.started``
    """

    __event_name__: ClassVar[str] = "system.started"
    event: str = Field(default="system.started", frozen=True)


class SystemStopped(SystemEvent):
    """系统停止事件 —— AI Runtime 进入优雅关闭流程。

    EventBus event name: ``system.stopped``
    """

    __event_name__: ClassVar[str] = "system.stopped"
    event: str = Field(default="system.stopped", frozen=True)


class SystemError(SystemEvent):
    """系统错误事件 —— 非特定任务级别的全局错误。

    EventBus event name: ``system.error``

    payload 通常包含 error / traceback。
    """

    __event_name__: ClassVar[str] = "system.error"
    event: str = Field(default="system.error", frozen=True)


class Heartbeat(SystemEvent):
    """心跳事件 —— 周期性发送以证明 Runtime 仍在运行。

    EventBus event name: ``heartbeat``
    （保留事件名，不要求命名空间前缀）

    payload 通常包含 timestamp / uptime_seconds。
    """

    __event_name__: ClassVar[str] = "heartbeat"
    event: str = Field(default="heartbeat", frozen=True)


# ───────────────────────────────────────────────────────────────
# 便捷工具
# ───────────────────────────────────────────────────────────────

def _safe_json_dumps(obj: Any) -> str:
    """安全 JSON 序列化，失败时返回错误描述字符串。"""
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return json.dumps({"error": "payload 序列化失败", "type": str(type(obj))})


def list_registered_events() -> List[str]:
    """返回所有已注册的事件名列表。"""
    return sorted(_EVENT_MODEL_REGISTRY.keys())


def get_event_model(event_name: str) -> Optional[Type[RuntimeEvent]]:
    """根据事件名获取对应的 Pydantic 模型类。

    Returns
    -------
    type 或 None
        未注册时返回 None。
    """
    return _EVENT_MODEL_REGISTRY.get(event_name)


# ───────────────────────────────────────────────────────────────
# 模块导出
# ───────────────────────────────────────────────────────────────

__all__ = [
    # 基类
    "RuntimeEvent",
    "AgentEvent",
    "TaskEvent",
    "ReviewEvent",
    "PluginEvent",
    "SystemEvent",
    # Agent 事件
    "PlanStarted",
    "PlanCompleted",
    "PlanFailed",
    "CodeGenerated",
    "CodeWritten",
    "GitCommitted",
    "ShellExecuted",
    "CodingStarted",
    "CodingCompleted",
    "CodingFailed",
    "ReviewStarted",
    "ReviewCompleted",
    # Task 事件
    "TaskCreated",
    "TaskStatusChanged",
    "TaskCompleted",
    "TaskPartiallyFailed",
    "TaskFailed",
    "TaskRetryExhausted",
    "TaskDoneNoSubtasks",
    "SubtaskCreated",
    # Review 事件
    "ReviewApproved",
    "ReviewRejected",
    "ReviewFailed",
    # Plugin 事件
    "PluginTodoFound",
    "PluginCheckComplete",
    # System 事件
    "SystemStarted",
    "SystemStopped",
    "SystemError",
    "Heartbeat",
    # 工具
    "list_registered_events",
    "get_event_model",
]
