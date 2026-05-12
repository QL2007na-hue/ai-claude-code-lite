"""
Event Tracing System —— 零依赖、线程安全的事件跟踪引擎。

提供 Span 上下文管理器、完整 trace 树重建、活跃 Span 查询、
JSON 导出调试等能力。与 EventBus / Orchestrator 完全解耦，
可独立用于任意任务的调用链跟踪。

Usage:
    from runtime.observability.tracer import EventTracer

    tracer = EventTracer()

    # 上下文管理器方式
    with tracer.trace("planner.plan", task_id="task-001", metadata={"goal": "写贪吃蛇"}):
        # ... 执行规划逻辑 ...
        with tracer.trace("llm.call", metadata={"model": "deepseek"}):
            # ... LLM 调用 ...

    # 查询完整 trace 树
    tree = tracer.get_trace("task-001")

    # 查看当前正在运行的 span
    active = tracer.get_active_spans()

    # JSON 导出
    print(tracer.export_json("task-001"))
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Generator, List, Optional


# ───────────────────────────────────────────────────────────────
# 数据模型
# ───────────────────────────────────────────────────────────────

@dataclass
class Span:
    """表示一段被跟踪的操作。

    Attributes
    ----------
    name : str
        Span 名称，如 "planner.plan", "llm.call", "coder.execute"。
    start_time : float
        Unix 时间戳（time.time()），开始时间。
    end_time : Optional[float]
        Unix 时间戳，结束时间；未结束时为 None。
    metadata : dict
        附加的键值对上下文信息。
    span_id : str
        唯一 Span ID（UUID4）。
    parent_span_id : Optional[str]
        父 Span ID；根 Span 为 None。
    task_id : str
        关联的任务 ID。
    children : list[str]
        子 Span ID 列表（用于重建 trace 树）。
    """

    name: str
    start_time: float
    end_time: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    span_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_span_id: Optional[str] = None
    task_id: str = ""
    children: List[str] = field(default_factory=list)

    @property
    def duration(self) -> Optional[float]:
        """Span 耗时（秒），未结束时返回 None。"""
        if self.end_time is None:
            return None
        return round(self.end_time - self.start_time, 6)

    @property
    def is_active(self) -> bool:
        """Span 是否仍在进行中。"""
        return self.end_time is None

    def to_dict(self) -> Dict[str, Any]:
        """转换为可 JSON 序列化的字典。"""
        d = asdict(self)
        d["duration"] = self.duration
        d["is_active"] = self.is_active
        return d


# ───────────────────────────────────────────────────────────────
# EventTracer
# ───────────────────────────────────────────────────────────────

class EventTracer:
    """线程安全的事件跟踪引擎。

    核心能力：
        - ``trace()`` 上下文管理器，自动记录 span 起止时间
        - ``get_trace(task_id)`` 重建完整 trace 树
        - ``get_active_spans()`` 查询当前进行中的 span
        - ``export_json(task_id)`` JSON 导出

    Thread Safety
    -------------
    所有 public 方法均持有 ``threading.Lock``，适合在多线程
    环境（如 Orchestrator 的 ThreadPoolExecutor）中使用。
    """

    def __init__(self) -> None:
        """初始化 EventTracer。"""
        # 所有 span，keyed by span_id
        self._spans: Dict[str, Span] = {}

        # 按 task_id 分组，value 为 span_id 列表（包含根 span 及子孙）
        self._task_index: Dict[str, List[str]] = {}

        # 当前线程 → 活跃 span 栈（支持嵌套 trace 调用）
        self._thread_stacks: Dict[int, List[str]] = {}

        # 线程锁
        self._lock: threading.Lock = threading.Lock()

        # 自身统计
        self._total_spans: int = 0
        self._total_traces: int = 0

    # ── 上下文管理器 ──────────────────────────────────────────

    @contextmanager
    def trace(
        self,
        name: str,
        task_id: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Generator[Span, None, None]:
        """创建并激活一个 Span，作为上下文管理器使用。

        Parameters
        ----------
        name : str
            Span 名称，建议使用点分命名（如 "planner.plan", "llm.call"）。
        task_id : str, optional
            关联的任务 ID；为空则与父 span 相同。
        metadata : dict, optional
            要附加到 span 的键值对。

        Yields
        ------
        Span
            新创建的 span 对象，可在 ``with`` 块内通过 ``as span`` 访问。

        Example
        -------
        with tracer.trace("coder.execute", task_id="t1", metadata={"file": "main.py"}) as sp:
            # 可在块内操作 sp.metadata
            sp.metadata["lines"] = 42
            # ... 执行业务逻辑 ...
        """
        thread_id = threading.get_ident()
        parent_span_id: Optional[str] = None
        inferred_task_id = task_id

        with self._lock:
            # 获取当前线程的父 span
            stack = self._thread_stacks.get(thread_id, [])
            if stack:
                parent_span_id = stack[-1]
                parent_span = self._spans.get(parent_span_id)
                if parent_span and not inferred_task_id:
                    inferred_task_id = parent_span.task_id

            # 创建新 span
            span = Span(
                name=name,
                start_time=time.time(),
                metadata=metadata or {},
                parent_span_id=parent_span_id,
                task_id=inferred_task_id,
            )

            # 注册 span
            self._spans[span.span_id] = span
            self._total_spans += 1

            # 关联 task_id
            if span.task_id:
                self._task_index.setdefault(span.task_id, []).append(span.span_id)
                if parent_span_id is None:
                    self._total_traces += 1

            # 建立父子关系
            if parent_span_id and parent_span_id in self._spans:
                self._spans[parent_span_id].children.append(span.span_id)

            # 压栈
            self._thread_stacks.setdefault(thread_id, []).append(span.span_id)

        # 进入上下文
        exc_raised = False
        try:
            yield span
        except Exception:
            exc_raised = True
            span.metadata["error"] = True
            raise
        finally:
            end_time = time.time()
            with self._lock:
                span.end_time = end_time
                if exc_raised:
                    span.metadata.setdefault("error_count", 0)
                    span.metadata["error_count"] += 1

                # 出栈
                stack = self._thread_stacks.get(thread_id, [])
                if stack and stack[-1] == span.span_id:
                    stack.pop()
                if not stack:
                    self._thread_stacks.pop(thread_id, None)

    # ── 查询 ───────────────────────────────────────────────────

    def get_trace(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取指定任务的完整 trace 树。

        Parameters
        ----------
        task_id : str
            任务 ID。

        Returns
        -------
        dict | None
            Trace 树结构，根 span 为顶层，
            包含 ``span`` 字段和 ``children`` 嵌套列表。
            若未找到该任务则返回 None。
        """
        with self._lock:
            span_ids = self._task_index.get(task_id, [])
            if not span_ids:
                return None

            # 找到根 span（没有 parent_span_id 的）
            root = None
            for sid in span_ids:
                sp = self._spans.get(sid)
                if sp and sp.parent_span_id is None:
                    root = sp
                    break

            if root is None:
                # 没有显式根节点，取第一个
                root = self._spans.get(span_ids[0])

            if root is None:
                return None

            return self._build_tree(root)

    def _build_tree(self, span: Span) -> Dict[str, Any]:
        """递归构建 span 子树。"""
        node: Dict[str, Any] = {
            "span": span.to_dict(),
            "children": [
                self._build_tree(self._spans[cid])
                for cid in span.children
                if cid in self._spans
            ],
        }
        return node

    def get_active_spans(self) -> List[Dict[str, Any]]:
        """返回当前所有正在进行中的 Span。

        Returns
        -------
        list[dict]
            活跃 span 的字典列表，包含 thread_id 信息。
        """
        active: List[Dict[str, Any]] = []
        with self._lock:
            for tid, stack in self._thread_stacks.items():
                for sid in stack:
                    sp = self._spans.get(sid)
                    if sp and sp.is_active:
                        entry = sp.to_dict()
                        entry["thread_id"] = tid
                        active.append(entry)
        return active

    def get_span(self, span_id: str) -> Optional[Dict[str, Any]]:
        """根据 span_id 查询单个 Span。

        Parameters
        ----------
        span_id : str
            Span 唯一 ID。

        Returns
        -------
        dict | None
        """
        with self._lock:
            sp = self._spans.get(span_id)
            return sp.to_dict() if sp else None

    @property
    def traces(self) -> Dict[str, Any]:
        """所有 trace 的只读快照，以 task_id 为 key。

        Returns
        -------
        dict
            ``{task_id: trace_tree, ...}``
        """
        with self._lock:
            result: Dict[str, Any] = {}
            for task_id in list(self._task_index.keys()):
                tree = self.get_trace(task_id)
                if tree is not None:
                    result[task_id] = tree
            return result

    # ── 导出 ───────────────────────────────────────────────────

    def export_json(self, task_id: str = "", indent: int = 2) -> str:
        """将 trace 数据导出为 JSON 字符串。

        Parameters
        ----------
        task_id : str, optional
            指定导出某个任务的 trace；留空则导出所有 trace。
        indent : int, default 2
            JSON 缩进空格数；设 0 或 None 可输出紧凑格式。

        Returns
        -------
        str
            JSON 字符串。
        """
        if task_id:
            tree = self.get_trace(task_id)
            data = tree if tree else {"error": f"task_id '{task_id}' not found"}
        else:
            data = {
                "total_spans": self._total_spans,
                "total_traces": self._total_traces,
                "traces": self.traces,
                "active_spans": len(self.get_active_spans()),
            }
        return json.dumps(data, ensure_ascii=False, indent=indent or None, default=str)

    def export_dict(self, task_id: str = "") -> Dict[str, Any]:
        """将 trace 数据导出为 Python 字典（用于 API 响应）。

        Parameters
        ----------
        task_id : str, optional
            指定导出某个任务的 trace；留空则导出所有。

        Returns
        -------
        dict
        """
        if task_id:
            tree = self.get_trace(task_id)
            return tree if tree else {"error": f"task_id '{task_id}' not found"}
        return {
            "total_spans": self._total_spans,
            "total_traces": self._total_traces,
            "traces": self.traces,
            "active_spans": self.get_active_spans(),
        }

    # ── 统计 ───────────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        """返回 tracer 的自身统计信息。

        Returns
        -------
        dict
        """
        with self._lock:
            active_count = sum(
                len(stack) for stack in self._thread_stacks.values()
            )
        return {
            "total_spans_created": self._total_spans,
            "total_traces": self._total_traces,
            "spans_in_memory": len(self._spans),
            "active_spans": active_count,
            "active_threads": len(self._thread_stacks),
            "tracked_tasks": len(self._task_index),
        }

    # ── 清理 ───────────────────────────────────────────────────

    def reset(self) -> None:
        """重置所有 trace 数据（谨慎使用）。"""
        with self._lock:
            self._spans.clear()
            self._task_index.clear()
            self._thread_stacks.clear()
            self._total_spans = 0
            self._total_traces = 0

    def cleanup_completed(self, older_than_seconds: float = 3600) -> int:
        """清理已完成且超过指定时间的旧 Span。

        Parameters
        ----------
        older_than_seconds : float, default 3600
            清理超过此秒数的已完成 span。

        Returns
        -------
        int
            被清理的 span 数量。
        """
        now = time.time()
        removed = 0
        with self._lock:
            stale_ids: List[str] = []
            for sid, sp in self._spans.items():
                if (sp.end_time is not None
                        and (now - sp.end_time) > older_than_seconds):
                    stale_ids.append(sid)

            for sid in stale_ids:
                sp = self._spans.pop(sid, None)
                if sp and sp.task_id in self._task_index:
                    self._task_index[sp.task_id] = [
                        x for x in self._task_index[sp.task_id] if x != sid
                    ]
                    if not self._task_index[sp.task_id]:
                        del self._task_index[sp.task_id]
                removed += 1

        return removed
