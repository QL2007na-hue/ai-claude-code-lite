"""
Observability Dashboard —— 面向 API 的实时可观测性聚合层。

聚合 Tracer / Metrics / Logger / TaskManager / Redis 等多数据源，
提供统一的状态查询接口，专为 FastAPI 端点设计。

Usage in api/server.py:
    from runtime.observability.dashboard import ObservabilityDashboard

    dashboard = ObservabilityDashboard(task_manager=tm, event_bus=bus)

    @app.get("/observability/status")
    def obs_status():
        return dashboard.get_status()

    @app.get("/observability/health")
    def obs_health():
        return dashboard.get_health()

    @app.get("/observability/active-tasks")
    def obs_active():
        return dashboard.get_active_tasks()

    @app.get("/observability/performance")
    def obs_perf():
        return dashboard.get_performance()

    @app.get("/observability/events")
    def obs_events(limit: int = 50):
        return dashboard.get_recent_events(limit)
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    try:
        import redis
    except ImportError:
        redis = None  # type: ignore[assignment]

from runtime.observability.tracer import EventTracer
from runtime.observability.metrics import RuntimeMetrics
from runtime.observability.logger import RuntimeLogger


# ───────────────────────────────────────────────────────────────
# 内部缓存条目
# ───────────────────────────────────────────────────────────────

@dataclass
class _CachedEvent:
    """最近事件的轻量级缓存条目。"""
    task_id: str
    agent: str
    event: str
    payload: Any
    timestamp: float
    trace_id: Optional[str] = None


# ───────────────────────────────────────────────────────────────
# ObservabilityDashboard
# ───────────────────────────────────────────────────────────────

class ObservabilityDashboard:
    """面向 API 的实时可观测性聚合层。

    聚合以下数据源：
        - ``EventTracer`` —— Span / Trace 数据
        - ``RuntimeMetrics`` —— Agent / Provider / Task 指标
        - ``RuntimeLogger`` —— 结构化日志
        - ``TaskManager`` —— 活跃任务查询
        - ``Redis`` —— 连接状态健康检查

    所有 public 方法均返回可直接 JSON 序列化的 dict，
    适合作为 FastAPI 端点的响应体。

    Usage
    -----
    在 api/server.py 中初始化：

        dashboard = ObservabilityDashboard(
            task_manager=tm,
            tracer=EventTracer(),
            metrics=RuntimeMetrics(),
            logger=RuntimeLogger(),
        )

    然后挂载到路由：

        @app.get("/observability/status")
        def obs_status():
            return dashboard.get_status()
    """

    def __init__(
        self,
        task_manager: Any = None,        # TaskManager instance
        tracer: Optional[EventTracer] = None,
        metrics: Optional[RuntimeMetrics] = None,
        logger: Optional[RuntimeLogger] = None,
        redis_client: Any = None,         # redis.Redis instance
        max_cached_events: int = 500,
    ) -> None:
        """初始化 ObservabilityDashboard。

        Parameters
        ----------
        task_manager : TaskManager, optional
            任务管理器实例，用于查询活跃任务。
        tracer : EventTracer, optional
            Span / Trace 跟踪器。
        metrics : RuntimeMetrics, optional
            指标采集器。
        logger : RuntimeLogger, optional
            结构化日志器。
        redis_client : redis.Redis, optional
            Redis 客户端，用于健康检查。
        max_cached_events : int, default 500
            内存中缓存的最大事件数。
        """
        self._tm = task_manager
        self._tracer = tracer or EventTracer()
        self._metrics = metrics or RuntimeMetrics()
        self._logger = logger
        self._redis = redis_client

        self._max_cached_events = max_cached_events
        self._events_cache: List[_CachedEvent] = []
        self._lock: threading.Lock = threading.Lock()
        self._start_time: float = time.time()

        # 延迟导入 orchestrator（避免循环依赖）
        self._orchestrator: Any = None

    # ── 注入外部实例 ──────────────────────────────────────────

    def set_orchestrator(self, orchestrator: Any) -> None:
        """注入 Orchestrator 实例，用于获取 DAG 状态。

        Parameters
        ----------
        orchestrator : Orchestrator
            编排引擎实例。
        """
        self._orchestrator = orchestrator

    def set_task_manager(self, tm: Any) -> None:
        """注入 TaskManager 实例。"""
        self._tm = tm

    def set_redis(self, redis_client: Any) -> None:
        """注入 Redis 客户端。"""
        self._redis = redis_client

    # ── 事件缓存 ──────────────────────────────────────────────

    def push_event(
        self,
        task_id: str,
        agent: str,
        event: str,
        payload: Any = None,
    ) -> None:
        """向内存事件缓存中推入一条事件。

        由 EventBus 的订阅回调或 API 的 stream_listener 调用。

        Parameters
        ----------
        task_id : str
            任务 ID。
        agent : str
            触发 Agent。
        event : str
            事件名称。
        payload : Any
            事件负载。
        """
        with self._lock:
            evt = _CachedEvent(
                task_id=task_id,
                agent=agent,
                event=event,
                payload=payload,
                timestamp=time.time(),
            )
            self._events_cache.append(evt)
            # 环形修剪
            if len(self._events_cache) > self._max_cached_events:
                self._events_cache = self._events_cache[
                    -self._max_cached_events:
                ]

    # ── get_status —— 高层状态快照 ────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """返回高层状态快照。

        聚合 Tracer、Metrics、TaskManager 的当前状态，
        适合作为监控面板的主状态视图。

        Returns
        -------
        dict
            ``{"system": ..., "tasks": ..., "agents": ..., "providers": ..., "traces": ...}``
        """
        # 系统
        system_info: Dict[str, Any] = {
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "cached_events": len(self._events_cache),
        }
        if self._orchestrator:
            orch_status = self._orchestrator.status()
            system_info["orchestrator"] = {
                "running": orch_status.get("running", False),
                "active_dags": orch_status.get("active_dags", 0),
            }

        # 任务
        tasks_info: Dict[str, Any] = self._metrics.get_task_stats()
        if self._tm:
            try:
                all_tasks = self._tm.list_tasks()
                statuses: Dict[str, int] = {}
                for t in all_tasks:
                    s = t.get("status", "unknown")
                    statuses[s] = statuses.get(s, 0) + 1
                tasks_info["current_by_status"] = statuses
            except Exception:
                tasks_info["current_by_status"] = {"error": "TaskManager unreachable"}

        # 活跃任务
        active = self.get_active_tasks()
        tasks_info["active_count"] = active.get("active_dag_count", 0)

        # Agent & Provider 指标
        agents_info = self._metrics.get_all_agent_stats()
        providers_info = self._metrics.get_all_provider_stats()

        # Trace 统计
        traces_info = self._tracer.stats()

        return {
            "system": system_info,
            "tasks": tasks_info,
            "agents": agents_info,
            "providers": providers_info,
            "traces": traces_info,
            "timestamp": time.time(),
        }

    # ── get_recent_events —— 最近事件 ──────────────────────────

    def get_recent_events(
        self,
        limit: int = 50,
        event_filter: Optional[str] = None,
        agent_filter: Optional[str] = None,
        task_id_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """返回最近事件列表，支持过滤。

        Parameters
        ----------
        limit : int, default 50
            返回的最大事件数。
        event_filter : str, optional
            按事件名前缀过滤（如 "task.", "review."）。
        agent_filter : str, optional
            按 Agent 名称过滤。
        task_id_filter : str, optional
            按任务 ID 过滤。

        Returns
        -------
        list[dict]
            事件字典列表，按时间倒序排列。
        """
        with self._lock:
            events = list(self._events_cache)

        # 过滤
        if event_filter:
            events = [e for e in events if e.event.startswith(event_filter)]
        if agent_filter:
            events = [e for e in events if e.agent == agent_filter]
        if task_id_filter:
            events = [e for e in events if e.task_id == task_id_filter]

        # 截取最后 limit 条，反转以实现时间倒序
        events = events[-limit:][::-1]

        return [
            {
                "task_id": e.task_id,
                "agent": e.agent,
                "event": e.event,
                "payload": e.payload,
                "timestamp": e.timestamp,
            }
            for e in events
        ]

    # ── get_active_tasks —— 活跃任务 ──────────────────────────

    def get_active_tasks(self) -> Dict[str, Any]:
        """返回当前活跃任务信息（含 DAG 跟踪）。

        依赖 Orchestrator 的 DAG 状态和 TaskManager 的 running 状态任务。

        Returns
        -------
        dict
            ``{"active_tasks": [...], "active_dag_count": int, "active_spans": [...]}``
        """
        result: Dict[str, Any] = {
            "active_tasks": [],
            "active_dag_count": 0,
            "active_spans": self._tracer.get_active_spans(),
        }

        # 从 Orchestrator 获取 DAG 中的活跃任务
        if self._orchestrator:
            try:
                orch_status = self._orchestrator.status()
                dag_info = orch_status.get("dags", {})
                result["active_dag_count"] = len(dag_info)
                result["dag_summary"] = dag_info
            except Exception:
                pass

        # 从 TaskManager 获取状态为 running / review / retry 的任务
        if self._tm:
            try:
                for status in ("running", "review", "retry"):
                    tasks = self._tm.list_tasks(status=status)
                    for t in tasks:
                        task_id = t.get("task_id", "")
                        # 检查是否有对应 trace
                        trace = self._tracer.get_trace(task_id)
                        result["active_tasks"].append({
                            "task_id": task_id,
                            "status": status,
                            "agent": t.get("agent", ""),
                            "created_at": t.get("created_at", 0),
                            "updated_at": t.get("updated_at", 0),
                            "has_trace": trace is not None,
                            "trace_spans": trace["span"]["name"] if trace else None,
                        })
            except Exception:
                pass

        return result

    # ── get_health —— 系统健康检查 ────────────────────────────

    def get_health(self) -> Dict[str, Any]:
        """系统健康检查 —— Redis / DB / Agent 响应性。

        Returns
        -------
        dict
            ``{"status": "healthy"|"degraded"|"unhealthy", "components": {...}}``
        """
        components: Dict[str, Any] = {}

        # Redis 健康
        redis_ok = False
        if self._redis:
            try:
                self._redis.ping()
                redis_ok = True
            except Exception:
                redis_ok = False
        components["redis"] = {
            "status": "up" if redis_ok else "down" if self._redis else "unconfigured",
        }

        # DB (SQLite) 健康
        db_ok = False
        if self._tm:
            try:
                # 尝试查询一条任务来验证 DB 可用
                self._tm.list_tasks(status="done")
                db_ok = True
            except Exception:
                db_ok = False
        components["database"] = {
            "status": "up" if db_ok else "down" if self._tm else "unconfigured",
        }

        # Agent 响应性 —— 通过指标判断
        agents_info = self._metrics.get_all_agent_stats()
        components["agents"] = {
            "count": len(agents_info),
            "details": {
                name: {
                    "last_seen": info.get("last_event_at", 0),
                    "call_count": info.get("call_count", 0),
                }
                for name, info in agents_info.items()
            },
        }

        # 综合判断
        up_count = sum(
            1 for c in components.values()
            if c.get("status") == "up"
        )
        configured_count = sum(
            1 for c in components.values()
            if c.get("status") != "unconfigured"
        )

        if up_count == configured_count and configured_count > 0:
            overall = "healthy"
        elif up_count > 0:
            overall = "degraded"
        else:
            overall = "unhealthy"

        return {
            "status": overall,
            "components": components,
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "timestamp": time.time(),
        }

    # ── get_performance —— 性能指标 ───────────────────────────

    def get_performance(self) -> Dict[str, Any]:
        """返回延迟分位数、吞吐量等性能指标。

        Returns
        -------
        dict
            ``{"throughput": ..., "latency": ..., "providers": ...}``
        """
        snap = self._metrics.snapshot()
        uptime = snap["system"]["uptime_seconds"]

        # 任务吞吐量（tasks/sec）
        tasks_processed = (
            snap["tasks"]["completed"] + snap["tasks"]["failed"]
        )
        throughput = tasks_processed / uptime if uptime > 0 else 0.0

        # Provider 延迟
        provider_latency: Dict[str, float] = {}
        for name, info in snap["providers"].items():
            provider_latency[name] = info.get("avg_latency", 0.0)

        # Agent 耗时
        agent_latency: Dict[str, float] = {}
        for name, info in snap["agents"].items():
            agent_latency[name] = info.get("avg_duration", 0.0)

        # 总成本
        total_cost = sum(
            p.get("total_cost", 0.0) for p in snap["providers"].values()
        )

        return {
            "throughput_tasks_per_sec": round(throughput, 4),
            "total_tasks_processed": tasks_processed,
            "avg_task_duration_sec": snap["tasks"]["avg_time_to_complete"],
            "provider_latency_sec": provider_latency,
            "agent_latency_sec": agent_latency,
            "total_cost_usd": round(total_cost, 6),
            "duration_histogram": snap["tasks"]["duration_histogram"],
            "timestamp": time.time(),
        }

    # ── get_trace —— Trace 查询 ──────────────────────────────

    def get_trace(self, task_id: str) -> Optional[Dict[str, Any]]:
        """查询指定任务的完整 Trace 树。

        Parameters
        ----------
        task_id : str
            任务 ID。

        Returns
        -------
        dict | None
        """
        return self._tracer.get_trace(task_id)

    # ── get_all_traces ────────────────────────────────────────

    def get_all_traces(self) -> Dict[str, Any]:
        """返回所有任务的 trace 汇总。

        Returns
        -------
        dict
        """
        return self._tracer.export_dict()

    # ── 快捷快照 ──────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """返回可观测性系统的全量快照。

        Returns
        -------
        dict
        """
        return {
            "status": self.get_status(),
            "health": self.get_health(),
            "performance": self.get_performance(),
            "active_tasks": self.get_active_tasks(),
            "recent_events": self.get_recent_events(limit=20),
            "traces_summary": self._tracer.stats(),
            "snapshot_at": time.time(),
        }
