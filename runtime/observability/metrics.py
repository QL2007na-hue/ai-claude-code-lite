"""
Runtime Metrics Collection —— 线程安全的可观测性指标采集引擎。

提供 Agent / Provider / Task 三维度的计数器与直方图式指标，
支持 Prometheus 风格导出、快照、重置等操作。

Usage:
    from runtime.observability.metrics import RuntimeMetrics

    metrics = RuntimeMetrics()

    # Agent 事件
    metrics.record_agent_event("planner", "task.completed", duration=1.23)
    metrics.record_agent_event("coder", "task.failed")

    # Provider 调用
    metrics.record_provider_call("deepseek", tokens=1500, cost=0.002, latency=0.8)

    # Task 生命周期
    metrics.record_task_lifecycle("task-001", "pending", "running")
    metrics.record_task_lifecycle("task-001", "running", "done")

    # 查询
    print(metrics.summary())
    snap = metrics.snapshot()

    # Prometheus 格式导出
    print(metrics.to_prometheus())
"""

from __future__ import annotations

import copy
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ───────────────────────────────────────────────────────────────
# 内部数据结构
# ───────────────────────────────────────────────────────────────

@dataclass
class _AgentStats:
    """单个 Agent 的统计信息。"""
    tasks_completed: int = 0
    tasks_failed: int = 0
    tasks_retried: int = 0
    total_duration: float = 0.0
    call_count: int = 0
    last_event_at: float = 0.0
    events: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def avg_duration(self) -> float:
        """平均任务耗时。"""
        if self.call_count == 0:
            return 0.0
        return self.total_duration / self.call_count


@dataclass
class _ProviderStats:
    """单个 Provider 的统计信息。"""
    total_tokens: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_cost: float = 0.0
    total_calls: int = 0
    total_latency: float = 0.0
    error_count: int = 0
    last_call_at: float = 0.0

    @property
    def avg_latency(self) -> float:
        """平均延迟。"""
        if self.total_calls == 0:
            return 0.0
        return self.total_latency / self.total_calls

    @property
    def avg_tokens_per_call(self) -> float:
        """每次调用平均 token 数。"""
        if self.total_calls == 0:
            return 0.0
        return self.total_tokens / self.total_calls


# ───────────────────────────────────────────────────────────────
# RuntimeMetrics
# ───────────────────────────────────────────────────────────────

class RuntimeMetrics:
    """线程安全的运行时指标采集引擎。

    三大维度：
        1. **Agent 指标** — 每个 agent 的任务完成/失败/重试/耗时统计
        2. **Provider 指标** — 每个 provider 的 token/成本/延迟/错误统计
        3. **Task 指标** — 全局任务生命周期统计（创建/完成/失败/重试/耗时分布）

    Thread Safety
    -------------
    所有 public 方法均通过 ``threading.Lock`` 保护内部状态。
    """

    def __init__(self) -> None:
        """初始化 RuntimeMetrics。"""
        self._lock: threading.Lock = threading.Lock()

        # Agent 维度: agent_name → _AgentStats
        self._agent_stats: Dict[str, _AgentStats] = {}

        # Provider 维度: provider_name → _ProviderStats
        self._provider_stats: Dict[str, _ProviderStats] = {}

        # Task 全局维度
        self._task_created: int = 0
        self._task_completed: int = 0
        self._task_failed: int = 0
        self._task_retried: int = 0
        self._task_total_time: float = 0.0
        self._task_time_count: int = 0  # 用于计算平均耗时

        # Task 时长分布（直方图桶，单位秒）
        self._task_duration_buckets: Dict[float, int] = {
            0.5: 0, 1.0: 0, 2.0: 0, 5.0: 0, 10.0: 0,
            30.0: 0, 60.0: 0, 120.0: 0, 300.0: 0, float("inf"): 0,
        }

        # 系统级
        self._start_time: float = time.time()
        self._total_events: int = 0

    # ── Agent 指标 ─────────────────────────────────────────────

    def record_agent_event(
        self,
        agent: str,
        event: str,
        duration: Optional[float] = None,
    ) -> None:
        """记录 Agent 事件并更新对应计数器。

        Parameters
        ----------
        agent : str
            Agent 名称（如 "planner", "coder", "reviewer"）。
        event : str
            事件名称（如 "task.completed", "task.failed", "task.retried"）。
        duration : float, optional
            事件耗时（秒），用于更新 avg_duration。
        """
        with self._lock:
            stats = self._agent_stats.setdefault(agent, _AgentStats())
            stats.events[event] += 1
            stats.last_event_at = time.time()
            stats.call_count += 1

            if "completed" in event or event == "task.done":
                stats.tasks_completed += 1
            elif "failed" in event or "error" in event:
                stats.tasks_failed += 1
            elif "retry" in event:
                stats.tasks_retried += 1

            if duration is not None:
                stats.total_duration += duration

    def get_agent_stats(self, agent: str) -> Optional[Dict[str, Any]]:
        """获取指定 Agent 的统计信息。

        Parameters
        ----------
        agent : str
            Agent 名称。

        Returns
        -------
        dict | None
        """
        with self._lock:
            stats = self._agent_stats.get(agent)
            if stats is None:
                return None
            return {
                "agent": agent,
                "tasks_completed": stats.tasks_completed,
                "tasks_failed": stats.tasks_failed,
                "tasks_retried": stats.tasks_retried,
                "avg_duration": round(stats.avg_duration, 4),
                "call_count": stats.call_count,
                "total_duration": round(stats.total_duration, 4),
                "last_event_at": stats.last_event_at,
                "events": dict(stats.events),
            }

    def get_all_agent_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取所有 Agent 的统计信息。

        Returns
        -------
        dict
            ``{agent_name: stats_dict, ...}``
        """
        result: Dict[str, Dict[str, Any]] = {}
        with self._lock:
            for agent in list(self._agent_stats.keys()):
                entry = self.get_agent_stats(agent)
                if entry:
                    result[agent] = entry
        return result

    # ── Provider 指标 ──────────────────────────────────────────

    def record_provider_call(
        self,
        provider: str,
        tokens: int = 0,
        cost: float = 0.0,
        latency: float = 0.0,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        error: bool = False,
    ) -> None:
        """记录一次 Provider 调用并更新统计。

        Parameters
        ----------
        provider : str
            Provider 名称（如 "openai", "deepseek", "ollama"）。
        tokens : int, default 0
            本次调用消耗的总 token 数。
        cost : float, default 0.0
            本次调用的费用（USD）。
        latency : float, default 0.0
            本次调用的延迟（秒）。
        prompt_tokens : int, default 0
            Prompt token 数量。
        completion_tokens : int, default 0
            Completion token 数量。
        error : bool, default False
            本次调用是否发生错误。
        """
        with self._lock:
            stats = self._provider_stats.setdefault(provider, _ProviderStats())
            if tokens == 0 and prompt_tokens + completion_tokens > 0:
                tokens = prompt_tokens + completion_tokens
            stats.total_tokens += tokens
            stats.prompt_tokens += prompt_tokens
            stats.completion_tokens += completion_tokens
            stats.total_cost += cost
            stats.total_calls += 1
            stats.total_latency += latency
            stats.last_call_at = time.time()
            if error:
                stats.error_count += 1

    def get_provider_stats(self, provider: str) -> Optional[Dict[str, Any]]:
        """获取指定 Provider 的统计信息。

        Parameters
        ----------
        provider : str
            Provider 名称。

        Returns
        -------
        dict | None
        """
        with self._lock:
            stats = self._provider_stats.get(provider)
            if stats is None:
                return None
            return {
                "provider": provider,
                "total_tokens": stats.total_tokens,
                "prompt_tokens": stats.prompt_tokens,
                "completion_tokens": stats.completion_tokens,
                "total_cost": round(stats.total_cost, 6),
                "total_calls": stats.total_calls,
                "avg_latency": round(stats.avg_latency, 4),
                "total_latency": round(stats.total_latency, 4),
                "avg_tokens_per_call": round(stats.avg_tokens_per_call, 2),
                "error_count": stats.error_count,
                "last_call_at": stats.last_call_at,
            }

    def get_all_provider_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取所有 Provider 的统计信息。

        Returns
        -------
        dict
            ``{provider_name: stats_dict, ...}``
        """
        result: Dict[str, Dict[str, Any]] = {}
        with self._lock:
            for provider in list(self._provider_stats.keys()):
                entry = self.get_provider_stats(provider)
                if entry:
                    result[provider] = entry
        return result

    # ── Task 生命周期指标 ──────────────────────────────────────

    def record_task_lifecycle(
        self,
        task_id: str,
        from_status: str,
        to_status: str,
        duration: Optional[float] = None,
    ) -> None:
        """记录任务状态变更。

        Parameters
        ----------
        task_id : str
            任务 ID。
        from_status : str
            变更前状态（"pending", "running", "review", "retry", "done", "failed"）。
        to_status : str
            变更后状态。
        duration : float, optional
            从创建到当前的时间（用于 done/failed 时计算平均完成时间）。
        """
        with self._lock:
            self._total_events += 1

            if from_status == "" or to_status == "pending":
                self._task_created += 1

            if to_status == "done":
                self._task_completed += 1
                if duration is not None:
                    self._task_total_time += duration
                    self._task_time_count += 1
                    self._record_duration_bucket(duration)
            elif to_status == "failed":
                self._task_failed += 1
                if duration is not None:
                    self._task_total_time += duration
                    self._task_time_count += 1
                    self._record_duration_bucket(duration)
            elif to_status == "retry":
                self._task_retried += 1

    def _record_duration_bucket(self, duration: float) -> None:
        """将耗时归入对应的直方图桶。"""
        for upper in sorted(self._task_duration_buckets.keys()):
            if duration <= upper:
                self._task_duration_buckets[upper] += 1
                break

    @property
    def avg_time_to_complete(self) -> float:
        """任务平均完成时间（秒）。"""
        with self._lock:
            if self._task_time_count == 0:
                return 0.0
            return self._task_total_time / self._task_time_count

    def get_task_stats(self) -> Dict[str, Any]:
        """获取任务维度的全局统计。

        Returns
        -------
        dict
        """
        with self._lock:
            return {
                "created": self._task_created,
                "completed": self._task_completed,
                "failed": self._task_failed,
                "retried": self._task_retried,
                "avg_time_to_complete": round(self.avg_time_to_complete, 4),
                "duration_histogram": dict(self._task_duration_buckets),
                "total_events_processed": self._total_events,
            }

    # ── 汇总 ───────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        """返回所有指标的完整汇总。

        Returns
        -------
        dict
            ``{"agents": {...}, "providers": {...}, "tasks": {...}, "system": {...}}``
        """
        return {
            "agents": self.get_all_agent_stats(),
            "providers": self.get_all_provider_stats(),
            "tasks": self.get_task_stats(),
            "system": {
                "uptime_seconds": round(time.time() - self._start_time, 2),
                "start_time": self._start_time,
            },
        }

    def snapshot(self) -> Dict[str, Any]:
        """返回所有指标的快照（深拷贝），可安全传递到其他线程。

        Returns
        -------
        dict
            与 ``summary()`` 相同结构，但数据为深拷贝，脱离锁保护。
        """
        return copy.deepcopy(self.summary())

    # ── Prometheus 导出 ────────────────────────────────────────

    def to_prometheus(self) -> str:
        """导出为 Prometheus 文本格式。

        输出符合 Prometheus exposition format，可直接被
        Prometheus scrape 或作为 /metrics 端点响应体。

        Returns
        -------
        str
            Prometheus 格式指标文本。

        Example
        -------
        # 在 FastAPI 端点中：
        @app.get("/metrics")
        def metrics_endpoint():
            return Response(
                content=metrics.to_prometheus(),
                media_type="text/plain; version=0.0.4",
            )
        """
        lines: List[str] = []
        snap = self.snapshot()

        # ── HELP / TYPE 声明 ───────────────────────────────────
        _emit_help_type(lines, "ai_runtime_uptime_seconds", "gauge",
                        "Runtime 运行时长（秒）")
        _emit_help_type(lines, "ai_runtime_events_total", "counter",
                        "处理的事件总数")

        _emit_help_type(lines, "ai_runtime_task_created_total", "counter",
                        "已创建任务总数")
        _emit_help_type(lines, "ai_runtime_task_completed_total", "counter",
                        "已完成任务总数")
        _emit_help_type(lines, "ai_runtime_task_failed_total", "counter",
                        "已失败任务总数")
        _emit_help_type(lines, "ai_runtime_task_retried_total", "counter",
                        "已重试任务总数")
        _emit_help_type(lines, "ai_runtime_task_avg_duration_seconds", "gauge",
                        "任务平均完成时间（秒）")

        _emit_help_type(lines, "ai_runtime_agent_completed_total", "counter",
                        "Agent 完成事件数")
        _emit_help_type(lines, "ai_runtime_agent_failed_total", "counter",
                        "Agent 失败事件数")
        _emit_help_type(lines, "ai_runtime_agent_avg_duration_seconds", "gauge",
                        "Agent 平均任务耗时（秒）")
        _emit_help_type(lines, "ai_runtime_agent_events_total", "counter",
                        "Agent 事件总数")

        _emit_help_type(lines, "ai_runtime_provider_tokens_total", "counter",
                        "Provider 总 token 数")
        _emit_help_type(lines, "ai_runtime_provider_cost_total", "counter",
                        "Provider 总费用（USD）")
        _emit_help_type(lines, "ai_runtime_provider_calls_total", "counter",
                        "Provider 总调用次数")
        _emit_help_type(lines, "ai_runtime_provider_avg_latency_seconds", "gauge",
                        "Provider 平均延迟（秒）")
        _emit_help_type(lines, "ai_runtime_provider_errors_total", "counter",
                        "Provider 错误次数")

        _emit_help_type(lines, "ai_runtime_task_duration_seconds", "histogram",
                        "任务耗时分布直方图")

        # ── 值 ─────────────────────────────────────────────────

        # System
        lines.append(f"ai_runtime_uptime_seconds {snap['system']['uptime_seconds']}")
        lines.append(f"ai_runtime_events_total {snap['tasks']['total_events_processed']}")

        # Task 全局
        tasks = snap["tasks"]
        lines.append(f"ai_runtime_task_created_total {tasks['created']}")
        lines.append(f"ai_runtime_task_completed_total {tasks['completed']}")
        lines.append(f"ai_runtime_task_failed_total {tasks['failed']}")
        lines.append(f"ai_runtime_task_retried_total {tasks['retried']}")
        lines.append(f"ai_runtime_task_avg_duration_seconds {tasks['avg_time_to_complete']}")

        # Task duration histogram
        for upper, count in sorted(tasks["duration_histogram"].items()):
            bucket_str = str(upper)
            if upper == float("inf"):
                bucket_str = "+Inf"
            lines.append(
                f'ai_runtime_task_duration_seconds_bucket{{le="{bucket_str}"}} {count}'
            )
        lines.append(
            f"ai_runtime_task_duration_seconds_count "
            f"{tasks['completed'] + tasks['failed']}"
        )
        total_time = (snap["system"]["uptime_seconds"]  # 近似
                      if tasks["completed"] + tasks["failed"] > 0 else 0)
        lines.append(f"ai_runtime_task_duration_seconds_sum {total_time}")

        # Agent 维度
        for agent_name, agent in snap["agents"].items():
            label = f'agent="{agent_name}"'
            lines.append(f"ai_runtime_agent_completed_total{{{label}}} {agent['tasks_completed']}")
            lines.append(f"ai_runtime_agent_failed_total{{{label}}} {agent['tasks_failed']}")
            lines.append(f"ai_runtime_agent_avg_duration_seconds{{{label}}} {agent['avg_duration']}")
            for evt_name, count in agent["events"].items():
                evt_label = f'agent="{agent_name}",event="{evt_name}"'
                lines.append(f"ai_runtime_agent_events_total{{{evt_label}}} {count}")

        # Provider 维度
        for prov_name, prov in snap["providers"].items():
            label = f'provider="{prov_name}"'
            lines.append(f"ai_runtime_provider_tokens_total{{{label}}} {prov['total_tokens']}")
            lines.append(f"ai_runtime_provider_cost_total{{{label}}} {prov['total_cost']}")
            lines.append(f"ai_runtime_provider_calls_total{{{label}}} {prov['total_calls']}")
            lines.append(f"ai_runtime_provider_avg_latency_seconds{{{label}}} {prov['avg_latency']}")
            lines.append(f"ai_runtime_provider_errors_total{{{label}}} {prov['error_count']}")

        return "\n".join(lines) + "\n"

    def to_prometheus_metrics(self) -> str:
        """``to_prometheus()`` 的别名。

        .. deprecated::
           请直接使用 ``to_prometheus()``。
        """
        return self.to_prometheus()

    # ── 生命周期 ──────────────────────────────────────────────

    def reset(self) -> None:
        """重置所有指标计数器。

        通常在 benchmark 轮次之间调用。
        """
        with self._lock:
            self._agent_stats.clear()
            self._provider_stats.clear()
            self._task_created = 0
            self._task_completed = 0
            self._task_failed = 0
            self._task_retried = 0
            self._task_total_time = 0.0
            self._task_time_count = 0
            self._task_duration_buckets = {
                k: 0 for k in self._task_duration_buckets
            }
            self._total_events = 0
            self._start_time = time.time()

    @property
    def uptime_seconds(self) -> float:
        """Runtime 的运行时长（秒）。"""
        return time.time() - self._start_time


# ───────────────────────────────────────────────────────────────
# Prometheus 格式工具
# ───────────────────────────────────────────────────────────────

def _emit_help_type(
    lines: List[str],
    name: str,
    typ: str,
    help_text: str,
) -> None:
    """向文本行列表追加 Prometheus HELP / TYPE 行。"""
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {typ}")
