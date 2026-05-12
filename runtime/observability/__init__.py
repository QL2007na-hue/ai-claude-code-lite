"""
AI Runtime Observability —— 可观测性子系统统一入口。

本模块提供运行时全维度可观测能力：
    - **EventTracer**     —— Span / Trace 分布式跟踪
    - **RuntimeMetrics**  —— Agent / Provider / Task 指标采集 + Prometheus 导出
    - **RuntimeLogger**   —— JSON Lines 结构化日志 + 轮转 + 上下文绑定
    - **ObservabilityDashboard** —— 面向 API 的实时聚合层

Usage:
    # 独立使用
    from runtime.observability import EventTracer, RuntimeMetrics

    tracer = EventTracer()
    with tracer.trace("my_op", task_id="t1"):
        ...

    metrics = RuntimeMetrics()
    metrics.record_agent_event("planner", "task.completed", duration=1.2)

    print(metrics.to_prometheus())

    # 一键初始化全套
    from runtime.observability import init_observability

    obs = init_observability(task_manager=tm)
    # obs.tracer, obs.metrics, obs.logger, obs.dashboard 均已就绪
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from runtime.observability.tracer import EventTracer, Span
from runtime.observability.metrics import RuntimeMetrics
from runtime.observability.logger import RuntimeLogger
from runtime.observability.dashboard import ObservabilityDashboard


# ───────────────────────────────────────────────────────────────
# 一键初始化
# ───────────────────────────────────────────────────────────────

@dataclass
class ObservabilitySuite:
    """可观测性套件 — 包含所有子系统的聚合实例。

    Attributes
    ----------
    tracer : EventTracer
        Span / Trace 跟踪器。
    metrics : RuntimeMetrics
        指标采集引擎。
    logger : RuntimeLogger
        结构化日志器。
    dashboard : ObservabilityDashboard
        API 聚合层。
    """
    tracer: EventTracer = field(default_factory=EventTracer)
    metrics: RuntimeMetrics = field(default_factory=RuntimeMetrics)
    logger: RuntimeLogger = field(default_factory=RuntimeLogger)
    dashboard: Optional[ObservabilityDashboard] = None


def init_observability(
    task_manager: Any = None,
    redis_client: Any = None,
    logger_name: str = "ai-runtime",
    log_dir: str = "logs",
    log_level: str = "INFO",
) -> ObservabilitySuite:
    """一键初始化完整可观测性套件。

    创建并关联 EventTracer、RuntimeMetrics、RuntimeLogger、
    ObservabilityDashboard 四个子系统。

    Parameters
    ----------
    task_manager : TaskManager, optional
        任务管理器实例。
    redis_client : redis.Redis, optional
        Redis 客户端，用于健康检查。
    logger_name : str, default "ai-runtime"
        日志器名称。
    log_dir : str, default "logs"
        日志文件目录。
    log_level : str, default "INFO"
        日志级别。

    Returns
    -------
    ObservabilitySuite
        包含所有子系统的聚合对象。

    Example
    -------
    from runtime.observability import init_observability
    from runtime.task_manager import TaskManager

    tm = TaskManager()
    obs = init_observability(task_manager=tm)

    # 开始跟踪
    with obs.tracer.trace("my_task", task_id="t1"):
        obs.metrics.record_agent_event("planner", "task.started")
        obs.logger.info("任务开始", task_id="t1")
    """
    tracer = EventTracer()
    metrics = RuntimeMetrics()
    logger = RuntimeLogger(
        name=logger_name,
        log_dir=log_dir,
        level=log_level,
    )
    dashboard = ObservabilityDashboard(
        task_manager=task_manager,
        tracer=tracer,
        metrics=metrics,
        logger=logger,
        redis_client=redis_client,
    )

    logger.info(
        "可观测性系统初始化完成",
        components=["tracer", "metrics", "logger", "dashboard"],
    )

    return ObservabilitySuite(
        tracer=tracer,
        metrics=metrics,
        logger=logger,
        dashboard=dashboard,
    )


# ───────────────────────────────────────────────────────────────
# 模块导出
# ───────────────────────────────────────────────────────────────

__all__ = [
    # 核心类
    "EventTracer",
    "Span",
    "RuntimeMetrics",
    "RuntimeLogger",
    "ObservabilityDashboard",
    # 套件
    "ObservabilitySuite",
    "init_observability",
]
