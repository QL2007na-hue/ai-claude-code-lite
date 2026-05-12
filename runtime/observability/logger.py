"""
Structured Runtime Logger —— JSON Lines 格式的运行时日志引擎。

提供结构化日志记录、事件日志、错误追踪、指标日志点、
日志轮转与上下文绑定能力。所有日志输出为 JSON Lines 格式，
可直接被日志聚合系统（ELK / Loki / Datadog）消费。

Usage:
    from runtime.observability.logger import RuntimeLogger

    logger = RuntimeLogger()

    # 结构化日志
    logger.log("INFO", "任务开始执行", task_id="task-001", agent="planner")

    # 事件日志
    logger.log_event({"event": "task.planned", "task_id": "task-001", "subtasks": 5})

    # 错误日志（自动附带 traceback）
    try:
        1 / 0
    except Exception as e:
        logger.log_error(e, {"task_id": "task-001", "phase": "coding"})

    # 指标日志点
    logger.log_metric("llm_latency", 0.823, tags={"model": "deepseek", "provider": "deepseek"})

    # 上下文绑定 —— 创建子 logger，自动附加字段
    task_logger = logger.bind(task_id="task-001", agent="planner")
    task_logger.log("DEBUG", "planning started")
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import traceback
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional


# ───────────────────────────────────────────────────────────────
# JSON 格式化器
# ───────────────────────────────────────────────────────────────

class _JsonLineFormatter(logging.Formatter):
    """将 LogRecord 格式化为单行 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        """重写 format，返回 JSON 行。

        Note
        ----
        不使用 ``formatTime``，而是使用 ISO 8601 时间戳。
        """
        log_entry: Dict[str, Any] = {
            "timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.localtime(record.created)
            ) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # 合并额外的上下文字段（由 bind 场景注入）
        if hasattr(record, "ctx") and isinstance(record.ctx, dict):  # type: ignore[union-attr]
            log_entry["context"] = record.ctx  # type: ignore[union-attr]

        # 如果 message 本身就是 dict（log_event 场景），合并进去
        if hasattr(record, "event_data") and record.event_data:  # type: ignore[union-attr]
            log_entry["event"] = record.event_data  # type: ignore[union-attr]

        # 如果存在 exc_info，追加错误详情
        if record.exc_info and record.exc_info[1]:
            log_entry["error"] = {
                "type": type(record.exc_info[1]).__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        # 模块 / 文件名 / 行号 —— 调试用
        log_entry["location"] = {
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }

        try:
            return json.dumps(log_entry, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            # 兜底：回退到普通字符串形式
            return json.dumps({
                "timestamp": log_entry["timestamp"],
                "level": record.levelname,
                "message": str(record.msg),
                "error": "log serialization failed",
            }, ensure_ascii=False)


# ───────────────────────────────────────────────────────────────
# RuntimeLogger
# ───────────────────────────────────────────────────────────────

class RuntimeLogger:
    """JSON Lines 格式的结构化运行时日志器。

    核心能力：
        - ``log()`` — 结构化日志，支持任意 **context 参数
        - ``log_event()`` — 记录事件数据（自动内嵌到 event 字段）
        - ``log_error()`` — 结构化错误日志，自动附带 traceback
        - ``log_metric()`` — 记录指标日志点
        - ``bind()`` — 创建绑定上下文的子 logger

    日志轮转
    --------
    默认开启 RotatingFileHandler，单文件最大 10 MB，保留 5 个备份。
    可通过 ``rotation_max_bytes`` 和 ``rotation_backup_count`` 配置。

    输出位置
    --------
    同时输出到文件（logs/runtime.jsonl）和 stdout。
    """

    # 有效日志级别
    VALID_LEVELS: frozenset = frozenset({
        "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL",
    })

    def __init__(
        self,
        name: str = "ai-runtime",
        log_dir: str = "logs",
        log_file: str = "runtime.jsonl",
        rotation_max_bytes: int = 10 * 1024 * 1024,  # 10 MB
        rotation_backup_count: int = 5,
        level: str = "INFO",
        console: bool = True,
    ) -> None:
        """初始化 RuntimeLogger。

        Parameters
        ----------
        name : str, default "ai-runtime"
            日志器名称，对应 JSON 中的 logger 字段。
        log_dir : str, default "logs"
            日志文件目录，会自动创建。
        log_file : str, default "runtime.jsonl"
            日志文件名。
        rotation_max_bytes : int, default 10 MB
            单个日志文件最大字节数，超出后轮转。
        rotation_backup_count : int, default 5
            保留的历史文件数量。
        level : str, default "INFO"
            日志级别（DEBUG / INFO / WARNING / ERROR / CRITICAL）。
        console : bool, default True
            是否同时输出到 stdout。
        """
        self.name = name
        self._context: Dict[str, Any] = {}
        self._lock: threading.Lock = threading.Lock()

        # 确保日志目录存在
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, log_file)

        # 创建内部 logger
        self._logger = logging.getLogger(name)
        self._logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        self._logger.propagate = False

        # 清空已有 handler（避免重复添加）
        self._logger.handlers.clear()

        # JSON Line 格式器
        formatter = _JsonLineFormatter()

        # 文件 handler —— 轮转
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=rotation_max_bytes,
            backupCount=rotation_backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

        # 控制台 handler
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)

    # ── 核心 log 方法 ─────────────────────────────────────────

    def log(self, level: str, message: str, **context: Any) -> None:
        """记录一条结构化日志。

        Parameters
        ----------
        level : str
            日志级别（DEBUG / INFO / WARNING / ERROR / CRITICAL）。
        message : str
            日志消息正文。
        **context : Any
            任意键值对上下文（如 task_id, agent, phase 等）。

        Raises
        ------
        ValueError
            如果 level 不是有效日志级别。
        """
        level_upper = level.upper()
        if level_upper not in self.VALID_LEVELS:
            raise ValueError(
                f"无效日志级别: {level}，合法值: {sorted(self.VALID_LEVELS)}"
            )

        log_level = getattr(logging, level_upper, logging.INFO)

        # 合并全局 context 与本次传入的 context
        merged_context: Dict[str, Any] = {}
        with self._lock:
            merged_context.update(self._context)
        merged_context.update(context)

        # 创建 record，通过 extra 注入 context
        record = self._logger.makeRecord(
            self._logger.name,
            log_level,
            "(unknown)",
            0,
            message,
            args=(),
            exc_info=None,
        )
        setattr(record, "ctx", merged_context)
        self._logger.handle(record)

    def log_event(self, event_data: Dict[str, Any]) -> None:
        """记录一条事件日志。

        将事件数据内嵌到 JSON 输出的 ``event`` 字段中。

        Parameters
        ----------
        event_data : dict
            事件数据字典。
            ``{"event": "task.planned", "task_id": "...", ...}``
        """
        level = event_data.get("level", "INFO")
        event_name = event_data.get("event", "unknown")
        message = f"event: {event_name}"

        log_level = getattr(logging, level.upper(), logging.INFO)
        record = self._logger.makeRecord(
            self._logger.name,
            log_level,
            "(unknown)",
            0,
            message,
            args=(),
            exc_info=None,
        )
        setattr(record, "event_data", event_data)

        with self._lock:
            ctx_copy = dict(self._context)
        setattr(record, "ctx", ctx_copy)

        self._logger.handle(record)

    def log_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """记录一条结构化错误日志，自动附带完整 traceback。

        Parameters
        ----------
        error : Exception
            异常对象。
        context : dict, optional
            错误发生时的附加上下文。
        """
        msg = f"error: {type(error).__name__}: {error}"
        merged_context: Dict[str, Any] = {}
        with self._lock:
            merged_context.update(self._context)
        if context:
            merged_context.update(context)

        record = self._logger.makeRecord(
            self._logger.name,
            logging.ERROR,
            "(unknown)",
            0,
            msg,
            args=(),
            exc_info=(type(error), error, error.__traceback__),
        )
        setattr(record, "ctx", merged_context)
        self._logger.handle(record)

    def log_metric(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """记录一条指标日志点。

        输出为 JSON 中包含 ``metric`` 字段的结构化条目。

        Parameters
        ----------
        name : str
            指标名称（如 "llm_latency", "memory_usage_mb"）。
        value : float
            指标数值。
        tags : dict, optional
            标签（如 {"model": "gpt-4o", "provider": "openai"}）。
        """
        metric_entry: Dict[str, Any] = {
            "metric": name,
            "value": value,
            "tags": tags or {},
            "timestamp": time.time(),
        }
        merged_context: Dict[str, Any] = {}
        with self._lock:
            merged_context.update(self._context)
        if tags:
            merged_context.update(tags)

        record = self._logger.makeRecord(
            self._logger.name,
            logging.INFO,
            "(unknown)",
            0,
            f"metric: {name}={value}",
            args=(),
            exc_info=None,
        )
        setattr(record, "ctx", merged_context)
        setattr(record, "event_data", metric_entry)
        self._logger.handle(record)

    # ── 便捷方法 ──────────────────────────────────────────────

    def debug(self, message: str, **context: Any) -> None:
        """DEBUG 级别日志快捷方法。"""
        self.log("DEBUG", message, **context)

    def info(self, message: str, **context: Any) -> None:
        """INFO 级别日志快捷方法。"""
        self.log("INFO", message, **context)

    def warning(self, message: str, **context: Any) -> None:
        """WARNING 级别日志快捷方法。"""
        self.log("WARNING", message, **context)

    def error(self, message: str, **context: Any) -> None:
        """ERROR 级别日志快捷方法。"""
        self.log("ERROR", message, **context)

    def critical(self, message: str, **context: Any) -> None:
        """CRITICAL 级别日志快捷方法。"""
        self.log("CRITICAL", message, **context)

    # ── 上下文绑定 ────────────────────────────────────────────

    def bind(self, **context: Any) -> "RuntimeLogger":
        """创建一个绑定固定上下文的子 Logger。

        返回一个新的 RuntimeLogger 实例，所有后续日志调用都会
        自动携带绑定的上下文字段。新实例与父实例共享同一个底层
        logging.Logger 和文件 handler。

        Parameters
        ----------
        **context : Any
            要绑定的上下文字段，如 task_id, agent 等。

        Returns
        -------
        RuntimeLogger
            绑定上下文后的新 RuntimeLogger 实例。

        Example
        -------
        task_logger = runtime_logger.bind(task_id="task-001", agent="planner")
        task_logger.info("plan started")  # 自动携带 task_id + agent
        task_logger.info("plan done", subtasks=5)  # 追加 subtasks
        """
        # 创建共享同一底层 logger 的新实例
        # 注意：我们不重新创建 handler，避免重复输出
        new_logger = RuntimeLogger.__new__(RuntimeLogger)
        new_logger.name = f"{self.name}.bound"
        new_logger._logger = self._logger  # 共享底层
        new_logger._lock = self._lock       # 共享锁
        # 合并上下文
        with self._lock:
            new_logger._context = {**self._context, **context}
        return new_logger

    def unbind(self, *keys: str) -> "RuntimeLogger":
        """创建一个移除了指定上下文字段的子 Logger。

        Parameters
        ----------
        *keys : str
            要移除的上下文字段名。

        Returns
        -------
        RuntimeLogger
        """
        new_logger = RuntimeLogger.__new__(RuntimeLogger)
        new_logger.name = f"{self.name}.unbound"
        new_logger._logger = self._logger
        new_logger._lock = self._lock
        with self._lock:
            base = dict(self._context)
            for k in keys:
                base.pop(k, None)
            new_logger._context = base
        return new_logger

    @property
    def context(self) -> Dict[str, Any]:
        """当前绑定的上下文字段（只读副本）。"""
        with self._lock:
            return dict(self._context)

    # ── 管理 ───────────────────────────────────────────────────

    def set_level(self, level: str) -> None:
        """动态修改日志级别。

        Parameters
        ----------
        level : str
            DEBUG / INFO / WARNING / ERROR / CRITICAL。
        """
        self._logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    def flush(self) -> None:
        """刷新所有 handler 缓冲区。"""
        for handler in self._logger.handlers:
            handler.flush()

    def close(self) -> None:
        """关闭所有 handler，释放文件句柄。"""
        for handler in list(self._logger.handlers):
            handler.close()
            self._logger.removeHandler(handler)
