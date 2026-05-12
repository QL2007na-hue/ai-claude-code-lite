"""
事件历史 —— 事件时间线记录与上下文重建。

本模块提供 EventHistory 类，用于记录所有事件并提供时间线查询、
时间范围查询、以及从事件历史重建任务上下文的能力。
支持内存环形缓冲区和可选的 SQLite 持久化。

特性:
  - record() 存储事件
  - get_timeline(task_id) 按任务获取时间线
  - get_recent(limit) 最近 N 个事件
  - get_range(start, end) 时间范围查询
  - rebuild_context(task_id) 从事件历史重建任务上下文
  - export(task_id) 导出 JSON
  - 内存环形缓冲区（默认 10000 条，可配置）
  - 可选 SQLite 持久化

Usage:
    from runtime.context.event_history import EventHistory

    history = EventHistory(max_events=5000, db_path="data/events.db")

    # 记录事件
    history.record({
        "task_id": "task-001",
        "agent": "planner",
        "event": "task.planned",
        "payload": {"goal": "写贪吃蛇", "subtasks": [...]},
        "timestamp": "1700000000.0",
    })

    # 查询时间线
    timeline = history.get_timeline("task-001")

    # 重建上下文
    ctx = history.rebuild_context("task-001")

    # 导出
    json_data = history.export("task-001")
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional


class EventHistory:
    """事件历史记录器，支持环形缓冲和可选 SQLite 持久化。

    Parameters
    ----------
    max_events : int
        内存环形缓冲区的最大事件数。默认为 10000。
        超出时最旧的事件会被覆盖。
    db_path : str or None
        SQLite 数据库路径。None 表示仅内存模式。
    """

    def __init__(
        self,
        max_events: int = 10000,
        db_path: Optional[str] = None,
    ) -> None:
        if max_events <= 0:
            raise ValueError(f"max_events 必须为正数，收到: {max_events}")
        self._max_events = max_events
        self._buffer: List[Dict[str, Any]] = []
        self._write_index = 0
        self._lock = threading.RLock()

        # SQLite 持久化
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        if db_path:
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._create_tables()

    # ── 核心操作 ──────────────────────────────────────────────

    def record(self, event_data: Dict[str, Any]) -> int:
        """记录一条事件。

        Parameters
        ----------
        event_data : dict
            事件数据字典。应包含 task_id, agent, event, payload, timestamp 等字段。
            自动补充 recorded_at 字段（记录时间）。

        Returns
        -------
        int
            事件序号（单调递增）。
        """
        if not isinstance(event_data, dict):
            raise TypeError(f"event_data 必须为 dict，收到: {type(event_data).__name__}")

        event = dict(event_data)  # 浅拷贝防止外部修改
        event.setdefault("recorded_at", time.time())

        with self._lock:
            # 环形缓冲区
            if len(self._buffer) < self._max_events:
                self._buffer.append(event)
            else:
                self._buffer[self._write_index % self._max_events] = event
            seq = self._write_index
            self._write_index += 1

            # SQLite 持久化
            if self._conn:
                self._persist_event(event)

            return seq

    # ── 查询方法 ──────────────────────────────────────────────

    def get_timeline(self, task_id: str) -> List[Dict[str, Any]]:
        """获取指定任务的所有事件，按时间升序排列。

        Parameters
        ----------
        task_id : str
            任务唯一标识。

        Returns
        -------
        list[dict]
            事件列表（时间升序）。
        """
        with self._lock:
            if self._conn:
                return self._query_timeline_from_db(task_id)
            return self._query_timeline_from_memory(task_id)

    def get_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取最近 N 个事件（跨所有任务）。

        Parameters
        ----------
        limit : int
            返回的事件数上限。

        Returns
        -------
        list[dict]
            事件列表（时间降序，最新的在前）。
        """
        with self._lock:
            if self._conn:
                return self._query_recent_from_db(limit)
            return self._query_recent_from_memory(limit)

    def get_range(
        self,
        start_time: float,
        end_time: float,
        task_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取指定时间范围内的事件。

        Parameters
        ----------
        start_time : float
            起始时间戳（Unix 秒）。
        end_time : float
            结束时间戳（Unix 秒）。
        task_id : str or None
            可选的任务 ID 过滤。

        Returns
        -------
        list[dict]
            符合条件的事件列表（时间升序）。
        """
        with self._lock:
            if self._conn:
                return self._query_range_from_db(start_time, end_time, task_id)
            return self._query_range_from_memory(start_time, end_time, task_id)

    # ── 上下文重建 ────────────────────────────────────────────

    def rebuild_context(self, task_id: str) -> Dict[str, Any]:
        """从事件历史重建任务的完整上下文。

        遍历任务的所有事件，合并 payload 中的上下文信息，构建任务状态摘要。

        Parameters
        ----------
        task_id : str
            任务唯一标识。

        Returns
        -------
        dict
            重建的上下文，包含:
              - "task_id": str
              - "events_count": int
              - "agents": list[str]（参与过的 Agent）
              - "event_types": list[str]（事件类型列表）
              - "first_event_at": float（首个事件时间）
              - "last_event_at": float（最近事件时间）
              - "merged_payload": dict（合并的 payload 数据）
              - "status": str（推断的任务状态）
        """
        timeline = self.get_timeline(task_id)
        if not timeline:
            return {
                "task_id": task_id,
                "events_count": 0,
                "agents": [],
                "event_types": [],
                "first_event_at": None,
                "last_event_at": None,
                "merged_payload": {},
                "status": "unknown",
            }

        agents: List[str] = []
        event_types: List[str] = []
        merged_payload: Dict[str, Any] = {}

        first_ts = float("inf")
        last_ts = 0.0
        status = "unknown"

        for evt in timeline:
            # 收集 Agent
            agent = evt.get("agent", "")
            if agent and agent not in agents:
                agents.append(agent)

            # 收集事件类型
            evt_type = evt.get("event", "")
            if evt_type and evt_type not in event_types:
                event_types.append(evt_type)

            # 合并 payload
            payload = evt.get("payload", {})
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    payload = {}
            if isinstance(payload, dict):
                merged_payload.update(payload)

            # 时间范围
            ts = self._extract_timestamp(evt)
            if ts:
                first_ts = min(first_ts, ts)
                last_ts = max(last_ts, ts)

            # 推断状态
            status = self._infer_status(evt_type, status)

        return {
            "task_id": task_id,
            "events_count": len(timeline),
            "agents": agents,
            "event_types": event_types,
            "first_event_at": first_ts if first_ts != float("inf") else None,
            "last_event_at": last_ts if last_ts > 0 else None,
            "merged_payload": merged_payload,
            "status": status,
        }

    # ── 导出 ──────────────────────────────────────────────────

    def export(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """导出事件历史为 JSON 可序列化列表。

        Parameters
        ----------
        task_id : str or None
            任务 ID。None 表示导出所有事件。

        Returns
        -------
        list[dict]
            事件列表（时间升序）。
        """
        if task_id:
            return self.get_timeline(task_id)
        with self._lock:
            if self._conn:
                rows = self._conn.execute(
                    "SELECT data FROM events ORDER BY recorded_at ASC"
                ).fetchall()
                return [self._deserialize_row(r) for r in rows]
            events = self._sorted_buffer()
            return [dict(e) for e in events]

    # ── 工具方法 ──────────────────────────────────────────────

    def event_count(self) -> int:
        """获取记录的事件总数。

        Returns
        -------
        int
        """
        with self._lock:
            if self._conn:
                row = self._conn.execute("SELECT COUNT(*) as cnt FROM events").fetchone()
                return row["cnt"] if row else 0
            return len(self._buffer)

    def clear(self) -> None:
        """清空所有事件记录。"""
        with self._lock:
            self._buffer.clear()
            self._write_index = 0
            if self._conn:
                self._conn.execute("DELETE FROM events")
                self._conn.commit()

    def close(self) -> None:
        """关闭数据库连接。"""
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── 内部实现 ──────────────────────────────────────────────

    def _create_tables(self) -> None:
        assert self._conn is not None
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id   TEXT NOT NULL,
                timestamp REAL NOT NULL,
                recorded_at REAL NOT NULL,
                data      TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp)"
        )
        self._conn.commit()

    def _persist_event(self, event: Dict[str, Any]) -> None:
        assert self._conn is not None
        task_id = event.get("task_id", "")
        ts = self._extract_timestamp(event)
        recorded_at = event.get("recorded_at", time.time())
        data_json = json.dumps(event, ensure_ascii=False, default=str)
        self._conn.execute(
            "INSERT INTO events (task_id, timestamp, recorded_at, data) VALUES (?, ?, ?, ?)",
            (task_id, ts, recorded_at, data_json),
        )
        self._conn.commit()

    # ── 内存查询辅助 ──────────────────────────────────────────

    def _sorted_buffer(self) -> List[Dict[str, Any]]:
        """返回按 recorded_at 排序的缓冲区副本。"""
        return sorted(self._buffer, key=lambda e: e.get("recorded_at", 0.0))

    def _query_timeline_from_memory(self, task_id: str) -> List[Dict[str, Any]]:
        result = [e for e in self._sorted_buffer() if e.get("task_id") == task_id]
        return result

    def _query_recent_from_memory(self, limit: int) -> List[Dict[str, Any]]:
        sorted_events = sorted(
            self._buffer,
            key=lambda e: e.get("recorded_at", 0.0),
            reverse=True,
        )
        return sorted_events[:limit]

    def _query_range_from_memory(
        self, start_time: float, end_time: float, task_id: Optional[str]
    ) -> List[Dict[str, Any]]:
        result = []
        for e in self._sorted_buffer():
            ts = self._extract_timestamp(e)
            if ts and start_time <= ts <= end_time:
                if task_id is None or e.get("task_id") == task_id:
                    result.append(e)
        return result

    # ── DB 查询辅助 ───────────────────────────────────────────

    def _deserialize_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        data_str = row["data"]
        try:
            return json.loads(data_str)
        except (json.JSONDecodeError, TypeError):
            return {"raw": data_str}

    def _query_timeline_from_db(self, task_id: str) -> List[Dict[str, Any]]:
        assert self._conn is not None
        rows = self._conn.execute(
            "SELECT data FROM events WHERE task_id = ? ORDER BY timestamp ASC",
            (task_id,),
        ).fetchall()
        return [self._deserialize_row(r) for r in rows]

    def _query_recent_from_db(self, limit: int) -> List[Dict[str, Any]]:
        assert self._conn is not None
        rows = self._conn.execute(
            "SELECT data FROM events ORDER BY recorded_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._deserialize_row(r) for r in rows]

    def _query_range_from_db(
        self, start_time: float, end_time: float, task_id: Optional[str]
    ) -> List[Dict[str, Any]]:
        assert self._conn is not None
        if task_id:
            rows = self._conn.execute(
                "SELECT data FROM events WHERE task_id = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC",
                (task_id, start_time, end_time),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT data FROM events WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC",
                (start_time, end_time),
            ).fetchall()
        return [self._deserialize_row(r) for r in rows]

    # ── 工具 ─────────────────────────────────────────────────_

    @staticmethod
    def _extract_timestamp(event: Dict[str, Any]) -> float:
        """从事件中提取时间戳。"""
        ts = event.get("timestamp")
        if ts is None:
            ts = event.get("recorded_at")
        if isinstance(ts, str):
            try:
                return float(ts)
            except (ValueError, TypeError):
                pass
        if isinstance(ts, (int, float)):
            return float(ts)
        return 0.0

    @staticmethod
    def _infer_status(event_type: str, current_status: str) -> str:
        """从事件类型推断任务状态。"""
        status_map = {
            "task.created": "pending",
            "task.plan_started": "planning",
            "task.planned": "planned",
            "task.coding_started": "coding",
            "task.code_generated": "coding",
            "task.code_written": "coding",
            "task.coding_completed": "review",
            "task.review_started": "review",
            "review.approved": "done",
            "review.rejected": "retry",
            "task.done": "done",
            "task.failed": "failed",
            "task.partially_failed": "partial_done",
            "task.retry_exhausted": "failed",
            "task.plan_failed": "failed",
            "task.coding_failed": "failed",
            "review.failed": "failed",
        }
        return status_map.get(event_type, current_status)
