import json
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional


class TaskManager:
    """基于 SQLite 的轻量级 Task Manager

    task 状态流转:
        pending → running → review → done
                       │         │
                       ▼         ▼
                     failed    retry → running

    Usage:
        tm = TaskManager()

        # 创建任务
        tm.create_task(agent="planner", payload={"goal": "..."})

        # 更新状态
        tm.update_task(task_id="task-1", status="running")

        # 查询任务
        task = tm.get_task("task-1")
    """

    VALID_STATUSES = {"pending", "running", "review", "retry", "done", "failed"}

    def __init__(self, db_path: str = "data/ai-runtime.db"):
        import os
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._create_table()

    def _create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id    TEXT PRIMARY KEY,
                agent      TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'pending',
                payload    TEXT DEFAULT '{}',
                result     TEXT DEFAULT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_status
            ON tasks(status)
            """
        )
        self._conn.commit()

    # ── 公开 API ────────────────────────────────────────────

    def create_task(
        self,
        agent: str,
        payload: Any = None,
        task_id: Optional[str] = None,
    ) -> str:
        """创建任务，默认状态 pending。返回 task_id"""
        task_id = task_id or str(uuid.uuid4())
        now = time.time()
        self._conn.execute(
            """
            INSERT INTO tasks (task_id, agent, status, payload, created_at, updated_at)
            VALUES (?, ?, 'pending', ?, ?, ?)
            """,
            (
                task_id,
                agent,
                json.dumps(payload) if not isinstance(payload, str) else payload,
                now,
                now,
            ),
        )
        self._conn.commit()
        return task_id

    def update_task(
        self,
        task_id: str,
        status: Optional[str] = None,
        payload: Optional[Any] = None,
        result: Optional[Any] = None,
    ) -> bool:
        """更新任务状态 / payload / 结果。返回是否更新成功"""
        if status and status not in self.VALID_STATUSES:
            raise ValueError(
                f"无效状态: {status}，合法值: {self.VALID_STATUSES}"
            )

        sets: List[str] = ["updated_at = ?"]
        params: List[Any] = [time.time()]

        if status:
            sets.append("status = ?")
            params.append(status)
        if payload is not None:
            sets.append("payload = ?")
            params.append(
                json.dumps(payload) if not isinstance(payload, str) else payload
            )
        if result is not None:
            sets.append("result = ?")
            params.append(
                json.dumps(result) if not isinstance(result, str) else result
            )

        params.append(task_id)
        sql = f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?"
        cur = self._conn.execute(sql, params)
        self._conn.commit()
        return cur.rowcount > 0

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """按 ID 查询任务，返回 dict 或 None"""
        row = self._conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def list_tasks(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """列出任务，可选按状态过滤"""
        if status:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY created_at DESC",
                (status,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        self._conn.close()
