"""
任务上下文管理器 —— 为每个 task_id 提供独立的键值存储，支持自动过期。

本模块提供 TaskContext 类，用于在多 Agent 协作期间为每个任务维护
独立的上下文状态。Agent 可以通过 set/get 读写任务级上下文，
并在任务完成后通过 delete 清理。

特性:
  - 按 task_id 分区的键值存储
  - 可配置 TTL 自动过期（默认 3600 秒）
  - threading.RLock 保证线程安全
  - snapshot / merge / list_tasks 便捷方法

Usage:
    from runtime.context.task_context import TaskContext

    ctx = TaskContext(ttl=7200)

    # 存储任务上下文
    ctx.set("task-001", "goal", "写一个贪吃蛇游戏")
    ctx.set("task-001", "language", "Python")

    # 读取
    goal = ctx.get("task-001", "goal")  # "写一个贪吃蛇游戏"

    # 快照
    snap = ctx.snapshot("task-001")
    # {"goal": "写一个贪吃蛇游戏", "language": "Python"}

    # 批量合并
    ctx.merge("task-001", {"framework": "pygame", "target": "Windows"})

    # 列出所有任务
    tasks = ctx.list_tasks()  # ["task-001"]

    # 清理
    ctx.delete("task-001")
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional


class TaskContext:
    """按 task_id 分区的线程安全键值存储，支持 TTL 自动过期。

    Parameters
    ----------
    ttl : float
        每个任务上下文的默认存活时间（秒）。默认为 3600。
        过期条目在下次访问时惰性清理。
    """

    def __init__(self, ttl: float = 3600.0) -> None:
        if ttl <= 0:
            raise ValueError(f"ttl 必须为正数，收到: {ttl}")
        self._ttl = ttl
        self._store: Dict[str, Dict[str, Any]] = {}
        self._created_at: Dict[str, float] = {}
        self._lock = threading.RLock()

    # ── 核心操作 ──────────────────────────────────────────────

    def set(self, task_id: str, key: str, value: Any) -> None:
        """为指定任务设置键值。

        Parameters
        ----------
        task_id : str
            任务唯一标识。
        key : str
            键名。
        value : Any
            键值（任意 JSON 可序列化类型）。
        """
        with self._lock:
            self._ensure_task(task_id)
            self._store[task_id][key] = value

    def get(self, task_id: str, key: str, default: Any = None) -> Any:
        """读取指定任务中某个键的值。

        Parameters
        ----------
        task_id : str
            任务唯一标识。
        key : str
            键名。
        default : Any
            键不存在或上下文已过期时的默认返回值。

        Returns
        -------
        Any
            键值或 default。
        """
        with self._lock:
            if not self._is_valid(task_id):
                return default
            return self._store.get(task_id, {}).get(key, default)

    def delete(self, task_id: str) -> bool:
        """删除指定任务的所有上下文。

        Parameters
        ----------
        task_id : str
            任务唯一标识。

        Returns
        -------
        bool
            True 表示成功删除，False 表示任务不存在。
        """
        with self._lock:
            existed = task_id in self._store
            self._store.pop(task_id, None)
            self._created_at.pop(task_id, None)
            return existed

    # ── 批量操作 ──────────────────────────────────────────────

    def snapshot(self, task_id: str) -> Dict[str, Any]:
        """返回指定任务的完整上下文快照（浅拷贝）。

        Parameters
        ----------
        task_id : str
            任务唯一标识。

        Returns
        -------
        dict
            任务的所有键值对。若任务不存在或已过期，返回空字典。
        """
        with self._lock:
            if not self._is_valid(task_id):
                return {}
            return dict(self._store.get(task_id, {}))

    def merge(self, task_id: str, data: Dict[str, Any]) -> None:
        """将字典数据批量合并到指定任务的上下文。

        已存在的键会被覆盖，不存在的键会被添加。

        Parameters
        ----------
        task_id : str
            任务唯一标识。
        data : dict
            要合并的键值对字典。
        """
        if not isinstance(data, dict):
            raise TypeError(f"merge 需要 dict 类型，收到: {type(data).__name__}")
        with self._lock:
            self._ensure_task(task_id)
            self._store[task_id].update(data)

    def list_tasks(self) -> List[str]:
        """列出所有仍有上下文的任务 ID。

        惰性清理已过期的任务。

        Returns
        -------
        list[str]
            当前有效的任务 ID 列表（按创建时间排序）。
        """
        with self._lock:
            self._evict_expired()
            # 按创建时间排序
            sorted_ids = sorted(
                self._store.keys(),
                key=lambda tid: self._created_at.get(tid, 0.0),
            )
            return sorted_ids

    # ── 工具方法 ──────────────────────────────────────────────

    def count(self) -> int:
        """返回当前有效的任务上下文数量。

        Returns
        -------
        int
        """
        with self._lock:
            self._evict_expired()
            return len(self._store)

    def exists(self, task_id: str) -> bool:
        """检查指定任务是否存在且未过期。

        Returns
        -------
        bool
        """
        with self._lock:
            return self._is_valid(task_id)

    def clear(self) -> None:
        """清空所有任务上下文。"""
        with self._lock:
            self._store.clear()
            self._created_at.clear()

    def cleanup(self) -> int:
        """强制清理所有已过期的任务上下文。

        Returns
        -------
        int
            被清理的任务数量。
        """
        with self._lock:
            before = len(self._store)
            self._evict_expired()
            after = len(self._store)
            return before - after

    # ── 内部实现 ──────────────────────────────────────────────

    def _ensure_task(self, task_id: str) -> None:
        """确保任务上下文存在，若不存在则初始化。"""
        if task_id not in self._store:
            self._store[task_id] = {}
            self._created_at[task_id] = time.time()

    def _is_valid(self, task_id: str) -> bool:
        """检查任务上下文是否存在且未过期。"""
        if task_id not in self._store:
            return False
        created = self._created_at.get(task_id, 0.0)
        if time.time() - created > self._ttl:
            # 惰性清理过期条目
            self._store.pop(task_id, None)
            self._created_at.pop(task_id, None)
            return False
        return True

    def _evict_expired(self) -> None:
        """批量清理所有已过期的任务上下文。"""
        now = time.time()
        expired_ids = [
            tid
            for tid, created in self._created_at.items()
            if now - created > self._ttl
        ]
        for tid in expired_ids:
            self._store.pop(tid, None)
            self._created_at.pop(tid, None)
