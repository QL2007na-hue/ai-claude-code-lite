"""
共享内存 —— 跨 Agent 的全局键值存储，支持 TTL 与变更通知。

本模块提供 SharedMemory 类，作为多 Agent 之间的公共黑板，
Agent 可以在其上安全地读写共享状态、订阅键变更事件。

特性:
  - put / get / delete / clear / has 基础 CRUD
  - get_or_create 原子获取或初始化
  - update 合并更新
  - list_keys 前缀扫描
  - size / items 内省
  - JSON 可序列化校验
  - threading.RLock 线程安全
  - 可选 TTL 每键独立过期
  - subscribe / unsubscribe 变更通知回调

Usage:
    from runtime.context.shared_memory import SharedMemory

    mem = SharedMemory()

    # 基础操作
    mem.put("model", "deepseek-chat")
    mem.put("temperature", 0.7, ttl=300)  # 5 分钟后过期

    # 原子获取或初始化
    counter = mem.get_or_create("counter", lambda: 0)
    mem.put("counter", counter + 1)

    # 合并更新
    mem.update("config", {"timeout": 30})

    # 前缀扫描
    keys = mem.list_keys("task:")  # ["task:001", "task:002"]

    # 变更订阅
    def on_change(key, old_val, new_val):
        print(f"{key}: {old_val} -> {new_val}")

    cb_id = mem.subscribe("model", on_change)
    mem.put("model", "gpt-4")  # 触发回调
    mem.unsubscribe("model", cb_id)
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple


class SharedMemory:
    """跨 Agent 的线程安全共享内存存储。

    Parameters
    ----------
    default_ttl : float or None
        键的默认存活时间（秒）。None 表示永不过期。
        可在 put() 时对单个键覆盖。
    """

    def __init__(self, default_ttl: Optional[float] = None) -> None:
        if default_ttl is not None and default_ttl <= 0:
            raise ValueError(f"default_ttl 必须为正数或 None，收到: {default_ttl}")
        self._default_ttl = default_ttl
        self._store: Dict[str, Any] = {}
        self._expires_at: Dict[str, float] = {}
        self._subscriptions: Dict[str, List[Tuple[str, Callable[[str, Any, Any], None]]]] = {}
        self._lock = threading.RLock()

    # ── 基础 CRUD ─────────────────────────────────────────────

    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """存储一个键值对。

        Parameters
        ----------
        key : str
            键名。支持 "a.b.c" 点分命名习惯。
        value : Any
            值，必须是 JSON 可序列化类型。
        ttl : float or None
            该键的独立存活时间（秒）。None 时使用 default_ttl。
            设为 0 或负数表示永不过期。

        Raises
        ------
        ValueError
            若 value 不可 JSON 序列化。
        """
        _validate_json_serializable(value, key)
        with self._lock:
            old_value = self._store.get(key, _SENTINEL)
            self._store[key] = value
            effective_ttl = ttl if ttl is not None else self._default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                self._expires_at[key] = time.time() + effective_ttl
            elif key in self._expires_at:
                del self._expires_at[key]
            # 触发变更通知
            if old_value is not _SENTINEL or old_value != value:
                self._notify(key, old_value, value)

    def get(self, key: str, default: Any = None) -> Any:
        """读取键值。

        Parameters
        ----------
        key : str
            键名。
        default : Any
            键不存在或已过期时的默认返回值。

        Returns
        -------
        Any
            键值或 default。
        """
        with self._lock:
            if not self._is_valid(key):
                return default
            return self._store[key]

    def delete(self, key: str) -> bool:
        """删除一个键。

        Returns
        -------
        bool
            True 表示键存在并被删除，False 表示键不存在。
        """
        with self._lock:
            existed = key in self._store
            old_value = self._store.pop(key, None)
            self._expires_at.pop(key, None)
            if existed:
                self._notify(key, old_value, _SENTINEL)
            return existed

    def clear(self) -> None:
        """清空所有存储的键值和订阅。"""
        with self._lock:
            self._store.clear()
            self._expires_at.clear()
            self._subscriptions.clear()

    def has(self, key: str) -> bool:
        """检查键是否存在且未过期。

        Returns
        -------
        bool
        """
        with self._lock:
            return self._is_valid(key)

    # ── 原子操作 ──────────────────────────────────────────────

    def get_or_create(self, key: str, factory: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        """原子获取键值，若不存在则调用 factory 创建并存储。

        Parameters
        ----------
        key : str
            键名。
        factory : Callable
            无参数的可调用对象，在键缺失时用于生成初始值。
        ttl : float or None
            新键的存活时间。

        Returns
        -------
        Any
            键的当前值（已存在值或 factory 生成的值）。
        """
        with self._lock:
            if self._is_valid(key):
                return self._store[key]
            value = factory()
            _validate_json_serializable(value, key)
            self._store[key] = value
            effective_ttl = ttl if ttl is not None else self._default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                self._expires_at[key] = time.time() + effective_ttl
            self._notify(key, _SENTINEL, value)
            return value

    def update(self, key: str, partial_dict: Dict[str, Any]) -> None:
        """将 partial_dict 合并到键的已有值中。

        仅当键的值是 dict 类型时有效，否则抛 TypeError。

        Parameters
        ----------
        key : str
            键名。
        partial_dict : dict
            要合并的字典。

        Raises
        ------
        TypeError
            若键的值不是 dict。
        ValueError
            若键不存在或已过期。
        """
        if not isinstance(partial_dict, dict):
            raise TypeError(f"update 需要 dict 类型，收到: {type(partial_dict).__name__}")
        with self._lock:
            if not self._is_valid(key):
                raise ValueError(f"键 '{key}' 不存在或已过期")
            current = self._store[key]
            if not isinstance(current, dict):
                raise TypeError(
                    f"键 '{key}' 的值为 {type(current).__name__}，"
                    f"不是 dict，无法 merge"
                )
            old_value = dict(current)
            current.update(partial_dict)
            _validate_json_serializable(current, key)
            self._notify(key, old_value, current)

    # ── 前缀扫描 ──────────────────────────────────────────────

    def list_keys(self, prefix: str = "") -> List[str]:
        """列出匹配前缀的所有有效键名。

        Parameters
        ----------
        prefix : str
            前缀字符串，空字符串表示列出所有键。

        Returns
        -------
        list[str]
            匹配的键名列表（字典序排序）。
        """
        with self._lock:
            self._evict_expired()
            keys = [k for k in self._store if k.startswith(prefix)]
            keys.sort()
            return keys

    # ── 内省 ──────────────────────────────────────────────────

    @property
    def size(self) -> int:
        """当前有效键的数量。"""
        with self._lock:
            self._evict_expired()
            return len(self._store)

    def items(self) -> Iterator[Tuple[str, Any]]:
        """返回所有有效键值对的迭代器（快照视图）。"""
        with self._lock:
            self._evict_expired()
            snapshot = list(self._store.items())
        return iter(snapshot)

    # ── 变更订阅 ──────────────────────────────────────────────

    def subscribe(self, key: str, callback: Callable[[str, Any, Any], None]) -> str:
        """订阅某个键的变更通知。

        当该键的值通过 put / update / get_or_create / delete 发生变化时，
        回调将被调用。

        Parameters
        ----------
        key : str
            要订阅的键名。
        callback : Callable
            回调函数，签名为 (key: str, old_value: Any, new_value: Any) -> None。
            delete 时 new_value 为特殊的 SENTINEL 值（可用 isinstance 判断）。

        Returns
        -------
        str
            订阅 ID，用于取消订阅。
        """
        callback_id = str(uuid.uuid4())
        with self._lock:
            if key not in self._subscriptions:
                self._subscriptions[key] = []
            self._subscriptions[key].append((callback_id, callback))
        return callback_id

    def unsubscribe(self, key: str, callback_id: str) -> bool:
        """取消某个键的变更订阅。

        Parameters
        ----------
        key : str
            键名。
        callback_id : str
            subscribe() 返回的订阅 ID。

        Returns
        -------
        bool
            True 表示成功取消，False 表示未找到该订阅。
        """
        with self._lock:
            subs = self._subscriptions.get(key, [])
            for i, (cb_id, _cb) in enumerate(subs):
                if cb_id == callback_id:
                    subs.pop(i)
                    if not subs:
                        del self._subscriptions[key]
                    return True
            return False

    # ── 工具方法 ──────────────────────────────────────────────

    def cleanup(self) -> int:
        """强制清理所有已过期的键。

        Returns
        -------
        int
            被清理的键数量。
        """
        with self._lock:
            before = len(self._store)
            self._evict_expired()
            return before - len(self._store)

    def ttl(self, key: str) -> Optional[float]:
        """查询某个键的剩余存活时间（秒）。

        Returns
        -------
        float or None
            剩余秒数。None 表示永不过期或键不存在。
        """
        with self._lock:
            if key not in self._store:
                return None
            expires = self._expires_at.get(key)
            if expires is None:
                return None
            remaining = expires - time.time()
            return max(0.0, remaining)

    # ── 内部实现 ──────────────────────────────────────────────

    def _is_valid(self, key: str) -> bool:
        """检查键是否存在且未过期。"""
        if key not in self._store:
            return False
        expires = self._expires_at.get(key)
        if expires is not None and time.time() >= expires:
            self._evict_key(key)
            return False
        return True

    def _evict_key(self, key: str) -> None:
        """清理单个过期键。"""
        old_value = self._store.pop(key, None)
        self._expires_at.pop(key, None)
        if old_value is not None:
            self._notify(key, old_value, _SENTINEL)

    def _evict_expired(self) -> None:
        """批量清理所有已过期的键。"""
        now = time.time()
        expired = [
            k for k, exp in self._expires_at.items()
            if now >= exp
        ]
        for k in expired:
            self._evict_key(k)

    def _notify(self, key: str, old_value: Any, new_value: Any) -> None:
        """触发键变更通知回调。"""
        subs = self._subscriptions.get(key)
        if not subs:
            return
        # 锁内直接调用回调（注意：回调不应长时间阻塞）
        for _cb_id, callback in subs:
            try:
                callback(key, old_value, new_value)
            except Exception:
                pass  # 回调异常不影响主流程


# ── 内部工具 ───────────────────────────────────────────────────

class _Sentinel:
    """哨兵值，用于区分"键不存在"和"值为 None"。"""
    def __repr__(self) -> str:
        return "<SENTINEL>"


_SENTINEL = _Sentinel()


_JSON_PRIMITIVES = (dict, list, str, int, float, bool, type(None))


def _validate_json_serializable(value: Any, key: str) -> None:
    """验证 value 可否安全 JSON 序列化（仅接受基础类型 + 嵌套容器）。"""
    if not isinstance(value, _JSON_PRIMITIVES):
        raise ValueError(
            f"键 '{key}' 的值类型 {type(value).__name__} 不可 JSON 序列化。"
            f"仅接受: dict, list, str, int, float, bool, None"
        )
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError(
                    f"键 '{key}' 的子键 '{k}' 类型 {type(k).__name__} 不是字符串，"
                    f"不可 JSON 序列化"
                )
            _validate_json_serializable(v, f"{key}.{k}")
    elif isinstance(value, list):
        for i, item in enumerate(value):
            _validate_json_serializable(item, f"{key}[{i}]")
