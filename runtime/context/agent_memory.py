"""
Agent 记忆系统 —— 每个 Agent 的独立记忆空间，支持短期/长期记忆分离。

本模块提供 AgentMemory 类，用于为每个 Agent 维护独立的知识库。
短期记忆有固定 TTL（默认 300 秒），长期记忆永久保存。
支持 consolidate() 将短期记忆提升为长期记忆，
以及 search() 跨 Agent 模糊搜索。

特性:
  - remember / recall / forget 基础记忆操作
  - 短期记忆（short_term）默认 TTL 300s，长期记忆（long_term）持久
  - consolidate() 将短期记忆提升为长期记忆
  - agent_knowledge() 获取 Agent 全部记忆
  - search() 跨 Agent 模糊搜索（基于子串匹配 + Levenshtein 相似度）
  - JSON 可序列化 + 容量限制
  - threading.RLock 线程安全

Usage:
    from runtime.context.agent_memory import AgentMemory

    mem = AgentMemory()

    # 存储短期记忆
    mem.remember("planner", "last_goal", "写一个贪吃蛇游戏")

    # 存储长期记忆（ttl=None）
    mem.remember("planner", "preferred_language", "Python", ttl=None)

    # 读取
    goal = mem.recall("planner", "last_goal")

    # 模糊搜索
    results = mem.search("贪吃蛇", threshold=0.3)
    # [{"agent": "planner", "key": "last_goal", "value": "写一个贪吃蛇游戏", "score": 0.75}]

    # 合并短期记忆到长期
    mem.consolidate("planner")

    # 列出 Agent 所有记忆
    all_knowledge = mem.agent_knowledge("planner")
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


# 尝试导入第三方 Levenshtein，否则回退到纯 Python 实现
try:
    from Levenshtein import ratio as _lev_ratio
except ImportError:
    def _lev_ratio(a: str, b: str) -> float:
        """纯 Python Levenshtein 距离比率。"""
        if not a and not b:
            return 1.0
        if not a or not b:
            return 0.0
        len_a, len_b = len(a), len(b)
        # 动态规划矩阵只用两行
        prev = list(range(len_b + 1))
        curr = [0] * (len_b + 1)
        for i in range(1, len_a + 1):
            curr[0] = i
            for j in range(1, len_b + 1):
                cost = 0 if a[i - 1] == b[j - 1] else 1
                curr[j] = min(
                    prev[j] + 1,        # 删除
                    curr[j - 1] + 1,    # 插入
                    prev[j - 1] + cost, # 替换
                )
            prev, curr = curr, prev
        distance = prev[len_b]
        max_len = max(len_a, len_b)
        return 1.0 - distance / max_len


# 内部哨兵值，用于区分"用户未传 ttl"和"用户显式传 ttl=None"
_TTL_NOT_SET = object()


class AgentMemory:
    """为每个 Agent 维护短期/长期记忆。

    Parameters
    ----------
    short_term_ttl : float
        短期记忆的默认存活时间（秒）。默认为 300。
        None 表示短期记忆也永久保存。
    long_term_ttl : float or None
        长期记忆的存活时间。默认为 None（永久）。
    max_facts_per_agent : int or None
        每个 Agent 的最大记忆条数。None 表示不限制。
        超出时自动淘汰最旧的短期记忆。
    """

    def __init__(
        self,
        short_term_ttl: Optional[float] = 300.0,
        long_term_ttl: Optional[float] = None,
        max_facts_per_agent: Optional[int] = None,
    ) -> None:
        if short_term_ttl is not None and short_term_ttl <= 0:
            raise ValueError(f"short_term_ttl 必须为正数或 None，收到: {short_term_ttl}")
        self._short_term_ttl = short_term_ttl
        self._long_term_ttl = long_term_ttl
        self._max_facts = max_facts_per_agent
        # 数据结构: _store[agent_name] = { key: _MemoryEntry }
        self._store: Dict[str, Dict[str, _MemoryEntry]] = {}
        self._lock = threading.RLock()

    # ── 核心操作 ──────────────────────────────────────────────

    def remember(
        self,
        agent_name: str,
        key: str,
        value: Any,
        ttl: Any = _TTL_NOT_SET,
        memory_type: str = "auto",
    ) -> None:
        """为指定 Agent 存储一条记忆（事实）。

        Parameters
        ----------
        agent_name : str
            Agent 名称（如 "planner", "coder", "reviewer"）。
        key : str
            记忆键名。
        value : Any
            记忆内容，须 JSON 可序列化。
        ttl : float, None, or omit
            存活时间（秒）。
            - 不传：默认短期记忆（使用 short_term_ttl）。
            - 传入正数：使用指定 TTL 作为短期记忆。
            - 显式传入 None：长期记忆（永不过期）。
        memory_type : str
            "short_term"、"long_term" 或 "auto"（默认）。
            "auto" 时：ttl=None 则为长期，否则为短期。
        """
        _validate_json_serializable(value, key)
        with self._lock:
            agent_store = self._ensure_agent(agent_name)
            now = time.time()

            # 决定记忆类型和过期时间
            effective_ttl: Optional[float]
            is_long_term: bool

            if memory_type == "long_term":
                effective_ttl = self._long_term_ttl
                is_long_term = True
            elif memory_type == "short_term":
                effective_ttl = ttl if ttl is not _TTL_NOT_SET else self._short_term_ttl
                is_long_term = False
            else:  # auto
                if ttl is _TTL_NOT_SET:
                    # 用户未传 ttl → 默认短期记忆
                    effective_ttl = self._short_term_ttl
                    is_long_term = False
                elif ttl is None:
                    # 用户显式传 None → 长期记忆
                    effective_ttl = self._long_term_ttl
                    is_long_term = True
                else:
                    # 用户传了正数 → 短期记忆（自定义 TTL）
                    effective_ttl = ttl
                    is_long_term = False

            expires_at: Optional[float] = None
            if effective_ttl is not None and effective_ttl > 0:
                expires_at = now + effective_ttl

            entry = _MemoryEntry(
                key=key,
                value=value,
                created_at=now,
                expires_at=expires_at,
                is_long_term=is_long_term,
            )
            agent_store[key] = entry

            # 容量限制：淘汰最旧的短期记忆
            if self._max_facts is not None and len(agent_store) > self._max_facts:
                self._evict_oldest_short_term(agent_name)

    def recall(self, agent_name: str, key: str, default: Any = None) -> Any:
        """检索 Agent 的某条记忆。

        Parameters
        ----------
        agent_name : str
            Agent 名称。
        key : str
            记忆键名。
        default : Any
            记忆不存在或已过期时的默认值。

        Returns
        -------
        Any
            记忆内容或 default。
        """
        with self._lock:
            if agent_name not in self._store:
                return default
            entry = self._store[agent_name].get(key)
            if entry is None:
                return default
            if entry.is_expired():
                del self._store[agent_name][key]
                return default
            return entry.value

    def forget(self, agent_name: str, key: str) -> bool:
        """删除 Agent 的某条记忆。

        Returns
        -------
        bool
            True 表示成功删除，False 表示记忆不存在。
        """
        with self._lock:
            if agent_name not in self._store:
                return False
            existed = key in self._store[agent_name]
            self._store[agent_name].pop(key, None)
            return existed

    # ── 查询方法 ──────────────────────────────────────────────

    def agent_knowledge(
        self, agent_name: str, include_expired: bool = False
    ) -> Dict[str, Any]:
        """获取 Agent 的所有有效记忆。

        Parameters
        ----------
        agent_name : str
            Agent 名称。
        include_expired : bool
            是否包含已过期但尚未清理的记忆。

        Returns
        -------
        dict
            {key: value} 快照。
        """
        with self._lock:
            if agent_name not in self._store:
                return {}
            result: Dict[str, Any] = {}
            expired_keys: List[str] = []
            for key, entry in self._store[agent_name].items():
                if entry.is_expired():
                    expired_keys.append(key)
                    if include_expired:
                        result[key] = entry.value
                else:
                    result[key] = entry.value
            # 清理过期条目
            for k in expired_keys:
                self._store[agent_name].pop(k, None)
            return result

    def search(
        self,
        query: str,
        threshold: float = 0.3,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """跨所有 Agent 模糊搜索记忆。

        搜索策略:
        1. 优先子串匹配（精确包含 query），score = 1.0
        2. 其次 Levenshtein 相似度匹配

        Parameters
        ----------
        query : str
            搜索查询字符串。
        threshold : float
            最低相似度阈值（0.0 ~ 1.0），低于此值的结果将被过滤。
        top_k : int
            返回的最大结果数。

        Returns
        -------
        list[dict]
            结果列表，每项包含 agent / key / value / score / is_long_term。
            按 score 降序排列。
        """
        with self._lock:
            self._evict_all_expired()
            candidates: List[Tuple[float, str, str, Any, bool]] = []
            query_lower = query.lower()

            for agent_name, entries in self._store.items():
                for key, entry in entries.items():
                    # 将 key 和 value 都转成字符串用于匹配
                    key_str = str(key).lower()
                    val_str = str(entry.value).lower()

                    # 精确子串匹配
                    if query_lower in key_str or query_lower in val_str:
                        candidates.append((1.0, agent_name, key, entry.value, entry.is_long_term))
                        continue

                    # Levenshtein 相似度取 key/value 中较大的
                    key_score = _lev_ratio(query_lower, key_str)
                    val_score = _lev_ratio(query_lower, val_str)
                    best_score = max(key_score, val_score)
                    if best_score >= threshold:
                        candidates.append((
                            best_score, agent_name, key, entry.value, entry.is_long_term,
                        ))

            # 按分数降序排序
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[:top_k]

            return [
                {
                    "agent": agent,
                    "key": key,
                    "value": value,
                    "score": round(score, 4),
                    "is_long_term": is_lt,
                }
                for score, agent, key, value, is_lt in top
            ]

    # ── 记忆管理 ──────────────────────────────────────────────

    def consolidate(self, agent_name: str) -> int:
        """将 Agent 的短期记忆合并为长期记忆。

        所有非长期记忆的过期时间将被清除（设为 None），
        标记为长期记忆。

        Parameters
        ----------
        agent_name : str
            Agent 名称。

        Returns
        -------
        int
            被合并的记忆条数。
        """
        with self._lock:
            if agent_name not in self._store:
                return 0
            count = 0
            for entry in self._store[agent_name].values():
                if not entry.is_long_term:
                    entry.expires_at = None
                    entry.is_long_term = True
                    count += 1
            return count

    def forget_all(self, agent_name: str) -> int:
        """删除 Agent 的所有记忆。

        Returns
        -------
        int
            被删除的记忆条数。
        """
        with self._lock:
            if agent_name not in self._store:
                return 0
            count = len(self._store[agent_name])
            del self._store[agent_name]
            return count

    def list_agents(self) -> List[str]:
        """列出所有有记忆的 Agent 名称。

        Returns
        -------
        list[str]
        """
        with self._lock:
            return sorted(self._store.keys())

    def memory_count(self, agent_name: Optional[str] = None) -> int:
        """获取记忆总数（可按 Agent 过滤）。

        Parameters
        ----------
        agent_name : str or None
            Agent 名称，None 表示全部 Agent。

        Returns
        -------
        int
        """
        with self._lock:
            self._evict_all_expired()
            if agent_name:
                return len(self._store.get(agent_name, {}))
            return sum(len(e) for e in self._store.values())

    def cleanup(self) -> int:
        """强制清理所有 Agent 的过期记忆。

        Returns
        -------
        int
            被清理的记忆条数。
        """
        with self._lock:
            before = sum(len(e) for e in self._store.values())
            self._evict_all_expired()
            after = sum(len(e) for e in self._store.values())
            return before - after

    # ── 导出 / 导入 ───────────────────────────────────────────

    def export(self) -> Dict[str, Any]:
        """导出所有 Agent 记忆为可序列化字典。

        Returns
        -------
        dict
        """
        with self._lock:
            self._evict_all_expired()
            result: Dict[str, Any] = {}
            for agent_name, entries in self._store.items():
                result[agent_name] = {}
                for key, entry in entries.items():
                    result[agent_name][key] = entry.to_dict()
            return result

    def import_data(self, data: Dict[str, Any]) -> None:
        """从 export() 导出的数据恢复记忆。

        Parameters
        ----------
        data : dict
            export() 返回的字典。
        """
        with self._lock:
            for agent_name, agent_data in data.items():
                agent_store = self._ensure_agent(agent_name)
                for key, entry_dict in agent_data.items():
                    entry = _MemoryEntry.from_dict(entry_dict)
                    if not entry.is_expired():
                        agent_store[key] = entry

    # ── 内部实现 ──────────────────────────────────────────────

    def _ensure_agent(self, agent_name: str) -> Dict[str, _MemoryEntry]:
        if agent_name not in self._store:
            self._store[agent_name] = {}
        return self._store[agent_name]

    def _evict_all_expired(self) -> None:
        for agent_name in list(self._store.keys()):
            entries = self._store[agent_name]
            expired = [k for k, e in entries.items() if e.is_expired()]
            for k in expired:
                del entries[k]
            if not entries:
                del self._store[agent_name]

    def _evict_oldest_short_term(self, agent_name: str) -> None:
        """淘汰 Agent 最旧的一条短期记忆。"""
        entries = self._store.get(agent_name, {})
        oldest_key: Optional[str] = None
        oldest_time = float("inf")
        for key, entry in entries.items():
            if not entry.is_long_term and entry.created_at < oldest_time:
                oldest_time = entry.created_at
                oldest_key = key
        if oldest_key is not None:
            del entries[oldest_key]


# ── 内部数据类 ────────────────────────────────────────────────

class _MemoryEntry:
    """单条记忆的内部表示。"""

    __slots__ = ("key", "value", "created_at", "expires_at", "is_long_term")

    def __init__(
        self,
        key: str,
        value: Any,
        created_at: float,
        expires_at: Optional[float],
        is_long_term: bool,
    ) -> None:
        self.key = key
        self.value = value
        self.created_at = created_at
        self.expires_at = expires_at
        self.is_long_term = is_long_term

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() >= self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "is_long_term": self.is_long_term,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "_MemoryEntry":
        return cls(
            key=data["key"],
            value=data["value"],
            created_at=data["created_at"],
            expires_at=data.get("expires_at"),
            is_long_term=data.get("is_long_term", False),
        )


def _validate_json_serializable(value: Any, key: str) -> None:
    try:
        json.dumps(value, default=str)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"记忆键 '{key}' 的值不可 JSON 序列化: {e}"
        ) from e
