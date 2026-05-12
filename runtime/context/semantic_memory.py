"""
语义记忆 —— 基于向量嵌入的长期语义存储与检索。

本模块提供 SemanticMemory 类，用于存储和检索语义事实。
默认使用基于关键词提取的 TF-IDF 风格嵌入（零依赖），
可选配置 OpenAI / 兼容 API 的 embedding 提供商以获取更高质量的语义表示。

特性:
  - embed(text) 生成文本的嵌入向量
  - store(text, metadata) 存储语义事实
  - search(query, top_k) 余弦相似度语义搜索
  - retrieve(context, top_k) 为给定上下文检索相关记忆
  - forget(id) 删除特定记忆
  - persist(path) / load(path) 保存/加载到 JSON
  - 默认使用关键短语提取（词频加权）作为 fallback
  - 可选：通过 provider 使用外部 embedding API

Usage:
    from runtime.context.semantic_memory import SemanticMemory

    mem = SemanticMemory()

    # 存储事实
    mem.store("Python 是动态类型语言", {"category": "language", "source": "docs"})
    mem.store("贪吃蛇游戏用 pygame 库开发", {"category": "project"})

    # 语义搜索
    results = mem.search("游戏开发用什么库", top_k=5)
    # [{"text": "贪吃蛇游戏用 pygame 库开发", "metadata": {...}, "score": 0.85}, ...]

    # 上下文检索
    facts = mem.retrieve("我要写一个游戏", top_k=3)

    # 持久化
    mem.persist("data/semantic_memory.json")
    mem.load("data/semantic_memory.json")
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
import uuid
from collections import Counter
from typing import Any, Callable, Dict, List, Optional


class SemanticMemory:
    """基于向量嵌入的语义记忆存储与检索系统。

    Parameters
    ----------
    embedding_provider : Callable or None
        可选的 embedding 函数，签名为 (text: str) -> List[float]。
        若为 None，使用内置的关键词提取（词频向量）。
    embedding_dim : int
        嵌入向量维度。仅当未提供 provider 时生效（关键词模式下自动调整）。
    stop_words : set or None
        自定义停用词集合。None 使用内置中文+英文停用词。
    """

    # 内置停用词（中文 + 英文常见词）
    _DEFAULT_STOP_WORDS: set = {
        # 中文
        "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一",
        "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会", "着",
        "没有", "看", "好", "自己", "这", "他", "她", "它", "们", "那", "些",
        "什么", "怎么", "如果", "因为", "所以", "但是", "然后", "可以", "这个",
        "那个", "已经", "还是", "或者", "应该", "可能", "需要", "能够", "已经",
        "把", "被", "让", "给", "从", "对", "与", "为", "以", "及", "或",
        # 英文
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "shall",
        "should", "may", "might", "must", "can", "could", "i", "you", "he",
        "she", "it", "we", "they", "me", "him", "her", "us", "them", "my",
        "your", "his", "its", "our", "their", "this", "that", "these", "those",
        "and", "but", "or", "nor", "not", "so", "if", "then", "else", "when",
        "where", "why", "how", "all", "each", "every", "both", "few", "more",
        "most", "other", "some", "such", "no", "only", "own", "same", "too",
        "very", "just", "about", "above", "after", "again", "against", "below",
        "between", "during", "into", "through", "under", "with", "without",
        "of", "to", "in", "for", "on", "at", "by", "from", "as", "up", "out",
    }

    def __init__(
        self,
        embedding_provider: Optional[Callable[[str], List[float]]] = None,
        embedding_dim: int = 256,
        stop_words: Optional[set] = None,
    ) -> None:
        self._provider = embedding_provider
        self._embedding_dim = embedding_dim
        self._stop_words = stop_words if stop_words is not None else self._DEFAULT_STOP_WORDS

        # 存储结构: _entries[id] = {"text": str, "embedding": list, "metadata": dict, "created_at": float}
        self._entries: Dict[str, Dict[str, Any]] = {}
        # 关键词→文档频率（用于 IDF 计算）
        self._doc_freq: Dict[str, int] = Counter()
        self._total_docs = 0
        self._lock = threading.RLock()

    # ── 嵌入 ──────────────────────────────────────────────────

    def embed(self, text: str) -> List[float]:
        """将文本转换为嵌入向量。

        若提供了 embedding_provider，直接使用它；
        否则使用内置的关键词提取 + 词频向量（词袋模型）。

        Parameters
        ----------
        text : str
            输入文本。

        Returns
        -------
        list[float]
            嵌入向量。
        """
        if not text or not isinstance(text, str):
            return [0.0] * self._embedding_dim

        if self._provider:
            try:
                vec = self._provider(text)
                if not isinstance(vec, list):
                    raise TypeError("embedding_provider 必须返回 list[float]")
                return [float(v) for v in vec]
            except Exception:
                pass  # fallback 到关键词提取

        return self._keyword_embed(text)

    def _keyword_embed(self, text: str) -> List[float]:
        """基于关键词提取生成嵌入向量（简化的 TF-IDF）。

        使用哈希技巧将关键词映射到固定维度的向量空间。
        """
        tokens = self._tokenize(text)
        if not tokens:
            return [0.0] * self._embedding_dim

        # 计算词频
        tf = Counter(tokens)
        total = sum(tf.values())

        # 构建稀疏向量，使用哈希映射到固定维度
        vec = [0.0] * self._embedding_dim
        for word, count in tf.items():
            # 使用 Python 内置 hash 映射到维度空间
            idx = abs(hash(word)) % self._embedding_dim
            # TF 归一化
            vec[idx] += count / total

        # L2 归一化
        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]

        return vec

    # ── 存储 ──────────────────────────────────────────────────

    def store(
        self,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
        memory_id: Optional[str] = None,
    ) -> str:
        """存储一条语义记忆。

        Parameters
        ----------
        text : str
            要记忆的文本事实。
        metadata : dict or None
            附加元数据（如来源、时间、类别）。
        memory_id : str or None
            自定义记忆 ID，None 时自动生成 UUID。

        Returns
        -------
        str
            记忆 ID。
        """
        if not text or not isinstance(text, str):
            raise ValueError("text 必须为非空字符串")

        memory_id = memory_id or str(uuid.uuid4())
        embedding = self.embed(text)
        now = time.time()

        with self._lock:
            self._entries[memory_id] = {
                "text": text,
                "embedding": embedding,
                "metadata": metadata or {},
                "created_at": now,
            }

            # 更新文档频率
            tokens = set(self._tokenize(text))
            for token in tokens:
                self._doc_freq[token] = self._doc_freq.get(token, 0) + 1
            self._total_docs += 1

        return memory_id

    # ── 搜索 ──────────────────────────────────────────────────

    def search(
        self,
        query: str,
        top_k: int = 10,
        threshold: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """语义搜索最匹配的记忆。

        使用余弦相似度计算查询向量与所有记忆向量的相似度。

        Parameters
        ----------
        query : str
            查询文本。
        top_k : int
            返回的最大结果数。
        threshold : float
            最低相似度阈值（0.0 ~ 1.0），低于此值的结果将被过滤。

        Returns
        -------
        list[dict]
            结果列表，每项包含 id / text / metadata / score / created_at。
            按 score 降序排列。
        """
        query_vec = self.embed(query)

        with self._lock:
            results = []
            for mem_id, entry in self._entries.items():
                score = self._cosine_similarity(query_vec, entry["embedding"])
                if score >= threshold:
                    results.append({
                        "id": mem_id,
                        "text": entry["text"],
                        "metadata": entry["metadata"],
                        "score": round(score, 4),
                        "created_at": entry["created_at"],
                    })

            results.sort(key=lambda x: x["score"], reverse=True)
            return results[:top_k]

    def retrieve(self, context: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """为给定上下文检索最相关的记忆。

        这是 search() 的别名，语义上强调"为上下文检索"。

        Parameters
        ----------
        context : str
            上下文描述文本。
        top_k : int
            返回的最大结果数。

        Returns
        -------
        list[dict]
            与 search() 相同格式的结果列表。
        """
        return self.search(context, top_k=top_k)

    # ── 管理 ──────────────────────────────────────────────────

    def forget(self, memory_id: str) -> bool:
        """删除一条记忆。

        Returns
        -------
        bool
            True 表示成功删除，False 表示记忆不存在。
        """
        with self._lock:
            if memory_id not in self._entries:
                return False
            entry = self._entries.pop(memory_id)
            # 更新文档频率
            tokens = set(self._tokenize(entry["text"]))
            for token in tokens:
                if token in self._doc_freq:
                    self._doc_freq[token] -= 1
                    if self._doc_freq[token] <= 0:
                        del self._doc_freq[token]
            self._total_docs = max(0, self._total_docs - 1)
            return True

    @property
    def memory_count(self) -> int:
        """当前存储的记忆数量。"""
        with self._lock:
            return len(self._entries)

    def list_memories(self) -> List[Dict[str, Any]]:
        """列出所有记忆的摘要（不含嵌入向量）。

        Returns
        -------
        list[dict]
        """
        with self._lock:
            return [
                {
                    "id": mem_id,
                    "text": entry["text"],
                    "metadata": entry["metadata"],
                    "created_at": entry["created_at"],
                }
                for mem_id, entry in sorted(
                    self._entries.items(),
                    key=lambda x: x[1]["created_at"],
                )
            ]

    def clear(self) -> None:
        """清空所有记忆。"""
        with self._lock:
            self._entries.clear()
            self._doc_freq.clear()
            self._total_docs = 0

    # ── 持久化 ────────────────────────────────────────────────

    def persist(self, path: str) -> None:
        """将记忆保存到 JSON 文件。

        Parameters
        ----------
        path : str
            文件路径。
        """
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with self._lock:
            data = {
                "entries": {
                    mem_id: {
                        "text": entry["text"],
                        "embedding": entry["embedding"],
                        "metadata": entry["metadata"],
                        "created_at": entry["created_at"],
                    }
                    for mem_id, entry in self._entries.items()
                },
                "doc_freq": dict(self._doc_freq),
                "total_docs": self._total_docs,
                "saved_at": time.time(),
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    def load(self, path: str) -> int:
        """从 JSON 文件加载记忆。

        Parameters
        ----------
        path : str
            文件路径。

        Returns
        -------
        int
            加载的记忆数量。
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"文件不存在: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        with self._lock:
            loaded = 0
            for mem_id, entry_data in data.get("entries", {}).items():
                if mem_id not in self._entries:
                    self._entries[mem_id] = {
                        "text": entry_data["text"],
                        "embedding": entry_data["embedding"],
                        "metadata": entry_data.get("metadata", {}),
                        "created_at": entry_data.get("created_at", time.time()),
                    }
                    loaded += 1

            # 恢复文档频率
            saved_df = data.get("doc_freq", {})
            if saved_df:
                for token, count in saved_df.items():
                    self._doc_freq[token] = self._doc_freq.get(token, 0) + count
                self._total_docs += data.get("total_docs", loaded)

        return loaded

    # ── 内部实现 ──────────────────────────────────────────────

    def _tokenize(self, text: str) -> List[str]:
        """中文+英文混合分词。

        - 英文按非字母字符分割，转为小写
        - 中文按单字切分（简单 unigram），也可提取连续中文字段
        - 过滤停用词和单字符
        """
        tokens: List[str] = []

        # 提取英文/数字单词
        eng_words = re.findall(r"[a-zA-Z0-9]+", text.lower())
        for w in eng_words:
            if w not in self._stop_words and len(w) > 1:
                tokens.append(w)

        # 提取中文字段
        chinese_chars = re.findall(r"[\u4e00-\u9fff]+", text)
        for segment in chinese_chars:
            if len(segment) == 1:
                if segment not in self._stop_words:
                    tokens.append(segment)
            else:
                # 使用 2-gram 滑动窗口捕获词组
                for i in range(len(segment)):
                    if segment[i] not in self._stop_words:
                        tokens.append(segment[i])
                    if i < len(segment) - 1:
                        bigram = segment[i : i + 2]
                        tokens.append(bigram)
                # 也添加整个连续片段
                if len(segment) > 2 and segment not in self._stop_words:
                    tokens.append(segment)

        return tokens

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        """计算两个向量的余弦相似度。

        Returns
        -------
        float
            相似度分数（0.0 ~ 1.0）。
        """
        if len(a) != len(b):
            return 0.0

        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(y * y for y in b))

        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0

        return dot / (norm_a * norm_b)
