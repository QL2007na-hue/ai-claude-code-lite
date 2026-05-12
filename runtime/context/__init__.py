"""
Runtime Context System —— 多 Agent 运行时上下文子系统。

本包提供 AI Runtime 中所有与上下文、记忆、状态管理相关的核心组件：

模块概览:
  - TaskContext      : 按任务分区的键值存储，支持 TTL 自动过期
  - SharedMemory     : 跨 Agent 共享内存，支持 TTL、前缀扫描、变更订阅
  - AgentMemory      : 每个 Agent 的独立记忆空间，短期/长期记忆分离
  - EventHistory     : 事件时间线记录与上下文重建，环形缓冲 + 可选 SQLite
  - SemanticMemory   : 基于向量的语义记忆，关键词提取 fallback + 可选 embedding API

Usage:
    from runtime.context import (
        TaskContext,
        SharedMemory,
        AgentMemory,
        EventHistory,
        SemanticMemory,
    )

    # 任务上下文
    tctx = TaskContext(ttl=3600)
    tctx.set("task-001", "goal", "写一个游戏")

    # 共享内存
    shm = SharedMemory()
    shm.put("model", "deepseek-chat")

    # Agent 记忆
    amem = AgentMemory()
    amem.remember("planner", "last_task", "贪吃蛇")

    # 事件历史
    hist = EventHistory(max_events=5000)
    hist.record({"task_id": "task-001", "event": "task.created"})

    # 语义记忆
    smem = SemanticMemory()
    smem.store("Python 是动态类型语言")
    results = smem.search("编程语言类型")
"""

from runtime.context.task_context import TaskContext
from runtime.context.shared_memory import SharedMemory
from runtime.context.agent_memory import AgentMemory
from runtime.context.event_history import EventHistory
from runtime.context.semantic_memory import SemanticMemory

__all__ = [
    # 核心类
    "TaskContext",
    "SharedMemory",
    "AgentMemory",
    "EventHistory",
    "SemanticMemory",
]
