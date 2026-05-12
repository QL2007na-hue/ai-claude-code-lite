"""
EchoAgent —— Agent SDK 最小化示例。

该 Agent 演示了 BaseAgent 的完整生命周期用法：
    1. 定义 name 属性
    2. 实现 on_event 处理传入事件
    3. 使用 emit_event 发射自定义事件
    4. 通过 self.ctx 访问基础设施

Usage::

    # 单文件直接运行
    python -m sdk.agent_sdk.examples.echo_agent

    或通过 asyncio 手动调度：
    import asyncio
    from sdk.agent_sdk.examples.echo_agent import demo
    asyncio.run(demo())
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

# 处理两种导入场景：包内相对导入 和 直接运行
try:
    from sdk.agent_sdk.base_agent import BaseAgent
    from sdk.agent_sdk.context import RuntimeContext
except ImportError:
    import sys
    from pathlib import Path
    _root = Path(__file__).resolve().parent.parent.parent.parent
    sys.path.insert(0, str(_root))
    from sdk.agent_sdk.base_agent import BaseAgent
    from sdk.agent_sdk.context import RuntimeContext


class EchoAgent(BaseAgent):
    """回声 Agent —— Agent SDK 最小可行示例。

    行为：
        - 接收到任何事件后，打印日志并发射 "echo.event_received" 事件。
        - 在 run() 中发射 "echo.started" 事件，作为启动通知。
        - 在 stop() 中发射 "echo.stopped" 事件，作为停止通知。

    Attributes
    ----------
    echo_count : int
        累计收到的事件数量。
    """

    # ── Agent 标识 ──────────────────────────────────────────

    @property
    def name(self) -> str:
        return "echo-agent"

    # ── 生命周期 ────────────────────────────────────────────

    async def init(self) -> None:
        """初始化：订阅所有事件（不传 subscribe 即接收全部事件）。"""
        await super().init()
        self.echo_count: int = 0
        self.logger.info(
            "EchoAgent 已就绪 "
            "(event_bus=%s, workspace_root=%s, provider=%s)",
            type(self.ctx.event_bus).__name__,
            self.ctx.workspace.root,
            type(self.ctx.provider).__name__,
        )

    async def run(self, task_id: str) -> None:
        """执行任务入口。

        发射 "echo.started" 事件，广播任务启动信息。
        """
        await super().run(task_id)
        self.logger.info("[%s] EchoAgent 开始回显", task_id)

        # 写入共享内存（演示跨 Agent 通信）
        with self.ctx.shared_memory_lock:
            self.ctx.shared_memory.setdefault("echo_history", [])
            self.ctx.shared_memory["echo_history"].append({
                "task_id": task_id,
                "action": "started",
            })

        self.emit_event("echo.started", {
            "task_id": task_id,
            "echo_count": self.echo_count,
        })

    async def stop(self) -> None:
        """停止时发射 "echo.stopped" 事件。"""
        if self.running and self._current_task_id:
            self.emit_event("echo.stopped", {
                "echo_count": self.echo_count,
            })
        await super().stop()

    # ── 事件处理 ────────────────────────────────────────────

    async def on_event(
        self,
        task_id: str,
        agent: str,
        event: str,
        payload: Any,
    ) -> None:
        """接收到任何事件后：打印日志 + 发射回声事件。

        同时也更新共享内存中的回声历史。
        """
        self.echo_count += 1

        self.logger.info(
            "[echo #%d] task=%s agent=%s event=%s payload=%s",
            self.echo_count,
            task_id,
            agent,
            event,
            payload,
        )

        # 发射回声事件（仅当自身有活跃 task_id 时）
        if self._current_task_id:
            self.emit_event("echo.event_received", {
                "original_event": event,
                "original_agent": agent,
                "original_task_id": task_id,
                "original_payload": payload,
                "echo_count": self.echo_count,
            })

        # 写入共享内存（演示跨 Agent 通信）
        with self.ctx.shared_memory_lock:
            history = self.ctx.shared_memory.setdefault("echo_history", [])
            history.append({
                "count": self.echo_count,
                "task_id": task_id,
                "agent": agent,
                "event": event,
            })
            # 只保留最近 100 条
            if len(history) > 100:
                self.ctx.shared_memory["echo_history"] = history[-100:]


# ---------------------------------------------------------------------------
# 演示入口
# ---------------------------------------------------------------------------

async def demo() -> None:
    """演示 EchoAgent 完整生命周期。

    模拟一个最小化的运行时环境，展示：
        1. RuntimeContext 构造
        2. Agent 初始化 / 运行 / 停止
        3. 事件收发
    """
    from runtime.event_bus import EventBus
    from runtime.task_manager import TaskManager
    from workspace.manager import WorkspaceManager
    from providers.base_provider import BaseProvider as _BaseProvider

    # 构造一个极简 Provider（无需真实 API Key）
    class NoopProvider(_BaseProvider):
        def chat(self, messages, **kwargs):
            return "EchoAgent 演示模式 —— 未接入真实模型"

    print("=" * 60)
    print("  EchoAgent —— Agent SDK 演示")
    print("=" * 60)

    # 1) 运行时上下文
    ctx = RuntimeContext(
        event_bus=EventBus(),
        task_manager=TaskManager(),
        workspace=WorkspaceManager(root_dir="workspace"),
        provider=NoopProvider(model="demo"),
    )

    # 2) 创建 EchoAgent
    agent = EchoAgent(ctx)

    # 3) 初始化
    await agent.init()
    print(f"[ok] Agent '{agent.name}' 已初始化")

    # 4) 执行一个虚拟任务
    task_id = ctx.task_manager.create_task(
        agent=agent.name,
        payload={"goal": "演示回声 Agent"},
    )
    await agent.run(task_id)
    print(f"[ok] Agent '{agent.name}' 开始执行任务 {task_id}")

    # 5) 模拟事件激发（验证 on_event）
    ctx.event_bus.emit_event(task_id, "planner", "task.planned", {
        "subtask_count": 3,
    })
    ctx.event_bus.emit_event(task_id, "coder", "task.coding_completed", {
        "files_count": 5,
    })
    ctx.event_bus.emit_event(task_id, "reviewer", "task.review_approved", {
        "score": 92,
    })

    # 等待事件处理
    await asyncio.sleep(0.5)

    # 6) 查看共享内存（跨 Agent 通信状态）
    with ctx.shared_memory_lock:
        history = ctx.shared_memory.get("echo_history", [])

    print(f"\n[统计] 回声 Agent 收到 {agent.echo_count} 个事件")
    print(f"[统计] 共享内存历史 {len(history)} 条")
    if history:
        print(f"[样例] 第一条: {history[0]}")
        print(f"[样例] 最后一条: {history[-1]}")

    # 7) 停止
    await agent.stop()
    print(f"[ok] Agent '{agent.name}' 已停止")

    # 8) 验证注册表
    print(f"[验证] agent_registry 是否为空: {ctx.agent_registry == {}}")
    print(f"[验证] get_agent('echo-agent'): {ctx.get_agent('echo-agent')}")

    print("\n" + "=" * 60)
    print("  EchoAgent 演示完成")
    print("=" * 60)


# 直接运行入口
if __name__ == "__main__":
    # 启用日志以观察 Agent 行为
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(demo())
