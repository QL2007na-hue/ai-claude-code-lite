"""
Agent SDK —— 面向第三方开发者的 Agent 开发套件。

提供：
    BaseAgent       —— 抽象基类，继承后实现自定义 Agent
    RuntimeContext  —— 运行时上下文，收敛所有基础设施依赖

快速开始::

    from runtime import EventBus, TaskManager
    from workspace import WorkspaceManager
    from providers import DeepSeekProvider
    from sdk.agent_sdk import BaseAgent, RuntimeContext

    # 1. 构造运行时上下文
    ctx = RuntimeContext(
        event_bus=EventBus(),
        task_manager=TaskManager(),
        workspace=WorkspaceManager(),
        provider=DeepSeekProvider(model="deepseek-chat", api_key="sk-xxx"),
    )

    # 2. 实现自定义 Agent
    class MyAgent(BaseAgent):
        @property
        def name(self) -> str:
            return "my-agent"

        async def on_event(self, task_id, agent, event, payload):
            self.logger.info("收到事件: %s", event)

    # 3. 使用 Agent
    import asyncio
    agent = MyAgent(ctx)

    async def main():
        await agent.init()
        await agent.run("task-123")
        await agent.stop()

    asyncio.run(main())
"""

from .base_agent import BaseAgent
from .context import RuntimeContext

__all__ = [
    "BaseAgent",
    "RuntimeContext",
]
