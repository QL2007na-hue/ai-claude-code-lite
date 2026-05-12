"""CapabilityRegistry —— 能力注册与权限管理中心。

管理所有能力的注册/查询，控制 Agent 对能力的授权/回收，
并在运行时将已授权的能力注入到 Agent 实例。

Usage:
    from runtime.capabilities import CapabilityRegistry, ShellCapability

    reg = CapabilityRegistry()
    reg.register(ShellCapability())
    reg.grant("my-agent", "shell")
    reg.check("my-agent", "shell")  # -> True
    reg.inject(agent_instance)      # agent.shell = ShellCapability()
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from .base import BaseCapability

logger = logging.getLogger("runtime.capabilities.registry")


class CapabilityError(Exception):
    """能力操作相关错误。"""
    pass


class CapabilityRegistry:
    """能力注册中心。

    管理能力实例的注册与查询，以及 Agent 粒度的授权控制。
    """

    def __init__(self) -> None:
        self._capabilities: Dict[str, BaseCapability] = {}
        self._grants: Dict[str, Set[str]] = defaultdict(set)

    # ── 注册 ──────────────────────────────────────────────────

    def register(self, capability: BaseCapability) -> None:
        if not capability.name:
            raise CapabilityError("能力必须设置 name 属性")
        if capability.name in self._capabilities:
            logger.warning("能力 '%s' 已注册，将被覆盖", capability.name)
        self._capabilities[capability.name] = capability
        logger.info("能力已注册: %s (权限: %s)", capability.name, capability.PERMISSIONS)

    def unregister(self, name: str) -> bool:
        cap = self._capabilities.pop(name, None)
        if cap:
            for grants in self._grants.values():
                grants.discard(name)
            logger.info("能力已注销: %s", name)
            return True
        return False

    # ── 查询 ──────────────────────────────────────────────────

    def get(self, name: str) -> Optional[BaseCapability]:
        return self._capabilities.get(name)

    def list_all(self) -> List[str]:
        return sorted(self._capabilities.keys())

    def has(self, name: str) -> bool:
        return name in self._capabilities

    # ── 授权管理 ──────────────────────────────────────────────

    def grant(self, agent_name: str, capability_name: str) -> None:
        if capability_name not in self._capabilities:
            raise CapabilityError(f"能力 '{capability_name}' 未注册")
        self._grants[agent_name].add(capability_name)
        logger.info("授权: agent='%s' capability='%s'", agent_name, capability_name)

    def revoke(self, agent_name: str, capability_name: str) -> None:
        self._grants[agent_name].discard(capability_name)
        logger.info("回收授权: agent='%s' capability='%s'", agent_name, capability_name)

    def check(self, agent_name: str, capability_name: str) -> bool:
        """检查 Agent 是否被授予指定能力。"""
        return capability_name in self._grants.get(agent_name, set())

    def get_agent_capabilities(self, agent_name: str) -> List[str]:
        """获取 Agent 已授权的所有能力名称列表。"""
        return sorted(self._grants.get(agent_name, set()))

    def get_agents_with_capability(self, capability_name: str) -> List[str]:
        """获取拥有指定能力的所有 Agent 名称。"""
        return sorted(a for a, caps in self._grants.items() if capability_name in caps)

    def grant_all(self, agent_name: str) -> None:
        """授予 Agent 所有已注册的能力（管理员操作）。"""
        for name in self._capabilities:
            self._grants[agent_name].add(name)
        logger.info("授予所有能力: agent='%s' count=%d", agent_name, len(self._capabilities))

    def revoke_all(self, agent_name: str) -> None:
        """回收 Agent 所有能力。"""
        self._grants[agent_name].clear()
        logger.info("回收所有能力: agent='%s'", agent_name)

    # ── 能力注入 ──────────────────────────────────────────────

    def inject(self, agent: Any) -> None:
        """将已授权的能力动态注入到 Agent 实例的属性上。

        注入后 Agent 可以通过 agent.shell / agent.filesystem 等方式调用能力。
        权限由 grants 表控制，Agent 只能获得已被授权的的能力。

        注入失败不影响 Agent 正常运行（仅打印 warning）。
        """
        agent_name = getattr(agent, "name", None) or getattr(agent, "__class__", type("_", (), {})).__name__
        granted = self._grants.get(agent_name, set())

        for cap_name in granted:
            cap = self._capabilities.get(cap_name)
            if cap is None:
                continue
            try:
                cap.validate()
            except Exception as e:
                logger.warning("能力 '%s' validate() 失败: %s", cap_name, e)
                continue
            try:
                setattr(agent, cap_name, cap)
                logger.debug("已注入能力: agent='%s' capability='%s'", agent_name, cap_name)
            except Exception as e:
                logger.warning("注入能力 '%s' 到 agent '%s' 失败: %s", cap_name, agent_name, e)

    def inject_all(self, agent: Any) -> None:
        """将 Agent 所有已授权能力注入。等价于 grant_all + inject。"""
        self.grant_all(getattr(agent, "name", "unknown"))
        self.inject(agent)

    # ── 权限校验 ──────────────────────────────────────────────

    def enforce(self, agent_name: str, capability_name: str) -> None:
        """强制校验：Agent 无此能力则抛出 CapabilityError。"""
        if not self.check(agent_name, capability_name):
            raise CapabilityError(
                f"Agent '{agent_name}' 无权使用能力 '{capability_name}'"
            )

    def get_required_permissions(self, agent_name: str) -> List[str]:
        """获取 Agent 已授权能力所需的全部权限（已去重）。"""
        perms: Set[str] = set()
        for cap_name in self._grants.get(agent_name, set()):
            cap = self._capabilities.get(cap_name)
            if cap:
                perms.update(cap.PERMISSIONS)
        return sorted(perms)
