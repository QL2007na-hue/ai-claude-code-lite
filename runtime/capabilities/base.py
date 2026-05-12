"""
BaseCapability —— 能力的抽象基类。

所有能力（Shell / Filesystem / Git / HTTP 等）都继承此类，
定义统一的生命周期钩子与权限声明接口。

能力是组合式功能单元，在运行时注入到 Agent 实例中，
由 CapabilityRegistry 管理权限授予与回收。

Usage:
    from runtime.capabilities.base import BaseCapability

    class MyCapability(BaseCapability):
        name = "my_cap"
        description = "描述此能力"
        PERMISSIONS = ["SHELL"]

        async def execute(self, *args, **kwargs):
            return await self._do_work(*args, **kwargs)

        def validate(self):
            # 自定义预校验逻辑
            pass

        def sanitize(self, *args):
            # 自定义输入清理
            return args
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar, List, Optional, Tuple


class BaseCapability(ABC):
    """能力抽象基类。

    子类必须覆盖：
        - name (property)
        - PERMISSIONS (class-level list)
        - execute (async method)

    可选覆盖：
        - description (property)
        - validate()
        - sanitize()
    """

    # ── 类属性 ────────────────────────────────────────────────

    PERMISSIONS: ClassVar[List[str]] = []
    """该能力需要的权限列表。

    格式为与 runtime.plugins.permissions.Permission 枚举名一致的字符串，如：
        ["FILESYSTEM_READ", "FILESYSTEM_WRITE", "SHELL", "NETWORK"]

    由 CapabilityRegistry 在 grant() / check() 时使用。
    """

    # ── 基础属性 ──────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """能力的唯一名称（如 "shell", "filesystem", "git", "http"）。"""
        ...

    @property
    def description(self) -> str:
        """能力的简要描述文本。

        默认返回类文档字符串第一行，子类可覆盖。
        """
        if self.__doc__:
            first_line = self.__doc__.strip().split("\n")[0]
            return first_line.strip()
        return f"{self.name} capability"

    # ── 生命周期钩子 ──────────────────────────────────────────

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """执行能力的核心逻辑。

        子类必须实现此方法，封装具体功能实现。
        在执行前会自动调用 validate() 和 sanitize()。

        Parameters
        ----------
        *args, **kwargs
            执行参数，由子类定义具体签名。

        Returns
        -------
        Any
            执行结果，由子类定义具体返回格式。
        """
        ...

    def validate(self) -> None:
        """预执行校验钩子。

        在执行 execute() 之前调用，用于检查前置条件是否满足。
        默认不做任何检查，子类可覆盖以添加自定义校验逻辑。

        Raises
        ------
        RuntimeError
            若校验不通过，应抛出带描述信息的 RuntimeError。
        """
        pass

    def sanitize(self, *args: Any) -> Tuple[Any, ...]:
        """输入清理钩子。

        在执行 execute() 之前调用，用于对输入参数做安全检查/转换。
        默认原样返回，子类可覆盖以添加自定义清理逻辑。

        Parameters
        ----------
        *args
            原始输入参数。

        Returns
        -------
        tuple
            清理后的参数元组，长度应与输入一致。
        """
        return args

    # ── 内部工具 ──────────────────────────────────────────────

    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(
            f"capability.{self.name}"
        )
        """预配置的日志器，名称格式为 capability.<name>。"""
        if not self.logger.handlers:
            _h = logging.StreamHandler()
            _h.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            ))
            self.logger.addHandler(_h)
            self.logger.setLevel(logging.DEBUG)

        self._enabled: bool = True
        """该能力是否启用。可由 CapabilityRegistry 控制。"""

    @property
    def enabled(self) -> bool:
        """该能力当前是否处于启用状态。"""
        return self._enabled

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} name={self.name!r} "
            f"enabled={self._enabled} permissions={self.PERMISSIONS}>"
        )
