"""
Plugin Permission System — 插件权限声明、校验与装饰器。

每个插件可以通过类属性 PERMISSIONS 声明所需权限，
PluginRuntime 在事件分发前自动执行权限检查。

Usage:
    from runtime.plugins.permissions import Permission, PermissionSet, requires, PermissionError

    class MyPlugin(BasePlugin):
        PERMISSIONS = ["FILESYSTEM_READ", "EVENT_EMIT"]

        @requires(Permission.SHELL)
        def dangerous_operation(self):
            ...
"""

from __future__ import annotations

import enum
import functools
import logging
from typing import Any, Callable, Container, FrozenSet, Iterable, Optional, Union

logger = logging.getLogger("runtime.plugins.permissions")


# ── 权限枚举 ────────────────────────────────────────────────

class Permission(enum.Enum):
    """Runtime 插件可声明的所有权限。

    FILESYSTEM_READ     — 读取工作区文件
    FILESYSTEM_WRITE    — 写入 / 删除工作区文件
    NETWORK             — 发起 HTTP / 外部网络请求
    SHELL               — 执行系统命令 / 子进程
    EVENT_EMIT           — 主动向 EventBus 发送事件
    TASK_MANAGE         — 创建 / 修改 / 删除任务
    PROVIDER_ACCESS     — 访问 AI Provider（如 DeepSeek API）
    """

    FILESYSTEM_READ = "filesystem_read"
    FILESYSTEM_WRITE = "filesystem_write"
    NETWORK = "network"
    SHELL = "shell"
    EVENT_EMIT = "event_emit"
    TASK_MANAGE = "task_manage"
    PROVIDER_ACCESS = "provider_access"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_string(cls, value: str) -> Permission:
        """从字符串（枚举名或 value）解析 Permission。"""
        # 先按 value 匹配
        for perm in cls:
            if perm.value == value:
                return perm
        # 再按枚举名匹配
        try:
            return cls[value.upper()]
        except KeyError:
            raise ValueError(f"未知的权限标识符: {value!r}")

    @classmethod
    def parse(cls, raw: Union[str, "Permission"]) -> "Permission":
        """统一解析入口：接受 str 或 Permission 实例。"""
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, str):
            return cls.from_string(raw)
        raise TypeError(f"无法解析权限: {raw!r}")


# ── 权限集合 ────────────────────────────────────────────────

class PermissionSet:
    """不可变权限集合，支持位掩码式快速查找与集合运算。

    Usage:
        ps = PermissionSet(["FILESYSTEM_READ", "NETWORK"])
        assert Permission.FILESYSTEM_READ in ps
        assert "filesystem_read" in ps          # str 也支持
        union = ps | PermissionSet(["SHELL"])
    """

    __slots__ = ("_perms", "_frozen")

    def __init__(self, permissions: Iterable[Union[str, Permission]] = ()):
        self._perms: FrozenSet[Permission] = frozenset(
            Permission.parse(p) for p in permissions
        )
        self._frozen = True

    # ── 容器协议 ─────────────────────────────────────────

    def __contains__(self, item: Union[str, Permission]) -> bool:
        return Permission.parse(item) in self._perms

    def __iter__(self):
        return iter(self._perms)

    def __len__(self) -> int:
        return len(self._perms)

    def __bool__(self) -> bool:
        return len(self._perms) > 0

    # ── 集合运算 ─────────────────────────────────────────

    def __or__(self, other: "PermissionSet") -> "PermissionSet":
        if not isinstance(other, PermissionSet):
            return NotImplemented
        new = PermissionSet()
        object.__setattr__(new, "_perms", self._perms | other._perms)
        return new

    def __and__(self, other: "PermissionSet") -> "PermissionSet":
        if not isinstance(other, PermissionSet):
            return NotImplemented
        new = PermissionSet()
        object.__setattr__(new, "_perms", self._perms & other._perms)
        return new

    def __sub__(self, other: "PermissionSet") -> "PermissionSet":
        if not isinstance(other, PermissionSet):
            return NotImplemented
        new = PermissionSet()
        object.__setattr__(new, "_perms", self._perms - other._perms)
        return new

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PermissionSet):
            return self._perms == other._perms
        return NotImplemented

    # ── 快捷方法 ─────────────────────────────────────────

    def has_all(self, *permissions: Union[str, Permission]) -> bool:
        """检查是否拥有全部指定权限。"""
        return all(p in self for p in permissions)

    def has_any(self, *permissions: Union[str, Permission]) -> bool:
        """检查是否拥有任一指定权限。"""
        return any(p in self for p in permissions)

    def as_strings(self) -> FrozenSet[str]:
        """返回权限的字符串集合（枚举名）。"""
        return frozenset(p.name for p in self._perms)

    def as_values(self) -> FrozenSet[str]:
        """返回权限的字符串集合（value）。"""
        return frozenset(p.value for p in self._perms)

    # ── 序列化 ───────────────────────────────────────────

    @classmethod
    def from_strings(cls, permissions: Iterable[str]) -> "PermissionSet":
        """从权限名字符串列表构建（如 PERMISSIONS 类属性）。"""
        return cls(permissions)

    def to_list(self) -> list:
        """序列化为存储格式（枚举名列表）。"""
        return sorted(p.name for p in self._perms)

    def __repr__(self) -> str:
        return f"PermissionSet({sorted(p.name for p in self._perms)})"

    def __hash__(self) -> int:
        return hash(self._perms)


# ── 自定义异常 ──────────────────────────────────────────────

class PermissionError(Exception):
    """插件权限不足时抛出的异常。

    Attributes
    ----------
    plugin : str
        插件名称。
    required : PermissionSet
        需要的权限。
    granted : PermissionSet
        插件已声明的权限。
    missing : PermissionSet
        缺少的权限。
    """

    def __init__(
        self,
        plugin: str,
        required: Union[str, Permission, PermissionSet],
        granted: Optional[PermissionSet] = None,
        message: Optional[str] = None,
    ):
        self.plugin = plugin
        self.granted = granted or PermissionSet()
        self.missing = PermissionSet()

        if isinstance(required, PermissionSet):
            self.required = required
        elif isinstance(required, (str, Permission)):
            self.required = PermissionSet([required])
        else:
            self.required = PermissionSet()

        self.missing = self.required - self.granted

        if message is None:
            missing_names = ", ".join(sorted(p.name for p in self.missing))
            message = (
                f"插件 '{plugin}' 权限不足。缺少: {missing_names}"
                if missing_names
                else f"插件 '{plugin}' 权限不足。"
            )
        super().__init__(message)


# ── requires() 装饰器 ───────────────────────────────────────

def requires(*permissions: Union[str, Permission]):
    """装饰器：标记插件方法需要特定权限才能调用。

    装饰后的方法在执行前自动校验调用对象的 PERMISSIONS 属性。
    适用于 BasePlugin 子类的方法。

    Usage:
        class MyPlugin(BasePlugin):
            PERMISSIONS = ["FILESYSTEM_READ", "NETWORK"]

            @requires(Permission.SHELL)
            def run_command(self, cmd: str):
                ...  # 若 PERMISSIONS 不含 SHELL 则抛出 PermissionError

            @requires("FILESYSTEM_WRITE", "NETWORK")
            def upload_report(self, path: str):
                ...
    """

    required_set = PermissionSet(permissions)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self_or_cls: Any, *args: Any, **kwargs: Any) -> Any:
            # 从实例或类上获取 PERMISSIONS
            raw_perms: Iterable[str] = getattr(self_or_cls, "PERMISSIONS", None) or []
            granted = PermissionSet.from_strings(raw_perms)

            if not granted.has_all(*required_set):
                raise PermissionError(
                    plugin=getattr(self_or_cls, "name", "<unknown>"),
                    required=required_set,
                    granted=granted,
                )

            logger.debug(
                "权限检查通过 [%s]: %s -> %s",
                getattr(self_or_cls, "name", "?"),
                func.__name__,
                required_set,
            )
            return func(self_or_cls, *args, **kwargs)

        # 暴露元信息供 introspection
        wrapper.__permission_required__ = required_set  # type: ignore[attr-defined]
        return wrapper

    return decorator


# ── 权限校验工具函数 ────────────────────────────────────────

def check_permissions(
    plugin_name: str,
    granted_perms: Iterable[str],
    required_perms: Iterable[Union[str, Permission]],
) -> None:
    """强制校验权限，不通过时抛出 PermissionError。

    通常由 PluginRuntime 在事件分发前调用。
    """
    granted = PermissionSet.from_strings(granted_perms)
    required = PermissionSet(required_perms)

    if not granted.has_all(*required):
        raise PermissionError(
            plugin=plugin_name,
            required=required,
            granted=granted,
        )


def merge_permissions(
    base: Iterable[str], extra: Iterable[str]
) -> PermissionSet:
    """合并两组权限声明，返回 PermissionSet。"""
    return PermissionSet.from_strings(base) | PermissionSet.from_strings(extra)
