"""
runtime.plugins — 插件运行时系统

提供类 OS 的插件管理能力：
  - PluginRuntime:    插件生命周期管理（加载/卸载/热重载/启用/禁用）
  - Permission:       权限枚举
  - PermissionSet:    权限集合
  - PermissionError:  权限异常
  - requires:         权限校验装饰器
  - PluginConfig:     插件配置
  - 配置加载/保存工具函数

Usage:
    from runtime.plugins import PluginRuntime, Permission, PermissionSet, requires
    from runtime.plugins import PluginConfig, load_plugin_config, save_plugin_config
"""

from runtime.plugins.permissions import (
    Permission,
    PermissionSet,
    PermissionError,
    requires,
    check_permissions,
    merge_permissions,
)
from runtime.plugins.config import (
    PluginConfig,
    load_plugin_config,
    save_plugin_config,
    load_all_configs,
    delete_plugin_config,
    DEFAULT_CONFIG_DIR,
)
from runtime.plugins.plugin_runtime import PluginRuntime

__all__ = [
    # ── 核心运行时 ──
    "PluginRuntime",
    # ── 权限系统 ──
    "Permission",
    "PermissionSet",
    "PermissionError",
    "requires",
    "check_permissions",
    "merge_permissions",
    # ── 配置系统 ──
    "PluginConfig",
    "load_plugin_config",
    "save_plugin_config",
    "load_all_configs",
    "delete_plugin_config",
    "DEFAULT_CONFIG_DIR",
]
