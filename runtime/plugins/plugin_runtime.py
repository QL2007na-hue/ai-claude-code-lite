"""
Plugin Runtime — 插件运行时系统，像 OS 一样管理插件生命周期。

PluginRuntime 封装并增强 PluginLoader，提供：
  - 动态加载 / 卸载 / 热重载
  - 启用 / 禁用切换（不卸载）
  - 权限声明与强制校验
  - 配置持久化
  - 禁用插件自动跳过事件分发

Usage:
    from plugin_sdk import PluginContext
    from runtime.plugins import PluginRuntime

    ctx = PluginContext(event_bus, task_manager, workspace_mgr)
    runtime = PluginRuntime(ctx)

    # 扫描目录加载
    runtime.load_plugin_dir("plugins/")

    # 动态加载单个文件
    runtime.load_plugin_from_file("plugins/my_plugin.py")

    # 管理
    runtime.enable_plugin("my-plugin")
    runtime.disable_plugin("code-reviewer")

    # 热重载
    runtime.reload_plugin("my-plugin")

    # 启动事件监听
    runtime.start()
    ...
    runtime.stop()
"""

from __future__ import annotations

import importlib
import logging
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Type

# 复用现有 plugin_sdk 基础设施
from plugin_sdk.base_plugin import BasePlugin, PluginContext
from plugin_sdk.plugin_loader import PluginLoader

from runtime.plugins.config import (
    PluginConfig,
    load_plugin_config,
    save_plugin_config,
    delete_plugin_config,
    DEFAULT_CONFIG_DIR,
)
from runtime.plugins.permissions import (
    Permission,
    PermissionSet,
    PermissionError as PermError,
    check_permissions,
)

logger = logging.getLogger("runtime.plugins.runtime")

# 预设的事件分类 → 权限映射表
# 用于自动判断事件处理需要什么权限
_EVENT_PERMISSION_MAP: Dict[str, List[str]] = {
    # 工作区操作类事件
    "task.code_written":      ["FILESYSTEM_WRITE"],
    "task.file_read":         ["FILESYSTEM_READ"],
    "task.file_deleted":      ["FILESYSTEM_WRITE"],
    # 任务管理类事件
    "task.created":           ["TASK_MANAGE"],
    "task.updated":           ["TASK_MANAGE"],
    "task.deleted":           ["TASK_MANAGE"],
    # Provider 调用
    "provider.request":       ["PROVIDER_ACCESS"],
    "provider.response":      ["PROVIDER_ACCESS"],
    # 对外通信
    "webhook.triggered":      ["NETWORK"],
}


class PluginRuntime:
    """插件运行时 —— 插件系统的统一入口。

    在 PluginLoader 之上提供：
      - 单个文件动态加载
      - 启用 / 禁用切换
      - 热重载
      - 权限系统集成
      - 配置持久化
    """

    def __init__(
        self,
        ctx: PluginContext,
        plugin_dir: str = "plugins",
        config_dir: Optional[str] = None,
        auto_config: bool = True,
    ):
        """初始化 PluginRuntime。

        Parameters
        ----------
        ctx : PluginContext
            插件运行时上下文。
        plugin_dir : str
            插件 .py 文件存放目录。
        config_dir : str, optional
            插件配置文件存放目录，默认 data/plugin_configs/。
        auto_config : bool
            是否在加载插件时自动读取配置。
        """
        self.ctx = ctx
        self.plugin_dir = Path(plugin_dir)

        # 内部 PluginLoader（复用它的事件循环 + 文件扫描能力）
        self._loader = PluginLoader(ctx, plugin_dir)

        # 配置目录
        self._config_dir = Path(config_dir) if config_dir else DEFAULT_CONFIG_DIR
        self._auto_config = auto_config

        # 权限注册表: {plugin_name: PermissionSet}
        self.plugin_permissions: Dict[str, PermissionSet] = {}

        # 配置注册表: {plugin_name: PluginConfig}
        self._configs: Dict[str, PluginConfig] = {}

        # 禁用集合: 这些插件不会收到事件（但仍在注册表中）
        self._disabled: Set[str] = set()

        # 热重载状态保存: {plugin_name: Dict}  — 声明 preserve_state=True 的插件
        self._preserved_state: Dict[str, Dict[str, Any]] = {}

        # 线程安全
        self._lock = threading.RLock()

        # 运行时状态
        self._running = False

    # ═══════════════════════════════════════════════════════
    #  插件加载
    # ═══════════════════════════════════════════════════════

    def load_plugin_from_file(self, filepath: str) -> BasePlugin:
        """动态加载单个 .py 文件为插件。

        约定：
          - 文件中定义一个名为 Plugin 的类，继承 BasePlugin
          - 或模块顶层直接定义一个 plugin 实例

        Parameters
        ----------
        filepath : str
            插件 .py 文件的绝对或相对路径。

        Returns
        -------
        BasePlugin
            加载并注册后的插件实例。

        Raises
        ------
        ImportError
            文件无法加载时。
        ValueError
            文件中找不到有效的 BasePlugin 子类或实例时。
        """
        filepath = str(Path(filepath).resolve())

        if not Path(filepath).exists():
            raise FileNotFoundError(f"插件文件不存在: {filepath}")

        if not filepath.endswith(".py"):
            raise ValueError(f"插件文件必须是 .py 文件: {filepath}")

        module_name = Path(filepath).stem
        logger.info("动态加载插件: %s", filepath)

        try:
            spec = importlib.util.spec_from_file_location(
                f"plugin_{module_name}", filepath
            )
            if spec is None or spec.loader is None:
                raise ImportError(f"无法创建模块规格: {filepath}")

            module = importlib.util.module_from_spec(spec)
            sys.modules[f"plugin_{module_name}"] = module
            spec.loader.exec_module(module)
        except Exception:
            logger.exception("加载插件模块失败: %s", filepath)
            raise ImportError(f"加载插件模块失败: {filepath}\n{traceback.format_exc()}")

        # 提取插件实例
        plugin = self._extract_plugin_from_module(module, module_name, filepath)

        # 应用配置
        if self._auto_config:
            self._apply_config_to_plugin(plugin)

        # 提取权限声明
        self._register_permissions(plugin)

        # 委托 PluginLoader 注册
        with self._lock:
            self._loader.register(plugin)

        logger.info("插件已加载: %s v%s (%s)", plugin.name, plugin.version, filepath)
        return plugin

    def load_plugin_dir(self, plugin_dir: Optional[str] = None) -> int:
        """批量加载插件目录下的所有 .py 文件。

        Parameters
        ----------
        plugin_dir : str, optional
            插件目录路径，默认使用初始化时的 plugin_dir。

        Returns
        -------
        int
            成功加载的插件数量。
        """
        directory = Path(plugin_dir) if plugin_dir else self.plugin_dir
        directory.mkdir(parents=True, exist_ok=True)

        loaded_count = 0
        for filepath in sorted(directory.glob("*.py")):
            if filepath.name.startswith("_"):
                continue
            try:
                self.load_plugin_from_file(str(filepath))
                loaded_count += 1
            except Exception:
                logger.exception("加载插件文件失败: %s", filepath)

        return loaded_count

    def _extract_plugin_from_module(
        self, module: Any, module_name: str, filepath: str
    ) -> BasePlugin:
        """从已加载的 Python 模块中提取 BasePlugin 实例。"""
        # 1. 找名为 Plugin 的类
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BasePlugin)
                and attr is not BasePlugin
            ):
                try:
                    instance = attr()
                    if not instance.name:
                        instance.name = module_name
                    return instance
                except Exception:
                    logger.exception("实例化插件类失败: %s", attr_name)
                    raise ValueError(f"无法实例化插件类 {attr_name}")

        # 2. 找名为 plugin 的实例
        if hasattr(module, "plugin") and isinstance(module.plugin, BasePlugin):
            instance = module.plugin
            if not instance.name:
                instance.name = module_name
            return instance

        raise ValueError(
            f"在 {filepath} 中未找到 BasePlugin 子类（期望类名 'Plugin'）或 'plugin' 实例"
        )

    # ═══════════════════════════════════════════════════════
    #  插件卸载 / 热重载
    # ═══════════════════════════════════════════════════════

    def unload_plugin(self, name: str) -> bool:
        """卸载指定插件：调用 on_unload()、清理注册信息。

        Parameters
        ----------
        name : str
            插件名称。

        Returns
        -------
        bool
            是否成功卸载。
        """
        with self._lock:
            # 保存插件引用以便在 unregister 后仍可访问其属性
            plugin = self._loader._plugins.get(name)

            # 从 PluginLoader 卸载（会调用 on_unload）
            success = self._loader.unregister(name)

            if success:
                # 清理权限注册
                self.plugin_permissions.pop(name, None)
                # 清理配置
                self._configs.pop(name, None)
                # 清理禁用状态
                self._disabled.discard(name)
                # 清理保留状态
                self._preserved_state.pop(name, None)
                # 清理模块缓存
                module_key = f"plugin_{name}"
                sys.modules.pop(module_key, None)

                logger.info("插件已卸载: %s", name)
            else:
                logger.warning("卸载插件失败（未找到）: %s", name)

            return success

    def reload_plugin(self, name: str) -> Optional[BasePlugin]:
        """热重载插件：卸载后重新加载，可选择性保留状态。

        工作流程：
          1. 查找插件原始文件路径
          2. 检查是否需要保留状态
          3. 卸载
          4. 重新加载
          5. 恢复状态

        Parameters
        ----------
        name : str
            插件名称。

        Returns
        -------
        BasePlugin or None
            重新加载后的插件实例，失败时返回 None。

        Raises
        ------
        FileNotFoundError
            无法定位插件源文件时。
        """
        with self._lock:
            plugin = self._loader._plugins.get(name)
            if not plugin:
                logger.warning("重载失败：插件 '%s' 不存在", name)
                return None

            # 查找文件路径：从 sys.modules 中追溯
            module_key = f"plugin_{name}"
            old_module = sys.modules.get(module_key)
            filepath = None

            if old_module and hasattr(old_module, "__file__"):
                filepath = old_module.__file__
            # 回退：在插件目录中搜索
            if not filepath or not Path(filepath).exists():
                candidate = self.plugin_dir / f"{name}.py"
                if candidate.exists():
                    filepath = str(candidate)
                else:
                    raise FileNotFoundError(
                        f"无法定位插件 '{name}' 的源文件。"
                        f"已搜索: {filepath or 'N/A'}, {candidate}"
                    )

            # 状态保留
            was_enabled = name not in self._disabled
            old_config = self._configs.get(name)

            # 检查插件是否声明 preserve_state
            preserve = getattr(plugin, "PRESERVE_STATE", False)
            saved_state: Optional[Dict[str, Any]] = None
            if preserve:
                try:
                    state = getattr(plugin, "get_state", None)
                    if callable(state):
                        saved_state = state()
                    else:
                        saved_state = {}
                    logger.debug("已保存插件状态 [%s]: %s", name, saved_state)
                except Exception:
                    logger.exception("保存插件状态失败: %s", name)
                    saved_state = {}

            # 卸载
            self.unload_plugin(name)

            # 重新加载
            try:
                new_plugin = self.load_plugin_from_file(filepath)
            except Exception:
                logger.exception("重载插件失败: %s (%s)", name, filepath)
                return None

            # 恢复状态
            if saved_state is not None and new_plugin:
                try:
                    restore = getattr(new_plugin, "restore_state", None)
                    if callable(restore):
                        restore(saved_state)
                        logger.info("已恢复插件状态 [%s]", name)
                    else:
                        self._preserved_state[name] = saved_state
                except Exception:
                    logger.exception("恢复插件状态失败: %s", name)

            # 恢复启用 / 禁用
            if not was_enabled:
                self._disabled.add(name)

            # 恢复配置
            if old_config and name not in self._configs:
                self._configs[name] = old_config

            logger.info("插件已热重载: %s v%s", name, new_plugin.version if new_plugin else "?")
            return new_plugin

    # ═══════════════════════════════════════════════════════
    #  启用 / 禁用
    # ═══════════════════════════════════════════════════════

    def enable_plugin(self, name: str) -> bool:
        """启用插件（解禁事件分发）。

        已启用的插件无影响。

        Returns
        -------
        bool
            False = 插件不存在。
        """
        with self._lock:
            if name not in self._loader._plugins:
                logger.warning("启用失败：插件 '%s' 不存在", name)
                return False
            if name in self._disabled:
                self._disabled.discard(name)
                # 同步配置
                if name in self._configs:
                    self._configs[name].enabled = True
                logger.info("插件已启用: %s", name)
            return True

    def disable_plugin(self, name: str) -> bool:
        """禁用插件（保留在注册表中，但不接收事件）。

        禁用的插件仍可通过 enable_plugin() 重新激活。
        不会调用 on_unload()，也不会清理状态。

        Returns
        -------
        bool
            False = 插件不存在。
        """
        with self._lock:
            if name not in self._loader._plugins:
                logger.warning("禁用失败：插件 '%s' 不存在", name)
                return False
            self._disabled.add(name)
            # 同步配置
            if name in self._configs:
                self._configs[name].enabled = False
            logger.info("插件已禁用: %s", name)
            return True

    def is_enabled(self, name: str) -> bool:
        """检查插件是否处于启用状态。"""
        return name in self._loader._plugins and name not in self._disabled

    # ═══════════════════════════════════════════════════════
    #  查询
    # ═══════════════════════════════════════════════════════

    def get_plugin(self, name: str) -> Optional[BasePlugin]:
        """按名称获取插件实例。"""
        return self._loader._plugins.get(name)

    def list_plugins(self) -> List[Dict[str, Any]]:
        """列出所有已加载插件及其元数据。

        Returns
        -------
        list[dict]
            每个 dict 包含: name, version, description, enabled,
            permissions, config, subscribed_events。
        """
        result: List[Dict[str, Any]] = []
        with self._lock:
            for name, plugin in self._loader._plugins.items():
                perms = self.plugin_permissions.get(name, PermissionSet())
                config = self._configs.get(name)

                # 尝试获取订阅的事件列表
                subscribed: List[str] = []
                try:
                    sub = plugin.subscribe()
                    subscribed = list(sub) if sub else []
                except Exception:
                    subscribed = []

                result.append({
                    "name": name,
                    "version": getattr(plugin, "version", "0.1.0"),
                    "description": getattr(plugin, "description", ""),
                    "enabled": name not in self._disabled,
                    "permissions": perms.to_list(),
                    "config": config.to_dict() if config else None,
                    "subscribed_events": subscribed,
                    "has_preserve_state": getattr(plugin, "PRESERVE_STATE", False),
                })
        return result

    @property
    def active_count(self) -> int:
        """当前注册的插件总数（含禁用）。"""
        return self._loader.active_count

    @property
    def enabled_count(self) -> int:
        """当前启用的插件数量。"""
        return self.active_count - len(self._disabled)

    @property
    def disabled_count(self) -> int:
        """当前禁用的插件数量。"""
        return len(self._disabled)

    def has_plugin(self, name: str) -> bool:
        """是否存在指定名称的插件。"""
        return self._loader.has_plugin(name)

    # ═══════════════════════════════════════════════════════
    #  权限系统
    # ═══════════════════════════════════════════════════════

    def _register_permissions(self, plugin: BasePlugin) -> None:
        """从插件实例提取并注册权限声明。

        优先级：类属性 PERMISSIONS > 配置中的 permissions > 空。
        """
        raw_perms: Iterable[str] = getattr(plugin, "PERMISSIONS", None) or []

        # 如果已有配置，合并配置中的权限
        config = self._configs.get(plugin.name)
        if config and config.permissions:
            from runtime.plugins.permissions import merge_permissions
            perm_set = merge_permissions(raw_perms, config.permissions)
        else:
            from runtime.plugins.permissions import PermissionSet
            perm_set = PermissionSet.from_strings(raw_perms)

        self.plugin_permissions[plugin.name] = perm_set
        logger.debug(
            "插件权限已注册 [%s]: %s", plugin.name, perm_set
        )

    def check_permission(self, plugin_name: str, permission: str) -> bool:
        """检查插件是否拥有指定权限。

        Parameters
        ----------
        plugin_name : str
            插件名称。
        permission : str
            权限标识符（枚举名或 value）。

        Returns
        -------
        bool
            是否拥有该权限。
        """
        perm_set = self.plugin_permissions.get(plugin_name)
        if perm_set is None:
            logger.warning("权限检查：插件 '%s' 未注册", plugin_name)
            return False
        return permission in perm_set

    def enforce_permission(self, plugin_name: str, *permissions: str) -> None:
        """强制校验权限，不通过抛出 PermissionError。

        Raises
        ------
        PermissionError
            插件缺少所需权限时。
        """
        perm_set = self.plugin_permissions.get(plugin_name)
        if perm_set is None:
            raise PermError(
                plugin=plugin_name,
                required=PermissionSet(permissions),
                message=f"插件 '{plugin_name}' 未注册或未声明任何权限",
            )

        missing = [p for p in permissions if p not in perm_set]
        if missing:
            raise PermError(
                plugin=plugin_name,
                required=PermissionSet(missing),
                granted=perm_set,
            )

    def grant_permission(self, plugin_name: str, *permissions: str) -> None:
        """运行时授予插件额外权限（追加到现有权限集）。

        注意：这是运行时操作，不会自动持久化到配置。
        """
        existing = self.plugin_permissions.get(plugin_name, PermissionSet())
        extra = PermissionSet.from_strings(permissions)
        self.plugin_permissions[plugin_name] = existing | extra
        logger.info("已授予额外权限 [%s]: %s", plugin_name, extra)

    def revoke_permission(self, plugin_name: str, *permissions: str) -> None:
        """运行时撤销插件权限。"""
        existing = self.plugin_permissions.get(plugin_name)
        if existing is None:
            return
        revoke_set = PermissionSet.from_strings(permissions)
        self.plugin_permissions[plugin_name] = existing - revoke_set
        logger.info("已撤销权限 [%s]: %s", plugin_name, revoke_set)

    # ═══════════════════════════════════════════════════════
    #  配置管理
    # ═══════════════════════════════════════════════════════

    def _apply_config_to_plugin(self, plugin: BasePlugin) -> None:
        """查找并应用插件的持久化配置。

        查找顺序：
          1. data/plugin_configs/{name}.json
          2. data/plugin_configs/{name}.yaml
          3. 若都不存在，从插件类属性创建默认配置
        """
        config = None
        for ext in (".json", ".yaml", ".yml"):
            candidate = self._config_dir / f"{plugin.name}{ext}"
            if candidate.exists():
                try:
                    config = load_plugin_config(str(candidate))
                    break
                except Exception:
                    logger.exception("加载配置失败: %s", candidate)

        if config is None:
            config = PluginConfig.from_plugin(plugin)

        # 根据配置的 enabled 字段设置启用/禁用状态
        if not config.enabled:
            self._disabled.add(plugin.name)

        self._configs[plugin.name] = config
        logger.debug("插件配置已应用 [%s]: enabled=%s", plugin.name, config.enabled)

    def get_plugin_config(self, name: str) -> Optional[PluginConfig]:
        """获取插件的运行时配置。"""
        return self._configs.get(name)

    def update_plugin_config(self, name: str, updates: Dict[str, Any]) -> bool:
        """更新插件配置（运行时 + 可选持久化）。"""
        config = self._configs.get(name)
        if config is None:
            logger.warning("更新配置失败：插件 '%s' 无配置", name)
            return False

        for key, value in updates.items():
            if hasattr(config, key):
                setattr(config, key, value)
            else:
                config.settings[key] = value

        # 同步启用/禁用
        if "enabled" in updates:
            if updates["enabled"]:
                self._disabled.discard(name)
            else:
                self._disabled.add(name)

        logger.debug("插件配置已更新 [%s]: %s", name, list(updates.keys()))
        return True

    def save_plugin_config(self, name: str, fmt: str = "json") -> Optional[str]:
        """持久化指定插件的配置。"""
        config = self._configs.get(name)
        if config is None:
            logger.warning("保存配置失败：插件 '%s' 无配置", name)
            return None
        return save_plugin_config(config, fmt=fmt)

    def save_all_configs(self, fmt: str = "json") -> int:
        """持久化所有插件的配置。返回保存数。"""
        count = 0
        for name in self._configs:
            try:
                self.save_plugin_config(name, fmt=fmt)
                count += 1
            except Exception:
                logger.exception("保存配置失败: %s", name)
        return count

    # ═══════════════════════════════════════════════════════
    #  事件分发（增强版——集成启用/禁用 + 权限检查）
    # ═══════════════════════════════════════════════════════

    def _is_plugin_active(self, name: str) -> bool:
        """检查插件是否应接收事件（已注册 且 未被禁用）。"""
        return name in self._loader._plugins and name not in self._disabled

    def _check_event_permissions(self, plugin: BasePlugin, event: str) -> bool:
        """检查插件是否有权处理某类事件（基于 _EVENT_PERMISSION_MAP）。

        Returns
        -------
        bool
            True = 有权限 或 事件无权限要求。
        """
        required = _EVENT_PERMISSION_MAP.get(event)
        if not required:
            return True

        granted = self.plugin_permissions.get(plugin.name, PermissionSet())
        has_all = granted.has_all(*required)
        if not has_all:
            # 不抛出异常，静默跳过
            logger.debug(
                "权限不足，跳过事件分发 [%s]: event=%s, required=%s, granted=%s",
                plugin.name, event, required, granted.as_strings(),
            )
            return False
        return True

    def dispatch(self, data: Dict[str, str]) -> None:
        """将事件分发给所有已启用且有权限的插件。

        相较于 PluginLoader.dispatch() 的增强：
          - 跳过禁用的插件
          - 执行事件级权限检查
        """
        import json as _json

        task_id = data.get("task_id", "")
        agent = data.get("agent", "")
        event = data.get("event", "")
        raw_payload = data.get("payload", "{}")

        payload: Any = {}
        try:
            payload = _json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        except (_json.JSONDecodeError, TypeError):
            payload = {"raw": raw_payload}

        with self._lock:
            plugins_snapshot = list(self._loader._plugins.items())

        for name, plugin in plugins_snapshot:
            # 1. 跳过禁用的插件
            if not self._is_plugin_active(name):
                continue

            # 2. 事件过滤（订阅白名单）
            try:
                subscribed = plugin.subscribe()
                if subscribed and event not in subscribed:
                    continue
            except Exception:
                logger.exception("插件 '%s' subscribe() 异常", name)
                continue

            # 3. 权限检查
            if not self._check_event_permissions(plugin, event):
                continue

            # 4. 分发
            try:
                plugin.on_event(task_id, agent, event, payload)
            except Exception:
                logger.exception("插件 '%s' 处理事件 '%s' 时异常", name, event)

    def dispatch_task_created(self, task: Dict[str, Any]) -> None:
        """分派 task_created 事件（仅已启用插件）。"""
        with self._lock:
            plugins_snapshot = list(self._loader._plugins.items())

        for name, plugin in plugins_snapshot:
            if not self._is_plugin_active(name):
                continue
            try:
                plugin.on_task_created(task)
            except Exception:
                logger.exception("插件 '%s' on_task_created 异常", name)

    def dispatch_task_updated(self, task: Dict[str, Any], old_status: str) -> None:
        """分派 task_updated 事件（仅已启用插件）。"""
        with self._lock:
            plugins_snapshot = list(self._loader._plugins.items())

        for name, plugin in plugins_snapshot:
            if not self._is_plugin_active(name):
                continue
            try:
                plugin.on_task_updated(task, old_status)
            except Exception:
                logger.exception("插件 '%s' on_task_updated 异常", name)

    # ═══════════════════════════════════════════════════════
    #  生命周期
    # ═══════════════════════════════════════════════════════

    def start(self) -> None:
        """启动插件运行时：扫描插件目录、读取配置、开始事件监听。

        此方法将替换 PluginLoader 的 _event_loop 逻辑，
        使用增强版的 dispatch() 处理事件。
        """
        if self._running:
            return

        # 扫描并加载插件目录
        loaded = self.load_plugin_dir()
        logger.info("PluginRuntime 启动：已加载 %d 个插件", loaded)

        # 启动后台事件监听线程（覆盖 PluginLoader 的 start/stop 行为）
        self._running = True
        self._listen_thread = threading.Thread(
            target=self._enhanced_event_loop, daemon=True, name="plugin-runtime-listener"
        )
        self._listen_thread.start()
        logger.info(
            "PluginRuntime 事件监听已启动 (启用=%d, 禁用=%d, 总计=%d)",
            self.enabled_count, self.disabled_count, self.active_count,
        )

    def stop(self) -> None:
        """停止插件运行时：停止事件监听、卸载所有插件、保存配置。"""
        self._running = False

        if hasattr(self, "_listen_thread") and self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=5)

        # 保存所有配置
        try:
            self.save_all_configs()
        except Exception:
            logger.exception("停止时保存配置异常")

        # 从 PluginLoader 中卸载所有插件（会调用 on_unload）
        # 注意：PluginLoader.stop() 会遍历 _plugins.keys() 并 unregister
        self._loader.stop()

        # 清理内存
        with self._lock:
            self.plugin_permissions.clear()
            self._configs.clear()
            self._disabled.clear()
            self._preserved_state.clear()

        logger.info("PluginRuntime 已停止")

    def _enhanced_event_loop(self) -> None:
        """增强版事件循环：使用 PluginRuntime 的 dispatch()。

        复刻 PluginLoader._event_loop() 的逻辑，
        但将 self._loader.dispatch(data) 替换为 self.dispatch(data)。
        """
        from runtime.event_bus import EventBus
        import uuid as _uuid

        bus = EventBus(
            group_name="plugin-group",
            consumer_name=f"plugin-{_uuid.uuid4().hex[:8]}",
        )

        def handler(data: Dict[str, str]):
            if not self._running:
                raise StopIteration()
            self.dispatch(data)

        try:
            bus.subscribe(handler)
        except StopIteration:
            pass
        except Exception:
            if self._running:
                logger.exception("PluginRuntime 事件监听异常")
