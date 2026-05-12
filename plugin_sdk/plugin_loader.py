import importlib
import json
import logging
import os
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type

from .base_plugin import BasePlugin, PluginContext

logger = logging.getLogger("plugin_sdk.loader")


class PluginLoader:
    """插件发现、加载与事件路由引擎。

    使用方式:
        from plugin_sdk import PluginLoader, PluginContext
        from runtime import EventBus, TaskManager
        from workspace.manager import WorkspaceManager

        ctx = PluginContext(EventBus(), TaskManager(), WorkspaceManager())
        loader = PluginLoader(ctx)
        loader.scan_plugin_dir("plugins/")
        loader.start()
        # ... Runtime 运行中 ...
        loader.stop()
    """

    def __init__(self, ctx: PluginContext, plugin_dir: str = "plugins"):
        self.ctx = ctx
        self.plugin_dir = Path(plugin_dir)
        self._plugins: Dict[str, BasePlugin] = {}
        self._running = False
        self._listen_thread: Optional[threading.Thread] = None

    # ── 插件注册 ────────────────────────────────────────────

    def register(self, plugin: BasePlugin) -> None:
        """手动注册一个插件实例。"""
        if not plugin.name:
            raise ValueError("插件必须设置 name 属性")
        if plugin.name in self._plugins:
            logger.warning("插件 '%s' 已存在，将覆盖", plugin.name)
        plugin._ctx = self.ctx
        self._plugins[plugin.name] = plugin
        try:
            plugin.on_load()
        except Exception:
            logger.exception("插件 '%s' on_load() 失败", plugin.name)
        logger.info("插件已注册: %s v%s", plugin.name, plugin.version)

    def unregister(self, name: str) -> bool:
        """按名称卸载插件。"""
        plugin = self._plugins.pop(name, None)
        if plugin:
            try:
                plugin.on_unload()
            except Exception:
                logger.exception("插件 '%s' on_unload() 失败", name)
            logger.info("插件已卸载: %s", name)
            return True
        return False

    # ── 文件扫描 ────────────────────────────────────────────

    def scan_plugin_dir(self, plugin_dir: Optional[str] = None) -> int:
        """扫描插件目录，自动发现并加载所有 .py 插件文件。

        约定：
          - 文件名为插件名（不含 .py）
          - 文件中必须定义一个名为 Plugin 的类，继承 BasePlugin
          - 或者模块顶层直接定义一个 plugin 实例

        Returns
        -------
        int
            成功加载的插件数量。
        """
        directory = Path(plugin_dir) if plugin_dir else self.plugin_dir
        directory.mkdir(parents=True, exist_ok=True)

        loaded = 0
        for filepath in sorted(directory.glob("*.py")):
            if filepath.name.startswith("_"):
                continue

            module_name = filepath.stem
            try:
                spec = importlib.util.spec_from_file_location(
                    f"plugin_{module_name}", str(filepath)
                )
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)
                sys.modules[f"plugin_{module_name}"] = module
                spec.loader.exec_module(module)

                # 查找 BasePlugin 子类或 plugin 实例
                plugin_instance = self._extract_plugin(module, module_name)
                if plugin_instance:
                    if not plugin_instance.name:
                        plugin_instance.name = module_name
                    self.register(plugin_instance)
                    loaded += 1

            except Exception:
                logger.exception("加载插件文件失败: %s", filepath)

        return loaded

    def _extract_plugin(
        self, module: Any, module_name: str
    ) -> Optional[BasePlugin]:
        # 1. 先找名为 Plugin 的类
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BasePlugin)
                and attr is not BasePlugin
            ):
                try:
                    return attr()
                except Exception:
                    logger.exception("实例化插件类失败: %s", attr_name)
                    return None

        # 2. 再找名为 plugin 的实例
        if hasattr(module, "plugin") and isinstance(module.plugin, BasePlugin):
            return module.plugin

        return None

    # ── 事件分发 ────────────────────────────────────────────

    def dispatch(self, data: Dict[str, str]) -> None:
        """将一条事件分发给所有已注册插件。

        Parameters
        ----------
        data : dict
            EventBus 事件数据，包含 task_id / agent / event / payload / timestamp。
        """
        task_id = data.get("task_id", "")
        agent = data.get("agent", "")
        event = data.get("event", "")
        raw_payload = data.get("payload", "{}")

        payload: Any = {}
        try:
            payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        except (json.JSONDecodeError, TypeError):
            payload = {"raw": raw_payload}

        for plugin in list(self._plugins.values()):
            try:
                # 事件过滤
                subscribed = plugin.subscribe()
                if subscribed and event not in subscribed:
                    continue
                plugin.on_event(task_id, agent, event, payload)
            except Exception:
                logger.exception(
                    "插件 '%s' 处理事件 '%s' 时异常", plugin.name, event
                )

    def dispatch_task_created(self, task: Dict[str, Any]) -> None:
        for plugin in list(self._plugins.values()):
            try:
                plugin.on_task_created(task)
            except Exception:
                logger.exception("插件 '%s' on_task_created 异常", plugin.name)

    def dispatch_task_updated(
        self, task: Dict[str, Any], old_status: str
    ) -> None:
        for plugin in list(self._plugins.values()):
            try:
                plugin.on_task_updated(task, old_status)
            except Exception:
                logger.exception("插件 '%s' on_task_updated 异常", plugin.name)

    # ── 后台事件监听 ────────────────────────────────────────

    def start(self) -> None:
        """启动后台线程，从 Redis Streams 监听事件并分发给插件。"""
        if self._running:
            return
        self._running = True
        self._listen_thread = threading.Thread(
            target=self._event_loop, daemon=True, name="plugin-listener"
        )
        self._listen_thread.start()
        logger.info("PluginLoader 事件监听已启动 (%d 个插件)", len(self._plugins))

    def stop(self) -> None:
        """停止后台监听线程。"""
        self._running = False
        if self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=5)
        for name in list(self._plugins.keys()):
            self.unregister(name)
        logger.info("PluginLoader 已停止")

    def _event_loop(self) -> None:
        """后台线程：通过 EventBus.subscribe() 持续监听事件。"""
        # 为插件创建一个独立的 EventBus 消费者
        import uuid
        bus = EventBus(
            group_name="plugin-group",
            consumer_name=f"plugin-{uuid.uuid4().hex[:8]}",
        )

        def handle(data: Dict[str, str]):
            if not self._running:
                raise StopIteration()
            self.dispatch(data)

        try:
            bus.subscribe(handle)
        except StopIteration:
            pass
        except Exception:
            if self._running:
                logger.exception("PluginLoader 事件监听异常")

    # ── 信息查询 ────────────────────────────────────────────

    @property
    def active_count(self) -> int:
        return len(self._plugins)

    def list_plugins(self) -> List[Dict[str, str]]:
        return [
            {"name": p.name, "version": p.version, "description": p.description}
            for p in self._plugins.values()
        ]

    def has_plugin(self, name: str) -> bool:
        return name in self._plugins
