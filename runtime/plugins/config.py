"""
Plugin Configuration — 插件配置的加载、持久化与运行时管理。

每个插件可以有一个独立的配置文件（YAML 或 JSON），PluginRuntime
在加载插件时自动读取配置；通过 save_plugin_config() 可持久化运行时变更。

配置结构示例 (plugin_config.json):
    {
        "name": "my-plugin",
        "version": "1.0.0",
        "enabled": true,
        "permissions": ["FILESYSTEM_READ", "EVENT_EMIT"],
        "settings": {"timeout": 30, "log_level": "INFO"},
        "auto_reload": false
    }
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger("runtime.plugins.config")

# 默认配置目录
DEFAULT_CONFIG_DIR = Path("data/plugin_configs")


# ── PluginConfig ────────────────────────────────────────────

@dataclass
class PluginConfig:
    """单个插件的运行时配置。

    Attributes
    ----------
    name : str
        插件名称（唯一标识）。
    version : str
        插件版本号，如 "1.0.0"。
    enabled : bool
        是否启用。禁用的插件不会收到事件。
    permissions : List[str]
        插件声明的权限列表（枚举名）。
    settings : Dict[str, Any]
        插件自定义设置键值对。
    auto_reload : bool
        是否启用自动热重载（文件变动时自动 reload）。
    config_path : Optional[str]
        配置文件路径（用于回写）。
    """

    name: str = ""
    version: str = "0.1.0"
    enabled: bool = True
    permissions: List[str] = field(default_factory=list)
    settings: Dict[str, Any] = field(default_factory=dict)
    auto_reload: bool = False
    config_path: Optional[str] = None

    # ── 快捷访问 ─────────────────────────────────────────

    def get(self, key: str, default: Any = None) -> Any:
        """从 settings 中读取配置值。"""
        return self.settings.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """向 settings 写入 / 更新配置值。"""
        self.settings[key] = value

    def update_settings(self, updates: Dict[str, Any]) -> None:
        """批量更新 settings。"""
        self.settings.update(updates)

    def has_permission(self, permission: str) -> bool:
        """检查是否声明了某个权限。"""
        from runtime.plugins.permissions import PermissionSet
        return PermissionSet.from_strings(self.permissions).has_all(permission)

    # ── 序列化 ───────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """转为字典，排除 config_path。"""
        d = asdict(self)
        d.pop("config_path", None)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any], config_path: Optional[str] = None) -> "PluginConfig":
        """从字典构建 PluginConfig。"""
        return cls(
            name=data.get("name", ""),
            version=data.get("version", "0.1.0"),
            enabled=data.get("enabled", True),
            permissions=data.get("permissions", []),
            settings=data.get("settings", {}),
            auto_reload=data.get("auto_reload", False),
            config_path=config_path,
        )

    @classmethod
    def from_plugin(cls, plugin: Any) -> "PluginConfig":
        """从插件实例提取配置（用于首次注册）。"""
        perms = getattr(plugin, "PERMISSIONS", None) or []
        if isinstance(perms, str):
            perms = [perms]
        return cls(
            name=getattr(plugin, "name", ""),
            version=getattr(plugin, "version", "0.1.0"),
            enabled=True,
            permissions=list(perms),
            settings=getattr(plugin, "CONFIG", None) or {},
        )

    # ── 校验 ─────────────────────────────────────────────

    def validate(self) -> List[str]:
        """校验配置完整性，返回错误列表（空 = 通过）。"""
        errors: List[str] = []
        if not self.name:
            errors.append("name 不能为空")
        if not self.version:
            errors.append("version 不能为空")
        if not isinstance(self.permissions, list):
            errors.append("permissions 必须是 list")
        if not isinstance(self.settings, dict):
            errors.append("settings 必须是 dict")
        return errors

    def __repr__(self) -> str:
        status = "enabled" if self.enabled else "disabled"
        return (
            f"PluginConfig(name={self.name!r}, version={self.version!r}, "
            f"{status}, perms={len(self.permissions)}, "
            f"auto_reload={self.auto_reload})"
        )


# ── 加载 / 保存 ────────────────────────────────────────────

def _ensure_config_dir(config_dir: Optional[str] = None) -> Path:
    """确保配置目录存在并返回 Path。"""
    directory = Path(config_dir) if config_dir else DEFAULT_CONFIG_DIR
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _guess_format(filepath: str) -> str:
    """根据扩展名推断文件格式，默认 json。"""
    if filepath.endswith((".yaml", ".yml")):
        return "yaml"
    return "json"


def _load_yaml(filepath: str) -> Dict[str, Any]:
    """加载 YAML 文件。"""
    try:
        import yaml
        with open(filepath, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, dict) else {}
    except ImportError:
        raise ImportError(
            "加载 YAML 配置文件需要 PyYAML: pip install pyyaml"
        )
    except Exception:
        logger.exception("加载 YAML 配置失败: %s", filepath)
        return {}


def _load_json(filepath: str) -> Dict[str, Any]:
    """加载 JSON 文件。"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.exception("加载 JSON 配置失败: %s", filepath)
        return {}


def load_plugin_config(
    path: str,
    defaults: Optional[Dict[str, Any]] = None,
) -> PluginConfig:
    """从文件加载插件配置。

    支持 .json / .yaml / .yml 格式，自动根据扩展名选择解析器。

    Parameters
    ----------
    path : str
        配置文件路径。
    defaults : dict, optional
        默认值，在文件数据之上合并。

    Returns
    -------
    PluginConfig
    """
    filepath = str(Path(path).resolve())
    fmt = _guess_format(filepath)

    if fmt == "yaml":
        data = _load_yaml(filepath)
    else:
        data = _load_json(filepath)

    if defaults:
        merged = {**defaults, **data}
    else:
        merged = data

    config = PluginConfig.from_dict(merged, config_path=filepath)

    # 校验
    errors = config.validate()
    if errors:
        logger.warning("配置校验警告 [%s]: %s", filepath, "; ".join(errors))

    logger.debug("已加载插件配置: %s (%s)", config.name, filepath)
    return config


def save_plugin_config(
    config: PluginConfig,
    path: Optional[str] = None,
    fmt: str = "json",
) -> str:
    """持久化插件配置到文件。

    Parameters
    ----------
    config : PluginConfig
        要保存的配置对象。
    path : str, optional
        目标文件路径。若不指定，使用 config.config_path；若仍为空，自动生成。
    fmt : str
        输出格式，"json" 或 "yaml"。

    Returns
    -------
    str
        实际写入的文件路径。
    """
    # 确定目标路径
    if path:
        filepath = str(Path(path).resolve())
    elif config.config_path:
        filepath = config.config_path
    else:
        _ensure_config_dir()
        ext = ".yaml" if fmt == "yaml" else ".json"
        filepath = str(DEFAULT_CONFIG_DIR / f"{config.name}{ext}")

    # 更新 config_path
    config.config_path = filepath

    data = config.to_dict()
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    if fmt == "yaml":
        try:
            import yaml
            with open(filepath, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
        except ImportError:
            raise ImportError("保存 YAML 需要 PyYAML: pip install pyyaml")
    else:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info("插件配置已保存: %s -> %s", config.name, filepath)
    return filepath


def load_all_configs(
    config_dir: Optional[str] = None,
) -> Dict[str, PluginConfig]:
    """批量加载配置目录下的所有插件配置文件。

    Parameters
    ----------
    config_dir : str, optional
        配置目录路径，默认为 data/plugin_configs/。

    Returns
    -------
    dict[str, PluginConfig]
        {plugin_name: PluginConfig}
    """
    directory = _ensure_config_dir(config_dir)
    configs: Dict[str, PluginConfig] = {}

    for pattern in ("*.json", "*.yaml", "*.yml"):
        for filepath in sorted(directory.glob(pattern)):
            try:
                config = load_plugin_config(str(filepath))
                if config.name:
                    configs[config.name] = config
            except Exception:
                logger.exception("加载配置失败: %s", filepath)

    logger.info("从 %s 加载了 %d 个插件配置", directory, len(configs))
    return configs


def delete_plugin_config(plugin_name: str, config_dir: Optional[str] = None) -> bool:
    """删除指定插件的配置文件。

    Returns
    -------
    bool
        是否成功删除。
    """
    directory = _ensure_config_dir(config_dir)
    deleted = False

    for pattern in ("*.json", "*.yaml", "*.yml"):
        for filepath in directory.glob(pattern):
            try:
                config = load_plugin_config(str(filepath))
                if config.name == plugin_name:
                    os.remove(filepath)
                    logger.info("已删除配置文件: %s", filepath)
                    deleted = True
            except Exception:
                continue

    return deleted
