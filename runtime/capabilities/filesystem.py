"""
FilesystemCapability —— 文件系统读写能力。

提供沙箱化的文件系统操作，所有路径限定在 workspace_root 内，
杜绝路径遍历攻击。支持：
    - read_file(path)
    - write_file(path, content)
    - list_dir(path)
    - file_exists(path)
    - delete_file(path)

权限声明：
    PERMISSIONS = ["FILESYSTEM_READ", "FILESYSTEM_WRITE"]
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


from runtime.capabilities.base import BaseCapability


# ── 结果数据类 ──────────────────────────────────────────────

@dataclass
class FileInfo:
    """文件/目录信息。

    Attributes
    ----------
    name : str
        文件/目录名。
    path : str
        相对于 workspace_root 的路径。
    is_dir : bool
        是否为目录。
    size : int
        文件大小（字节），目录为 0。
    modified : float
        最后修改时间戳（epoch 秒）。
    """
    name: str
    path: str
    is_dir: bool = False
    size: int = 0
    modified: float = 0.0


@dataclass
class FileResult:
    """文件操作结果。

    Attributes
    ----------
    ok : bool
        操作是否成功。
    path : str
        操作的目标路径。
    content : str | None
        读取操作的文件内容。
    error : str | None
        错误信息（仅在 ok=False 时）。
    files : list[FileInfo]
        目录列表操作的文件列表。
    """
    ok: bool = True
    path: str = ""
    content: Optional[str] = None
    error: Optional[str] = None
    files: List[FileInfo] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "ok": self.ok,
            "path": self.path,
        }
        if self.content is not None:
            result["content"] = self.content
        if self.error:
            result["error"] = self.error
        if self.files:
            result["files"] = [
                {
                    "name": f.name,
                    "path": f.path,
                    "is_dir": f.is_dir,
                    "size": f.size,
                    "modified": f.modified,
                }
                for f in self.files
            ]
        return result


# ── FilesystemCapability ────────────────────────────────────

class FilesystemCapability(BaseCapability):
    """沙箱化文件系统操作能力。

    所有路径操作均限定在 workspace_root 内，
    自动阻止路径遍历攻击（如 "../../etc/passwd"）。

    使用示例::

        fs = FilesystemCapability(workspace_root="/workspace/task-abc")
        await fs.write_file("src/main.py", "print('hello')")
        content = await fs.read_file("src/main.py")
        files = await fs.list_dir("src")
    """

    PERMISSIONS = ["FILESYSTEM_READ", "FILESYSTEM_WRITE"]
    """所需权限声明。"""

    # ── 初始化 ────────────────────────────────────────────

    def __init__(
        self,
        workspace_root: Union[str, Path] = ".",
        max_file_size: int = 10_000_000,  # 10 MB
        allowed_extensions: Optional[List[str]] = None,
        auto_create_dirs: bool = True,
    ) -> None:
        """初始化文件系统能力。

        Parameters
        ----------
        workspace_root : str | Path
            工作区根路径，所有文件操作限定在此目录内。
        max_file_size : int
            允许读取/写入的最大文件大小（字节）。
        allowed_extensions : list[str] | None
            允许的文件扩展名列表。None 表示不限制。
        auto_create_dirs : bool
            写入文件时是否自动创建父目录。
        """
        super().__init__()
        self.workspace_root: Path = Path(workspace_root).resolve()
        self.max_file_size: int = max_file_size
        self.allowed_extensions: Optional[List[str]] = (
            [ext.lower().lstrip(".") for ext in allowed_extensions]
            if allowed_extensions is not None
            else None
        )
        self.auto_create_dirs: bool = auto_create_dirs

        # 确保 workspace 存在
        self.workspace_root.mkdir(parents=True, exist_ok=True)

        self.logger.debug(
            "FilesystemCapability 初始化: root=%s max_size=%d",
            self.workspace_root, self.max_file_size,
        )

    # ── 属性 ──────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "filesystem"

    @property
    def description(self) -> str:
        return "Sandboxed filesystem operations: read, write, list, delete files within a workspace root with path-traversal protection."

    # ── 核心执行 ─────────────────────────────────────────

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """根据 operation 参数路由到具体方法。

        Parameters
        ----------
        operation : str
            操作类型：'read' / 'write' / 'list' / 'exists' / 'delete'
        **kwargs
            各操作的具体参数。

        Returns
        -------
        FileResult | str
            操作结果。
        """
        operation = kwargs.pop("operation", None)
        if not operation:
            # fallback: 通过第一个位置参数推断
            operation = args[0] if args else "read"

        if operation == "read":
            return await self.read_file(kwargs.get("path", ""))
        elif operation == "write":
            return await self.write_file(
                kwargs.get("path", ""),
                kwargs.get("content", ""),
            )
        elif operation == "list":
            return await self.list_dir(kwargs.get("path", "."))
        elif operation == "exists":
            return await self.file_exists(kwargs.get("path", ""))
        elif operation == "delete":
            return await self.delete_file(kwargs.get("path", ""))
        else:
            raise ValueError(f"未知的文件系统操作: {operation!r}")

    # ── 读取文件 ─────────────────────────────────────────

    async def read_file(self, path: str) -> FileResult:
        """读取文件内容。

        Parameters
        ----------
        path : str
            相对于 workspace_root 的文件路径。

        Returns
        -------
        FileResult
            包含文件内容或错误信息。
        """
        if not self._enabled:
            return self._error_result(path, "FilesystemCapability 未启用")

        try:
            target = self._resolve_and_validate(path, must_exist=True)
            if target is None:
                return self._error_result(path, "路径校验失败")

            if target.is_dir():
                return self._error_result(path, "目标是一个目录，无法读取")

            file_size = target.stat().st_size
            if file_size > self.max_file_size:
                return self._error_result(
                    path,
                    f"文件过大 ({file_size} bytes > {self.max_file_size} bytes max)",
                )

            content = target.read_text(encoding="utf-8")
            self.logger.debug("读取文件: %s (%d bytes)", target, len(content))
            return FileResult(ok=True, path=str(target), content=content)

        except UnicodeDecodeError:
            return self._error_result(path, "无法以 UTF-8 解码文件内容")
        except PermissionError:
            return self._error_result(path, "没有读取权限")
        except Exception as exc:
            self.logger.exception("读取文件异常: %s", path)
            return self._error_result(path, str(exc))

    # ── 写入文件 ─────────────────────────────────────────

    async def write_file(self, path: str, content: str) -> FileResult:
        """写入文件内容。自动创建父目录。

        Parameters
        ----------
        path : str
            相对于 workspace_root 的文件路径。
        content : str
            要写入的文件内容。

        Returns
        -------
        FileResult
            写入结果。
        """
        if not self._enabled:
            return self._error_result(path, "FilesystemCapability 未启用")

        try:
            target = self._resolve_and_validate(path, must_exist=False)
            if target is None:
                return self._error_result(path, "路径校验失败")

            # 检查文件大小
            content_bytes = len(content.encode("utf-8"))
            if content_bytes > self.max_file_size:
                return self._error_result(
                    path,
                    f"内容过大 ({content_bytes} bytes > {self.max_file_size} bytes max)",
                )

            # 检查扩展名
            if not self._check_extension(target):
                return self._error_result(
                    path,
                    f"不允许的文件扩展名: {target.suffix} (允许: {self.allowed_extensions})",
                )

            # 自动创建父目录
            if self.auto_create_dirs:
                target.parent.mkdir(parents=True, exist_ok=True)

            # 确保内容以换行结尾
            cleaned = content.replace("\r\n", "\n")
            if not cleaned.endswith("\n"):
                cleaned += "\n"

            target.write_text(cleaned, encoding="utf-8")
            self.logger.debug("写入文件: %s (%d bytes)", target, len(cleaned))
            return FileResult(ok=True, path=str(target))

        except PermissionError:
            return self._error_result(path, "没有写入权限")
        except Exception as exc:
            self.logger.exception("写入文件异常: %s", path)
            return self._error_result(path, str(exc))

    # ── 目录列表 ─────────────────────────────────────────

    async def list_dir(self, path: str = ".") -> FileResult:
        """列出目录内容。

        Parameters
        ----------
        path : str
            相对于 workspace_root 的目录路径。

        Returns
        -------
        FileResult
            包含 files 列表的结果。
        """
        if not self._enabled:
            return self._error_result(path, "FilesystemCapability 未启用")

        try:
            target = self._resolve_and_validate(path, must_exist=True)
            if target is None:
                return self._error_result(path, "路径校验失败")

            if not target.is_dir():
                return self._error_result(path, "目标不是一个目录")

            files: List[FileInfo] = []
            for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
                rel_path = entry.relative_to(self.workspace_root)
                files.append(FileInfo(
                    name=entry.name,
                    path=rel_path.as_posix(),
                    is_dir=entry.is_dir(),
                    size=entry.stat().st_size if entry.is_file() else 0,
                    modified=entry.stat().st_mtime,
                ))

            self.logger.debug("列出目录: %s (%d 条目)", target, len(files))
            return FileResult(ok=True, path=str(target), files=files)

        except PermissionError:
            return self._error_result(path, "没有读取权限")
        except Exception as exc:
            self.logger.exception("列出目录异常: %s", path)
            return self._error_result(path, str(exc))

    # ── 文件存在检查 ─────────────────────────────────────

    async def file_exists(self, path: str) -> FileResult:
        """检查文件或目录是否存在。

        Parameters
        ----------
        path : str
            相对于 workspace_root 的路径。

        Returns
        -------
        FileResult
            ok=True 表示文件存在。
        """
        if not self._enabled:
            return self._error_result(path, "FilesystemCapability 未启用")

        try:
            target = self._resolve_and_validate(path, must_exist=False)
            if target is None:
                return self._error_result(path, "路径校验失败")

            exists = target.exists()
            is_dir = target.is_dir() if exists else False
            self.logger.debug("检查存在: %s → %s", target, exists)
            return FileResult(
                ok=exists,
                path=str(target),
                files=[
                    FileInfo(
                        name=target.name,
                        path=str(target.relative_to(self.workspace_root)),
                        is_dir=is_dir,
                        size=target.stat().st_size if exists and not is_dir else 0,
                        modified=target.stat().st_mtime if exists else 0.0,
                    )
                ] if exists else [],
            )

        except Exception as exc:
            self.logger.exception("检查文件存在异常: %s", path)
            return self._error_result(path, str(exc))

    # ── 删除文件 ─────────────────────────────────────────

    async def delete_file(self, path: str) -> FileResult:
        """删除文件或空目录。

        Parameters
        ----------
        path : str
            相对于 workspace_root 的路径。

        Returns
        -------
        FileResult
            删除结果。
        """
        if not self._enabled:
            return self._error_result(path, "FilesystemCapability 未启用")

        try:
            target = self._resolve_and_validate(path, must_exist=True)
            if target is None:
                return self._error_result(path, "路径校验失败")

            # 防止删除 workspace_root 自身
            if target.resolve() == self.workspace_root.resolve():
                return self._error_result(path, "不允许删除 workspace 根目录")

            # 防止删除 .git 目录
            if ".git" in target.parts:
                return self._error_result(path, "不允许删除 .git 目录")

            if target.is_dir():
                # 只允许删除空目录
                if any(target.iterdir()):
                    return self._error_result(path, "目录非空，无法删除")
                target.rmdir()
                self.logger.debug("删除目录: %s", target)
            else:
                target.unlink()
                self.logger.debug("删除文件: %s", target)

            return FileResult(ok=True, path=str(target))

        except PermissionError:
            return self._error_result(path, "没有删除权限")
        except Exception as exc:
            self.logger.exception("删除文件异常: %s", path)
            return self._error_result(path, str(exc))

    # ── 校验与清理 ───────────────────────────────────────

    def validate(self) -> None:
        """校验前置条件：workspace_root 存在且可读写。

        Raises
        ------
        RuntimeError
            若 workspace_root 不满足条件。
        """
        if not self.workspace_root.exists():
            raise RuntimeError(
                f"FilesystemCapability 工作区不存在: {self.workspace_root}"
            )
        if not os.access(self.workspace_root, os.R_OK):
            raise RuntimeError(
                f"FilesystemCapability 工作区不可读: {self.workspace_root}"
            )
        if not os.access(self.workspace_root, os.W_OK):
            raise RuntimeError(
                f"FilesystemCapability 工作区不可写: {self.workspace_root}"
            )

    def sanitize(self, *args: Any) -> Tuple[Any, ...]:
        """对输入参数做基本清理。"""
        return args

    # ── 内部方法 ─────────────────────────────────────────

    def _resolve_and_validate(
        self, relpath: str, must_exist: bool = False
    ) -> Optional[Path]:
        """解析并校验路径，确保在 workspace_root 内。

        Parameters
        ----------
        relpath : str
            相对于 workspace_root 的相对路径。
        must_exist : bool
            是否要求路径必须已存在。

        Returns
        -------
        Path | None
            解析后的绝对路径；若路径越权或不存在则返回 None。
        """
        clean = relpath.strip().lstrip("/\\")
        if not clean:
            return self.workspace_root

        # 分解路径并过滤危险组件
        parts = clean.replace("\\", "/").split("/")
        safe_parts = [p for p in parts if p not in ("", ".", "..")]

        resolved = self.workspace_root.joinpath(*safe_parts).resolve()

        # 路径遍历保护
        workspace_resolved = self.workspace_root.resolve()
        if not str(resolved).startswith(str(workspace_resolved)):
            self.logger.warning(
                "路径遍历拦截: %s → %s (workspace=%s)",
                relpath, resolved, workspace_resolved,
            )
            return None

        if must_exist and not resolved.exists():
            self.logger.debug("路径不存在: %s", resolved)
            return None

        return resolved

    def _check_extension(self, filepath: Path) -> bool:
        """检查文件扩展名是否在允许列表中。"""
        if self.allowed_extensions is None:
            return True
        ext = filepath.suffix.lstrip(".").lower()
        if not ext:
            return True  # 无扩展名默认允许
        return ext in self.allowed_extensions

    @staticmethod
    def _error_result(path: str, error: str) -> FileResult:
        """生成错误结果。"""
        return FileResult(ok=False, path=path, error=error)
