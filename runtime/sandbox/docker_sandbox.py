"""Docker-based sandbox runtime for isolated task execution.

Each AI-agent task gets its own transient Docker container.  The sandbox
supports:

* Starting a container with resource limits
* Executing shell commands inside the container
* Copying files in/out (host ↔ container)
* Exporting build artifacts back to the host
* Cleanup (stop + remove container + remove optional image)

Usage::

    from runtime.sandbox.docker_sandbox import DockerSandbox
    from runtime.sandbox.resource_limits import ResourceLimits

    sb = DockerSandbox()
    sb.create("task-abc", image="python:3.11-slim")
    sb.write_file("src/main.py", 'print("hello from sandbox")')
    result = sb.execute("python src/main.py", timeout=30)
    print(result["stdout"])  # "hello from sandbox"
    sb.cleanup()
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from .resource_limits import FileLimits, ResourceLimits

logger = logging.getLogger("runtime.sandbox")


# ── Exception hierarchy ───────────────────────────────────────

class SandboxError(Exception):
    """Base exception for all sandbox-related failures."""


class SandboxNotCreatedError(SandboxError):
    """Raised when an operation is attempted before ``create()`` succeeds."""


class SandboxExecTimeoutError(SandboxError):
    """Raised when a command inside the container exceeds its timeout."""

    def __init__(self, command: str, timeout: int, container_id: str):
        super().__init__(
            f"容器 {container_id[:12]} 内命令超时 ({timeout}s): {command}"
        )
        self.command = command
        self.timeout = timeout
        self.container_id = container_id


class DockerUnavailableError(SandboxError):
    """Raised when the docker CLI is not found on the host."""


# ── Sandbox ───────────────────────────────────────────────────

class DockerSandbox:
    """Manage a single transient Docker container for a task.

    Relies on the ``docker`` CLI being installed and accessible.

    Design goals
    ------------
    * Minimal dependency — uses ``subprocess`` to talk to Docker CLI
      (no ``docker-py`` SDK needed).
    * Immutable after creation — the image, resource limits, mount mode
      are set once in ``create()`` and cannot change.
    * Safe by default — network disabled, PID limit enforced, files
      checked against ``FileLimits`` before being written.

    Parameters
    ----------
    default_image : str
        Image name used when ``create()`` is called without an explicit image.
        Default ``python:3.11-slim``.

    Attributes
    ----------
    container_id : str or None
        Docker container ID (64-char hex).  ``None`` before ``create()``.
    limits : ResourceLimits or None
        Active resource constraints.
    """

    def __init__(self, default_image: str = "python:3.11-slim") -> None:
        self.default_image = default_image
        self.container_id: Optional[str] = None
        self.limits: Optional[ResourceLimits] = None
        self._task_id: Optional[str] = None
        self._workspace_host_path: Optional[Path] = None
        self._workspace_readonly: bool = False
        self._network_allowed: bool = False
        self._created_at: float = 0.0

    # ═══════════════════════════════════════════════════════════
    #  Lifecycle
    # ═══════════════════════════════════════════════════════════

    def create(
        self,
        task_id: str,
        image: Optional[str] = None,
        limits: Optional[ResourceLimits] = None,
        workspace_host_path: Optional[str] = None,
        workspace_readonly: bool = False,
        network_access: Optional[bool] = None,
        pull: bool = True,
    ) -> str:
        """Create (pull + run) a new Docker container for *task_id*.

        Parameters
        ----------
        task_id : str
            Unique task identifier used to derive the container name.
        image : str, optional
            Docker image. Defaults to ``self.default_image``.
        limits : ResourceLimits, optional
            Resource constraints. Defaults to ``ResourceLimits()``.
        workspace_host_path : str, optional
            Host directory to mount as ``/workspace`` inside the container.
            If omitted, a temp directory is created.
        workspace_readonly : bool
            If ``True`` the workspace volume is mounted read-only.
        network_access : bool, optional
            Override ``limits.network_access``.
        pull : bool
            If ``True``, run ``docker pull`` before creating the container.

        Returns
        -------
        str
            Docker container ID (64-char hex).

        Raises
        ------
        DockerUnavailableError
            If ``docker`` CLI is not on PATH.
        SandboxError
            If ``pull`` fails or the container fails to start.
        """
        _ensure_docker_available()

        self._task_id = task_id
        self.limits = limits or ResourceLimits()
        self._network_allowed = (
            network_access if network_access is not None
            else self.limits.network_access
        )
        self._workspace_readonly = workspace_readonly

        image = image or self.default_image

        # Pull image
        if pull:
            logger.info("拉取 Docker 镜像: %s", image)
            result = _run(["docker", "pull", image], timeout=300)
            if result.returncode != 0:
                raise SandboxError(
                    f"docker pull {image} 失败 (code={result.returncode}): "
                    f"{result.stderr.strip()}"
                )

        # Prepare workspace host directory
        if workspace_host_path:
            self._workspace_host_path = Path(workspace_host_path).resolve()
            self._workspace_host_path.mkdir(parents=True, exist_ok=True)
        else:
            base = Path(tempfile.gettempdir()) / "ai-runtime-sandboxes"
            base.mkdir(parents=True, exist_ok=True)
            self._workspace_host_path = base / task_id
            self._workspace_host_path.mkdir(parents=True, exist_ok=True)

        container_name = f"ai-runtime-{task_id}-{uuid.uuid4().hex[:8]}"
        logger.info("创建工作区: %s", self._workspace_host_path)

        # Build docker run args
        cmd = ["docker", "run", "-d", "--rm", "--name", container_name]

        # Resource limits
        cmd.extend(self.limits.to_docker_args())

        # Network
        if not self._network_allowed:
            cmd.extend(["--network", "none"])

        # Environment
        cmd.extend(self.limits.to_env_args())

        # Workspace volume
        mount_opts = ""
        if self._workspace_readonly:
            mount_opts = ":ro"
        cmd.extend([
            "-v",
            f"{self._workspace_host_path}:/workspace{mount_opts}",
            "-w", "/workspace",
        ])

        # Keep container alive with a sleep loop
        cmd.extend([image, "sleep", "infinity"])

        logger.debug("docker run 命令: %s", " ".join(cmd))
        result = _run(cmd, timeout=30)
        if result.returncode != 0:
            raise SandboxError(
                f"docker run 失败 (code={result.returncode}): {result.stderr.strip()}"
            )

        self.container_id = result.stdout.strip()
        self._created_at = time.time()

        # Validate the container actually started
        if not self._container_exists():
            raise SandboxError(
                f"容器 {self.container_id[:12]} 已返回 ID 但未在 docker ps 中检测到"
            )

        logger.info(
            "容器已创建: id=%s name=%s image=%s",
            self.container_id[:12], container_name, image,
        )
        return self.container_id

    # ═══════════════════════════════════════════════════════════
    #  Execution
    # ═══════════════════════════════════════════════════════════

    def execute(
        self,
        command: str,
        timeout: Optional[int] = None,
        workdir: str = "/workspace",
    ) -> Dict[str, Any]:
        """Run *command* inside the container via ``docker exec``.

        Parameters
        ----------
        command : str
            Shell command to run.
        timeout : int, optional
            Timeout in seconds.  Falls back to ``self.limits.timeout_sec``.
        workdir : str
            Working directory inside the container.  Default ``/workspace``.

        Returns
        -------
        dict
            ``{"command": str, "exit_code": int, "stdout": str, "stderr": str,
              "container_id": str, "elapsed_ms": float}``
        """
        self._require_container()

        timeout = timeout if timeout is not None else (
            self.limits.timeout_sec if self.limits else 300
        )

        logger.info("执行命令 (timeout=%ds): %s", timeout, command)
        start = time.monotonic()

        try:
            result = _run(
                ["docker", "exec", "-w", workdir, self.container_id,
                 "sh", "-c", command],
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            raise SandboxExecTimeoutError(command, timeout, self.container_id)

        elapsed_ms = (time.monotonic() - start) * 1000

        return {
            "command": command,
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "container_id": self.container_id,
            "elapsed_ms": round(elapsed_ms, 1),
        }

    # ═══════════════════════════════════════════════════════════
    #  File I/O (host ↔ container via workspace volume)
    # ═══════════════════════════════════════════════════════════

    def write_file(self, path: str, content: str) -> str:
        """Write *content* to *path* inside the container's workspace.

        Because the workspace is a bind-mount, this method writes to the host
        directory directly for speed (no ``docker cp`` overhead).

        Parameters
        ----------
        path : str
            File path relative to ``/workspace`` (e.g. ``src/main.py``).
        content : str
            Text content to write.

        Returns
        -------
        str
            Absolute host path of the written file.

        Raises
        ------
        SandboxError
            If the file violates ``FileLimits`` (extension or size).
        """
        self._require_container()

        # Sanitise path
        parts = Path(path).parts
        safe_parts = [p for p in parts if p not in ("", ".", "..")]
        clean_path = Path(*safe_parts) if safe_parts else Path("file.txt")

        # Check file limits
        fl = self.limits.file_limits if self.limits else FileLimits()
        if not fl.is_extension_allowed(clean_path.name):
            allowed = ", ".join(fl.allowed_extensions)
            raise SandboxError(
                f"文件扩展名不允许: '{clean_path.name}'."
                f"允许的扩展名: [{allowed}]"
            )
        if not fl.is_size_allowed(len(content.encode("utf-8"))):
            raise SandboxError(
                f"文件大小超出限制: {len(content.encode('utf-8'))} bytes "
                f"(最大 {fl.max_file_size} bytes)"
            )

        host_target = self._workspace_host_path / clean_path
        host_target.parent.mkdir(parents=True, exist_ok=True)

        cleaned = content.replace("\r\n", "\n")
        if not cleaned.endswith("\n"):
            cleaned += "\n"
        host_target.write_text(cleaned, encoding="utf-8")

        logger.debug("写入文件: %s (%d bytes)", host_target, len(cleaned))
        return str(host_target)

    def read_file(self, path: str) -> str:
        """Read a file from the container's workspace.

        Reads directly from the host bind-mount.

        Parameters
        ----------
        path : str
            File path relative to ``/workspace``.

        Returns
        -------
        str
            File content.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        """
        self._require_container()

        host_path = self._resolve_workspace_path(path)
        if not host_path.is_file():
            raise FileNotFoundError(f"容器工作区文件不存在: {path}")
        return host_path.read_text(encoding="utf-8")

    # ═══════════════════════════════════════════════════════════
    #  Artifact export
    # ═══════════════════════════════════════════════════════════

    def export_artifacts(self, dest_dir: str, glob_pattern: str = "*") -> List[str]:
        """Copy matching files from the container workspace to *dest_dir*.

        Uses ``shutil.copy2`` via the host bind-mount (fast, no ``docker cp``).

        Parameters
        ----------
        dest_dir : str
            Destination directory on the host.
        glob_pattern : str
            Glob pattern relative to workspace root (e.g. ``dist/*.whl``).

        Returns
        -------
        list of str
            Absolute paths of copied files.
        """
        self._require_container()

        dest = Path(dest_dir).resolve()
        dest.mkdir(parents=True, exist_ok=True)

        source_root = self._workspace_host_path
        copied: List[str] = []

        for src_file in source_root.glob(glob_pattern):
            if src_file.is_file():
                rel = src_file.relative_to(source_root)
                target = dest / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, target)
                copied.append(str(target))
                logger.debug("导出: %s → %s", src_file, target)

        logger.info("导出 %d 个文件到 %s", len(copied), dest_dir)
        return copied

    # ═══════════════════════════════════════════════════════════
    #  Cleanup
    # ═══════════════════════════════════════════════════════════

    def stop(self) -> bool:
        """Stop (but do NOT remove) the running container."""
        if not self.container_id:
            return False
        logger.info("停止容器: %s", self.container_id[:12])
        try:
            _run(["docker", "stop", "--time=10", self.container_id], timeout=15)
            return True
        except Exception:
            logger.exception("停止容器失败: %s", self.container_id[:12])
            return False

    def cleanup(self, remove_workspace: bool = True) -> None:
        """Stop and remove the container, and optionally clean up workspace.

        Parameters
        ----------
        remove_workspace : bool
            If ``True``, delete the host workspace directory.
        """
        cid = self.container_id
        if cid:
            logger.info("清理容器: %s", cid[:12])
            try:
                _run(["docker", "rm", "-f", cid], timeout=10)
            except Exception:
                logger.warning("强制删除容器失败 (可能已被删除): %s", cid[:12])

        self.container_id = None
        self.limits = None
        self._task_id = None
        self._created_at = 0.0

        if remove_workspace and self._workspace_host_path and self._workspace_host_path.is_dir():
            logger.info("删除工作区: %s", self._workspace_host_path)
            shutil.rmtree(self._workspace_host_path, ignore_errors=True)
        self._workspace_host_path = None

    # ═══════════════════════════════════════════════════════════
    #  Status
    # ═══════════════════════════════════════════════════════════

    def status(self) -> Dict[str, Any]:
        """Return container status information.

        Returns
        -------
        dict
            Keys: ``container_id``, ``task_id``, ``running``, ``image``,
            ``uptime_seconds``, ``limits``, ``workspace_readonly``,
            ``network_allowed``.
        """
        running = self._container_exists() if self.container_id else False
        uptime = (time.time() - self._created_at) if self._created_at > 0 else 0.0

        return {
            "container_id": self.container_id,
            "task_id": self._task_id,
            "running": running,
            "uptime_seconds": round(uptime, 1),
            "limits": repr(self.limits) if self.limits else None,
            "workspace_readonly": self._workspace_readonly,
            "network_allowed": self._network_allowed,
        }

    # ═══════════════════════════════════════════════════════════
    #  Context manager
    # ═══════════════════════════════════════════════════════════

    def __enter__(self) -> "DockerSandbox":
        return self

    def __exit__(self, *args: Any) -> None:
        self.cleanup()

    # ═══════════════════════════════════════════════════════════
    #  Internal helpers
    # ═══════════════════════════════════════════════════════════

    def _require_container(self) -> None:
        if not self.container_id:
            raise SandboxNotCreatedError(
                "容器尚未创建。请先调用 create(task_id, image)。"
            )
        if not self._container_exists():
            raise SandboxError(
                f"容器 {self.container_id[:12]} 已不存在（可能已被外部删除）"
            )

    def _container_exists(self) -> bool:
        """Check if the container is currently running."""
        if not self.container_id:
            return False
        try:
            result = _run(
                ["docker", "ps", "-q", "--no-trunc", "--filter",
                 f"id={self.container_id}"],
                timeout=5,
            )
            return self.container_id in result.stdout
        except Exception:
            return False

    def _resolve_workspace_path(self, relpath: str) -> Path:
        """Resolve and sanitise a workspace-relative path."""
        parts = Path(relpath).parts
        safe_parts = [p for p in parts if p not in ("", ".", "..")]
        resolved = (self._workspace_host_path / Path(*safe_parts)).resolve()
        if not str(resolved).startswith(str(self._workspace_host_path.resolve())):
            raise ValueError(f"路径越权拒绝: {relpath} → {resolved}")
        return resolved


# ═══════════════════════════════════════════════════════════════
#  Utility
# ═══════════════════════════════════════════════════════════════

def _ensure_docker_available() -> None:
    """Raise :class:`DockerUnavailableError` if ``docker`` is not on PATH."""
    if shutil.which("docker") is None:
        raise DockerUnavailableError(
            "未找到 docker CLI。请安装 Docker Desktop 或 docker-ce。"
        )
    # Also check the daemon is reachable
    try:
        result = _run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=5)
        if result.returncode != 0:
            raise DockerUnavailableError(
                f"docker daemon 不可用: {result.stderr.strip()}"
            )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        raise DockerUnavailableError(f"无法连接 docker daemon: {e}")


def _run(cmd: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
    """Run a subprocess with unified error handling."""
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
