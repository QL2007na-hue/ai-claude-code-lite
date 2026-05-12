"""Resource constraints for Docker-based sandbox isolation.

Defines `ResourceLimits` dataclass to describe per-container resource caps
and provides conversion to docker CLI ``--memory`` / ``--cpus`` / ``--pids-limit`` /
``--network`` / ``--storage-opt`` style arguments.

Usage::

    from runtime.sandbox.resource_limits import ResourceLimits

    limits = ResourceLimits(
        memory_mb=512,
        cpu_shares=1024,
        timeout_sec=300,
        disk_quota_mb=512,
        network_access=False,
    )

    docker_args = limits.to_docker_args()
    # ['--memory', '512m', '--cpu-shares', '1024', '--pids-limit', '100',
    #  '--network', 'none', '--storage-opt', 'size=512m']
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass
class FileLimits:
    """Per-container file-system safety guards.

    Attributes
    ----------
    max_file_size : int
        Maximum allowed size for any single file (bytes).  Default 10 MB.
    allowed_extensions : tuple of str
        Whitelist of file extensions that may be written.
        Empty tuple means all extensions allowed.
    """

    max_file_size: int = 10 * 1024 * 1024  # 10 MB
    allowed_extensions: Tuple[str, ...] = ()

    def is_extension_allowed(self, filename: str) -> bool:
        """Check whether *filename* passes the extension whitelist.

        Returns ``True`` if the whitelist is empty or the file's extension
        (lowercased) appears in the whitelist.
        """
        if not self.allowed_extensions:
            return True

        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        return ext in self.allowed_extensions

    def is_size_allowed(self, size: int) -> bool:
        """Check whether *size* bytes falls under ``max_file_size``."""
        return size <= self.max_file_size


@dataclass
class ResourceLimits:
    """Container resource constraints.

    All values use sensible defaults that work well for AI-agent workspace
    tasks — e.g. running ``pytest``, ``mypy``, formatting.

    Attributes
    ----------
    memory_mb : int
        Hard memory limit in MiB.  Default ``512``.
    cpu_shares : int
        Relative CPU weight (Docker ``--cpu-shares``).  Default ``1024``
        (one full logical core equivalent).
    timeout_sec : int
        Maximum wall-clock lifetime for a command executed inside the container.
        Default ``300`` (5 minutes).
    disk_quota_mb : int
        Maximum writable layer size for the container.  Default ``1024``.
        **Requires** the ``overlay2`` storage driver with quota support.
        Set to ``0`` to disable.
    network_access : bool
        If ``False`` (default), ``--network none`` is applied, blocking all
        outbound/inbound connectivity.
    allowed_env_vars : list of str
        Explicit list of environment variables forwarded into the container.
        Empty list means none are forwarded (safe default).
    pids_limit : int
        Maximum number of processes inside the container.  Default ``100``.
    file_limits : FileLimits
        File-system safety constraints (max file size / extension whitelist).
    """

    memory_mb: int = 512
    cpu_shares: int = 1024
    timeout_sec: int = 300
    disk_quota_mb: int = 1024
    network_access: bool = False
    allowed_env_vars: List[str] = field(default_factory=list)
    pids_limit: int = 100
    file_limits: FileLimits = field(default_factory=FileLimits)

    # ── Validation ───────────────────────────────────────────

    def __post_init__(self) -> None:
        """Validate numeric ranges at construction time."""
        if self.memory_mb < 16:
            raise ValueError(f"memory_mb 不能低于 16 MiB，收到 {self.memory_mb}")
        if self.cpu_shares < 2:
            raise ValueError(f"cpu_shares 不能低于 2，收到 {self.cpu_shares}")
        if self.timeout_sec < 1:
            raise ValueError(f"timeout_sec 必须 ≥ 1，收到 {self.timeout_sec}")
        if self.disk_quota_mb < 0:
            raise ValueError(f"disk_quota_mb 不能为负数，收到 {self.disk_quota_mb}")
        if self.pids_limit < 1:
            raise ValueError(f"pids_limit 必须 ≥ 1，收到 {self.pids_limit}")

    # ── Conversion to docker CLI args ────────────────────────

    def to_docker_args(self) -> List[str]:
        """Convert resource limits to ``docker run``-compatible CLI arguments.

        Returns
        -------
        list of str
            e.g. ``['--memory', '512m', '--cpu-shares', '1024', ...]``
        """
        args: List[str] = []

        # Memory
        args.extend(["--memory", f"{self.memory_mb}m"])
        args.extend(["--memory-swap", f"{self.memory_mb}m"])  # disable swap

        # CPU
        args.extend(["--cpu-shares", str(self.cpu_shares)])

        # PIDs
        args.extend(["--pids-limit", str(self.pids_limit)])

        # Network
        if not self.network_access:
            args.extend(["--network", "none"])

        # Disk quota (storage-opt size=... — overlay2 only)
        if self.disk_quota_mb > 0:
            args.extend(["--storage-opt", f"size={self.disk_quota_mb}m"])

        return args

    # ── Environment variable helpers ─────────────────────────

    def to_env_args(self) -> List[str]:
        """Build ``-e KEY=VALUE`` args from the current process env for
        every key listed in ``allowed_env_vars``.

        Returns
        -------
        list of str
            e.g. ``['-e', 'PYTHONPATH=/src', '-e', 'HOME=/root']``
        """
        import os

        env_args: List[str] = []
        for key in self.allowed_env_vars:
            value = os.environ.get(key, "")
            env_args.extend(["-e", f"{key}={value}"])
        return env_args

    # ── Convenience ──────────────────────────────────────────

    @classmethod
    def relaxed(cls) -> "ResourceLimits":
        """Return limits suitable for developer exploration (less restrictive).

        - 2048 MiB memory
        - 4096 CPU shares (4x weight)
        - 600 s timeout
        - network **enabled**
        """
        return cls(
            memory_mb=2048,
            cpu_shares=4096,
            timeout_sec=600,
            disk_quota_mb=4096,
            network_access=True,
        )

    @classmethod
    def strict(cls) -> "ResourceLimits":
        """Return highly restrictive limits for untrusted code execution.

        - 128 MiB memory
        - 64 CPU shares
        - 30 s timeout
        - network **disabled**
        - only ``.py``, ``.txt``, ``.json`` files allowed
        """
        return cls(
            memory_mb=128,
            cpu_shares=64,
            timeout_sec=30,
            network_access=False,
            file_limits=FileLimits(
                max_file_size=512 * 1024,  # 512 KB
                allowed_extensions=("py", "txt", "json", "md"),
            ),
        )

    # ── Representation ───────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"ResourceLimits(mem={self.memory_mb}MiB, cpu={self.cpu_shares}, "
            f"timeout={self.timeout_sec}s, disk={self.disk_quota_mb}MiB, "
            f"net={'on' if self.network_access else 'off'})"
        )
