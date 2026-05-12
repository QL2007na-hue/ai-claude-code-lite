"""Sandbox runtime — Docker-based task isolation.

Provides:

- :class:`DockerSandbox`:  Manage a transient Docker container per task
  (create, execute commands, read/write files, export artifacts, cleanup).
- :class:`ResourceLimits`:  Per-container resource caps (memory, CPU, disk,
  network, PID limit, file whitelist).  Convertible to ``docker run`` args
  via ``.to_docker_args()``.
- :class:`FileLimits`:  Per-container file-system guards (max file size,
  extension whitelist).

Exception hierarchy:

- :class:`SandboxError`:  Base.
- :class:`SandboxNotCreatedError`:  Operation before ``create()``.
- :class:`SandboxExecTimeoutError`:  Command exceeded timeout.
- :class:`DockerUnavailableError`:  ``docker`` CLI not found / daemon down.

Typical usage::

    from runtime.sandbox import DockerSandbox, ResourceLimits

    with DockerSandbox() as sb:
        sb.create("task-abc", image="python:3.11-slim")
        sb.write_file("src/main.py", 'print("ok")')
        out = sb.execute("python src/main.py")
        assert out["exit_code"] == 0
        # container auto-stops on context manager exit
"""

from .docker_sandbox import (
    DockerSandbox,
    DockerUnavailableError,
    SandboxError,
    SandboxExecTimeoutError,
    SandboxNotCreatedError,
)
from .resource_limits import FileLimits, ResourceLimits

__all__ = [
    # Core
    "DockerSandbox",
    "ResourceLimits",
    "FileLimits",
    # Exceptions
    "SandboxError",
    "SandboxNotCreatedError",
    "SandboxExecTimeoutError",
    "DockerUnavailableError",
]
