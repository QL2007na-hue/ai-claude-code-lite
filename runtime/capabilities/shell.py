"""
ShellCapability —— Shell 命令执行能力。

提供在沙箱化的工作目录中执行 Shell 命令的能力，具备：
    - 命令超时控制
    - 工作目录限制（仅允许 workspace 内）
    - 环境变量白名单（只透传安全的环境变量）
    - 输出捕获（stdout / stderr / exit_code）
    - 命令 allowlist / blocklist 模式匹配

权限声明：
    PERMISSIONS = ["SHELL", "FILESYSTEM_READ"]
"""
from __future__ import annotations

import asyncio
import fnmatch
import os
import re
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from runtime.capabilities.base import BaseCapability


# ── 安全环境变量白名单 ──────────────────────────────────────

SAFE_ENV_WHITELIST: Tuple[str, ...] = (
    "PATH",
    "HOME",
    "USER",
    "USERNAME",
    "TEMP",
    "TMP",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "PYTHONPATH",
    "PYTHONUNBUFFERED",
    "PYTHONIOENCODING",
    "VIRTUAL_ENV",
    "CONDA_PREFIX",
    "NODE_PATH",
    "GOPATH",
    "JAVA_HOME",
    "SHELL",
    "TERM",
    "COLORTERM",
    "NO_COLOR",
    "FORCE_COLOR",
    "CI",
    "GITHUB_ACTIONS",
)


# ── 默认黑名单（危险命令模式） ──────────────────────────────

DEFAULT_BLOCKLIST: Tuple[str, ...] = (
    "rm -rf /*",
    "rm -rf /",
    "rm -rf ~",
    "mkfs.*",
    "dd if=*",
    ">:*",
    "chmod 777 /*",
    "chown -R * /*",
    "shutdown*",
    "reboot*",
    "poweroff*",
    "halt*",
    "init *",
    "systemctl stop*",
    "systemctl disable*",
    "kill -9 *",
    "pkill *",
    "killall *",
    "wget * | sh",
    "curl * | sh",
    "curl * | bash",
    ":(){ :|:& };:",          # fork bomb
    "eval *",
    "exec *",
    "__import__*",
    "import os*system*",
    "os.system*",
    "subprocess*",
)

# ── 默认白名单（始终允许的基础命令） ────────────────────────

DEFAULT_ALLOWLIST: Tuple[str, ...] = (
    "ls",
    "dir",
    "pwd",
    "cd",
    "echo",
    "cat",
    "head",
    "tail",
    "wc",
    "grep",
    "find",
    "sort",
    "uniq",
    "touch",
    "mkdir",
    "cp",
    "mv",
    "rm",
    "chmod",
    "chown",
    "file",
    "stat",
    "tree",
    "diff",
    "patch",
    "tar",
    "gzip",
    "zip",
    "unzip",
    "python",
    "python3",
    "pip",
    "pip3",
    "node",
    "npm",
    "npx",
    "yarn",
    "go",
    "rustc",
    "cargo",
    "make",
    "cmake",
    "gcc",
    "g++",
    "clang",
    "git",
    "curl",
    "wget",
    "ssh*",
    "scp",
    "rsync",
    "docker",
    "docker-compose",
    "kubectl",
    "helm",
    "terraform",
)


# ── 结果数据类 ──────────────────────────────────────────────

@dataclass
class ShellResult:
    """Shell 命令执行结果。

    Attributes
    ----------
    command : str
        执行的原始命令。
    exit_code : int
        进程退出码。0 表示成功，-1 表示执行异常/超时。
    stdout : str
        标准输出内容。
    stderr : str
        标准错误输出内容。
    timed_out : bool
        命令是否因超时被终止。
    truncated : bool
        输出是否因超过限制而被截断。
    """
    command: str
    exit_code: int = -1
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    truncated: bool = False

    @property
    def ok(self) -> bool:
        """命令是否成功执行（exit_code == 0 且未超时）。"""
        return self.exit_code == 0 and not self.timed_out

    def to_dict(self) -> Dict[str, Any]:
        return {
            "command": self.command,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "timed_out": self.timed_out,
            "truncated": self.truncated,
            "ok": self.ok,
        }


# ── ShellCapability ─────────────────────────────────────────

class ShellCapability(BaseCapability):
    """执行 Shell 命令的能力。

    特性：
        - 命令在指定工作目录（默认 workspace 根）下执行
        - 环境变量仅透传安全白名单内的变量
        - 支持命令 allowlist / blocklist 模式匹配
        - 内置超时保护与输出大小限制

    使用示例::

        shell = ShellCapability(workspace_root="/path/to/workspace")
        result = await shell.execute("ls -la")
        print(result.stdout)
    """

    PERMISSIONS = ["SHELL", "FILESYSTEM_READ"]
    """所需权限声明。"""

    # ── 初始化 ────────────────────────────────────────────

    def __init__(
        self,
        workspace_root: Union[str, Path] = ".",
        default_timeout: int = 120,
        max_output_bytes: int = 1_000_000,  # 1 MB
        allowlist: Optional[List[str]] = None,
        blocklist: Optional[List[str]] = None,
        env_whitelist: Optional[Tuple[str, ...]] = None,
    ) -> None:
        """初始化 Shell 执行能力。

        Parameters
        ----------
        workspace_root : str | Path
            命令执行的工作目录根路径，所有命令限定在此目录内执行。
        default_timeout : int
            默认超时时间（秒）。
        max_output_bytes : int
            stdout/stderr 最大输出字节数，超过将截断。
        allowlist : list[str] | None
            命令白名单模式列表。若提供，则仅允许匹配的命令执行。
            None 表示使用默认白名单。
        blocklist : list[str] | None
            命令黑名单模式列表。若提供，则阻止匹配的命令执行。
            None 表示使用默认黑名单。
        env_whitelist : tuple[str] | None
            环境变量白名单。None 表示使用默认白名单。
        """
        super().__init__()
        self.workspace_root: Path = Path(workspace_root).resolve()
        self.default_timeout: int = default_timeout
        self.max_output_bytes: int = max_output_bytes

        self.allowlist: List[str] = (
            list(allowlist) if allowlist is not None
            else list(DEFAULT_ALLOWLIST)
        )
        self.blocklist: List[str] = (
            list(blocklist) if blocklist is not None
            else list(DEFAULT_BLOCKLIST)
        )
        self.env_whitelist: Tuple[str, ...] = (
            env_whitelist if env_whitelist is not None
            else SAFE_ENV_WHITELIST
        )

        self.logger.debug(
            "ShellCapability 初始化: root=%s timeout=%ds max_output=%d",
            self.workspace_root, self.default_timeout, self.max_output_bytes,
        )

    # ── 属性 ──────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "shell"

    @property
    def description(self) -> str:
        return "Execute shell commands in a sandboxed workspace directory with allowlist/blocklist control, timeout, and output capture."

    # ── 核心执行 ─────────────────────────────────────────

    async def execute(
        self,
        command: str,
        *,
        timeout: Optional[int] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ShellResult:
        """执行 Shell 命令。

        Parameters
        ----------
        command : str
            要执行的 Shell 命令字符串。
        timeout : int | None
            超时时间（秒）。None 使用默认超时。
        cwd : str | None
            工作目录（相对于 workspace_root）。None 使用 workspace_root。
        env : dict | None
            额外的环境变量（会合并到安全环境变量中）。

        Returns
        -------
        ShellResult
            包含 exit_code / stdout / stderr / timed_out 的结果对象。

        Raises
        ------
        RuntimeError
            若能力未启用。
        ValueError
            若命令被黑名单阻止或白名单不允许。
        """
        if not self._enabled:
            raise RuntimeError("ShellCapability 未启用")

        # ── 清理与校验 ──────────────────────────────────
        command = self._sanitize_command(command)
        self._check_command(command)
        work_dir = self._resolve_cwd(cwd or ".")

        timeout = timeout if timeout is not None else self.default_timeout
        safe_env = self._build_env(env or {})

        self.logger.info("执行命令: %s (cwd=%s, timeout=%ds)", command, work_dir, timeout)

        try:
            proc = await asyncio.wait_for(
                asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=str(work_dir),
                    env=safe_env,
                ),
                timeout=timeout,
            )

            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout,
            )

            exit_code = proc.returncode or -1
            stdout, out_trunc = self._truncate_output(stdout_bytes)
            stderr, err_trunc = self._truncate_output(stderr_bytes)

            result = ShellResult(
                command=command,
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                timed_out=False,
                truncated=out_trunc or err_trunc,
            )

            if exit_code == 0:
                self.logger.debug("命令成功: %s (exit=0)", command)
            else:
                self.logger.warning(
                    "命令失败: %s (exit=%d stderr=%s)",
                    command, exit_code, stderr[:200],
                )
            return result

        except asyncio.TimeoutError:
            self.logger.warning("命令超时: %s (%ds)", command, timeout)
            return ShellResult(
                command=command,
                exit_code=-1,
                stdout="",
                stderr=f"Command timed out after {timeout}s",
                timed_out=True,
                truncated=False,
            )

        except Exception as exc:
            self.logger.exception("命令执行异常: %s", command)
            return ShellResult(
                command=command,
                exit_code=-1,
                stdout="",
                stderr=f"Execution error: {exc}",
                timed_out=False,
                truncated=False,
            )

    # ── 校验 ─────────────────────────────────────────────

    def validate(self) -> None:
        """校验前置条件。

        Raises
        ------
        RuntimeError
            若 workspace_root 不存在或不可访问。
        """
        if not self.workspace_root.exists():
            raise RuntimeError(
                f"ShellCapability 工作目录不存在: {self.workspace_root}"
            )
        if not os.access(self.workspace_root, os.R_OK):
            raise RuntimeError(
                f"ShellCapability 工作目录不可读: {self.workspace_root}"
            )

    # ── 清理 ─────────────────────────────────────────────

    def sanitize(self, *args: Any) -> Tuple[Any, ...]:
        """对执行参数做清理（此处仅做基本类型校验）。

        实际清理在 _sanitize_command 中完成。
        """
        return args

    # ── 内部方法 ─────────────────────────────────────────

    def _sanitize_command(self, command: str) -> str:
        """清理命令字符串：去除首尾空白、危险控制字符。"""
        cleaned = command.strip()
        # 移除 ANSI 转义序列和不可见控制字符
        cleaned = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", cleaned)
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", cleaned)
        # 移除尾部分号
        cleaned = cleaned.rstrip(";")
        return cleaned

    def _check_command(self, command: str) -> None:
        """检查命令是否符合 allowlist/blocklist 规则。

        Raises
        ------
        ValueError
            若命令被阻止或不被允许。
        """
        # 提取命令名（第一个非选项单词）
        cmd_name = command.split()[0] if command.strip() else ""

        # 黑名单检查（先于白名单）
        if not cmd_name:
            return

        if self._matches_any(command, self.blocklist):
            raise ValueError(
                f"命令被黑名单阻止: {command!r}"
            )

        # 白名单检查
        if self.allowlist and not self._matches_any(command, self.allowlist):
            # 也只用命令名试试
            if not self._matches_any(cmd_name, self.allowlist):
                raise ValueError(
                    f"命令不在白名单中: {cmd_name!r} (full: {command!r})"
                )

    @staticmethod
    def _matches_any(pattern_target: str, patterns: List[str]) -> bool:
        """检查目标是否匹配任一 fnmatch 模式。"""
        for pattern in patterns:
            if fnmatch.fnmatch(pattern_target, pattern):
                return True
            # 也尝试匹配命令名部分
            cmd = pattern_target.split()[0]
            if fnmatch.fnmatch(cmd, pattern):
                return True
        return False

    def _resolve_cwd(self, cwd: str) -> Path:
        """解析工作目录，确保在 workspace_root 内。"""
        if not cwd:
            return self.workspace_root

        raw = Path(cwd)
        if raw.is_absolute():
            resolved = raw.resolve()
        else:
            resolved = (self.workspace_root / raw).resolve()

        # 路径遍历保护
        workspace_resolved = self.workspace_root.resolve()
        if not str(resolved).startswith(str(workspace_resolved)):
            raise ValueError(
                f"工作目录越权: {cwd} → {resolved} (workspace={workspace_resolved})"
            )

        # 确保目标目录存在
        resolved.mkdir(parents=True, exist_ok=True)
        return resolved

    def _build_env(self, extra_env: Dict[str, str]) -> Dict[str, str]:
        """根据白名单构建安全环境变量字典。"""
        safe_env: Dict[str, str] = {}
        for key in self.env_whitelist:
            val = os.environ.get(key)
            if val is not None:
                safe_env[key] = val
        safe_env.update(extra_env)
        return safe_env

    def _truncate_output(self, data: bytes) -> Tuple[str, bool]:
        """截断过长输出。

        Returns
        -------
        (text, truncated)
        """
        limit = self.max_output_bytes
        if len(data) <= limit:
            text = data.decode("utf-8", errors="replace")
            return text, False
        else:
            text = data[:limit].decode("utf-8", errors="replace")
            text += f"\n\n... [输出截断: {len(data)} 字节，仅显示前 {limit} 字节]"
            return text, True

    # ── 配置方法 ─────────────────────────────────────────

    def add_to_allowlist(self, *patterns: str) -> None:
        """向白名单追加模式。"""
        for p in patterns:
            if p not in self.allowlist:
                self.allowlist.append(p)
                self.logger.debug("白名单新增: %s", p)

    def remove_from_allowlist(self, *patterns: str) -> None:
        """从白名单移除模式。"""
        for p in patterns:
            if p in self.allowlist:
                self.allowlist.remove(p)
                self.logger.debug("白名单移除: %s", p)

    def add_to_blocklist(self, *patterns: str) -> None:
        """向黑名单追加模式。"""
        for p in patterns:
            if p not in self.blocklist:
                self.blocklist.append(p)
                self.logger.debug("黑名单新增: %s", p)

    def remove_from_blocklist(self, *patterns: str) -> None:
        """从黑名单移除模式。"""
        for p in patterns:
            if p in self.blocklist:
                self.blocklist.remove(p)
                self.logger.debug("黑名单移除: %s", p)
