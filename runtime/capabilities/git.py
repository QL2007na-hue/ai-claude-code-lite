"""
GitCapability —— Git 版本控制能力。

在 workspace 目录中执行 Git 操作，提供：
    - git_init() — 初始化仓库
    - git_add() — 暂存文件
    - git_commit(message) — 提交变更
    - git_status() — 查看状态
    - git_log() — 查看日志
    - git_diff() — 查看差异
    - git_branch(name) — 创建分支
    - git_checkout(branch) — 切换分支

所有命令在 workspace_root 目录下执行。

权限声明：
    PERMISSIONS = ["FILESYSTEM_READ", "FILESYSTEM_WRITE", "SHELL"]
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from runtime.capabilities.base import BaseCapability


# ── 结果数据类 ──────────────────────────────────────────────

@dataclass
class GitResult:
    """Git 操作结果。

    Attributes
    ----------
    ok : bool
        操作是否成功。
    command : str
        执行的 Git 命令。
    exit_code : int
        命令退出码。
    stdout : str
        标准输出内容。
    stderr : str
        标准错误输出内容。
    data : dict | list | None
        结构化数据（如 git_status 解析后的文件列表）。
    """
    ok: bool = True
    command: str = ""
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    data: Optional[Union[Dict[str, Any], List[Any], str]] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "ok": self.ok,
            "command": self.command,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }
        if self.data is not None:
            result["data"] = self.data
        return result


# ── GitCapability ────────────────────────────────────────────

class GitCapability(BaseCapability):
    """沙箱化 Git 版本控制能力。

    所有 Git 命令在 workspace_root 目录下执行，
    自动为仓库配置默认 user.name / user.email。

    使用示例::

        git = GitCapability(workspace_root="/workspace/task-abc")
        await git.git_init()
        await git.git_add(".")
        await git.git_commit("Initial commit")
        log = await git.git_log()
    """

    PERMISSIONS = ["FILESYSTEM_READ", "FILESYSTEM_WRITE", "SHELL"]
    """所需权限声明。"""

    # ── 初始化 ────────────────────────────────────────────

    def __init__(
        self,
        workspace_root: Union[str, Path] = ".",
        git_user_name: str = "ai-runtime",
        git_user_email: str = "ai-runtime@local",
        default_timeout: int = 60,
    ) -> None:
        """初始化 Git 能力。

        Parameters
        ----------
        workspace_root : str | Path
            Git 仓库所在的根目录路径。
        git_user_name : str
            提交时使用的用户名（若仓库未配置）。
        git_user_email : str
            提交时使用的邮箱（若仓库未配置）。
        default_timeout : int
            Git 命令默认超时时间（秒）。
        """
        super().__init__()
        self.workspace_root: Path = Path(workspace_root).resolve()
        self.git_user_name: str = git_user_name
        self.git_user_email: str = git_user_email
        self.default_timeout: int = default_timeout

        self.logger.debug(
            "GitCapability 初始化: root=%s timeout=%ds",
            self.workspace_root, self.default_timeout,
        )

    # ── 属性 ──────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "git"

    @property
    def description(self) -> str:
        return "Sandboxed Git version control: init, add, commit, status, log, diff, branch, checkout within a workspace directory."

    # ── 核心执行 ─────────────────────────────────────────

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """根据 operation 参数路由到具体方法。

        Parameters
        ----------
        operation : str
            操作类型：'init' / 'add' / 'commit' / 'status' / 'log' /
                     'diff' / 'branch' / 'checkout'
        **kwargs
            各操作的具体参数。

        Returns
        -------
        GitResult
        """
        operation = kwargs.pop("operation", None)
        if not operation and args:
            operation = args[0]

        if operation == "init":
            return await self.git_init()
        elif operation == "add":
            return await self.git_add(kwargs.get("files", "."))
        elif operation == "commit":
            return await self.git_commit(kwargs.get("message", ""))
        elif operation == "status":
            return await self.git_status()
        elif operation == "log":
            return await self.git_log(kwargs.get("max_count", 10))
        elif operation == "diff":
            return await self.git_diff(kwargs.get("target", ""))
        elif operation == "branch":
            return await self.git_branch(kwargs.get("name", ""))
        elif operation == "checkout":
            return await self.git_checkout(kwargs.get("branch", ""))
        else:
            raise ValueError(f"未知的 Git 操作: {operation!r}")

    # ── git init ─────────────────────────────────────────

    async def git_init(self) -> GitResult:
        """初始化 Git 仓库。

        Returns
        -------
        GitResult
        """
        if not self._enabled:
            return GitResult(ok=False, command="git init", stderr="GitCapability 未启用")

        try:
            proc = await self._run_git("init")
            if proc.returncode == 0:
                self.logger.info("Git init 成功: %s", self.workspace_root)
                # 确保 user.name / user.email 已配置
                await self._ensure_user_config()
            return GitResult(
                ok=proc.returncode == 0,
                command="git init",
                exit_code=proc.returncode,
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
            )
        except Exception as exc:
            self.logger.exception("git init 异常")
            return GitResult(
                ok=False,
                command="git init",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── git add ──────────────────────────────────────────

    async def git_add(self, files: str = ".") -> GitResult:
        """暂存文件。

        Parameters
        ----------
        files : str
            要暂存的文件路径模式，默认为 "."（全部）。

        Returns
        -------
        GitResult
        """
        if not self._enabled:
            return GitResult(ok=False, command=f"git add {files}", stderr="GitCapability 未启用")

        try:
            proc = await self._run_git("add", files)
            self.logger.debug("git add %s → exit=%d", files, proc.returncode)
            return GitResult(
                ok=proc.returncode == 0,
                command=f"git add {files}",
                exit_code=proc.returncode,
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
            )
        except Exception as exc:
            self.logger.exception("git add 异常")
            return GitResult(
                ok=False,
                command=f"git add {files}",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── git commit ───────────────────────────────────────

    async def git_commit(self, message: str) -> GitResult:
        """提交变更。

        Parameters
        ----------
        message : str
            提交消息。

        Returns
        -------
        GitResult
        """
        if not self._enabled:
            return GitResult(ok=False, command="git commit", stderr="GitCapability 未启用")

        if not message.strip():
            return GitResult(
                ok=False,
                command="git commit",
                stderr="提交消息不能为空",
            )

        try:
            await self._ensure_user_config()
            proc = await self._run_git("commit", "-m", message, "--allow-empty")
            self.logger.info("git commit → exit=%d msg=%s", proc.returncode, message)
            return GitResult(
                ok=proc.returncode == 0,
                command=f"git commit -m '{message}'",
                exit_code=proc.returncode,
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
            )
        except Exception as exc:
            self.logger.exception("git commit 异常")
            return GitResult(
                ok=False,
                command="git commit",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── git status ───────────────────────────────────────

    async def git_status(self) -> GitResult:
        """查看仓库状态。

        Returns
        -------
        GitResult
            data 字段包含解析后的文件列表。
        """
        if not self._enabled:
            return GitResult(ok=False, command="git status", stderr="GitCapability 未启用")

        try:
            proc = await self._run_git("status", "--short")
            if proc.returncode != 0:
                return GitResult(
                    ok=False,
                    command="git status",
                    exit_code=proc.returncode,
                    stderr=proc.stderr.strip(),
                )

            # 解析文件列表
            files: List[Dict[str, str]] = []
            for line in proc.stdout.strip().split("\n"):
                if line.strip():
                    status_code = line[:2].strip()
                    filename = line[3:].strip()
                    files.append({"status": status_code, "file": filename})

            self.logger.debug("git status → %d 文件", len(files))
            return GitResult(
                ok=True,
                command="git status",
                exit_code=0,
                stdout=proc.stdout.strip() or "clean",
                data=files,
            )
        except Exception as exc:
            self.logger.exception("git status 异常")
            return GitResult(
                ok=False,
                command="git status",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── git log ──────────────────────────────────────────

    async def git_log(self, max_count: int = 10) -> GitResult:
        """查看提交日志。

        Parameters
        ----------
        max_count : int
            最大条目数。

        Returns
        -------
        GitResult
            data 字段包含解析后的日志条目列表。
        """
        if not self._enabled:
            return GitResult(ok=False, command="git log", stderr="GitCapability 未启用")

        try:
            proc = await self._run_git(
                "log",
                f"--max-count={max_count}",
                "--format=%h|%s|%ai",
            )
            if proc.returncode != 0:
                return GitResult(
                    ok=False,
                    command="git log",
                    exit_code=proc.returncode,
                    stderr=proc.stderr.strip(),
                )

            entries: List[Dict[str, str]] = []
            for line in proc.stdout.strip().split("\n"):
                if not line:
                    continue
                parts = line.split("|", 2)
                if len(parts) == 3:
                    entries.append({
                        "hash": parts[0],
                        "message": parts[1],
                        "date": parts[2],
                    })

            self.logger.debug("git log → %d 条目", len(entries))
            return GitResult(
                ok=True,
                command="git log",
                exit_code=0,
                data=entries,
            )
        except Exception as exc:
            self.logger.exception("git log 异常")
            return GitResult(
                ok=False,
                command="git log",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── git diff ─────────────────────────────────────────

    async def git_diff(self, target: str = "") -> GitResult:
        """查看差异。

        Parameters
        ----------
        target : str
            比较目标（commit hash / branch name / 文件路径）。
            为空则显示未暂存的变更。

        Returns
        -------
        GitResult
            data 字段包含 diff 文本。
        """
        if not self._enabled:
            return GitResult(ok=False, command="git diff", stderr="GitCapability 未启用")

        try:
            if target:
                proc = await self._run_git("diff", target)
            else:
                proc = await self._run_git("diff")
            self.logger.debug("git diff → exit=%d", proc.returncode)
            return GitResult(
                ok=proc.returncode == 0,
                command=f"git diff {target}".strip(),
                exit_code=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr.strip(),
                data=proc.stdout.strip() or "",
            )
        except Exception as exc:
            self.logger.exception("git diff 异常")
            return GitResult(
                ok=False,
                command="git diff",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── git branch ───────────────────────────────────────

    async def git_branch(self, name: str) -> GitResult:
        """创建新分支。

        Parameters
        ----------
        name : str
            分支名称。

        Returns
        -------
        GitResult
        """
        if not self._enabled:
            return GitResult(ok=False, command=f"git branch {name}", stderr="GitCapability 未启用")

        if not name.strip():
            return GitResult(
                ok=False,
                command="git branch",
                stderr="分支名称不能为空",
            )

        try:
            proc = await self._run_git("branch", name)
            self.logger.info("git branch %s → exit=%d", name, proc.returncode)
            return GitResult(
                ok=proc.returncode == 0,
                command=f"git branch {name}",
                exit_code=proc.returncode,
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
            )
        except Exception as exc:
            self.logger.exception("git branch 异常")
            return GitResult(
                ok=False,
                command=f"git branch {name}",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── git checkout ─────────────────────────────────────

    async def git_checkout(self, branch: str) -> GitResult:
        """切换分支。

        Parameters
        ----------
        branch : str
            目标分支名称。

        Returns
        -------
        GitResult
        """
        if not self._enabled:
            return GitResult(ok=False, command=f"git checkout {branch}", stderr="GitCapability 未启用")

        if not branch.strip():
            return GitResult(
                ok=False,
                command="git checkout",
                stderr="分支名称不能为空",
            )

        try:
            proc = await self._run_git("checkout", branch)
            self.logger.info("git checkout %s → exit=%d", branch, proc.returncode)
            return GitResult(
                ok=proc.returncode == 0,
                command=f"git checkout {branch}",
                exit_code=proc.returncode,
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
            )
        except Exception as exc:
            self.logger.exception("git checkout 异常")
            return GitResult(
                ok=False,
                command=f"git checkout {branch}",
                exit_code=-1,
                stderr=str(exc),
            )

    # ── 校验与清理 ───────────────────────────────────────

    def validate(self) -> None:
        """校验前置条件：workspace_root 存在且可访问。

        Raises
        ------
        RuntimeError
            若 workspace_root 不可访问。
        """
        if not self.workspace_root.exists():
            raise RuntimeError(
                f"GitCapability 工作区不存在: {self.workspace_root}"
            )

    def sanitize(self, *args: Any) -> Tuple[Any, ...]:
        """对输入参数做基本清理。"""
        return args

    # ── 内部方法 ─────────────────────────────────────────

    async def _run_git(self, *args: str) -> asyncio.subprocess.Process:
        """异步执行 Git 命令。

        Parameters
        ----------
        *args : str
            Git 子命令及参数。

        Returns
        -------
        subprocess.Process
            已完成的进程对象。
        """
        proc = await asyncio.create_subprocess_exec(
            "git", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(self.workspace_root),
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=self.default_timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            self.logger.warning("Git 命令超时: git %s", " ".join(args))
        return proc

    async def _ensure_user_config(self) -> None:
        """确保仓库已配置 user.name / user.email。"""
        # 检查 config
        name_proc = await self._run_git("config", "user.name")
        if name_proc.returncode != 0 or not name_proc.stdout.strip():
            await self._run_git("config", "user.name", self.git_user_name)
            self.logger.debug("设置 git user.name: %s", self.git_user_name)

        email_proc = await self._run_git("config", "user.email")
        if email_proc.returncode != 0 or not email_proc.stdout.strip():
            await self._run_git("config", "user.email", self.git_user_email)
            self.logger.debug("设置 git user.email: %s", self.git_user_email)
