import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional


class WorkspaceManager:
    """每个任务独立 workspace 目录 + 独立 Git 仓库

    目录结构:
        workspace/
        ├── task-<task_id_1>/
        │   ├── .git/
        │   └── ... (代码文件)
        └── task-<task_id_2>/
            ├── .git/
            └── ... (代码文件)

    Usage:
        wm = WorkspaceManager()
        wm.create("task-abc")
        wm.write_file("task-abc", "main.py", 'print("hello")')
        wm.run_shell("task-abc", "python main.py")
        wm.git_commit("task-abc", "init")
    """

    def __init__(self, root_dir: str = "workspace"):
        self.root = Path(root_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _task_dir(self, task_id: str) -> Path:
        return self.root / f"task-{task_id}"

    # ── 生命周期 ────────────────────────────────────────────

    def create(self, task_id: str) -> Path:
        """创建任务 workspace 目录并 git init。返回目录路径。"""
        task_dir = self._task_dir(task_id)
        task_dir.mkdir(parents=True, exist_ok=True)
        git_dir = task_dir / ".git"
        if not git_dir.exists():
            result = self._run("git", "init", cwd=task_dir)
            if result.returncode != 0 and result.returncode != 127:
                pass
        return task_dir

    def exists(self, task_id: str) -> bool:
        return self._task_dir(task_id).is_dir()

    def delete(self, task_id: str) -> bool:
        """删除任务 workspace 目录（不可逆）。"""
        task_dir = self._task_dir(task_id)
        if not task_dir.is_dir():
            return False
        shutil.rmtree(task_dir)
        return True

    def get_path(self, task_id: str) -> Path:
        return self._task_dir(task_id)

    # ── 文件操作 ────────────────────────────────────────────

    def write_file(self, task_id: str, relpath: str, content: str) -> Path:
        """写入文件到任务 workspace。自动创建父目录。"""
        task_dir = self._task_dir(task_id)
        if not task_dir.is_dir():
            self.create(task_id)
        target = self._sanitize(task_dir, relpath)
        target.parent.mkdir(parents=True, exist_ok=True)
        cleaned = content.replace("\r\n", "\n")
        if not cleaned.endswith("\n"):
            cleaned += "\n"
        target.write_text(cleaned, encoding="utf-8")
        return target

    def read_file(self, task_id: str, relpath: str) -> str:
        target = self._sanitize(self._task_dir(task_id), relpath)
        return target.read_text(encoding="utf-8")

    def list_files(self, task_id: str) -> List[str]:
        """列举 workspace 内所有文件（相对路径，排除 .git）。"""
        task_dir = self._task_dir(task_id)
        if not task_dir.is_dir():
            return []
        files: List[str] = []
        for entry in task_dir.rglob("*"):
            if entry.is_file() and ".git" not in entry.parts:
                rel = entry.relative_to(task_dir).as_posix()
                files.append(rel)
        return sorted(files)

    # ── Shell 执行 ──────────────────────────────────────────

    def run_shell(
        self,
        task_id: str,
        command: str,
        timeout: int = 120,
    ) -> Dict[str, Any]:
        """在任务 workspace 下执行 shell 命令。"""
        task_dir = self._task_dir(task_id)
        try:
            proc = subprocess.run(
                command,
                shell=True,
                cwd=str(task_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return {
                "command": command,
                "exit_code": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
        except subprocess.TimeoutExpired:
            return {
                "command": command,
                "exit_code": -1,
                "stdout": "",
                "stderr": f"命令超时 ({timeout}s)",
            }
        except Exception as e:
            return {
                "command": command,
                "exit_code": -1,
                "stdout": "",
                "stderr": str(e),
            }

    # ── Git 操作 ────────────────────────────────────────────

    def git_commit(self, task_id: str, message: str) -> Dict[str, Any]:
        """git add -A && git commit。"""
        cwd = self._task_dir(task_id)
        r1 = self._run("git", "add", "-A", cwd=cwd)
        config = self._run("git", "config", "user.email", cwd=cwd)
        if config.returncode != 0:
            self._run("git", "config", "user.email", "ai-runtime@local", cwd=cwd)
            self._run("git", "config", "user.name", "ai-runtime", cwd=cwd)
        r2 = self._run("git", "commit", "-m", message, "--allow-empty", cwd=cwd)
        return {
            "exit_code": r2.returncode,
            "stdout": r2.stdout.strip(),
            "stderr": r2.stderr.strip(),
        }

    def git_status(self, task_id: str) -> Dict[str, Any]:
        r = self._run("git", "status", "--short", cwd=self._task_dir(task_id))
        return {
            "exit_code": r.returncode,
            "files": [ln for ln in r.stdout.split("\n") if ln.strip()],
            "raw": r.stdout.strip() or "clean",
        }

    def git_log(self, task_id: str, max_count: int = 10) -> List[Dict[str, str]]:
        r = self._run(
            "git", "log",
            f"--max-count={max_count}",
            "--format=%h|%s|%ai",
            cwd=self._task_dir(task_id),
        )
        entries: List[Dict[str, str]] = []
        for line in r.stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("|", 2)
            if len(parts) == 3:
                entries.append({"hash": parts[0], "message": parts[1], "date": parts[2]})
        return entries

    # ── 内部工具 ────────────────────────────────────────────

    def _sanitize(self, base: Path, relpath: str) -> Path:
        clean = relpath.strip().lstrip("/\\")
        parts = clean.replace("\\", "/").split("/")
        safe = [p for p in parts if p not in ("", ".", "..")]
        resolved = base.joinpath(*safe).resolve()
        if not str(resolved).startswith(str(base.resolve())):
            raise ValueError(f"路径越权拒绝: {relpath} → {resolved}")
        return resolved

    @staticmethod
    def _run(*args: str, cwd: Path) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(args, cwd=str(cwd), capture_output=True, text=True)
        except FileNotFoundError:
            return subprocess.CompletedProcess(args, 127, stdout="", stderr=f"git not available: {args[0]} not found")
