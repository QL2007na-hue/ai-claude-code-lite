import json
import re
import time
from typing import Any, Dict, List

from plugin_sdk.base_plugin import BasePlugin

# 匹配 TODO / FIXME / HACK 标记
_TODO_RE = re.compile(
    r"(?:#|//|<!--)\s*(TODO|FIXME|HACK|XXX|BUG|WORKAROUND|TEMP)\b",
    re.IGNORECASE,
)


class TodoCheckerPlugin(BasePlugin):
    """示例插件：监听 coding 完成事件，检查代码中的 TODO/FIXME/HACK 标记。

    工作流：
      1. 监听 task.code_written 事件
      2. 读取被写入的源代码文件
      3. 扫描 TODO/FIXME/HACK/XXX/BUG 标记
      4. 通过 ctx.emit() 广播 plugin.todo_found 事件
      5. 可选：将 TODO 数量写入 TaskManager 任务的 result 中

    安装方式：
      PluginLoader 扫描 plugins/ 目录时自动发现，
      或手动 loader.register(TodoCheckerPlugin())
    """

    name = "todo-checker"
    version = "1.0.0"
    description = "检查代码中的 TODO/FIXME/HACK 未完成标记"

    # 累计统计
    _total_scanned: int = 0
    _total_todos: int = 0

    def subscribe(self) -> List[str]:
        """只关心代码写入事件。"""
        return [
            "task.code_written",
            "task.coding_completed",
        ]

    def on_event(
        self,
        task_id: str,
        agent: str,
        event: str,
        payload: Any,
    ) -> None:
        if not isinstance(payload, dict):
            return

        if event == "task.code_written":
            filepath = payload.get("file", "")
            if filepath:
                self._check_file(task_id, filepath)

        elif event == "task.coding_completed":
            self._emit_summary(task_id)

    def _check_file(self, task_id: str, filepath: str) -> None:
        """读取并扫描单个文件中的 TODO 标记。"""
        try:
            content = self.ctx.workspace_mgr.read_file(task_id, filepath)
        except Exception:
            return

        matches: List[Dict[str, Any]] = []
        for lineno, line in enumerate(content.splitlines(), start=1):
            m = _TODO_RE.search(line)
            if m:
                matches.append({
                    "line": lineno,
                    "tag": m.group(1).upper(),
                    "text": line.strip()[:120],
                })

        self._total_scanned += 1
        self._total_todos += len(matches)

        if matches:
            self.ctx.emit(
                task_id,
                "plugin.todo_found",
                {
                    "plugin": self.name,
                    "file": filepath,
                    "count": len(matches),
                    "items": matches,
                },
            )

    def _emit_summary(self, task_id: str) -> None:
        """在 coding 完成后发出汇总。"""
        self.ctx.emit(
            task_id,
            "plugin.todo_check_complete",
            {
                "plugin": self.name,
                "files_scanned": self._total_scanned,
                "todos_found": self._total_todos,
            },
        )
        self._total_scanned = 0
        self._total_todos = 0
