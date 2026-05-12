"""
Reviewer Agent —— 代码审查代理。

对 workspace/ 目录中的代码文件执行静态审查，检测：
  - TODO / FIXME / HACK 标记
  - 虚假/桩实现（仅 return None / pass / raise NotImplementedError）
  - 空函数体（无实际逻辑）
  - 缺失 docstring
  - 语法错误 / 安全关切

可选接入 DeepSeek API 进行深度语义审查。
"""

from __future__ import annotations

import ast
import logging
import re
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from runtime.event_bus import EventBus
from runtime.task_manager import TaskManager
from workspace.manager import WorkspaceManager

# ---------------------------------------------------------------------------
# 模块级日志
# ---------------------------------------------------------------------------
logger = logging.getLogger("agents.reviewer")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_h)


# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
AGENT_NAME = "reviewer"

# 支持审查的文件扩展名
_CODE_EXTENSIONS: Set[str] = {
    ".py", ".pyx", ".pyi",          # Python
    ".js", ".ts", ".jsx", ".tsx",   # JavaScript / TypeScript
    ".c", ".cpp", ".cc", ".cxx", ".h", ".hpp", ".hxx",  # C / C++
    ".java", ".kt", ".kts",         # Java / Kotlin
    ".go", ".rs", ".rb",            # Go / Rust / Ruby
    ".swift", ".m", ".mm",          # Swift / ObjC
    ".cs", ".fs", ".fsx",           # C# / F#
    ".lua", ".php",                 # Lua / PHP
    ".sh", ".bash", ".zsh",         # Shell
    ".sql",                         # SQL
}

# TODO/FIXME/HACK 匹配模式（大小写不敏感）
_TODO_PATTERN = re.compile(
    r"#\s*(TODO|FIXME|HACK|XXX|BUG|WORKAROUND|TEMP|KLUDGE)\b",
    re.IGNORECASE,
)
_LINE_TODO_PATTERN = re.compile(
    r"//\s*(TODO|FIXME|HACK|XXX|BUG|WORKAROUND|TEMP|KLUDGE)\b",
    re.IGNORECASE,
)

# Python 无意义函数体模式
_STUB_PATTERNS = [
    re.compile(r"^\s*raise\s+NotImplementedError", re.IGNORECASE),
    re.compile(r"^\s*return\s+None\s*$"),
    re.compile(r"^\s*return\s*$"),
    re.compile(r"^\s*pass\s*$"),
    re.compile(r"^\s*\.\.\.\s*$"),
]

# 函数 docstring 行用作 stub 的标记文本
_DOCSTRING_STUB_KEYWORDS = {"todo", "fixme", "stub", "not implemented", "placeholder"}


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------
class Issue:
    """一条审查发现的问题。"""

    __slots__ = ("category", "file", "line", "message", "snippet")

    def __init__(self, category: str, file: str, line: int, message: str, snippet: str = ""):
        self.category = category
        self.file = file
        self.line = line
        self.message = message
        self.snippet = snippet

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "file": self.file,
            "line": self.line,
            "message": self.message,
            "snippet": self.snippet,
        }

    def __repr__(self) -> str:
        return f"Issue({self.category!r}, {self.file!r}:{self.line}, {self.message!r})"


# ---------------------------------------------------------------------------
# 权重配置（用于扣分）
# ---------------------------------------------------------------------------
_SEVERITY_WEIGHTS: Dict[str, int] = {
    "todo": 2,
    "fake_impl": 15,
    "empty_func": 10,
    "missing_docstring": 1,
    "syntax_error": 20,
    "security_concern": 25,
}


# ---------------------------------------------------------------------------
# 核心 Reviewer
# ---------------------------------------------------------------------------
class Reviewer:
    """代码审查代理。

    Usage::

        from runtime.event_bus import EventBus
        from runtime.task_manager import TaskManager
        from workspace.manager import WorkspaceManager
        from agents.reviewer import Reviewer

        bus = EventBus()
        tm = TaskManager()
        wm = WorkspaceManager()
        reviewer = Reviewer(bus, tm, wm)

        result = reviewer.review("task-123")
        print(result)  # {"approved": False, "issues": [...], "score": 72}

    Parameters
    ----------
    bus : EventBus
        项目事件总线。
    tm : TaskManager
        项目任务管理器。
    workspace_mgr : WorkspaceManager, optional
        工作区管理器，自动扫描 workspace/task-<id>/ 目录。
    deepseek_api_key : Optional[str]
        DeepSeek API Key，提供后会启用深度语义审查。
    deepseek_base_url : str
        DeepSeek API 地址。
    max_file_size_kb : int
        超过此大小的文件跳过审查（默认 512 KB）。
    """

    def __init__(
        self,
        bus: EventBus,
        tm: TaskManager,
        workspace_mgr: Optional[WorkspaceManager] = None,
        deepseek_api_key: Optional[str] = None,
        deepseek_base_url: str = "https://api.deepseek.com/v1",
        max_file_size_kb: int = 512,
    ) -> None:
        self._bus = bus
        self._tm = tm
        self._wm = workspace_mgr or WorkspaceManager()
        self._deepseek_api_key = deepseek_api_key
        self._deepseek_base_url = deepseek_base_url.rstrip("/")
        self._max_file_bytes = max_file_size_kb * 1024

    # ── 公开入口 ─────────────────────────────────────────────
    def review(self, task_id: str) -> Dict[str, Any]:
        """对指定任务进行代码审查。

        状态流转：
            pending → running → done  (approved)
            pending → running → retry  (rejected)

        Returns
        -------
        dict
            {
                "approved": bool,
                "issues": [{"category": str, "file": str, "line": int, "message": str, "snippet": str}, ...],
                "score": int,
                "task_id": str,
            }
        """
        logger.info("开始审查 task_id=%s", task_id)

        # 1. 获取任务
        task = self._tm.get_task(task_id)
        if task is None:
            raise ValueError(f"任务不存在: {task_id}")

        # 2. 更新状态 pending → running
        self._tm.update_task(task_id, status="running")

        # 3. 广播审查开始事件
        ws_path = self._wm.get_path(task_id)
        self._bus.emit_event(
            task_id=task_id,
            agent=AGENT_NAME,
            event="task.review_started",
            payload={"workspace": str(ws_path)},
        )

        issues: List[Issue] = []

        # 4. 收集工作区代码文件
        code_files = self._collect_code_files(task_id)
        logger.info("发现 %d 个待审查文件", len(code_files))

        # 5. 对每个文件执行静态审查
        for filepath in code_files:
            issues.extend(self._static_review(filepath))

        # 6. 可选：DeepSeek 深度审查（仅 .py 文件）
        if self._deepseek_api_key:
            py_files = [f for f in code_files if f.suffix == ".py"]
            for py_file in py_files:
                try:
                    deep_issues = self._deepseek_review(py_file)
                    issues.extend(deep_issues)
                except Exception:
                    logger.exception("DeepSeek 审查失败 file=%s，跳过", py_file)

        # 7. 计算评分
        score = self._calculate_score(issues)

        # 8. 判定是否通过
        #    通过条件: score >= 70 且 无 fake_impl / syntax_error / security_concern
        fatal_categories = {"fake_impl", "syntax_error", "security_concern"}
        has_fatal = any(i.category in fatal_categories for i in issues)
        approved = score >= 70 and not has_fatal

        result = {
            "approved": approved,
            "issues": [i.to_dict() for i in issues],
            "score": score,
            "task_id": task_id,
        }

        # 9. 更新 TaskManager 并广播结果事件
        if approved:
            self._tm.update_task(task_id, status="done", result=result)
            self._bus.emit_event(
                task_id=task_id,
                agent=AGENT_NAME,
                event="task.review_approved",
                payload={"score": score, "issues_count": len(issues)},
            )
            logger.info("任务 %s 审查通过 score=%d", task_id, score)
        else:
            self._tm.update_task(task_id, status="retry", result=result)
            self._bus.emit_event(
                task_id=task_id,
                agent=AGENT_NAME,
                event="task.review_rejected",
                payload={"score": score, "issues": result["issues"]},
            )
            logger.warning(
                "任务 %s 审查不通过 score=%d fatal=%s",
                task_id,
                score,
                has_fatal,
            )

        return result

    # ── 文件收集 ─────────────────────────────────────────────
    def _collect_code_files(self, task_id: str) -> List[Path]:
        """递归收集任务 workspace 下所有受支持代码文件。"""
        files: List[Path] = []
        task_dir = self._wm.get_path(task_id)
        if not task_dir.is_dir():
            logger.warning("workspace 目录不存在: %s", task_dir)
            return files

        for entry in task_dir.rglob("*"):
            if entry.is_file() and entry.suffix.lower() in _CODE_EXTENSIONS:
                if entry.stat().st_size <= self._max_file_bytes:
                    files.append(entry)
                else:
                    logger.info("跳过超大文件: %s (%d bytes)", entry, entry.stat().st_size)
        return files

    # ── 静态审查 ─────────────────────────────────────────────
    def _static_review(self, filepath: Path) -> List[Issue]:
        """单文件静态审查入口（按类型分流）。"""
        issues: List[Issue] = []
        suffix = filepath.suffix.lower()

        try:
            source = filepath.read_text(encoding="utf-8", errors="replace")
        except Exception:
            logger.exception("读取文件失败: %s", filepath)
            return issues

        # 通用文本检查（所有文件类型）
        issues.extend(self._check_todos(filepath, source))
        issues.extend(self._check_security_concerns(filepath, source))

        # Python 专项 AST 审查
        if suffix == ".py":
            issues.extend(self._check_python_ast(filepath, source))

        return issues

    # ── TODO / FIXME / HACK 检测 ─────────────────────────────
    def _check_todos(self, filepath: Path, source: str) -> List[Issue]:
        """检测注释中的 TODO/FIXME/HACK 标记。"""
        issues: List[Issue] = []
        rel_path = self._rel(filepath)

        for lineno, line in enumerate(source.splitlines(), start=1):
            stripped = line.strip()
            # 跳过纯空行
            if not stripped:
                continue
            m = _TODO_PATTERN.search(stripped) or _LINE_TODO_PATTERN.search(stripped)
            if m:
                issues.append(
                    Issue(
                        category="todo",
                        file=rel_path,
                        line=lineno,
                        message=f"发现 {m.group(1).upper()} 标记",
                        snippet=stripped[:120],
                    )
                )
        return issues

    # ── Python AST 审查 ──────────────────────────────────────
    def _check_python_ast(self, filepath: Path, source: str) -> List[Issue]:
        """Python 深度审查：语法错误 / 假实现 / 空函数 / 缺失 docstring。"""
        issues: List[Issue] = []
        rel_path = self._rel(filepath)

        # 语法错误检测
        try:
            tree = ast.parse(source, filename=str(filepath))
        except SyntaxError as exc:
            issues.append(
                Issue(
                    category="syntax_error",
                    file=rel_path,
                    line=exc.lineno or 1,
                    message=f"语法错误: {exc.msg}",
                    snippet=(exc.text or "").rstrip()[:120],
                )
            )
            return issues  # 语法错误后不再继续 AST 分析

        # 遍历顶层节点
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.FunctionDef):
                issues.extend(self._check_function(node, rel_path, source))
            elif isinstance(node, ast.AsyncFunctionDef):
                issues.extend(self._check_function(node, rel_path, source))
            elif isinstance(node, ast.ClassDef):
                issues.extend(self._check_class(node, rel_path, source))

        # 检查模块级 docstring
        if not ast.get_docstring(tree):
            issues.append(
                Issue(
                    category="missing_docstring",
                    file=rel_path,
                    line=1,
                    message="模块缺失 docstring",
                )
            )

        return issues

    # ── 函数 / 方法审查 ─────────────────────────────────────
    def _check_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, rel_path: str, source: str
    ) -> List[Issue]:
        """对单个函数节点执行多维度审查。"""
        issues: List[Issue] = []
        func_name = node.name
        line = node.lineno

        # 跳过特殊/魔术方法中的 __init_subclass__ 之类，但保留 __init__
        # 跳过 test_ 前缀的测试函数对 docstring 的强制要求
        is_test = func_name.startswith("test_")
        is_dunder = func_name.startswith("__") and func_name.endswith("__")

        # ── docstring 检查 ──
        docstring = ast.get_docstring(node)
        if not docstring:
            # 非私有、非 dunder、非测试函数强制要求 docstring
            if not is_dunder and not is_test and not func_name.startswith("_"):
                issues.append(
                    Issue(
                        category="missing_docstring",
                        file=rel_path,
                        line=line,
                        message=f"函数 '{func_name}' 缺失 docstring",
                    )
                )
        else:
            # docstring 中包含 TODO/stub 关键词视为“假实现”线索
            if any(kw in docstring.lower() for kw in _DOCSTRING_STUB_KEYWORDS):
                issues.append(
                    Issue(
                        category="fake_impl",
                        file=rel_path,
                        line=line,
                        message=f"函数 '{func_name}' docstring 表明为桩实现",
                        snippet=textwrap.shorten(docstring, width=120, placeholder="..."),
                    )
                )

        # ── 空函数体 / 桩实现 ──
        body_issues = self._check_function_body(node, func_name, rel_path, source)
        issues.extend(body_issues)

        return issues

    def _check_function_body(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        func_name: str,
        rel_path: str,
        source: str,
    ) -> List[Issue]:
        """检查函数体是否为空或为桩实现。"""
        issues: List[Issue] = []
        body = node.body

        # 只有 docstring 没有其他语句 → 空函数
        meaningful_stmts = [
            s for s in body
            if not (isinstance(s, ast.Expr) and isinstance(s.value, (ast.Constant, ast.Str)))
        ]
        if not meaningful_stmts and body:
            # 有 body 但全部是 docstring
            if len(body) == 1 and isinstance(body[0], ast.Expr):
                expr_val = body[0].value
                if isinstance(expr_val, (ast.Constant, ast.Str)):
                    doc_val = getattr(expr_val, "value", "") or getattr(expr_val, "s", "")
                    if isinstance(doc_val, str) and any(
                        kw in doc_val.lower() for kw in _DOCSTRING_STUB_KEYWORDS
                    ):
                        cat = "fake_impl"
                        msg = f"函数 '{func_name}' 仅有桩 docstring，无实际实现"
                    else:
                        cat = "empty_func"
                        msg = f"函数 '{func_name}' 仅有 docstring，无实际实现"
                    issues.append(Issue(category=cat, file=rel_path, line=node.lineno, message=msg))
                    return issues

            issues.append(
                Issue(
                    category="empty_func",
                    file=rel_path,
                    line=node.lineno,
                    message=f"函数 '{func_name}' 体为空（无有意义语句）",
                )
            )
            return issues

        # 体只有一条语句时检查是否为桩
        if len(meaningful_stmts) == 1:
            stmt = meaningful_stmts[0]
            stmt_line = self._get_stmt_source(stmt, source)
            if any(pat.search(stmt_line) for pat in _STUB_PATTERNS):
                issues.append(
                    Issue(
                        category="fake_impl",
                        file=rel_path,
                        line=stmt.lineno,
                        message=f"函数 '{func_name}' 是桩实现 ({stmt_line.strip()[:80]})",
                        snippet=stmt_line.strip()[:120],
                    )
                )
            elif isinstance(stmt, ast.Raise) and isinstance(stmt.exc, ast.Name):
                if stmt.exc.id == "NotImplementedError":
                    issues.append(
                        Issue(
                            category="fake_impl",
                            file=rel_path,
                            line=stmt.lineno,
                            message=f"函数 '{func_name}' 是桩实现 (raise NotImplementedError)",
                        )
                    )

        return issues

    # ── 类审查 ───────────────────────────────────────────────
    def _check_class(self, node: ast.ClassDef, rel_path: str, source: str) -> List[Issue]:
        """对类节点执行审查。"""
        issues: List[Issue] = []

        # 类级 docstring
        if not ast.get_docstring(node):
            issues.append(
                Issue(
                    category="missing_docstring",
                    file=rel_path,
                    line=node.lineno,
                    message=f"类 '{node.name}' 缺失 docstring",
                )
            )

        # 遍历类内方法
        for child in node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                issues.extend(self._check_function(child, rel_path, source))

        return issues

    # ── 安全关切 ─────────────────────────────────────────────
    def _check_security_concerns(self, filepath: Path, source: str) -> List[Issue]:
        """检测常见安全隐患模式。"""
        issues: List[Issue] = []
        rel_path = self._rel(filepath)

        patterns: List[Tuple[str, str]] = [
            (r"os\.system\s*\(", "使用 os.system() 可能导致命令注入"),
            (r"subprocess\.(call|Popen|run)\s*\([^)]*shell\s*=\s*True", "subprocess 使用 shell=True 存在注入风险"),
            (r"eval\s*\(", "eval() 存在代码注入风险"),
            (r"exec\s*\(", "exec() 存在代码注入风险"),
            (r"pickle\.(loads?|dump)\s*\(", "pickle 反序列化不安全数据存在 RCE 风险"),
            (r"yaml\.load\s*\([^)]*\)", "yaml.load() 不安全，请使用 yaml.safe_load()"),
            (r"(?:password|secret|token|api_key|apikey)\s*=\s*['\"][^'\"]{8,}['\"]", "硬编码凭据"),
            (r"(?:password|secret|token|api_key)\s*=\s*['\"][^'\"]{3,}['\"]", "疑似硬编码凭据"),
        ]

        for lineno, line in enumerate(source.splitlines(), start=1):
            for pattern, msg in patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    issues.append(
                        Issue(
                            category="security_concern",
                            file=rel_path,
                            line=lineno,
                            message=msg,
                            snippet=line.strip()[:120],
                        )
                    )
                    break  # 每行只报一次最高优

        return issues

    # ── 评分 ─────────────────────────────────────────────────
    def _calculate_score(self, issues: List[Issue]) -> int:
        """根据问题列表计算 0-100 评分。"""
        score = 100
        for issue in issues:
            score -= _SEVERITY_WEIGHTS.get(issue.category, 5)
        return max(0, min(100, score))

    # ── DeepSeek 深度审查 ────────────────────────────────────
    def _deepseek_review(self, filepath: Path) -> List[Issue]:
        """调用 DeepSeek API 进行更深入的语义审查（仅 .py）。"""
        import json as _json
        import urllib.request

        rel_path = self._rel(filepath)
        try:
            source = filepath.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return []

        if len(source) > 30000:
            logger.info("文件过大，跳过 DeepSeek 审查: %s", filepath)
            return []

        prompt = f"""你是一位资深 Python 代码审查专家。请审查以下代码，找出其中存在的问题。
仅关注以下类别，不要输出其他内容：
- fake_impl: 虚假/桩实现（只返回 None、pass、raise NotImplementedError、TODO 注释但没有实际实现）
- empty_func: 空函数体
- missing_docstring: 缺失 docstring
- security_concern: 安全关切（命令注入、代码注入、硬编码凭据等）
- syntax_error: 语法错误

请以 JSON 数组格式返回，每项包含：
  "line": 行号(int),
  "category": 类别(str),
  "message": 描述(str)

如果没有发现问题，返回空数组 []。

=== 代码开始 ===
{source}
=== 代码结束 ===

请只输出 JSON 数组，不要输出其他任何内容。"""

        payload = _json.dumps({
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": "你是一个严格的代码审查专家。只输出 JSON 数组。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 2048,
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{self._deepseek_base_url}/chat/completions",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._deepseek_api_key}",
            },
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_data = _json.loads(resp.read().decode("utf-8"))

        content = resp_data["choices"][0]["message"]["content"].strip()
        # 移除可能的 markdown 代码块包裹
        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?\s*", "", content, flags=re.MULTILINE)
            content = re.sub(r"\s*```$", "", content, flags=re.MULTILINE)

        deep_issues_raw = _json.loads(content)
        if not isinstance(deep_issues_raw, list):
            logger.warning("DeepSeek 返回格式异常: %s", type(deep_issues_raw))
            return []

        issues: List[Issue] = []
        valid_categories = {"todo", "fake_impl", "empty_func", "missing_docstring", "syntax_error", "security_concern"}
        for item in deep_issues_raw:
            cat = item.get("category", "")
            if cat not in valid_categories:
                continue
            issues.append(
                Issue(
                    category=cat,
                    file=rel_path,
                    line=int(item.get("line", 1)),
                    message=str(item.get("message", "")),
                )
            )
        return issues

    # ── 工具方法 ─────────────────────────────────────────────
    def _rel(self, filepath: Path) -> str:
        """返回相对 workspace 根目录的路径字符串。"""
        try:
            return filepath.resolve().relative_to(self._wm.root.resolve()).as_posix()
        except ValueError:
            return filepath.as_posix()

    @staticmethod
    def _get_stmt_source(stmt: ast.stmt, source: str) -> str:
        """从原始源码中提取某条语句的源码行。"""
        lines = source.splitlines()
        lineno = stmt.lineno - 1  # 0-based
        end_lineno = getattr(stmt, "end_lineno", stmt.lineno)
        if 0 <= lineno < len(lines):
            return "\n".join(lines[lineno:end_lineno])
        return ""
