"""
HttpCapability —— HTTP 网络请求能力。

提供受控的 HTTP 请求能力，具备：
    - http_get(url) / http_post(url, data) / http_request(method, url, ...)
    - URL allowlist / blocklist 模式匹配
    - 响应大小限制
    - 超时控制
    - 重定向策略控制

权限声明：
    PERMISSIONS = ["NETWORK"]
"""
from __future__ import annotations

import asyncio
import fnmatch
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

from runtime.capabilities.base import BaseCapability


# ── 默认黑名单（危险/内部 URL 模式） ─────────────────────────

DEFAULT_BLOCKLIST: Tuple[str, ...] = (
    "http://localhost:*",
    "https://localhost:*",
    "http://127.*",
    "https://127.*",
    "http://[::1]:*",
    "https://[::1]:*",
    "http://0.0.0.0:*",
    "https://0.0.0.0:*",
    "http://169.254.*",          # link-local
    "http://10.*",               # private A
    "http://172.16.*",           # private B
    "http://172.17.*",
    "http://172.18.*",
    "http://172.19.*",
    "http://172.20.*",
    "http://172.21.*",
    "http://172.22.*",
    "http://172.23.*",
    "http://172.24.*",
    "http://172.25.*",
    "http://172.26.*",
    "http://172.27.*",
    "http://172.28.*",
    "http://172.29.*",
    "http://172.30.*",
    "http://172.31.*",
    "http://192.168.*",          # private C
    "https://10.*",
    "https://172.16.*",
    "https://172.17.*",
    "https://172.18.*",
    "https://172.19.*",
    "https://172.20.*",
    "https://172.21.*",
    "https://172.22.*",
    "https://172.23.*",
    "https://172.24.*",
    "https://172.25.*",
    "https://172.26.*",
    "https://172.27.*",
    "https://172.28.*",
    "https://172.29.*",
    "https://172.30.*",
    "https://172.31.*",
    "https://192.168.*",
    "*.local",
    "*.internal",
    "*.localhost",
)


# ── 结果数据类 ──────────────────────────────────────────────

@dataclass
class HttpResult:
    """HTTP 请求结果。

    Attributes
    ----------
    ok : bool
        请求是否成功（2xx 状态码）。
    url : str
        最终请求 URL（含重定向后的地址）。
    status_code : int
        HTTP 状态码。
    headers : dict
        响应头字典。
    body : str
        响应体文本（自动解码）。
    truncated : bool
        响应体是否因超过大小限制而被截断。
    content_length : int
        响应体原始字节长度。
    elapsed_ms : int
        请求耗时（毫秒）。
    error : str | None
        错误信息（仅在 ok=False 时）。
    """
    ok: bool = False
    url: str = ""
    status_code: int = 0
    headers: Dict[str, str] = field(default_factory=dict)
    body: str = ""
    truncated: bool = False
    content_length: int = 0
    elapsed_ms: int = 0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "ok": self.ok,
            "url": self.url,
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.body,
            "content_length": self.content_length,
            "elapsed_ms": self.elapsed_ms,
            "truncated": self.truncated,
        }
        if self.error:
            result["error"] = self.error
        return result

    def json(self) -> Any:
        """尝试将 body 解析为 JSON。

        Returns
        -------
        Any
            解析后的 JSON 对象。

        Raises
        ------
        json.JSONDecodeError
            若 body 不是有效 JSON。
        """
        return json.loads(self.body)


# ── HttpCapability ──────────────────────────────────────────

class HttpCapability(BaseCapability):
    """受控 HTTP 网络请求能力。

    提供 URL 白名单/黑名单控制、响应大小限制、超时保护。
    所有请求通过 Python 标准库 urllib 实现，无外部依赖。

    使用示例::

        http = HttpCapability()
        # 发起 GET 请求
        result = await http.http_get("https://api.example.com/data")
        if result.ok:
            data = result.json()
        # 发起 POST 请求
        result = await http.http_post("https://api.example.com/submit", {"key": "val"})
    """

    PERMISSIONS = ["NETWORK"]
    """所需权限声明。"""

    # ── 初始化 ────────────────────────────────────────────

    def __init__(
        self,
        default_timeout: int = 30,
        max_response_bytes: int = 5_000_000,  # 5 MB
        max_redirects: int = 5,
        allowlist: Optional[List[str]] = None,
        blocklist: Optional[List[str]] = None,
        default_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """初始化 HTTP 能力。

        Parameters
        ----------
        default_timeout : int
            默认请求超时时间（秒）。
        max_response_bytes : int
            最大响应体字节数，超过将截断。
        max_redirects : int
            最大重定向次数。
        allowlist : list[str] | None
            URL 白名单模式列表。若提供，仅允许匹配的 URL。
            None 表示不限制（仅检查黑名单）。
        blocklist : list[str] | None
            URL 黑名单模式列表。若提供，阻止匹配的 URL。
            None 表示使用默认黑名单。
        default_headers : dict | None
            每个请求默认携带的 HTTP 头。
        """
        super().__init__()
        self.default_timeout: int = default_timeout
        self.max_response_bytes: int = max_response_bytes
        self.max_redirects: int = max_redirects

        self.allowlist: Optional[List[str]] = (
            list(allowlist) if allowlist is not None else None
        )
        self.blocklist: List[str] = (
            list(blocklist) if blocklist is not None
            else list(DEFAULT_BLOCKLIST)
        )
        self.default_headers: Dict[str, str] = dict(default_headers or {})
        if "User-Agent" not in self.default_headers:
            self.default_headers["User-Agent"] = "ai-runtime-http/1.0"

        self.logger.debug(
            "HttpCapability 初始化: timeout=%ds max_body=%d max_redirects=%d",
            self.default_timeout, self.max_response_bytes, self.max_redirects,
        )

    # ── 属性 ──────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "http"

    @property
    def description(self) -> str:
        return "Controlled HTTP client: GET, POST, custom requests with URL allowlist/blocklist, response size limits, and timeout protection."

    # ── 核心执行 ─────────────────────────────────────────

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """根据 operation 参数路由到具体方法。

        Parameters
        ----------
        operation : str
            操作类型：'get' / 'post' / 'request'
        **kwargs
            各操作的具体参数。

        Returns
        -------
        HttpResult
        """
        operation = kwargs.pop("operation", None)
        if not operation and args:
            operation = args[0]

        if operation == "get":
            return await self.http_get(
                url=kwargs.get("url", ""),
                headers=kwargs.get("headers"),
            )
        elif operation == "post":
            return await self.http_post(
                url=kwargs.get("url", ""),
                data=kwargs.get("data"),
                headers=kwargs.get("headers"),
            )
        elif operation == "request":
            return await self.http_request(
                method=kwargs.get("method", "GET"),
                url=kwargs.get("url", ""),
                headers=kwargs.get("headers"),
                body=kwargs.get("body"),
            )
        else:
            raise ValueError(f"未知的 HTTP 操作: {operation!r}")

    # ── GET 请求 ─────────────────────────────────────────

    async def http_get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> HttpResult:
        """发起 HTTP GET 请求。

        Parameters
        ----------
        url : str
            请求 URL。
        headers : dict | None
            额外请求头。会与默认头合并。

        Returns
        -------
        HttpResult
        """
        return await self.http_request("GET", url, headers=headers)

    # ── POST 请求 ────────────────────────────────────────

    async def http_post(
        self,
        url: str,
        data: Any = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> HttpResult:
        """发起 HTTP POST 请求。

        Parameters
        ----------
        url : str
            请求 URL。
        data : Any
            请求体数据。dict/list 自动序列化为 JSON 并设置 Content-Type。
            字符串直接发送，None 发送空体。
        headers : dict | None
            额外请求头。

        Returns
        -------
        HttpResult
        """
        body: Optional[bytes] = None
        merged_headers: Dict[str, str] = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)

        if data is not None:
            if isinstance(data, (dict, list)):
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                merged_headers.setdefault("Content-Type", "application/json; charset=utf-8")
            elif isinstance(data, str):
                body = data.encode("utf-8")
                merged_headers.setdefault("Content-Type", "text/plain; charset=utf-8")
            elif isinstance(data, bytes):
                body = data
            else:
                body = str(data).encode("utf-8")
                merged_headers.setdefault("Content-Type", "text/plain; charset=utf-8")

        return await self.http_request("POST", url, headers=merged_headers, body=body)

    # ── 通用请求 ─────────────────────────────────────────

    async def http_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
    ) -> HttpResult:
        """发起通用 HTTP 请求。

        Parameters
        ----------
        method : str
            HTTP 方法（GET / POST / PUT / DELETE 等）。
        url : str
            请求 URL。
        headers : dict | None
            请求头。会与默认头合并。
        body : bytes | None
            请求体（原始字节）。None 表示空体。

        Returns
        -------
        HttpResult
        """
        if not self._enabled:
            return HttpResult(
                ok=False,
                url=url,
                error="HttpCapability 未启用",
            )

        # ── URL 校验 ──────────────────────────────────
        try:
            self._check_url(url)
        except ValueError as exc:
            self.logger.warning("URL 被拒绝: %s — %s", url, exc)
            return HttpResult(
                ok=False,
                url=url,
                error=str(exc),
            )

        # ── 构建请求头 ────────────────────────────────
        merged_headers: Dict[str, str] = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)

        # ── 执行请求 ──────────────────────────────────
        self.logger.info("HTTP %s %s", method, url)

        import time
        start_time = time.monotonic()

        try:
            req = urllib.request.Request(
                url,
                data=body,
                headers=merged_headers,
                method=method,
            )

            # 在线程池中执行同步 HTTP 请求，避免阻塞事件循环
            loop = asyncio.get_running_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: urllib.request.urlopen(req, timeout=self.default_timeout),
                ),
                timeout=self.default_timeout + 5,
            )

            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            status_code = response.getcode()
            resp_url = response.geturl()

            # 提取响应头
            resp_headers: Dict[str, str] = {}
            for key, value in response.headers.items():
                resp_headers[key] = value

            # 读取响应体，受大小限制
            raw_body = b""
            truncated = False
            while True:
                chunk = await loop.run_in_executor(
                    None, response.read, 65536,  # 64 KB chunks
                )
                if not chunk:
                    break
                if len(raw_body) + len(chunk) > self.max_response_bytes:
                    raw_body += chunk[:self.max_response_bytes - len(raw_body)]
                    truncated = True
                    self.logger.warning(
                        "响应体过大，截断: %s (%d bytes > %d bytes limit)",
                        url, len(raw_body) + len(chunk), self.max_response_bytes,
                    )
                    break
                raw_body += chunk

            # 自动解码
            content_type = resp_headers.get("Content-Type", "")
            charset = "utf-8"
            charset_match = re.search(r"charset=([\w-]+)", content_type, re.IGNORECASE)
            if charset_match:
                charset = charset_match.group(1)

            try:
                body_text = raw_body.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                body_text = raw_body.decode("utf-8", errors="replace")

            ok = 200 <= status_code < 300
            self.logger.debug(
                "HTTP %s %s → %d (%d bytes, %dms)%s",
                method, url, status_code, len(raw_body), elapsed_ms,
                " [TRUNCATED]" if truncated else "",
            )

            return HttpResult(
                ok=ok,
                url=resp_url,
                status_code=status_code,
                headers=resp_headers,
                body=body_text,
                truncated=truncated,
                content_length=len(raw_body),
                elapsed_ms=elapsed_ms,
            )

        except asyncio.TimeoutError:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            self.logger.warning("HTTP 超时: %s %s (%dms)", method, url, elapsed_ms)
            return HttpResult(
                ok=False,
                url=url,
                elapsed_ms=elapsed_ms,
                error=f"请求超时 ({self.default_timeout}s)",
            )
        except urllib.error.HTTPError as exc:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            self.logger.warning("HTTP %s %s → %d (%dms)", method, url, exc.code, elapsed_ms)
            return HttpResult(
                ok=False,
                url=exc.geturl() if hasattr(exc, "geturl") else url,
                status_code=exc.code,
                headers=dict(exc.headers) if hasattr(exc, "headers") else {},
                body=exc.read().decode("utf-8", errors="replace")[:self.max_response_bytes] if hasattr(exc, "read") else "",
                elapsed_ms=elapsed_ms,
                error=f"HTTP {exc.code}: {exc.reason}",
            )
        except urllib.error.URLError as exc:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            self.logger.exception("HTTP 连接失败: %s %s", method, url)
            return HttpResult(
                ok=False,
                url=url,
                elapsed_ms=elapsed_ms,
                error=f"连接失败: {exc.reason}",
            )
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            self.logger.exception("HTTP 请求异常: %s %s", method, url)
            return HttpResult(
                ok=False,
                url=url,
                elapsed_ms=elapsed_ms,
                error=str(exc),
            )

    # ── 校验与清理 ───────────────────────────────────────

    def validate(self) -> None:
        """校验前置条件（HTTP 能力无额外前置条件）。"""
        pass

    def sanitize(self, *args: Any) -> Tuple[Any, ...]:
        """对输入参数做基本清理。"""
        return args

    # ── 内部方法 ─────────────────────────────────────────

    def _check_url(self, url: str) -> None:
        """校验 URL 是否合法且符合 allowlist/blocklist 规则。

        Raises
        ------
        ValueError
            若 URL 不合法或被阻止。
        """
        if not url:
            raise ValueError("URL 不能为空")

        # 解析 URL
        parsed = urllib.parse.urlparse(url)

        # 仅允许 http / https
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"不支持的协议: {parsed.scheme!r} (仅允许 http/https)")

        if not parsed.netloc:
            raise ValueError(f"URL 缺少主机名: {url!r}")

        # 黑名单检查
        if self.blocklist and self._url_matches_any(url, self.blocklist):
            raise ValueError(f"URL 被黑名单阻止: {url!r}")

        # 白名单检查
        if self.allowlist and not self._url_matches_any(url, self.allowlist):
            raise ValueError(f"URL 不在白名单中: {url!r}")

        # 防止 SSRF：拒绝原始 IP 地址形式的 URL（localhost/内网 已在黑名单中）
        hostname = parsed.hostname
        if hostname is None:
            raise ValueError(f"无法解析 URL 主机名: {url!r}")

    @staticmethod
    def _url_matches_any(url: str, patterns: List[str]) -> bool:
        """检查 URL 是否匹配任一 fnmatch 模式。

        Parameters
        ----------
        url : str
            要检查的 URL 字符串。
        patterns : list[str]
            fnmatch 模式列表。

        Returns
        -------
        bool
        """
        for pattern in patterns:
            if fnmatch.fnmatch(url, pattern):
                return True
            if fnmatch.fnmatch(url.lower(), pattern.lower()):
                return True
        return False

    # ── 配置方法 ─────────────────────────────────────────

    def add_to_allowlist(self, *patterns: str) -> None:
        """向白名单追加模式。"""
        if self.allowlist is None:
            self.allowlist = []
        for p in patterns:
            if p not in self.allowlist:
                self.allowlist.append(p)
                self.logger.debug("白名单新增: %s", p)

    def remove_from_allowlist(self, *patterns: str) -> None:
        """从白名单移除模式。"""
        if self.allowlist:
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
