"""Enhanced AI Provider Abstraction Layer.

BaseProvider is the unified interface for all model backends. Subclasses
implement chat / chat_stream / chat_with_tools; the base class provides
retry, timeout, cost tracking, and metrics infrastructure.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Optional


class BaseProvider(ABC):
    """Abstract base for every AI model provider.

    Subclasses must implement:
        chat()
        chat_stream()
        chat_with_tools()

    Infrastructure provided out-of-the-box:
        - Exponential-backoff retry (``retry_config``)
        - Connect / read timeouts (``timeout``)
        - Per-instance usage metrics (``metrics``)
        - Token cost tracking (``cost_per_1k_tokens``)

    Usage::

        class MyProvider(BaseProvider):
            def chat(self, messages, temperature, max_tokens, **kwargs):
                ...
            def chat_stream(self, messages, temperature, max_tokens, **kwargs):
                ...
            def chat_with_tools(self, messages, tools, **kwargs):
                ...
    """

    # ------------------------------------------------------------------
    #  Cost per 1 000 tokens (USD).  Subclasses SHOULD override.
    #  Format: {"prompt": float, "completion": float}
    # ------------------------------------------------------------------
    cost_per_1k_tokens: Dict[str, float] = {
        "prompt": 0.0,
        "completion": 0.0,
    }

    # ------------------------------------------------------------------
    #  Instance lifecycle
    # ------------------------------------------------------------------

    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        retry_config: Optional[Dict[str, Any]] = None,
        timeout: Optional[Dict[str, float]] = None,
    ) -> None:
        self._model = model
        self._api_key = api_key
        self._base_url = base_url

        # ---- Retry config -------------------------------------------------
        self.retry_config: Dict[str, Any] = {
            "max_retries": 3,
            "backoff_base": 1.0,   # seconds → 1, 2, 4, ...
            "backoff_max": 30.0,   # cap at 30 s
        }
        if retry_config:
            self.retry_config.update(retry_config)

        # ---- Timeout config -----------------------------------------------
        self.timeout: Dict[str, float] = {
            "connect_timeout": 30.0,
            "read_timeout": 120.0,
        }
        if timeout:
            self.timeout.update(timeout)

        # ---- Metrics (per-instance accumulator) ---------------------------
        self.metrics: Dict[str, float] = {
            "total_tokens": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_calls": 0,
            "total_cost": 0.0,
        }

    # ------------------------------------------------------------------
    #  Properties
    # ------------------------------------------------------------------

    @property
    def model(self) -> str:
        """Model identifier (e.g. ``gpt-4o-mini``)."""
        return self._model

    @property
    def base_url(self) -> Optional[str]:
        """API base URL (e.g. ``https://api.openai.com/v1``)."""
        return self._base_url

    # ------------------------------------------------------------------
    #  Abstract interface — subclasses MUST override
    # ------------------------------------------------------------------

    @abstractmethod
    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> str:
        """Synchronous chat completion → returns the full response text.

        Parameters
        ----------
        messages : list[dict]
            OpenAI-compatible message list.
            ``[{"role": "system", "content": "..."}, ...]``
        temperature : float, default 0.3
            Sampling temperature (0–2).
        max_tokens : int, default 4096
            Maximum tokens in the completion.

        Returns
        -------
        str
            Model response text.
        """
        ...

    @abstractmethod
    def chat_stream(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> Generator[str, None, None]:
        """Streaming chat completion — yields text chunks as they arrive.

        Parameters
        ----------
        messages : list[dict]
            OpenAI-compatible message list.
        temperature : float, default 0.3
        max_tokens : int, default 4096

        Yields
        ------
        str
            Incremental text chunks from the model.
        """
        ...

    @abstractmethod
    def chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict[str, Any]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Chat completion with tool-calling (function-calling) support.

        Parameters
        ----------
        messages : list[dict]
        tools : list[dict]
            OpenAI-compatible tool definitions.
            ``[{"type": "function", "function": {...}}, ...]``
        temperature : float, default 0.3
        max_tokens : int, default 4096

        Returns
        -------
        dict
            ``{"content": str | None, "tool_calls": list[dict] | None}``
        """
        ...

    # ------------------------------------------------------------------
    #  Retry infrastructure (concrete, reusable by subclasses)
    # ------------------------------------------------------------------

    def _execute_with_retry(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Call *func* with exponential-backoff retry.

        When a provider's low-level API call raises a *retryable* exception
        (e.g. ``RateLimitError``, ``APIConnectionError``), wrap it with this
        helper::

            def _call_api(self, ...):
                return self._execute_with_retry(self._client.chat.completions.create, **params)
        """
        last_exc: Optional[Exception] = None
        max_retries: int = self.retry_config["max_retries"]
        base: float = self.retry_config["backoff_base"]
        cap: float = self.retry_config["backoff_max"]

        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                last_exc = exc
                if attempt == max_retries:
                    raise
                delay = min(base * (2 ** attempt), cap)
                time.sleep(delay)

        # Should never reach here, but keep the type-checker happy.
        raise RuntimeError("Unexpected retry exhaustion") from last_exc

    # ------------------------------------------------------------------
    #  Metrics helpers (concrete, call from subclasses after each API call)
    # ------------------------------------------------------------------

    def _update_metrics(
        self,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
    ) -> None:
        """Record usage statistics after a successful API call.

        Expected usage in a subclass ``chat()`` implementation::

            response = self._execute_with_retry(...)
            prompt_tokens = response.usage.prompt_tokens
            completion_tokens = response.usage.completion_tokens
            self._update_metrics(prompt_tokens, completion_tokens)
        """
        total = prompt_tokens + completion_tokens
        self.metrics["total_tokens"] += total
        self.metrics["prompt_tokens"] += prompt_tokens
        self.metrics["completion_tokens"] += completion_tokens
        self.metrics["total_calls"] += 1

        # Cost calculation
        prompt_cost = (prompt_tokens / 1000.0) * self.cost_per_1k_tokens.get(
            "prompt", 0.0
        )
        completion_cost = (completion_tokens / 1000.0) * self.cost_per_1k_tokens.get(
            "completion", 0.0
        )
        self.metrics["total_cost"] += prompt_cost + completion_cost

    def reset_metrics(self) -> None:
        """Zero out all metrics counters (useful between benchmark runs)."""
        for key in self.metrics:
            self.metrics[key] = 0.0 if key == "total_cost" else 0

    # ------------------------------------------------------------------
    #  Helpers
    # ------------------------------------------------------------------

    def _raise_for_api_key(self, provider_name: str, env_var: str) -> None:
        """Convenience: raise a clear message when the API key is missing."""
        if not self._api_key:
            raise ValueError(
                f"{provider_name} API key not set. "
                f"Pass ``api_key`` or set the ``{env_var}`` environment variable."
            )
