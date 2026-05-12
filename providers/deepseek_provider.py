"""DeepSeek Provider —— 基于 OpenAI SDK 调用 DeepSeek API。

DeepSeek API 完全兼容 OpenAI SDK，只需修改 base_url。

Usage:
    provider = DeepSeekProvider(api_key="sk-xxx")
    result = provider.chat([
        {"role": "system", "content": "你是助手"},
        {"role": "user", "content": "你好"}
    ])
"""

from __future__ import annotations

import os
from typing import Any, Dict, Generator, List, Optional

from openai import OpenAI

from .base_provider import BaseProvider


class DeepSeekProvider(BaseProvider):
    """DeepSeek chat-completion provider.

    Requires an API key passed directly or via the ``DEEPSEEK_API_KEY``
    environment variable.
    """

    DEFAULT_BASE_URL = "https://api.deepseek.com/v1"
    DEFAULT_MODEL = "deepseek-chat"

    # DeepSeek pricing (USD per 1K tokens) — approximate, update as needed.
    cost_per_1k_tokens: Dict[str, float] = {
        "prompt": 0.00014,
        "completion": 0.00028,
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        retry_config: Optional[Dict[str, Any]] = None,
        timeout: Optional[Dict[str, float]] = None,
    ) -> None:
        api_key = api_key or os.environ.get("DEEPSEEK_API_KEY", "")
        model = model or self.DEFAULT_MODEL
        base_url = base_url or self.DEFAULT_BASE_URL

        super().__init__(
            model=model,
            api_key=api_key,
            base_url=base_url,
            retry_config=retry_config,
            timeout=timeout,
        )

        self._raise_for_api_key("DeepSeek", "DEEPSEEK_API_KEY")

        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=self.timeout["read_timeout"],
        )

    # ------------------------------------------------------------------
    #  Core interface
    # ------------------------------------------------------------------

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> str:
        """Synchronous chat completion."""
        response = self._execute_with_retry(
            self._client.chat.completions.create,
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        self._update_metrics(
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
        )
        return response.choices[0].message.content

    def chat_stream(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> Generator[str, None, None]:
        """Stream text chunks as they arrive from DeepSeek."""
        stream = self._execute_with_retry(
            self._client.chat.completions.create,
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
            stream_options={"include_usage": True},
            **kwargs,
        )
        prompt_tokens = 0
        completion_tokens = 0
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
            if hasattr(chunk, "usage") and chunk.usage:
                prompt_tokens = chunk.usage.prompt_tokens
                completion_tokens = chunk.usage.completion_tokens

        self._update_metrics(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

    def chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict[str, Any]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Chat completion with tool-calling (function-calling)."""
        response = self._execute_with_retry(
            self._client.chat.completions.create,
            model=self._model,
            messages=messages,
            tools=tools,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        self._update_metrics(
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
        )
        choice = response.choices[0].message
        result: Dict[str, Any] = {
            "content": choice.content,
            "tool_calls": None,
        }
        if choice.tool_calls:
            result["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": tc.type,
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in choice.tool_calls
            ]
        return result
