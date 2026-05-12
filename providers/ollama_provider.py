"""Ollama Provider — local LLMs via the Ollama OpenAI-compatible endpoint.

Default base URL: ``http://localhost:11434/v1``
Default model: ``llama3.2``

No API key required — Ollama runs entirely on your machine.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Generator, List, Optional

from openai import OpenAI

from .base_provider import BaseProvider


class OllamaProvider(BaseProvider):
    """Ollama local-model provider.

    Communicates with a running Ollama server using its OpenAI-compatible
    chat endpoint.  No API key needed.

    Usage::

        provider = OllamaProvider(model="llama3.2")
        reply = provider.chat([{"role": "user", "content": "Hello!"}])
    """

    DEFAULT_BASE_URL = "http://localhost:11434/v1"
    DEFAULT_MODEL = "llama3.2"

    # Local models are free.
    cost_per_1k_tokens: Dict[str, float] = {
        "prompt": 0.0,
        "completion": 0.0,
    }

    def __init__(
        self,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        retry_config: Optional[Dict[str, Any]] = None,
        timeout: Optional[Dict[str, float]] = None,
    ) -> None:
        model = model or self.DEFAULT_MODEL
        base_url = base_url or os.environ.get("OLLAMA_HOST", self.DEFAULT_BASE_URL)
        # Ollama doesn't need an API key, but the OpenAI client expects one.
        api_key = api_key or "ollama"

        super().__init__(
            model=model,
            api_key=api_key,
            base_url=base_url,
            retry_config=retry_config,
            timeout=timeout,
        )

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
        # Ollama may not always return usage info.
        prompt_tokens = getattr(response.usage, "prompt_tokens", 0) if response.usage else 0
        completion_tokens = getattr(response.usage, "completion_tokens", 0) if response.usage else 0
        self._update_metrics(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
        return response.choices[0].message.content

    def chat_stream(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> Generator[str, None, None]:
        """Stream text chunks as they arrive."""
        stream = self._execute_with_retry(
            self._client.chat.completions.create,
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
            **kwargs,
        )
        prompt_tokens = 0
        completion_tokens = 0
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
            if hasattr(chunk, "usage") and chunk.usage:
                prompt_tokens = getattr(chunk.usage, "prompt_tokens", 0)
                completion_tokens = getattr(chunk.usage, "completion_tokens", 0)

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
        """Chat completion with tool-calling.

        .. note::
            Tool calling requires an Ollama model that supports it
            (e.g. ``llama3.2``, ``mistral``).  Older models may not
            return ``tool_calls``.
        """
        response = self._execute_with_retry(
            self._client.chat.completions.create,
            model=self._model,
            messages=messages,
            tools=tools,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        prompt_tokens = getattr(response.usage, "prompt_tokens", 0) if response.usage else 0
        completion_tokens = getattr(response.usage, "completion_tokens", 0) if response.usage else 0
        self._update_metrics(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
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
