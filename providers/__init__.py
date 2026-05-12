"""Providers package — AI model backend abstraction layer.

Exports
-------
- ``BaseProvider`` — abstract base class
- ``DeepSeekProvider`` — DeepSeek API (OpenAI-compatible)
- ``OpenAIProvider`` — OpenAI API
- ``OllamaProvider`` — local Ollama models
- ``registry`` — singleton :class:`ProviderRegistry`
"""

from .base_provider import BaseProvider
from .deepseek_provider import DeepSeekProvider
from .openai_provider import OpenAIProvider
from .ollama_provider import OllamaProvider
from .registry import registry

__all__ = [
    "BaseProvider",
    "DeepSeekProvider",
    "OpenAIProvider",
    "OllamaProvider",
    "registry",
]
