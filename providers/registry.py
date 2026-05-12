"""Provider Registry — centralised factory and discovery for AI backends.

Usage::

    from providers.registry import registry

    # Create by name
    provider = registry.create("openai", api_key="sk-...")

    # Auto-discover from environment
    provider = registry.discover_from_env()

    # List available providers
    for name in registry.list_providers():
        print(name)
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Type

from .base_provider import BaseProvider

# Lazy imports to avoid circular dependencies at module level.
# Providers are imported when first registered / instantiated.
_PROVIDER_IMPORT_PATHS: Dict[str, str] = {
    "deepseek": ".deepseek_provider:DeepSeekProvider",
    "openai": ".openai_provider:OpenAIProvider",
    "ollama": ".ollama_provider:OllamaProvider",
}


def _import_class(dotted_path: str) -> Type[BaseProvider]:
    """Resolve ``module:Class`` to an actual class object."""
    mod_relpath, class_name = dotted_path.split(":")
    import importlib

    mod = importlib.import_module(mod_relpath, package="providers")
    return getattr(mod, class_name)


class ProviderRegistry:
    """Registry of available AI provider classes.

    Thread-safe only if callers serialise access; for single-threaded
    agent runtimes that is always true.
    """

    def __init__(self) -> None:
        self._registry: Dict[str, Type[BaseProvider]] = {}
        self._default: Optional[str] = None

    # ------------------------------------------------------------------
    #  Registration
    # ------------------------------------------------------------------

    def register(self, name: str, provider_class: Type[BaseProvider]) -> None:
        """Register a provider class under *name*.

        Parameters
        ----------
        name : str
            Short name (e.g. ``"openai"``).
        provider_class : type
            A concrete subclass of :class:`BaseProvider`.
        """
        if not issubclass(provider_class, BaseProvider):
            raise TypeError(
                f"{provider_class.__name__} must be a subclass of BaseProvider."
            )
        self._registry[name] = provider_class

        # First registered provider becomes the default.
        if self._default is None:
            self._default = name

    # ------------------------------------------------------------------
    #  Factory
    # ------------------------------------------------------------------

    def create(self, name: str, **kwargs: Any) -> BaseProvider:
        """Instantiate a provider by its registered *name*.

        Parameters
        ----------
        name : str
            Registered provider name.
        **kwargs
            Forwarded to the provider's ``__init__``.

        Returns
        -------
        BaseProvider
            Ready-to-use provider instance.

        Raises
        ------
        KeyError
            If *name* is not registered.
        """
        # Lazy-load if not yet imported.
        if name not in self._registry:
            if name in _PROVIDER_IMPORT_PATHS:
                cls = _import_class(_PROVIDER_IMPORT_PATHS[name])
                self.register(name, cls)
            else:
                raise KeyError(
                    f"Unknown provider '{name}'. "
                    f"Available: {list(self._registry.keys())}"
                )
        return self._registry[name](**kwargs)

    # ------------------------------------------------------------------
    #  Introspection
    # ------------------------------------------------------------------

    def list_providers(self) -> list:
        """Return all registered provider names."""
        return list(self._registry.keys())

    def get_default(self) -> Optional[str]:
        """Return the name of the default provider, or ``None``."""
        return self._default

    # ------------------------------------------------------------------
    #  Environment discovery
    # ------------------------------------------------------------------

    def discover_from_env(self, **override_kwargs: Any) -> BaseProvider:
        """Auto-select and instantiate a provider from environment variables.

        Resolution order:
            1. ``AI_PROVIDER`` — explicit provider name (e.g. ``"deepseek"``).
            2. ``DEEPSEEK_API_KEY`` present → DeepSeek.
            3. ``OPENAI_API_KEY`` present → OpenAI.
            4. Fallback → Ollama (local, no key needed).

        ``AI_MODEL`` overrides the default model of the chosen provider.

        Parameters
        ----------
        **override_kwargs
            Keyword arguments that override environment-derived values
            (e.g. ``model="gpt-4o"``).

        Returns
        -------
        BaseProvider
            Configured provider instance.
        """
        provider_name = os.environ.get("AI_PROVIDER", "").strip().lower()

        if not provider_name:
            if os.environ.get("DEEPSEEK_API_KEY"):
                provider_name = "deepseek"
            elif os.environ.get("OPENAI_API_KEY"):
                provider_name = "openai"
            else:
                provider_name = "ollama"

        kwargs: Dict[str, Any] = {}

        # Model override
        env_model = os.environ.get("AI_MODEL")
        if env_model:
            kwargs["model"] = env_model

        # Base URL overrides
        if provider_name == "ollama":
            ollama_host = os.environ.get("OLLAMA_HOST")
            if ollama_host:
                kwargs["base_url"] = ollama_host

        # Let caller override everything.
        kwargs.update(override_kwargs)

        return self.create(provider_name, **kwargs)


# ----------------------------------------------------------------------
#  Singleton instance + eager pre-registration
# ----------------------------------------------------------------------

registry = ProviderRegistry()

# Pre-register built-in providers (lazy imports on first .create() call).
for _name in _PROVIDER_IMPORT_PATHS:
    try:
        _cls = _import_class(_PROVIDER_IMPORT_PATHS[_name])
        registry.register(_name, _cls)
    except Exception:  # noqa: BLE001 — optional dependency may be missing
        pass
