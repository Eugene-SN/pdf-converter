"""Translator package initialization and lightweight public API helpers."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .vllm_client import (
    build_vllm_headers,
    create_vllm_requests_session,
    get_vllm_api_key,
)

if TYPE_CHECKING:  # pragma: no cover - used only for type checkers
    from .translator import Translator  # noqa: F401

__all__ = [
    "Translator",
    "build_vllm_headers",
    "create_vllm_requests_session",
    "get_vllm_api_key",
]


def __getattr__(name: str) -> Any:
    """Lazily load heavy submodules on demand.

    This avoids importing optional FastAPI dependencies when only the
    ``translator.vllm_client`` helpers are required (e.g. in Airflow DAGs).
    """

    if name == "Translator":
        module = import_module(".translator", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
