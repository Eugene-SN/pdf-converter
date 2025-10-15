"""Translator package initialization and lightweight public API helpers."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

try:
    from .vllm_client import (
        build_vllm_headers,
        create_vllm_requests_session,
        get_vllm_api_key,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency guard
    if exc.name != "requests":
        raise

    def _missing_requests(*_args, **_kwargs):
        raise ModuleNotFoundError(
            "translator optional dependency 'requests' is required for HTTP helpers"
        ) from exc

    build_vllm_headers = _missing_requests
    create_vllm_requests_session = _missing_requests
    get_vllm_api_key = _missing_requests

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
