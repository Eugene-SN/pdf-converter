"""Utility helpers for constructing vLLM HTTP clients."""

from __future__ import annotations

import os
from typing import Dict, Optional, TYPE_CHECKING

import requests

try:  # pragma: no cover - optional dependency guard
    import aiohttp
except ModuleNotFoundError:  # pragma: no cover
    aiohttp = None  # type: ignore

if TYPE_CHECKING:  # pragma: no cover
    import aiohttp


DEFAULT_VLLM_URL = "http://vllm-server:8000"


class MissingVLLMApiKeyError(RuntimeError):
    """Raised when a vLLM API key is required but not configured."""


def get_vllm_server_url() -> str:
    """Return the configured vLLM base URL."""

    return os.getenv("VLLM_SERVER_URL", DEFAULT_VLLM_URL).rstrip("/")


def get_vllm_api_key() -> Optional[str]:
    """Return the vLLM API key if it is configured."""

    api_key = os.getenv("VLLM_API_KEY", "").strip()
    return api_key or None


def require_vllm_api_key() -> str:
    """Return the configured vLLM API key or raise a clear error."""

    api_key = get_vllm_api_key()
    if not api_key:
        raise MissingVLLMApiKeyError(
            "VLLM_API_KEY environment variable is required for vLLM requests"
        )
    return api_key


def build_vllm_headers(
    content_type: Optional[str] = None,
    *,
    api_key: Optional[str] = None,
) -> Dict[str, str]:
    """Construct default headers for talking to vLLM."""

    headers: Dict[str, str] = {}
    resolved_api_key = (api_key or "").strip() or require_vllm_api_key()
    headers["Authorization"] = f"Bearer {resolved_api_key}"
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def create_vllm_requests_session(*, api_key: Optional[str] = None) -> requests.Session:
    """Return a ``requests`` session that includes vLLM auth headers."""

    session = requests.Session()
    headers = build_vllm_headers(api_key=api_key)
    if headers:
        session.headers.update(headers)
    return session


def create_vllm_aiohttp_session(
    *, timeout: Optional[aiohttp.ClientTimeout] = None, api_key: Optional[str] = None, **kwargs
) -> aiohttp.ClientSession:
    """Return an ``aiohttp`` client session with vLLM auth headers."""

    if aiohttp is None:  # pragma: no cover - dependency guard
        raise ModuleNotFoundError("aiohttp is required to create an async vLLM client")

    base_headers = build_vllm_headers(api_key=api_key)
    extra_headers = kwargs.pop("headers", None) or {}
    headers = {**base_headers, **extra_headers}
    return aiohttp.ClientSession(timeout=timeout, headers=headers, **kwargs)


__all__ = [
    "MissingVLLMApiKeyError",
    "build_vllm_headers",
    "create_vllm_aiohttp_session",
    "create_vllm_requests_session",
    "get_vllm_api_key",
    "get_vllm_server_url",
    "require_vllm_api_key",
]

