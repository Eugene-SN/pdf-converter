"""Shared vLLM configuration for Quality Assurance components."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

try:  # pragma: no cover - optional import when used outside Airflow
    from airflow.dags.shared_utils import ConfigUtils  # type: ignore
except Exception:  # pragma: no cover - fallback for standalone execution
    try:
        from shared_utils import ConfigUtils  # type: ignore
    except Exception:  # pragma: no cover - ultimate fallback
        ConfigUtils = None  # type: ignore

_DEFAULT_BASE_URL = os.getenv("VLLM_SERVER_URL", "http://vllm-server:8000").rstrip("/")
_DEFAULT_ENDPOINT = f"{_DEFAULT_BASE_URL}/v1/chat/completions"
_DEFAULT_MODEL = os.getenv(
    "VLLM_MODEL_NAME", "Qwen/Qwen3-30B-A3B-Instruct-2507"
)


def _optional_env(key: str) -> Optional[str]:
    value = os.getenv(key)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _resolve_api_key(*, refresh: bool = False) -> Optional[str]:
    """Return the currently configured vLLM API key without caching ``None``."""

    if ConfigUtils is not None:
        return ConfigUtils.get_vllm_api_key(refresh=refresh)
    return _optional_env("VLLM_API_KEY")


_BASE_CONFIG: Dict[str, Any] = {
    "endpoint": os.getenv("VLLM_QA_ENDPOINT", _DEFAULT_ENDPOINT),
    "model": _DEFAULT_MODEL,
    "timeout": float(os.getenv("VLLM_QA_TIMEOUT", os.getenv("VLLM_STANDARD_TIMEOUT", "240"))),
    "max_tokens": int(os.getenv("VLLM_QA_MAX_TOKENS", "2048")),
    "temperature": float(os.getenv("VLLM_QA_TEMPERATURE", "0.25")),
    "top_p": float(os.getenv("VLLM_QA_TOP_P", "0.9")),
    "max_retries": int(os.getenv("VLLM_QA_MAX_RETRIES", "5")),
    "retry_delay": float(os.getenv("VLLM_QA_RETRY_DELAY", "5")),
    "retry_jitter": float(os.getenv("VLLM_QA_RETRY_JITTER", "0.35")),
    "retry_backoff_factor": float(
        os.getenv("VLLM_QA_RETRY_BACKOFF", "2.0")
    ),
}


VLLM_CONFIG: Dict[str, Any] = {**_BASE_CONFIG, "api_key": _resolve_api_key()}


def get_vllm_config(*, refresh: bool = True) -> Dict[str, Any]:
    """Return a fresh copy of the shared vLLM configuration."""

    config = dict(_BASE_CONFIG)
    config["api_key"] = _resolve_api_key(refresh=refresh)
    return config


__all__ = ["VLLM_CONFIG", "get_vllm_config"]
