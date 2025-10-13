"""Shared vLLM configuration for Quality Assurance components."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

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


VLLM_CONFIG: Dict[str, Any] = {
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
    "api_key": _optional_env("VLLM_API_KEY"),
}


def get_vllm_config() -> Dict[str, Any]:
    """Return a copy of the shared vLLM configuration."""

    return dict(VLLM_CONFIG)


__all__ = ["VLLM_CONFIG", "get_vllm_config"]
