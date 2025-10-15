"""Utilities for managing vLLM context window limits."""

from __future__ import annotations

import re
from typing import Optional

MAX_TOKENS_ERROR_PATTERN = re.compile(
    (
        r"is too large: (?P<requested>\d+)\. This model's maximum context length is "
        r"(?P<context>\d+) tokens and your request has (?P<input>\d+) input tokens"
    ),
    re.IGNORECASE,
)


def compute_safe_max_tokens_from_error(
    error_message: str,
    *,
    requested_tokens: int,
    safety_margin: int,
    api_max_tokens: int,
) -> Optional[int]:
    """Calculate a safer ``max_tokens`` value based on a vLLM error message.

    Parameters
    ----------
    error_message:
        The descriptive error string returned by vLLM.
    requested_tokens:
        The ``max_tokens`` value that caused the error.
    safety_margin:
        A number of tokens to keep in reserve to prevent subsequent off-by-one
        errors when the prompt size fluctuates slightly.
    api_max_tokens:
        The hard upper bound configured for the client.

    Returns
    -------
    Optional[int]
        A reduced ``max_tokens`` value or ``None`` when the message could not be
        parsed or when no adjustment is required.
    """

    match = MAX_TOKENS_ERROR_PATTERN.search(error_message or "")
    if not match:
        return None

    try:
        requested_in_message = int(match.group("requested"))
        context_limit = int(match.group("context"))
        prompt_tokens = int(match.group("input"))
    except (TypeError, ValueError):
        return None

    available_tokens = context_limit - prompt_tokens - safety_margin
    if available_tokens < 1:
        candidate = 1
    else:
        candidate = min(requested_tokens, requested_in_message, available_tokens, api_max_tokens)

    if candidate < 1:
        candidate = 1

    if candidate >= requested_tokens:
        return None

    return candidate


__all__ = [
    "compute_safe_max_tokens_from_error",
    "MAX_TOKENS_ERROR_PATTERN",
]
