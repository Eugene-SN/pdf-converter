import asyncio
import importlib

import pytest
from prometheus_client import REGISTRY

from translator.vllm_limits import compute_safe_max_tokens_from_error


def test_compute_safe_max_tokens_from_error_parses_message():
    message = (
        "'max_tokens' or 'max_completion_tokens' is too large: 4096. "
        "This model's maximum context length is 4096 tokens and your request has "
        "462 input tokens (4096 > 4096 - 462)."
    )

    result = compute_safe_max_tokens_from_error(
        message,
        requested_tokens=4096,
        safety_margin=64,
        api_max_tokens=4096,
    )

    assert result == 3570


def test_compute_safe_max_tokens_from_error_ignores_unmatched_message():
    message = "Some other validation error"

    result = compute_safe_max_tokens_from_error(
        message,
        requested_tokens=2048,
        safety_margin=64,
        api_max_tokens=4096,
    )

    assert result is None


def test_translate_single_increments_api_requests(monkeypatch):
    try:
        translator_module = importlib.import_module("translator.translator")
    except ModuleNotFoundError as exc:
        pytest.skip(f"translator module dependencies missing: {exc}")

    client = translator_module.VLLMAPIClient()
    stats = translator_module.TranslationStats()

    translator_module.translation_cache.clear()
    translator_module.translation_api_requests_current.set(0)

    async def fake_enhanced_api_request(messages, timeout=translator_module.REQUEST_TIMEOUT):
        return "Переведенный текст"

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(client, "enhanced_api_request", fake_enhanced_api_request)
    monkeypatch.setattr(translator_module.asyncio, "sleep", fast_sleep)

    result = asyncio.run(client.translate_single("源文本", "zh-CN", "ru", stats))

    assert result == "Переведенный текст"
    assert stats.api_requests == 1
    assert REGISTRY.get_sample_value("translation_api_requests_current") == pytest.approx(1.0)
