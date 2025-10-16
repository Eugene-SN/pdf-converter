import asyncio
import importlib

import pytest

pytest.importorskip("prometheus_client")

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


def test_compute_safe_max_tokens_from_error_handles_generic_message():
    message = (
        "However, your request has 3500 input tokens which exceeds the limit of 4096 tokens. "
        "Please reduce the prompt or completion length."
    )

    result = compute_safe_max_tokens_from_error(
        message,
        requested_tokens=4096,
        safety_margin=128,
        api_max_tokens=4096,
    )

    assert result == 468


def test_translate_single_increments_api_requests(monkeypatch):
    try:
        translator_module = importlib.import_module("translator.translator")
    except ModuleNotFoundError as exc:
        pytest.skip(f"translator module dependencies missing: {exc}")

    client = translator_module.VLLMAPIClient()
    stats = translator_module.TranslationStats()

    translator_module.translation_cache.clear()
    translator_module.translation_api_requests_current.set(0)

    async def fake_enhanced_api_request(
        messages,
        timeout=translator_module.REQUEST_TIMEOUT,
        *,
        max_tokens=None,
    ):
        return "Переведенный текст"

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(client, "enhanced_api_request", fake_enhanced_api_request)
    monkeypatch.setattr(translator_module.asyncio, "sleep", fast_sleep)

    result = asyncio.run(client.translate_single("源文本", "zh-CN", "ru", stats))

    assert result == "Переведенный текст"
    assert stats.api_requests == 1
    assert REGISTRY.get_sample_value("translation_api_requests_current") == pytest.approx(1.0)


def test_translate_single_counts_failed_requests(monkeypatch):
    try:
        translator_module = importlib.import_module("translator.translator")
    except ModuleNotFoundError as exc:
        pytest.skip(f"translator module dependencies missing: {exc}")

    client = translator_module.VLLMAPIClient()
    stats = translator_module.TranslationStats()

    translator_module.translation_cache.clear()
    translator_module.translation_api_requests_current.set(0)

    async def fake_enhanced_api_request(
        messages,
        timeout=translator_module.REQUEST_TIMEOUT,
        *,
        max_tokens=None,
    ):
        return None

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(client, "enhanced_api_request", fake_enhanced_api_request)
    monkeypatch.setattr(translator_module.asyncio, "sleep", fast_sleep)

    failures_metric_name = "translation_api_request_failures_total"
    failures_before = REGISTRY.get_sample_value(failures_metric_name) or 0.0

    result = asyncio.run(client.translate_single("源文本", "zh-CN", "ru", stats))

    assert result == "源文本"
    assert stats.api_requests == 1
    assert stats.api_failures == 1
    assert REGISTRY.get_sample_value("translation_api_requests_current") == pytest.approx(1.0)
    assert REGISTRY.get_sample_value(failures_metric_name) == pytest.approx(failures_before + 1.0)


def test_translate_single_splits_when_prompt_too_long(monkeypatch):
    try:
        translator_module = importlib.import_module("translator.translator")
    except ModuleNotFoundError as exc:
        pytest.skip(f"translator module dependencies missing: {exc}")

    translator_module.MODEL_CONTEXT_LENGTH = 120
    translator_module.CONTEXT_SAFETY_MARGIN = 20
    translator_module.SAFE_CONTEXT_LIMIT = max(
        1, translator_module.MODEL_CONTEXT_LENGTH - translator_module.CONTEXT_SAFETY_MARGIN
    )
    translator_module.DEFAULT_COMPLETION_TOKENS = max(
        1,
        min(
            translator_module.API_MAX_TOKENS,
            translator_module.SAFE_CONTEXT_LIMIT,
            int(translator_module.MODEL_CONTEXT_LENGTH * translator_module.MAX_COMPLETION_RATIO)
            if translator_module.MAX_COMPLETION_RATIO > 0
            else translator_module.API_MAX_TOKENS,
        ),
    )

    client = translator_module.VLLMAPIClient()
    stats = translator_module.TranslationStats()

    translator_module.translation_cache.clear()
    translator_module.translation_api_requests_current.set(0)

    calls = []

    async def fake_enhanced_api_request(
        messages,
        timeout=translator_module.REQUEST_TIMEOUT,
        *,
        max_tokens=None,
    ):
        user_content = messages[1]["content"]
        chunk_text = user_content.split("\n\n", 1)[-1]
        calls.append((chunk_text, max_tokens))
        return chunk_text.replace("字", "译")

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(client, "enhanced_api_request", fake_enhanced_api_request)
    monkeypatch.setattr(translator_module.asyncio, "sleep", fast_sleep)

    long_text = "字" * (translator_module.SAFE_CONTEXT_LIMIT * 2)

    result = asyncio.run(client.translate_single(long_text, "zh-CN", "ru", stats))

    assert len(calls) > 1
    assert result == "译" * len(long_text)
    assert stats.api_requests == len(calls)
    assert all(
        (max_tokens or translator_module.DEFAULT_COMPLETION_TOKENS)
        <= translator_module.SAFE_CONTEXT_LIMIT
        for _, max_tokens in calls
    )


def test_translate_single_value_error_triggers_chunking(monkeypatch):
    try:
        translator_module = importlib.import_module("translator.translator")
    except ModuleNotFoundError as exc:
        pytest.skip(f"translator module dependencies missing: {exc}")

    translator_module.MODEL_CONTEXT_LENGTH = 256
    translator_module.CONTEXT_SAFETY_MARGIN = 32
    translator_module.SAFE_CONTEXT_LIMIT = max(
        1, translator_module.MODEL_CONTEXT_LENGTH - translator_module.CONTEXT_SAFETY_MARGIN
    )
    translator_module.DEFAULT_COMPLETION_TOKENS = max(
        1,
        min(
            translator_module.API_MAX_TOKENS,
            translator_module.SAFE_CONTEXT_LIMIT,
            int(translator_module.MODEL_CONTEXT_LENGTH * translator_module.MAX_COMPLETION_RATIO)
            if translator_module.MAX_COMPLETION_RATIO > 0
            else translator_module.API_MAX_TOKENS,
        ),
    )

    translator_module.translation_cache.clear()
    translator_module.translation_api_requests_current.set(0)

    client = translator_module.VLLMAPIClient()
    stats = translator_module.TranslationStats()

    attempts = []

    async def fake_enhanced_api_request(
        messages,
        timeout=translator_module.REQUEST_TIMEOUT,
        *,
        max_tokens=None,
    ):
        user_content = messages[1]["content"]
        chunk_text = user_content.split("\n\n", 1)[-1]
        attempts.append(chunk_text)
        if len(chunk_text) > 60:
            raise ValueError(
                "However, your request has 8192 input tokens which exceeds the limit of 4096 tokens."
            )
        return chunk_text.replace("字", "译")

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(client, "enhanced_api_request", fake_enhanced_api_request)
    monkeypatch.setattr(translator_module.asyncio, "sleep", fast_sleep)

    long_text = "字" * 240

    result = asyncio.run(client.translate_single(long_text, "zh-CN", "ru", stats))

    assert len(attempts) > 1
    assert result == "译" * len(long_text)
    assert stats.api_requests == len(attempts)
    assert stats.api_failures >= 1


def test_vllm_translate_batches_respect_context(monkeypatch):
    try:
        translator_module = importlib.import_module("translator.translator")
    except ModuleNotFoundError as exc:
        pytest.skip(f"translator module dependencies missing: {exc}")

    translator_module.MODEL_CONTEXT_LENGTH = 128
    translator_module.CONTEXT_SAFETY_MARGIN = 16
    translator_module.SAFE_CONTEXT_LIMIT = max(
        1, translator_module.MODEL_CONTEXT_LENGTH - translator_module.CONTEXT_SAFETY_MARGIN
    )
    translator_module.DEFAULT_COMPLETION_TOKENS = max(
        1,
        min(
            translator_module.API_MAX_TOKENS,
            translator_module.SAFE_CONTEXT_LIMIT,
            int(translator_module.MODEL_CONTEXT_LENGTH * translator_module.MAX_COMPLETION_RATIO)
            if translator_module.MAX_COMPLETION_RATIO > 0
            else translator_module.API_MAX_TOKENS,
        ),
    )

    translator_module.translation_cache.clear()

    calls = []

    async def fake_translate_single(self, text, source_lang, target_lang, stats):
        tokens = self.estimate_prompt_tokens(text, source_lang, target_lang)
        calls.append(tokens)
        stats.api_requests += 1
        translator_module.translation_api_requests_current.set(stats.api_requests)
        return text.replace("字", "译")

    async def fast_fix(text, source_lang, target_lang, client, stats):
        return text

    monkeypatch.setattr(translator_module.VLLMAPIClient, "translate_single", fake_translate_single, raising=False)
    monkeypatch.setattr(translator_module, "intelligent_fix_remaining", fast_fix)

    long_lines = ["字" * (translator_module.SAFE_CONTEXT_LIMIT) for _ in range(3)]
    document = "\n".join(long_lines)

    result = asyncio.run(translator_module.vllm_translate(document, "zh-CN", "ru"))

    assert len(calls) > 1
    assert all(tokens < translator_module.SAFE_CONTEXT_LIMIT for tokens in calls)
    assert "译" in result["translated_content"]
