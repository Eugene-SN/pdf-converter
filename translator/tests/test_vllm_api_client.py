import pytest
from prometheus_client import REGISTRY

from .. import translator as translator_module


@pytest.mark.asyncio
async def test_translate_single_increments_api_requests(monkeypatch):
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

    result = await client.translate_single("源文本", "zh-CN", "ru", stats)

    assert result == "Переведенный текст"
    assert stats.api_requests == 1
    assert REGISTRY.get_sample_value("translation_api_requests_current") == pytest.approx(1.0)
