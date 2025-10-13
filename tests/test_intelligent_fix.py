import asyncio
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from translator.translator import TranslationStats, intelligent_fix_remaining


class DummyClient:
    def __init__(self, responses):
        self._responses = responses
        self._index = 0

    async def translate_single(self, fragment, source_lang, target_lang, stats):
        response = self._responses[min(self._index, len(self._responses) - 1)]
        self._index += 1
        return response


def test_intelligent_fix_reduces_or_keeps_chinese_characters():
    text = "这是一个测试。更多的中文内容需要翻译。"
    responses = [
        "Это тест.",
        "Больше китайского текста переведено.",
    ]
    client = DummyClient(responses)
    stats = TranslationStats()

    before = len(re.findall(r"[\u4e00-\u9fff]", text))

    result = asyncio.run(intelligent_fix_remaining(text, "zh-CN", "ru", client, stats))

    after = len(re.findall(r"[\u4e00-\u9fff]", result))

    assert after <= before
    assert stats.fixes_successful == max(before - after, 0)
