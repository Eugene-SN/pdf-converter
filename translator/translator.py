#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🚀 vLLM Translator Service v2.0 (ИСПРАВЛЕНО)
=====================================================================
Переработанный модуль перевода на основе ollama-translator
Адаптация для работы с vLLM API вместо Ollama
Интеграция с PDF Converter Pipeline v2.0

ОСНОВНЫЕ ИЗМЕНЕНИЯ:
✅ Замена Ollama API на vLLM OpenAI-совместимый API
✅ Использование модели Qwen3-30B-A3B-Instruct-2507
✅ Отключение thinking режима для стабильности
✅ Сохранение всех технических терминов и команд
✅ Интеграция с микросервисной архитектурой
"""

import os
import re
import time
import json
import hashlib
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field

# FastAPI и веб-сервер
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# HTTP клиенты
import aiohttp
import requests

# Прогресс и мониторинг
from tqdm.asyncio import tqdm as async_tqdm
import prometheus_client
from prometheus_client import Counter, Histogram, Gauge

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==============================================
# 🔧 КОНФИГУРАЦИЯ И НАСТРОЙКИ v2.0
# ==============================================

# vLLM API конфигурация (ИСПРАВЛЕНО)
VLLM_API_URL = os.getenv('VLLM_SERVER_URL', 'http://vllm-server:8000')
VLLM_API_ENDPOINT = "/v1/chat/completions"
TRANSLATION_MODEL = os.getenv('VLLM_TRANSLATION_MODEL', 'Qwen/Qwen3-30B-A3B-Instruct-2507')

# API параметры
API_TEMPERATURE = float(os.getenv('VLLM_TEMPERATURE', '0.1'))
API_MAX_TOKENS = int(os.getenv('VLLM_MAX_TOKENS', '4096'))
API_TOP_P = float(os.getenv('VLLM_TOP_P', '0.9'))
API_TOP_K = int(os.getenv('VLLM_TOP_K', '50'))

# Настройки производительности
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '600'))
INTER_REQUEST_DELAY = float(os.getenv('INTER_REQUEST_DELAY', '0.2'))
DISABLE_THINKING = os.getenv('DISABLE_THINKING', 'true').lower() == 'true'

# Батчинг
BATCH_SIZE_HEADERS = int(os.getenv('BATCH_SIZE_HEADERS', '4'))
BATCH_SIZE_TABLES = int(os.getenv('BATCH_SIZE_TABLES', '3'))
BATCH_SIZE_COMMANDS = int(os.getenv('BATCH_SIZE_COMMANDS', '2'))
BATCH_SIZE_TEXT = int(os.getenv('BATCH_SIZE_TEXT', '6'))
BATCH_SIZE_MIXED = int(os.getenv('BATCH_SIZE_MIXED', '4'))

# Кэширование
ENABLE_CACHING = os.getenv('ENABLE_CACHING', 'true').lower() == 'true'
CACHE_SIZE_LIMIT = int(os.getenv('CACHE_SIZE_LIMIT', '5000'))
translation_cache = {}

# ==============================================
# 📚 СЛОВАРЬ ТЕХНИЧЕСКИХ ТЕРМИНОВ v2.0
# ==============================================

TECHNICAL_TERMINOLOGY = {
    # Brand names (КРИТИЧЕСКИ ВАЖНО!)
    "问天": "WenTian",
    "联想问天": "Lenovo WenTian", 
    "天擎": "ThinkSystem",
    "AnyBay": "AnyBay",
    
    # Processors
    "至强": "Xeon",
    "可扩展处理器": "Scalable Processors",
    "英特尔": "Intel",
    "处理器": "Processor",
    "内核": "Core",
    "线程": "Thread",
    "睿频": "Turbo Boost",
    
    # Memory and storage
    "内存": "Memory",
    "存储": "Storage", 
    "硬盘": "Drive",
    "固态硬盘": "SSD",
    "机械硬盘": "HDD",
    "热插拔": "Hot-swap",
    "冗余": "Redundancy",
    "背板": "Backplane",
    "托架": "Tray",
    
    # Network
    "以太网": "Ethernet",
    "光纤": "Fiber",
    "带宽": "Bandwidth",
    "延迟": "Latency",
    "网卡": "Network Adapter",
    
    # Cooling
    "风冷": "Air cooling",
    "液冷": "Liquid cooling",
    "散热": "Cooling",
    "风扇": "Fan",
    "散热器": "Heatsink",
    
    # Form factors
    "英寸": "inch",
    "机架": "Rack",
    "插槽": "Slot",
    "转接卡": "Riser Card",
    
    # Power
    "电源": "Power Supply",
    "铂金": "Platinum", 
    "钛金": "Titanium",
    "CRPS": "CRPS",
    
    # Components
    "芯片组": "Chipset",
    "控制器": "Controller",
    "适配器": "Adapter",
    "光驱": "Optical Drive"
}

# Русская терминология
TECHNICAL_TERMINOLOGY_RU = {
    "问天": "WenTian",
    "联想问天": "Lenovo WenTian",
    "至强": "Xeon",
    "可扩展处理器": "Scalable процессоры",
    "英特尔": "Intel",
    "处理器": "процессор",
    "内核": "ядро",
    "线程": "поток",
    "内存": "память",
    "存储": "хранилище",
    "硬盘": "диск",
    "固态硬盘": "SSD",
    "机械硬盘": "HDD",
    "热插拔": "горячая замена",
    "冗余": "резервирование",
    "托架": "лоток",
    "以太网": "Ethernet",
    "带宽": "пропускная способность",
    "延迟": "задержка",
    "风冷": "воздушное охлаждение",
    "液冷": "жидкостное охлаждение",
    "散热器": "радиатор",
    "机架": "стойка",
    "插槽": "слот",
    "电源": "блок питания",
    "铂金": "платиновый",
    "钛金": "титановый"
}

# ==============================================
# 📊 СТАТИСТИКА И МОНИТОРИНГ v2.0
# ==============================================

@dataclass
class TranslationStats:
    """Статистика перевода с Prometheus метриками"""
    total_lines: int = 0
    api_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    chinese_remaining_start: int = 0
    chinese_remaining_end: int = 0
    fixes_attempted: int = 0
    fixes_successful: int = 0
    processing_time: float = 0
    start_time: float = field(default_factory=time.time)
    quality_checks: List[Dict] = field(default_factory=list)
    technical_terms_fixed: int = 0

    def add_quality_check(self, original: str, translated: str, target_lang: str) -> dict:
        """Добавление проверки качества"""
        validation = validate_technical_translation(original, translated, target_lang)
        self.quality_checks.append(validation)
        return validation

    def get_average_quality(self) -> float:
        """Средний балл качества"""
        if not self.quality_checks:
            return 0
        return sum(check['quality_score'] for check in self.quality_checks) / len(self.quality_checks)

# Prometheus метрики
translation_requests = Counter('translation_requests_total', 'Total translation requests')
translation_duration = Histogram('translation_duration_seconds', 'Translation duration')
translation_quality = Gauge('translation_quality_score', 'Average translation quality score')
cache_hit_ratio = Gauge('cache_hit_ratio', 'Cache hit ratio')

# ==============================================
# 🧠 ПРОМПТЫ v2.0 (ОТКЛЮЧЕНИЕ THINKING)
# ==============================================

def get_system_prompt(source_lang_name: str, target_lang_name: str) -> str:
    """Системный промпт с отключением thinking режима"""
    return f"""Вы - эксперт по переводу технической документации серверного оборудования.

ОСНОВНАЯ ЗАДАЧА: Переводите технические тексты с {source_lang_name} на {target_lang_name} язык.

КРИТИЧЕСКИ ВАЖНЫЕ ПРАВИЛА:
1. ТОЛЬКО готовый перевод, БЕЗ пояснений и комментариев
2. НИКОГДА не используйте фразы "Вот перевод", "Here is translation"
3. НЕ добавляйте размышления в тегах <думаю>, или любых других
4. Сохраняйте ВСЕ числа, коды, форматирование ТОЧНО
5. НЕ используйте thinking режим - отвечайте сразу и кратко

БРЕНДЫ (НЕ МЕНЯТЬ!):
- "问天" ВСЕГДА → "WenTian"
- "联想问天" ВСЕГДА → "Lenovo WenTian"
- Сохраняйте: Intel, Xeon, PCIe, DDR5, NVMe, SAS, SATA

КОМАНДЫ НЕ ПЕРЕВОДИТЬ:
- IPMI: chassis, power, mc, sensor, sel, sdr, fru
- API: get, set, list, status, activate, deactivate
- Hex коды: 0x30, 0x02
- Аббревиатуры: GPU, CPU, SSD, HDD, RAID, BMC

ФОРМАТИРОВАНИЕ:
- Сохраняйте структуру Markdown таблиц ТОЧНО
- НЕ изменяйте количество столбцов
- Сохраняйте HTML теги и отступы

Выводите ТОЛЬКО {target_lang_name} перевод без дополнительного текста!"""

def get_user_prompt(text_to_translate: str, source_lang_name: str, target_lang_name: str) -> str:
    """Пользовательский промпт"""
    return f"""Переведите этот {source_lang_name} технический текст на {target_lang_name} язык:

{text_to_translate}"""

# ==============================================
# 🔍 ВАЛИДАЦИЯ КАЧЕСТВА v2.0
# ==============================================

def validate_technical_translation(original: str, translated: str, target_lang: str) -> dict:
    """Улучшенная валидация перевода"""
    quality_score = 100
    issues = []

    # 1. Китайские символы
    chinese_count = len(re.findall(r'[\u4e00-\u9fff]', translated))
    if chinese_count > 0:
        quality_score -= min(50, chinese_count * 3)
        issues.append(f"Остались китайские символы: {chinese_count}")

    # 2. Размышления и thinking
    thinking_patterns = [
        r'[Хх]орошо[,\s]*мне', r'[Сс]начала посмотр', r'Let me', r'First I',
        r'[Вв]от перевод', r'Here is', r'[Нн]иже представлен',
        r'<думаю>', r'</думаю>', r'<thinking>', r'</thinking>'
    ]
    
    thinking_count = sum(len(re.findall(pattern, translated)) for pattern in thinking_patterns)
    if thinking_count > 0:
        quality_score -= min(30, thinking_count * 10)
        issues.append(f"Обнаружены размышления: {thinking_count}")

    # 3. ВАЖНО: Проверка правильности перевода брендов
    if "问天" in original:
        if "WenTian" not in translated:
            quality_score -= 20
            issues.append("Неправильный перевод бренда 问天")

    # 4. Таблицы
    orig_table_rows = len(re.findall(r'^\|.*\|$', original, re.MULTILINE))
    trans_table_rows = len(re.findall(r'^\|.*\|$', translated, re.MULTILINE))
    if orig_table_rows > 0:
        table_preservation = trans_table_rows / orig_table_rows
        if table_preservation < 0.9:
            quality_score -= 15
            issues.append(f"Нарушена структура таблиц: {table_preservation:.1%}")

    # 5. Размер
    size_ratio = len(translated) / max(len(original), 1)
    if size_ratio > 2.5 or size_ratio < 0.4:
        quality_score -= 10
        issues.append(f"Неправильное соотношение размеров: {size_ratio:.2f}")

    # 6. Числа с единицами
    orig_numbers = re.findall(r'\d+(?:\.\d+)?(?:W|MHz|GB|TB|GHz|mm)', original)
    trans_numbers = re.findall(r'\d+(?:\.\d+)?(?:W|MHz|GB|TB|GHz|mm)', translated)
    if len(orig_numbers) != len(trans_numbers):
        quality_score -= 10
        issues.append("Не все числовые значения сохранены")

    quality_score = max(0, quality_score)
    
    status = ("ОТЛИЧНО" if quality_score >= 95 else
              "ХОРОШО" if quality_score >= 85 else
              "УДОВЛЕТВОРИТЕЛЬНО" if quality_score >= 70 else
              "ПЛОХО")

    return {
        'quality_score': quality_score,
        'status': status,
        'issues': issues,
        'chinese_fragments': chinese_count,
        'thinking_count': thinking_count,
        'table_preservation': trans_table_rows / max(orig_table_rows, 1) if orig_table_rows > 0 else 1,
        'size_ratio': size_ratio
    }

# ==============================================
# 🗄️ КЭШИРОВАНИЕ v2.0
# ==============================================

def get_cache_key(text: str, source_lang: str, target_lang: str) -> str:
    """Создание ключа для кэша"""
    content = f"{source_lang}-{target_lang}-{text}"
    return hashlib.md5(content.encode()).hexdigest()[:16]

def get_cached_translation(text: str, source_lang: str, target_lang: str) -> Optional[str]:
    """Получение перевода из кэша"""
    if not ENABLE_CACHING:
        return None
    cache_key = get_cache_key(text, source_lang, target_lang)
    return translation_cache.get(cache_key)

def cache_translation(text: str, source_lang: str, target_lang: str, translation: str):
    """Сохранение перевода в кэш"""
    if not ENABLE_CACHING or len(translation_cache) >= CACHE_SIZE_LIMIT:
        return
    cache_key = get_cache_key(text, source_lang, target_lang)
    translation_cache[cache_key] = translation

# ==============================================
# 🔗 vLLM API КЛИЕНТ v2.0 (ИСПРАВЛЕНО)
# ==============================================

class VLLMAPIClient:
    """Клиент для работы с vLLM API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = REQUEST_TIMEOUT
        
    async def enhanced_api_request(self, messages: List[Dict], timeout: int = REQUEST_TIMEOUT) -> Optional[str]:
        """Асинхронный запрос к vLLM API"""
        try:
            # Подготовка payload для vLLM OpenAI-совместимого API
            payload = {
                "model": TRANSLATION_MODEL,
                "messages": messages,
                "temperature": API_TEMPERATURE,
                "max_tokens": API_MAX_TOKENS,
                "top_p": API_TOP_P,
                "stream": False
            }

            # Отключение thinking режима для Qwen3
            if DISABLE_THINKING and "qwen" in TRANSLATION_MODEL.lower():
                if messages and messages[0]["role"] == "system":
                    messages[0]["content"] += "\n\nIMPORTANT: Respond directly without thinking process. No tags."

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                async with session.post(
                    VLLM_API_URL + VLLM_API_ENDPOINT,
                    json=payload
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if "choices" in result and len(result["choices"]) > 0:
                            translation_requests.inc()
                            return result["choices"][0]["message"]["content"]
                    else:
                        logger.error(f"vLLM API error: {response.status}")
                        
        except asyncio.TimeoutError:
            logger.warning("Таймаут vLLM API запроса")
        except Exception as e:
            logger.error(f"Ошибка vLLM API: {e}")
        
        return None

    async def translate_single(self, text: str, source_lang: str, target_lang: str, stats: TranslationStats) -> str:
        """Перевод одного фрагмента с кэшированием"""
        # Проверяем кэш
        cached_result = get_cached_translation(text, source_lang, target_lang)
        if cached_result:
            stats.cache_hits += 1
            cache_hit_ratio.set(stats.cache_hits / (stats.cache_hits + stats.cache_misses))
            return cached_result

        stats.cache_misses += 1

        if not text.strip():
            return text

        source_name = get_language_name(source_lang)
        target_name = get_language_name(target_lang)

        # Создаем сообщения
        system_prompt = get_system_prompt(source_name, target_name)
        user_prompt = get_user_prompt(text.strip(), source_name, target_name)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        # Получаем ответ от vLLM API
        response = await self.enhanced_api_request(messages)

        if response is None:
            return text

        # Постобработка
        cleaned = self._postprocess_translation(response, target_lang, stats)

        # Валидация качества
        validation = stats.add_quality_check(text, cleaned, target_lang)
        translation_quality.set(stats.get_average_quality())

        # Сохраняем в кэш
        cache_translation(text, source_lang, target_lang, cleaned)

        # Задержка между запросами
        await asyncio.sleep(INTER_REQUEST_DELAY)

        return cleaned

    def _postprocess_translation(self, response: str, target_lang: str, stats: TranslationStats) -> str:
        """Постобработка перевода с очисткой thinking режима"""
        if not response:
            return ""

        # Базовая очистка
        cleaned = response.strip()

        # Удаляем thinking теги и размышления
        thinking_patterns = [
            r'<думаю>.*?</думаю>',
            r'<thinking>.*?</thinking>',
            r'[Хх]орошо[,\s]*мне нужно[^.]*?\.',
            r'[Сс]начала посмотр[^.]*?\.',
            r'Let me[^.]*?\.',
            r'First[,\s]*I[^.]*?\.',
            r'[Вв]от перевод[^:]*:?\s*',
            r'Here is[^:]*:?\s*',
            r'[Нн]иже представлен[^:]*:?\s*'
        ]

        for pattern in thinking_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE | re.DOTALL)

        # Исправляем технические термины
        cleaned = self._fix_technical_terms(cleaned, target_lang, stats)

        # Финальная очистка
        cleaned = re.sub(r'\n\s*\n\s*\n+', '\n\n', cleaned)
        cleaned = cleaned.strip()

        return cleaned

    def _fix_technical_terms(self, text: str, target_lang: str, stats: TranslationStats) -> str:
        """Исправление технических терминов"""
        fixes_made = 0

        # ВАЖНО: НЕ МЕНЯТЬ WenTian на ThinkSystem!
        brand_fixes = {
            r'\b[Qq]itian\b': 'WenTian',
            r'\bSkyland\b': 'WenTian', 
            r'\bSkyStorage\b': 'WenTian'
        }

        for wrong_pattern, correct in brand_fixes.items():
            if re.search(wrong_pattern, text):
                text = re.sub(wrong_pattern, correct, text)
                fixes_made += 1

        # Выбор словаря терминов
        if target_lang == "ru":
            terminology = TECHNICAL_TERMINOLOGY_RU
        else:
            terminology = TECHNICAL_TERMINOLOGY

        # Применение технических терминов
        for chinese_term, translation in terminology.items():
            if chinese_term in text:
                text = text.replace(chinese_term, translation)
                fixes_made += 1

        stats.technical_terms_fixed += fixes_made
        return text

def get_language_name(lang_code: str) -> str:
    """Получение полного имени языка"""
    languages = {
        "zh-CN": "китайский",
        "zh": "китайский", 
        "ru": "русский",
        "en": "английский"
    }
    return languages.get(lang_code, lang_code)

# ==============================================
# 🎯 АНАЛИЗ КОНТЕНТА v2.0
# ==============================================

def analyze_content_complexity(text: str) -> str:
    """Определение сложности контента"""
    if not text.strip():
        return 'empty'

    lines = text.strip().split('\n')

    # Заголовки
    if any(line.strip().startswith('#') for line in lines):
        return 'header'

    # Таблицы
    table_lines = [line for line in lines if line.strip().startswith('|')]
    if table_lines:
        max_cols = max(line.count('|') for line in table_lines)
        if max_cols > 6:
            return 'complex_table'
        elif len(table_lines) > 5:
            return 'table'

    # Технические характеристики
    if any(re.search(r'\d+\s*(GB|MB|GHz|MHz|W|TB)', line) for line in lines):
        return 'technical_specs'

    # Команды и коды
    if any(re.search(r'\b(ipmitool|chassis|power|0x[0-9a-f]+)\b', line, re.I) for line in lines):
        return 'commands'

    # Смешанный контент
    if len(lines) > 3 and any('|' in line for line in lines):
        return 'mixed'

    return 'text'

def get_optimal_batch_size(content_type: str) -> int:
    """Получение оптимального размера батча"""
    batch_mapping = {
        'empty': 20,
        'header': BATCH_SIZE_HEADERS,
        'table': BATCH_SIZE_TABLES,
        'complex_table': 1,
        'technical_specs': 3,
        'commands': BATCH_SIZE_COMMANDS,
        'mixed': BATCH_SIZE_MIXED,
        'text': BATCH_SIZE_TEXT
    }
    return batch_mapping.get(content_type, BATCH_SIZE_TEXT)

# ==============================================
# 🚀 ОСНОВНОЙ ПЕРЕВОДЧИК v2.0
# ==============================================

async def vllm_translate(text: str, source_lang: str = "zh-CN", target_lang: str = "ru") -> Dict:
    """Основная функция перевода с vLLM"""
    logger.info(f"🚀 Запуск vLLM Translator v2.0")
    logger.info(f"🌐 {source_lang} → {target_lang}")
    logger.info(f"🤖 Модель: {TRANSLATION_MODEL}")
    logger.info(f"🧠 Thinking отключен: {DISABLE_THINKING}")

    # Инициализация
    client = VLLMAPIClient()
    stats = TranslationStats()
    
    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    stats.total_lines = len(lines)

    # Анализ контента
    content_analysis = {}
    for line in lines:
        content_type = analyze_content_complexity(line)
        content_analysis[content_type] = content_analysis.get(content_type, 0) + 1

    logger.info("📊 АНАЛИЗ КОНТЕНТА:")
    for content_type, count in content_analysis.items():
        logger.info(f"  {content_type}: {count} строк")

    translated_lines = []
    
    # Батчированная обработка
    i = 0
    with translation_duration.time():
        while i < len(lines):
            line = lines[i]
            if not line.strip():
                translated_lines.append(line)
                i += 1
                continue

            content_type = analyze_content_complexity(line)
            batch_size = get_optimal_batch_size(content_type)

            # Создаем батч
            batch_lines = []
            for j in range(i, min(i + batch_size, len(lines))):
                if j < len(lines) and lines[j].strip():
                    batch_lines.append(lines[j])

            if batch_lines:
                if len(batch_lines) == 1:
                    # Одна строка
                    translated = await client.translate_single(batch_lines[0], source_lang, target_lang, stats)
                    translated_lines.append(translated)
                else:
                    # Несколько строк - объединяем в один запрос
                    batch_text = '\n'.join(batch_lines)
                    translated_batch = await client.translate_single(batch_text, source_lang, target_lang, stats)
                    
                    # Разбиваем результат обратно
                    batch_result_lines = translated_batch.split('\n')
                    if len(batch_result_lines) == len(batch_lines):
                        translated_lines.extend(batch_result_lines)
                    else:
                        # Если разбивка не удалась, переводим по одной
                        for single_line in batch_lines:
                            single_translated = await client.translate_single(single_line, source_lang, target_lang, stats)
                            translated_lines.append(single_translated)

                i += len(batch_lines)
            else:
                i += 1

    # Объединение результата
    result = '\n'.join(translated_lines)

    # Финальная валидация
    final_validation = validate_technical_translation(text, result, target_lang)
    logger.info(f"📊 ФИНАЛЬНАЯ ВАЛИДАЦИЯ: {final_validation['quality_score']:.1f}/100")

    if final_validation['issues']:
        logger.warning("⚠️ Обнаруженные проблемы:")
        for issue in final_validation['issues']:
            logger.warning(f"  - {issue}")

    # Исправление остатков если нужно
    stats.chinese_remaining_start = len(re.findall(r'[\u4e00-\u9fff]', result))
    if stats.chinese_remaining_start > 0:
        logger.info(f"🔧 Обнаружено {stats.chinese_remaining_start} китайских символов, запускаем исправление...")
        result = await intelligent_fix_remaining(result, source_lang, target_lang, client, stats)
    
    stats.chinese_remaining_end = len(re.findall(r'[\u4e00-\u9fff]', result))
    stats.processing_time = time.time() - stats.start_time

    return {
        'translated_content': result,
        'stats': {
            'total_lines': stats.total_lines,
            'api_requests': stats.api_requests,
            'cache_hits': stats.cache_hits,
            'cache_misses': stats.cache_misses,
            'chinese_remaining': stats.chinese_remaining_end,
            'processing_time': stats.processing_time,
            'average_quality': stats.get_average_quality(),
            'fixes_applied': stats.fixes_successful
        },
        'quality_score': final_validation['quality_score']
    }

async def intelligent_fix_remaining(text: str, source_lang: str, target_lang: str, client: VLLMAPIClient, stats: TranslationStats) -> str:
    """Интеллектуальное исправление оставшихся китайских фрагментов"""
    logger.info("🔧 ИНТЕЛЛЕКТУАЛЬНОЕ ИСПРАВЛЕНИЕ v2.0")

    # Находим китайские фрагменты
    chinese_fragments = []
    for match in re.finditer(r'[\u4e00-\u9fff]+', text):
        fragment = match.group()
        if len(fragment) >= 2:
            chinese_fragments.append((fragment, match.span()))

    if not chinese_fragments:
        logger.info("✅ Китайских фрагментов не найдено")
        return text

    logger.info(f"🇨🇳 Найдено {len(chinese_fragments)} китайских фрагментов")

    # Ограничиваем количество исправлений
    fragments_to_fix = chinese_fragments[:10]  # Максимум 10
    current_text = text
    fixes_count = 0

    # Исправление по одному фрагменту
    for fragment, span in fragments_to_fix:
        stats.fixes_attempted += 1

        translated = await client.translate_single(fragment, source_lang, target_lang, stats)
        
        if translated and fragment != translated and not re.search(r'[\u4e00-\u9fff]', translated):
            current_text = current_text.replace(fragment, translated, 1)
            stats.fixes_successful += 1
            fixes_count += 1
            logger.info(f"✅ Исправлен #{fixes_count}: {fragment} → {translated[:20]}...")

    final_fragments = len(re.findall(r'[\u4e00-\u9fff]', current_text))
    improvement = len(chinese_fragments) - final_fragments

    logger.info("📈 РЕЗУЛЬТАТ ИСПРАВЛЕНИЯ:")
    logger.info(f"  Было: {len(chinese_fragments)}")
    logger.info(f"  Стало: {final_fragments}")
    logger.info(f"  Исправлено: {improvement}")

    return current_text

# ==============================================
# 🌐 FASTAPI СЕРВЕР v2.0
# ==============================================

# Pydantic модели
class TranslationRequest(BaseModel):
    text: str
    source_lang: str = "zh-CN"
    target_lang: str = "ru"

class TranslationResponse(BaseModel):
    translated_content: str
    stats: Dict
    quality_score: float
    processing_time: float

# FastAPI приложение
app = FastAPI(
    title="vLLM Translator Service v2.0",
    description="Высококачественный перевод технической документации с vLLM",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "vLLM Translator Service v2.0",
        "status": "running",
        "model": TRANSLATION_MODEL,
        "thinking_disabled": DISABLE_THINKING
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "vLLM Translator v2.0",
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(translation_cache),
        "cache_limit": CACHE_SIZE_LIMIT
    }

@app.post("/api/v1/translate", response_model=TranslationResponse)
async def translate_text(request: TranslationRequest) -> TranslationResponse:
    """Основной endpoint для перевода"""
    try:
        start_time = time.time()
        
        result = await vllm_translate(
            text=request.text,
            source_lang=request.source_lang,
            target_lang=request.target_lang
        )
        
        processing_time = time.time() - start_time
        
        return TranslationResponse(
            translated_content=result['translated_content'],
            stats=result['stats'],
            quality_score=result['quality_score'],
            processing_time=processing_time
        )
        
    except Exception as e:
        logger.error(f"Translation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/stats")
async def get_translation_stats():
    """Статистика сервиса"""
    return {
        "cache_size": len(translation_cache),
        "cache_limit": CACHE_SIZE_LIMIT,
        "cache_hit_ratio": cache_hit_ratio._value.get() if cache_hit_ratio._value else 0,
        "total_requests": translation_requests._value.get(),
        "average_quality": translation_quality._value.get() if translation_quality._value else 0,
        "model": TRANSLATION_MODEL,
        "thinking_disabled": DISABLE_THINKING
    }

@app.get("/metrics")
async def metrics():
    """Prometheus метрики"""
    return prometheus_client.generate_latest()

# ==============================================
# 💎 ЗАПУСК СЕРВЕРА
# ==============================================

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Запуск сервера
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv('SERVICE_PORT', '8003')),
        workers=1,
        log_level="info"
    )