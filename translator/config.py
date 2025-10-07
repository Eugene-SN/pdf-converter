#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🚀 vLLM Translator Config v2.0 (ИСПРАВЛЕНО)
=====================================================================
Конфигурационный файл для vLLM Translator Service
Переработанный на основе ollama-translator config.py
Адаптация для работы с vLLM API вместо Ollama

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ v2.0:
✅ Замена Ollama endpoint на vLLM OpenAI-совместимый API
✅ Использование модели Qwen3-30B-A3B-Instruct-2507
✅ Отключение thinking режима для стабильности 
✅ Сохранение правильного перевода: "问天": "WenTian"
✅ Интеграция с микросервисной архитектурой
"""

import os
import re
from typing import Dict, List, Tuple

# ==============================================
# 🔧 ОСНОВНЫЕ НАСТРОЙКИ API v2.0 (ИСПРАВЛЕНО)
# ==============================================

# vLLM Server конфигурация (ИСПРАВЛЕНО)
VLLM_API_URL = os.getenv('VLLM_SERVER_URL', 'http://vllm-server:8000')
VLLM_API_KEY = os.getenv('VLLM_API_KEY', 'pdf-converter-secure-key-2024')

# ✅ ИСПРАВЛЕНО: Правильная модель для перевода
TRANSLATION_MODEL = os.getenv('VLLM_TRANSLATION_MODEL', 'Qwen/Qwen3-30B-A3B-Instruct-2507')

# ✅ ИСПРАВЛЕНО: OpenAI-совместимый endpoint для vLLM
API_ENDPOINT = "/v1/chat/completions"

# ==============================================
# ⚙️ НАСТРОЙКИ МОДЕЛИ v2.0
# ==============================================

API_TEMPERATURE = float(os.getenv('VLLM_TEMPERATURE', '0.1'))
API_MAX_TOKENS = int(os.getenv('VLLM_MAX_TOKENS', '4096'))
API_MAX_OUTPUT_TOKENS = int(os.getenv('VLLM_MAX_OUTPUT_TOKENS', '4096'))
API_TOP_P = float(os.getenv('VLLM_TOP_P', '0.9'))
API_TOP_K = int(os.getenv('VLLM_TOP_K', '50'))
API_FREQUENCY_PENALTY = float(os.getenv('VLLM_FREQUENCY_PENALTY', '0.0'))
API_PRESENCE_PENALTY = float(os.getenv('VLLM_PRESENCE_PENALTY', '0.0'))

# ✅ КРИТИЧЕСКИ ВАЖНО: Отключение thinking режима для Qwen3
DISABLE_THINKING = os.getenv('DISABLE_THINKING', 'true').lower() == 'true'

# ==============================================
# ⏱️ НАСТРОЙКИ ПРОИЗВОДИТЕЛЬНОСТИ
# ==============================================

REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '600'))
INTER_REQUEST_DELAY = float(os.getenv('INTER_REQUEST_DELAY', '0.2'))

# ==============================================
# 🎯 БАТЧИНГ (ОПТИМИЗИРОВАНО)
# ==============================================

BATCH_SIZE_HEADERS = int(os.getenv('BATCH_SIZE_HEADERS', '4'))
BATCH_SIZE_TABLES = int(os.getenv('BATCH_SIZE_TABLES', '3'))
BATCH_SIZE_COMMANDS = int(os.getenv('BATCH_SIZE_COMMANDS', '2'))
BATCH_SIZE_TEXT = int(os.getenv('BATCH_SIZE_TEXT', '6'))
BATCH_SIZE_MIXED = int(os.getenv('BATCH_SIZE_MIXED', '4'))

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

ВАЖНО: НЕ используйте thinking теги или любые размышления. Отвечайте прямо и кратко!

Выводите ТОЛЬКО {target_lang_name} перевод без дополнительного текста!"""

def get_user_prompt(text_to_translate: str, source_lang_name: str, target_lang_name: str) -> str:
    """Пользовательский промпт с отключением thinking"""
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
# 📊 КЭШИРОВАНИЕ v2.0
# ==============================================

ENABLE_CACHING = os.getenv('ENABLE_CACHING', 'true').lower() == 'true'
CACHE_SIZE_LIMIT = int(os.getenv('CACHE_SIZE_LIMIT', '5000'))
translation_cache = {}

def get_cache_key(text: str, source_lang: str, target_lang: str) -> str:
    """Создать ключ для кэша"""
    import hashlib
    content = f"{source_lang}-{target_lang}-{text}"
    return hashlib.md5(content.encode()).hexdigest()[:16]

def get_cached_translation(text: str, source_lang: str, target_lang: str) -> str:
    """Получить перевод из кэша"""
    if not ENABLE_CACHING:
        return None
    cache_key = get_cache_key(text, source_lang, target_lang)
    return translation_cache.get(cache_key)

def cache_translation(text: str, source_lang: str, target_lang: str, translation: str):
    """Сохранить перевод в кэш"""
    if not ENABLE_CACHING or len(translation_cache) >= CACHE_SIZE_LIMIT:
        return
    cache_key = get_cache_key(text, source_lang, target_lang)
    translation_cache[cache_key] = translation

# ==============================================
# 🎯 АНАЛИЗ КОНТЕНТА v2.0
# ==============================================

def analyze_content_complexity(text: str) -> str:
    """Определить сложность контента"""
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
    """Получить оптимальный размер батча"""
    batch_mapping = {
        'empty': 20,
        'header': BATCH_SIZE_HEADERS,
        'table': BATCH_SIZE_TABLES,
        'complex_table': 1,  # Сложные таблицы по одной
        'technical_specs': 3,
        'commands': BATCH_SIZE_COMMANDS,
        'mixed': BATCH_SIZE_MIXED,
        'text': BATCH_SIZE_TEXT
    }
    return batch_mapping.get(content_type, BATCH_SIZE_TEXT)

# ==============================================
# 🌐 ЯЗЫКОВЫЕ НАСТРОЙКИ
# ==============================================

SUPPORTED_LANGUAGES = {
    "zh-CN": "китайский",
    "zh-TW": "китайский традиционный",
    "ru": "русский",
    "en": "английский",
    "ja": "японский",
    "ko": "корейский",
    "de": "немецкий",
    "fr": "французский",
    "es": "испанский",
    "pt": "португальский"
}

DEFAULT_SOURCE_LANG = "zh-CN"
DEFAULT_TARGET_LANG = "ru"

# ==============================================
# 📄 НАСТРОЙКИ СЕРВИСА
# ==============================================

# Микросервис конфигурация
SERVICE_HOST = os.getenv('SERVICE_HOST', '0.0.0.0')
SERVICE_PORT = int(os.getenv('SERVICE_PORT', '8003'))

# Мониторинг
PROMETHEUS_ENABLED = os.getenv('PROMETHEUS_ENABLED', 'true').lower() == 'true'
METRICS_PORT = int(os.getenv('METRICS_PORT', '9003'))

# Логирование
VERBOSE_PROGRESS = os.getenv('VERBOSE_PROGRESS', 'true').lower() == 'true'
SHOW_STATISTICS = os.getenv('SHOW_STATISTICS', 'true').lower() == 'true'
SHOW_DETAILED_PROGRESS = os.getenv('SHOW_DETAILED_PROGRESS', 'true').lower() == 'true'
LOG_ERRORS = os.getenv('LOG_ERRORS', 'true').lower() == 'true'
EMOJI_PROGRESS = os.getenv('EMOJI_PROGRESS', 'false').lower() == 'true'

# Безопасность
MAX_REMAINING_FIXES = int(os.getenv('MAX_REMAINING_FIXES', '20'))
REMAINING_TIMEOUT = int(os.getenv('REMAINING_TIMEOUT', '300'))  # 5 минут
BATCH_SIZE_REMAINING = int(os.getenv('BATCH_SIZE_REMAINING', '3'))
MAX_FIX_ATTEMPTS = int(os.getenv('MAX_FIX_ATTEMPTS', '2'))
MIN_FRAGMENT_LENGTH = int(os.getenv('MIN_FRAGMENT_LENGTH', '2'))

# Пути и файлы
SUPPORTED_EXTENSIONS = [".md", ".markdown", ".txt", ".rst"]
DEFAULT_OUTPUT_SUFFIX = "{target_lang}"
CREATE_BACKUP = os.getenv('CREATE_BACKUP', 'true').lower() == 'true'