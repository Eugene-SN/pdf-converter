#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ Content Transformation - ПОЛНАЯ система трансформации
Исправления:
- Правильный парсинг OpenAI-совместимого ответа от vLLM (choices — список)
- Выход из ретраев после первого 200 OK и валидного парсинга
- task_type='content_transformation' в payload для серверного автосвитча модели
- Исправлен баг с возвратом списка вместо строки при merge_enhanced_chunks
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import logging
import time
import re
import hashlib
from collections import OrderedDict
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional, Tuple
from requests.adapters import HTTPAdapter

# ✅ logger до любых try/except
logger = logging.getLogger(__name__)

# Утилиты
from shared_utils import (
    SharedUtils, NotificationUtils,
    MetricsUtils, ErrorHandlingUtils
)

# Конфигурация DAG
DEFAULT_ARGS = {
    'owner': 'pdf-converter',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'content_transformation',
    default_args=DEFAULT_ARGS,
    description='✅ Content Transformation - ПОЛНАЯ система трансформации китайских документов',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    tags=['pdf-converter', 'dag2', 'transformation', 'chinese-docs', 'vllm-enhanced', 'enterprise'],
)

# ================================================================================
# ПОЛНАЯ КОНФИГУРАЦИЯ ТРАНСФОРМАЦИИ КОНТЕНТА
# ================================================================================

# ✅ Технические термины
PRESERVE_TERMS: Dict[str, str] = {
    '问天': 'WenTian',
    '联想问天': 'Lenovo WenTian',
    '天擎': 'ThinkSystem',
    '至强': 'Xeon',
    '英特尔': 'Intel',
    '处理器': 'Processor',
    '内存': 'Memory',
    '存储': 'Storage',
    '网卡': 'Network Adapter',
    '电源': 'Power Supply',
    '服务器': 'Server',
    '机架': 'Rack',
    '刀片': 'Blade',
    '交换机': 'Switch',
}

# ✅ Паттерны заголовков
HEADING_PATTERNS: List[str] = [
    r'^[第章节]\s*[一二三四五六七八九十\d]+\s*[章节]',
    r'^[一二三四五六七八九十]+[、．]',
    r'^\d+[、．]\s*[\u4e00-\u9fff]',
    r'^[\u4e00-\u9fff]+[:：]',
]

# ✅ vLLM конфигурация
VLLM_CONFIG: Dict[str, Any] = {
    # Используем имя сервиса Docker Compose для корректного DNS
    'endpoint': os.getenv('VLLM_SERVER_URL', 'http://vllm-server:8000') + '/v1/chat/completions',
    'model': os.getenv('VLLM_CONTENT_MODEL', 'Qwen/Qwen2.5-VL-32B-Instruct'),
    'timeout': int(os.getenv('VLLM_STANDARD_TIMEOUT', '300')),
    'max_tokens': 2048,  # безопаснее для 8-16k контекстов
    'temperature': 0.2,
    'top_p': 0.9,
    'top_k': 50,
    'max_retries': 3,
    'retry_delay': 5,
    'enable_fallback': True,
    'max_concurrent_requests': max(1, int(os.getenv('VLLM_MAX_CONCURRENT', '2'))),
}

# Семафор для ограничения параллелизма запросов к vLLM
_VLLM_SEMAPHORE = threading.Semaphore(VLLM_CONFIG.get('max_concurrent_requests', 1))

# Общая HTTP‑сессия с расширенным пулом соединений
_VLLM_HTTP_POOL = max(4, VLLM_CONFIG['max_concurrent_requests'] * 4)
_VLLM_SESSION = requests.Session()
_VLLM_ADAPTER = HTTPAdapter(pool_connections=_VLLM_HTTP_POOL, pool_maxsize=_VLLM_HTTP_POOL)
_VLLM_SESSION.mount('http://', _VLLM_ADAPTER)
_VLLM_SESSION.mount('https://', _VLLM_ADAPTER)

VLLM_CACHE_ENABLED = os.getenv('VLLM_ENABLE_CACHE', 'true').lower() == 'true'
VLLM_CACHE_SIZE = max(0, int(os.getenv('VLLM_CACHE_SIZE', '128')))
VLLM_CACHE_TTL = int(os.getenv('VLLM_CACHE_TTL_SECONDS', '900'))
_VLLM_CACHE: OrderedDict[str, Tuple[str, float]] = OrderedDict()
_CACHE_LOCK = threading.Lock()


def _cache_lookup(key: str) -> Optional[str]:
    if not VLLM_CACHE_ENABLED or VLLM_CACHE_SIZE <= 0:
        return None
    with _CACHE_LOCK:
        cached = _VLLM_CACHE.get(key)
        if not cached:
            return None
        content, ts = cached
        if VLLM_CACHE_TTL and (time.time() - ts) > VLLM_CACHE_TTL:
            _VLLM_CACHE.pop(key, None)
            return None
        _VLLM_CACHE.move_to_end(key)
        return content


def _cache_store(key: str, value: str) -> None:
    if not VLLM_CACHE_ENABLED or VLLM_CACHE_SIZE <= 0:
        return
    with _CACHE_LOCK:
        _VLLM_CACHE[key] = (value, time.time())
        _VLLM_CACHE.move_to_end(key)
        while len(_VLLM_CACHE) > VLLM_CACHE_SIZE:
            _VLLM_CACHE.popitem(last=False)

# ✅ Конфигурация чанкования
CHUNKING_CONFIG: Dict[str, Any] = {
    'max_chunk_size': 6000,
    'chunk_overlap': 500,
    'min_chunk_size': 1000,
    'preserve_sections': True,
    'split_on_headers': True,
}

# ✅ Конфигурация улучшений
ENHANCEMENT_CONFIG: Dict[str, Any] = {
    'enable_vllm_enhancement': True,
    'enhancement_quality_threshold': 0.7,
    'max_enhancement_retries': 2,
    'fallback_to_basic': True,
    'preserve_chinese_terms': True,
    'technical_focus': True,
}

# ================================================================================
# ДОКУМЕНТНЫЕ УТИЛИТЫ
# ================================================================================

def normalize_document_payload(document_data: Dict[str, Any]) -> Dict[str, Any]:
    """Приводит payload Stage1 к ожидаемому виду или поднимает ошибку."""
    if not isinstance(document_data, dict):
        raise ValueError("Document payload must be dict")

    markdown = document_data.get('markdown_content') or ''
    raw_text = document_data.get('raw_text') or ''
    sections = document_data.get('sections') or []

    if not markdown and raw_text:
        document_data['markdown_content'] = raw_text
        markdown = raw_text

    if not sections:
        raise ValueError("Document payload missing sections for transformation")

    metadata = document_data.setdefault('metadata', {})
    metadata.setdefault('title', document_data.get('title'))
    metadata.setdefault('sections_count', len(sections))

    return document_data


def build_markdown_from_sections(document_data: Dict[str, Any]) -> str:
    """Формирует Markdown из секций, если Docling не вернул готовый контент."""
    lines: List[str] = []
    sections = document_data.get('sections') or []

    for section in sections:
        level = int(section.get('level') or 1)
        level = max(1, min(level, 6))
        title = str(section.get('title') or '').strip()
        content = str(section.get('content') or '').strip()

        if title:
            lines.append(f"{'#' * level} {title}")
        if content:
            lines.append(content)
        if lines and lines[-1] != "":
            lines.append("")

    tables = document_data.get('tables') or []
    if tables:
        lines.append("## Tables")
        for idx, table in enumerate(tables, start=1):
            table_markdown = render_table_markdown(table)
            if table_markdown:
                lines.append(f"### Table {idx}")
                lines.append(table_markdown)
                lines.append("")

    markdown = "\n".join(line for line in lines if line is not None)
    return markdown.strip()


def render_table_markdown(table_entry: Dict[str, Any]) -> str:
    """Преобразует таблицу из payload в Markdown; при ошибке возвращает строку"""
    content = table_entry.get('content')
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, dict):
        data = content.get('data') or content.get('rows')
        if isinstance(data, list) and data:
            row_lines: List[str] = []
            header = data[0]
            if isinstance(header, list):
                row_lines.append('| ' + ' | '.join(str(cell).strip() for cell in header) + ' |')
                row_lines.append('|' + ' --- |' * len(header))
                for row in data[1:]:
                    if isinstance(row, list):
                        row_lines.append('| ' + ' | '.join(str(cell).strip() for cell in row) + ' |')
                return "\n".join(row_lines)

    return ''


def has_headings(content: str) -> bool:
    return bool(re.search(r'^#+\s', content or '', re.MULTILINE))


CODE_FENCE_PATTERN = re.compile(r'^\s*```')


def normalize_code_fences(content: str) -> Tuple[str, bool]:
    """Автоматически закрывает незавершённые markdown-блоки кода."""
    lines = content.splitlines()
    fence_open = False
    normalized_lines: List[str] = []

    for line in lines:
        normalized_lines.append(line)
        if CODE_FENCE_PATTERN.match(line.strip()):
            fence_open = not fence_open

    fixed = False
    if fence_open:
        normalized_lines.append('```')
        fixed = True

    normalized_content = "\n".join(normalized_lines)
    if content.endswith('\n') or fixed:
        normalized_content += '\n'

    return normalized_content, fixed


def validate_markdown_structure(content: str, min_headings: int = 1) -> None:
    heading_count = len(re.findall(r'^#+\s', content or '', re.MULTILINE))
    if heading_count < min_headings:
        raise ValueError(f"Markdown structure invalid: expected >= {min_headings} headings, found {heading_count}")

    fence_state = False
    for line in content.splitlines():
        if CODE_FENCE_PATTERN.match(line.strip()):
            fence_state = not fence_state

    if fence_state:
        raise ValueError("Markdown structure invalid: unclosed code fence detected")

# ================================================================================
# ОСНОВНЫЕ ФУНКЦИИ ТРАНСФОРМАЦИИ

# ================================================================================
# ОСНОВНЫЕ ФУНКЦИИ ТРАНСФОРМАЦИИ
# ================================================================================
# ================================================================================
# ОСНОВНЫЕ ФУНКЦИИ ТРАНСФОРМАЦИИ
# ================================================================================

def load_intermediate_data(**context) -> Dict[str, Any]:
    start_time = time.time()
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info("📥 Загрузка данных для полной трансформации контента")

        airflow_temp = os.getenv('AIRFLOW_TEMP_DIR', '/opt/airflow/temp')

        def map_to_airflow_temp(path: str) -> str:
            if path.startswith("/app/temp"):
                return path.replace("/app/temp", airflow_temp, 1)
            return path

        intermediate_file = dag_run_conf.get('intermediate_file')
        if not intermediate_file:
            raise ValueError("Не указан intermediate_file для Stage 2")
        intermediate_file = map_to_airflow_temp(intermediate_file)
        if not os.path.exists(intermediate_file):
            raise ValueError(f"Файл не существует: {intermediate_file}")

        with open(intermediate_file, 'r', encoding='utf-8') as f:
            document_data = json.load(f)

        document_data = normalize_document_payload(document_data)
        document_title = document_data.get('title') or document_data.get('metadata', {}).get('title')
        if not document_title:
            document_title = Path(intermediate_file).stem.replace('_intermediate', '')
            document_data['title'] = document_title

        transformation_session: Dict[str, Any] = {
            'session_id': f"transform_{int(time.time())}",
            'document_data': document_data,
            'original_config': dag_run_conf.get('original_config', {}),
            'intermediate_file': intermediate_file,
            'transformation_start_time': datetime.now().isoformat(),
            'vllm_enhancement_enabled': dag_run_conf.get('vllm_enhancement', True),
            'chunking_config': CHUNKING_CONFIG,
            'enhancement_config': ENHANCEMENT_CONFIG,
            'preserve_terms': PRESERVE_TERMS,
            'document_type': 'chinese_technical',
            'document_title': document_title,
        }

        content_length = len(document_data.get('markdown_content', ''))
        logger.info(f"✅ Данные загружены для полной трансформации: {content_length} символов")

        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='load_intermediate_data',
            processing_time=time.time() - start_time,
            success=True,
        )
        return transformation_session
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='load_intermediate_data',
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error(f"❌ Ошибка загрузки данных: {e}")
        raise


def perform_basic_transformations(**context) -> Dict[str, Any]:
    start_time = time.time()
    try:
        transformation_session = context['task_instance'].xcom_pull(task_ids='load_intermediate_data')
        logger.info("🔄 Выполнение базовых китайских трансформаций")

        document_data = transformation_session['document_data']
        original_content = document_data.get('markdown_content', '')
        if not original_content.strip():
            raise ValueError("Исходный контент пустой")

        if not has_headings(original_content):
            logger.info("⚠️ Markdown без заголовков – формируем контент из структурных данных")
            original_content = build_markdown_from_sections(document_data)

        logger.info("📝 Применение китайских трансформаций")
        transformed_content = apply_chinese_transformations(original_content)

        logger.info("🏗️ Улучшение структуры документа")
        structured_content = improve_document_structure(
            transformed_content,
            document_title=transformation_session.get('document_title'),
            sections=document_data.get('sections')
        )

        logger.info("🎨 Финальное базовое форматирование")
        final_content = finalize_basic_formatting(structured_content)
        final_content, fence_fixed = normalize_code_fences(final_content)
        if fence_fixed:
            logger.info("🔧 Базовый контент дополнен закрывающей тройной кавычкой")
        validate_markdown_structure(final_content, min_headings=max(1, document_data.get('metadata', {}).get('sections_count', 1)))

        basic_quality_score = calculate_basic_transformation_quality(original_content, final_content)

        basic_result: Dict[str, Any] = {
            'session_id': transformation_session['session_id'],
            'original_content_length': len(original_content),
            'basic_transformed_content': final_content,
            'basic_content_length': len(final_content),
            'basic_quality_score': basic_quality_score,
            'chinese_chars_preserved': count_chinese_characters(final_content),
            'technical_terms_preserved': count_preserved_terms(final_content),
            'basic_processing_time': time.time() - start_time,
            'ready_for_enhancement': basic_quality_score >= 70.0,
        }

        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='perform_basic_transformations',
            processing_time=time.time() - start_time,
            success=True,
        )
        logger.info(f"✅ Базовые трансформации завершены: качество {basic_quality_score:.1f}%")
        return basic_result
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='perform_basic_transformations',
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error(f"❌ Ошибка базовых трансформаций: {e}")
        raise


def perform_vllm_enhancement(**context) -> Dict[str, Any]:
    start_time = time.time()
    try:
        transformation_session = context['task_instance'].xcom_pull(task_ids='load_intermediate_data')
        basic_results = context['task_instance'].xcom_pull(task_ids='perform_basic_transformations')
        logger.info("🤖 Начало vLLM интеллектуального улучшения")

        if not basic_results['ready_for_enhancement']:
            logger.warning("Базовое качество слишком низкое для vLLM улучшения")
            return {
                'session_id': transformation_session['session_id'],
                'enhancement_skipped': True,
                'reason': f"Basic quality too low: {basic_results['basic_quality_score']:.1f}%",
                'enhanced_content': basic_results['basic_transformed_content'],
                'enhancement_quality': 0.0,
            }

        basic_content = basic_results['basic_transformed_content']
        vllm_enabled = transformation_session['vllm_enhancement_enabled']

        enhancement_result: Dict[str, Any] = {
            'session_id': transformation_session['session_id'],
            'enhancement_attempted': vllm_enabled,
            'enhanced_content': basic_content,
            'enhancement_quality': 0.0,
            'chunks_processed': 0,
            'enhancement_time': 0.0,
        }

        if vllm_enabled and ENHANCEMENT_CONFIG['enable_vllm_enhancement']:
            logger.info("📊 Выполнение интеллектуального чанкования")
            content_chunks = perform_intelligent_chunking(basic_content)
            if not content_chunks:
                logger.warning("Не удалось создать чанки для обработки")
                enhancement_result['enhancement_skipped'] = True
                return enhancement_result

            logger.info(f"🚀 Обработка {len(content_chunks)} чанков через vLLM")
            enhanced_chunks: List[Optional[str]] = [None] * len(content_chunks)
            failed_chunks = 0
            max_allowed_failures = max(1, len(content_chunks) // 2)

            executor_workers = max(1, VLLM_CONFIG.get('max_concurrent_requests', 1))
            if executor_workers > 1 and len(content_chunks) > 1:
                logger.info(f"🧵 Параллельная обработка чанков ({executor_workers} потоков)")
                with ThreadPoolExecutor(max_workers=executor_workers) as executor:
                    future_map = {
                        executor.submit(enhance_chunk_with_vllm, chunk, idx, len(content_chunks)): idx
                        for idx, chunk in enumerate(content_chunks)
                    }

                    for future in as_completed(future_map):
                        idx = future_map[future]
                        chunk = content_chunks[idx]
                        try:
                            result_chunk = future.result()
                        except Exception as chunk_error:
                            logger.error(f"❌ Ошибка обработки чанка {idx + 1}: {chunk_error}")
                            result_chunk = chunk

                        if not result_chunk or result_chunk == chunk:
                            failed_chunks += 1
                            result_chunk = chunk

                        enhanced_chunks[idx] = result_chunk
                        if failed_chunks > max_allowed_failures:
                            logger.warning(f"Слишком много падений vLLM ({failed_chunks}), используем fallback")
                            enhancement_result.update({
                                'enhancement_failed': True,
                                'fallback_reason': f'Too many vLLM failures: {failed_chunks}/{len(content_chunks)}',
                                'enhanced_content': basic_content,
                            })
                            return enhancement_result
            else:
                for i, chunk in enumerate(content_chunks):
                    logger.info(f"Обработка чанка {i + 1}/{len(content_chunks)}")
                    enhanced_chunk = enhance_chunk_with_vllm(chunk, i, len(content_chunks))
                    if not enhanced_chunk or enhanced_chunk == chunk:
                        failed_chunks += 1
                        enhanced_chunk = chunk
                        if failed_chunks > max_allowed_failures:
                            logger.warning(f"Слишком много падений vLLM ({failed_chunks}), используем fallback")
                            enhancement_result.update({
                                'enhancement_failed': True,
                                'fallback_reason': f'Too many vLLM failures: {failed_chunks}/{len(content_chunks)}',
                                'enhanced_content': basic_content,
                            })
                            return enhancement_result
                    enhanced_chunks[i] = enhanced_chunk

            if enhanced_chunks:
                logger.info("🔗 Объединение улучшенных чанков")
                normalized_chunks = [c if isinstance(c, str) and c.strip() else content_chunks[idx]
                                     for idx, c in enumerate(enhanced_chunks)]
                final_enhanced_content = merge_enhanced_chunks(normalized_chunks)
                enhancement_quality = evaluate_enhancement_quality(basic_content, final_enhanced_content)
                if enhancement_quality >= ENHANCEMENT_CONFIG['enhancement_quality_threshold']:
                    enhancement_result.update({
                        'enhanced_content': final_enhanced_content,
                        'enhancement_quality': enhancement_quality,
                        'chunks_processed': len(normalized_chunks),
                        'enhancement_successful': True,
                    })
                    logger.info(f"✅ vLLM улучшение успешно: качество {enhancement_quality:.3f}")
                else:
                    logger.warning(
                        f"vLLM улучшение отклонено: качество {enhancement_quality:.3f} "
                        f"< {ENHANCEMENT_CONFIG['enhancement_quality_threshold']}"
                    )
                    enhancement_result['enhancement_rejected'] = True
        else:
            enhancement_result['enhancement_skipped'] = True
            logger.info("vLLM улучшение отключено в конфигурации")

        enhanced_content = enhancement_result.get('enhanced_content', '')
        sanitized_content, fence_fixed = normalize_code_fences(enhanced_content)
        if fence_fixed:
            logger.info("🔧 Результат vLLM дополнен закрывающей тройной кавычкой")
            enhancement_result['enhanced_content'] = sanitized_content

        enhancement_result['enhancement_time'] = time.time() - start_time
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='perform_vllm_enhancement',
            processing_time=time.time() - start_time,
            success=True,
        )
        return enhancement_result

    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='perform_vllm_enhancement',
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error(f"❌ Ошибка vLLM улучшения: {e}")
        if ENHANCEMENT_CONFIG['fallback_to_basic']:
            basic_results = context['task_instance'].xcom_pull(task_ids='perform_basic_transformations')
            return {
                'session_id': transformation_session['session_id'],
                'enhancement_failed': True,
                'fallback_used': True,
                'enhanced_content': basic_results['basic_transformed_content'],
                'error': str(e),
            }
        raise

# ================================================================================
# БАЗОВЫЕ ФУНКЦИИ ТРАНСФОРМАЦИИ
# ================================================================================

def apply_chinese_transformations(content: str) -> str:
    try:
        logger.info("🔄 Применение китайских трансформаций")
        for chinese_term, english_term in PRESERVE_TERMS.items():
            if chinese_term in content:
                content = content.replace(chinese_term, f"{chinese_term} ({english_term})")
        content = improve_chinese_headings(content)
        content = enhance_chinese_tables(content)
        content = clean_chinese_formatting(content)
        return content
    except Exception as e:
        logger.warning(f"Ошибка китайских трансформаций: {e}")
        return content


def improve_chinese_headings(content: str) -> str:
    try:
        lines = content.split('\n')
        improved_lines: List[str] = []
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                improved_lines.append(line)
                continue
            heading_level = detect_chinese_heading_level(line_stripped)
            if heading_level > 0 and not line_stripped.startswith('#'):
                markdown_prefix = '#' * heading_level + ' '
                improved_lines.append(f"{markdown_prefix}{line_stripped}")
            else:
                improved_lines.append(line)
        return '\n'.join(improved_lines)
    except Exception as e:
        logger.warning(f"Ошибка улучшения заголовков: {e}")
        return content


def detect_chinese_heading_level(text: str) -> int:
    for pattern in HEADING_PATTERNS:
        if re.match(pattern, text):
            if '第' in text and '章' in text:
                return 1
            elif '第' in text and '节' in text:
                return 2
            elif re.match(r'^[一二三四五六七八九十]+[、．]', text):
                return 3
            elif re.match(r'^\d+[、．]', text):
                return 2
            else:
                return 2
    return 0


def enhance_chinese_tables(content: str) -> str:
    try:
        lines = content.split('\n')
        enhanced_lines: List[str] = []
        in_table = False
        for i, line in enumerate(lines):
            if '|' in line and len([cell for cell in line.split('|') if cell.strip()]) >= 2:
                if not in_table:
                    in_table = True
                    enhanced_lines.append(line)
                    if (i + 1 < len(lines) and not re.match(r'^\|[\s\-:|]+\|', lines[i + 1])):
                        cols = len([cell for cell in line.split('|') if cell.strip()])
                        separator = '|' + ' --- |' * cols
                        enhanced_lines.append(separator)
                else:
                    enhanced_lines.append(line)
            else:
                if in_table and line.strip() == '':
                    in_table = False
                enhanced_lines.append(line)
        return '\n'.join(enhanced_lines)
    except Exception as e:
        logger.warning(f"Ошибка улучшения таблиц: {e}")
        return content


def clean_chinese_formatting(content: str) -> str:
    try:
        content = re.sub(r'([\u4e00-\u9fff])\s+([\u4e00-\u9fff])', r'\1\2', content)
        content = re.sub(r'([\u4e00-\u9fff])\s*([，。；：！？])', r'\1\2', content)
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        lines = [line.rstrip() for line in content.split('\n')]
        content = '\n'.join(lines)
        return content.strip()
    except Exception as e:
        logger.warning(f"Ошибка очистки форматирования: {e}")
        return content


def improve_document_structure(content: str, document_title: Optional[str] = None, sections: Optional[List[Dict[str, Any]]] = None) -> str:
    try:
        lines = content.split('\n')
        structured_lines: List[str] = []

        existing_heading_match = re.search(r'^#\s+(.+)', content, re.MULTILINE)
        title_to_insert = (document_title or '').strip()
        if title_to_insert and not existing_heading_match:
            structured_lines.append(f"# {title_to_insert}")
            structured_lines.append("")

        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                structured_lines.append('')
                continue

            if stripped.startswith('|') and stripped.count('|') >= 2:
                if structured_lines and structured_lines[-1] != '':
                    structured_lines.append('')
                structured_lines.append(line)
                continue

            structured_lines.append(line)

        structured_text = '\n'.join(structured_lines)

        if sections:
            missing_titles = [sec.get('title') for sec in sections if sec.get('title') and sec.get('title') not in structured_text]
            for title in missing_titles:
                structured_text += f"\n\n## {title}\n"

        return structured_text.strip()
    except Exception as e:
        logger.warning(f"Ошибка улучшения структуры: {e}")
        return content


def finalize_basic_formatting(content: str) -> str:
    try:
        content = content.strip()
        content = re.sub(r'(\n#+.*?)\n\n+', r'\1\n\n', content)
        content = re.sub(r'(#+\s+.*?)\n([^\n])', r'\1\n\n\2', content)
        content = re.sub(r'\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b', r'`\1`', content)
        content = re.sub(r'\b(0x[0-9a-fA-F]+)\b', r'`\1`', content)
        return content
    except Exception as e:
        logger.warning(f"Ошибка финального форматирования: {e}")
        return content

# ================================================================================
# vLLM ИНТЕЛЛЕКТУАЛЬНЫЕ ФУНКЦИИ
# ================================================================================

def perform_intelligent_chunking(content: str) -> List[str]:
    try:
        logger.info("📊 Выполнение интеллектуального чанкования")
        if len(content) <= CHUNKING_CONFIG['max_chunk_size']:
            return [content]

        chunks: List[str] = []
        if CHUNKING_CONFIG['split_on_headers']:
            sections = re.split(r'\n(#+\s+.*?)\n', content)
            current_chunk = ""
            for section in sections:
                if re.match(r'^#+\s+', section or ''):
                    if len(current_chunk) >= CHUNKING_CONFIG['min_chunk_size']:
                        chunks.append(current_chunk.strip())
                        overlap_part = current_chunk[-CHUNKING_CONFIG['chunk_overlap']:]
                        current_chunk = overlap_part + '\n' + section
                    else:
                        current_chunk += '\n' + section
                else:
                    current_chunk += section or ''
                if len(current_chunk) >= CHUNKING_CONFIG['max_chunk_size']:
                    chunks.append(current_chunk.strip())
                    overlap_part = current_chunk[-CHUNKING_CONFIG['chunk_overlap']:]
                    current_chunk = overlap_part
            if current_chunk.strip():
                chunks.append(current_chunk.strip())
        else:
            step = CHUNKING_CONFIG['max_chunk_size'] - CHUNKING_CONFIG['chunk_overlap']
            for i in range(0, len(content), step):
                chunk = content[i:i + CHUNKING_CONFIG['max_chunk_size']]
                if chunk.strip():
                    chunks.append(chunk)

        logger.info(f"✅ Создано {len(chunks)} чанков для обработки")
        return [c for c in chunks if len(c) >= CHUNKING_CONFIG['min_chunk_size']]
    except Exception as e:
        logger.error(f"Ошибка чанкования: {e}")
        return [content]


def enhance_chunk_with_vllm(chunk: str, chunk_index: int, total_chunks: int) -> Optional[str]:
    with _VLLM_SEMAPHORE:
        try:
            logger.info(f"🤖 vLLM обработка чанка {chunk_index + 1}/{total_chunks}")
            enhancement_prompt = create_specialized_enhancement_prompt(chunk, chunk_index, total_chunks)
            enhanced_content = call_vllm_with_retry(enhancement_prompt)
            if enhanced_content and enhanced_content != chunk:
                logger.info(f"✅ Чанк {chunk_index + 1} улучшен")
                return enhanced_content
            logger.warning(f"Чанк {chunk_index + 1} не улучшен")
            return chunk
        except Exception as e:
            logger.error(f"Ошибка улучшения чанка {chunk_index + 1}: {e}")
            return chunk


def create_specialized_enhancement_prompt(chunk: str, chunk_index: int, total_chunks: int) -> str:
    chinese_terms_context = ", ".join([f"{ch} ({en})" for ch, en in list(PRESERVE_TERMS.items())[:5]])
    prompt = f"""You are a professional technical documentation specialist focusing on Chinese enterprise hardware documentation.
Your task is to enhance this markdown content while preserving all technical accuracy and Chinese terminology:
CONTEXT: This is chunk {chunk_index + 1} of {total_chunks} from a Chinese technical document about enterprise server hardware.
ENHANCEMENT REQUIREMENTS:
1. Preserve ALL Chinese technical terms exactly as they appear
2. Maintain Chinese-English term pairs like: {chinese_terms_context}
3. Improve markdown structure and formatting
4. Enhance technical clarity while keeping original meaning
5. Fix any formatting issues (tables, headers, lists)
6. Ensure proper technical terminology consistency
7. Keep all specific technical details (model numbers, specifications, etc.)
CONTENT TO ENHANCE:
{chunk}
Please provide the enhanced markdown content that follows all requirements above. Respond with ONLY the enhanced markdown content, no explanations."""
    return prompt


def _parse_vllm_chat_response(resp_json: Dict[str, Any]) -> Tuple[str, int, int, int]:
    """
    Возвращает (text, prompt_tokens, completion_tokens, total_tokens)
    Соответствует OpenAI chat completions формату: choices — список, message.content — строка.
    """
    choices = resp_json.get("choices") or []
    if not choices:
        raise ValueError("Empty choices in vLLM response")

    # ИСПРАВЛЕНО: choices - это список, берём первый элемент
    message = choices[0].get("message") or {}
    text = (message.get("content") or "").strip()

    usage = resp_json.get("usage") or {}
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))
    return text, prompt_tokens, completion_tokens, total_tokens

def call_vllm_with_retry(prompt: str) -> Optional[str]:
    """
    Делает до max_retries попыток вызвать vLLM chat/completions, выходит при первом успехе.
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Task-Type": "content_transformation",
    }
    cache_key = hashlib.sha256(prompt.encode('utf-8')).hexdigest()
    cached_response = _cache_lookup(cache_key)
    if cached_response:
        logger.info("♻️ Используем кешированный ответ vLLM")
        return cached_response

    payload = {
        "model": VLLM_CONFIG['model'],
        "messages": [
            {"role": "system", "content": "You are a helpful technical editor."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": VLLM_CONFIG['max_tokens'],
        "temperature": VLLM_CONFIG['temperature'],
        "top_p": VLLM_CONFIG['top_p'],
        "top_k": VLLM_CONFIG['top_k'],
        # Явно указываем тип задачи для автосвитча модели на сервере
        "task_type": "content_transformation",
        "stream": False,
    }

    for attempt in range(VLLM_CONFIG['max_retries']):
        try:
            logger.info(f"vLLM API вызов (попытка {attempt + 1})")
            response = _VLLM_SESSION.post(
                VLLM_CONFIG['endpoint'],
                json=payload,
                timeout=VLLM_CONFIG['timeout'],
                headers=headers,
            )

            if response.status_code == 200:
                result = response.json()
                try:
                    content, p_tok, c_tok, t_tok = _parse_vllm_chat_response(result)
                except Exception as parse_err:
                    logger.warning(f"vLLM API 200, но ошибка парсинга: {parse_err}")
                    if attempt < VLLM_CONFIG['max_retries'] - 1:
                        time.sleep(VLLM_CONFIG['retry_delay'])
                        continue
                    return None

                if content:
                    logger.info("✅ vLLM API успешен")
                    stripped = content.strip()
                    _cache_store(cache_key, stripped)
                    return stripped

                logger.warning("vLLM API 200, но пустой content")
                if attempt < VLLM_CONFIG['max_retries'] - 1:
                    time.sleep(VLLM_CONFIG['retry_delay'])
                    continue
                return None

            elif response.status_code == 500:
                logger.warning(f"vLLM API 500 (препроцессинг): {response.text[:200]}")
                if attempt < VLLM_CONFIG['max_retries'] - 1:
                    time.sleep(VLLM_CONFIG['retry_delay'] * 2)
                    continue
                logger.error("Все попытки vLLM неудачны (500 ошибка)")
                return None

            elif response.status_code in (429, 503):
                logger.warning(f"vLLM перегружен ({response.status_code}), повтор через задержку")
                if attempt < VLLM_CONFIG['max_retries'] - 1:
                    time.sleep(VLLM_CONFIG['retry_delay'] * 2)
                    continue
                return None

            else:
                logger.warning(f"vLLM API ошибка: {response.status_code} {response.text[:200]}")

        except Exception as e:
            logger.warning(f"vLLM попытка {attempt + 1} неудачна: {e}")
            if attempt < VLLM_CONFIG['max_retries'] - 1:
                time.sleep(VLLM_CONFIG['retry_delay'])
                continue

    logger.error("Все попытки vLLM API неудачны")
    return None


def merge_enhanced_chunks(chunks: List[str]) -> str:
    try:
        logger.info(f"🔗 Объединение {len(chunks)} улучшенных чанков")
        if not chunks:
            return ""
        if len(chunks) == 1:
            return chunks[0]  # ИСПРАВЛЕНО: возвращаем строку, не список

        # ИСПРАВЛЕНО: начинаем со строки, не со списка
        merged_content = chunks[0]
        for i in range(1, len(chunks)):
            chunk = chunks[i]
            overlap_removed = remove_chunk_overlap(merged_content, chunk)
            if not merged_content.endswith('\n\n') and not overlap_removed.startswith('\n'):
                merged_content += '\n\n'
            merged_content += overlap_removed

        logger.info("✅ Чанки успешно объединены")
        return merged_content.strip()
    except Exception as e:
        logger.error(f"Ошибка объединения чанков: {e}")
        return '\n\n'.join(chunks)

def remove_chunk_overlap(content1: str, content2: str) -> str:
    try:
        max_overlap = min(CHUNKING_CONFIG['chunk_overlap'], len(content1), len(content2))
        for overlap_len in range(max_overlap, 50, -10):
            suffix = content1[-overlap_len:]
            prefix = content2[:overlap_len]
            # простая эвристика схожести
            similarity = len(set(suffix.split()) & set(prefix.split())) / max(len(suffix.split()), 1)
            if similarity > 0.3:
                return content2[overlap_len:]
        return content2
    except Exception:
        return content2


def evaluate_enhancement_quality(original: str, enhanced: str) -> float:
    try:
        if not enhanced or enhanced == original:
            return 0.0
        quality_score = 0.0

        length_ratio = len(enhanced) / max(len(original), 1)
        if 0.9 <= length_ratio <= 1.3:
            quality_score += 0.3
        elif 0.8 <= length_ratio <= 1.5:
            quality_score += 0.1

        if ENHANCEMENT_CONFIG['preserve_chinese_terms']:
            original_chinese = count_chinese_characters(original)
            enhanced_chinese = count_chinese_characters(enhanced)
            if original_chinese > 0:
                chinese_preservation = enhanced_chinese / original_chinese
                if chinese_preservation >= 0.95:
                    quality_score += 0.2
                elif chinese_preservation >= 0.8:
                    quality_score += 0.1

        if ENHANCEMENT_CONFIG['technical_focus']:
            original_terms = count_preserved_terms(original)
            enhanced_terms = count_preserved_terms(enhanced)
            if original_terms > 0:
                terms_preservation = enhanced_terms / original_terms
                if terms_preservation >= 0.9:
                    quality_score += 0.2
                elif terms_preservation >= 0.7:
                    quality_score += 0.1

        original_headers = len(re.findall(r'^#+\s', original, re.MULTILINE))
        enhanced_headers = len(re.findall(r'^#+\s', enhanced, re.MULTILINE))
        if enhanced_headers >= original_headers:
            quality_score += 0.15

        original_tables = len(re.findall(r'\|.*\|', original))
        enhanced_tables = len(re.findall(r'\|.*\|', enhanced))
        if enhanced_tables >= original_tables:
            quality_score += 0.15

        return min(1.0, quality_score)
    except Exception as e:
        logger.warning(f"Ошибка оценки качества улучшения: {e}")
        return 0.5

# ================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ================================================================================

def calculate_basic_transformation_quality(original: str, transformed: str) -> float:
    try:
        quality_score = 100.0
        length_ratio = len(transformed) / max(len(original), 1)
        if length_ratio < 0.8 or length_ratio > 1.3:
            quality_score -= 10

        original_headers = len(re.findall(r'^#+\s', original, re.MULTILINE))
        transformed_headers = len(re.findall(r'^#+\s', transformed, re.MULTILINE))
        if transformed_headers < original_headers:
            quality_score -= 15

        original_tables = len(re.findall(r'\|.*\|', original))
        transformed_tables = len(re.findall(r'\|.*\|', transformed))
        if original_tables > 0:
            table_preservation = transformed_tables / original_tables
            if table_preservation < 0.9:
                quality_score -= 10

        original_chinese = count_chinese_characters(original)
        transformed_chinese = count_chinese_characters(transformed)
        if original_chinese > 0:
            chinese_preservation = transformed_chinese / original_chinese
            if chinese_preservation < 0.9:
                quality_score -= 20

        return max(0, quality_score)
    except Exception:
        return 75.0


def count_chinese_characters(text: str) -> int:
    return len(re.findall(r'[\u4e00-\u9fff]', text))


def count_preserved_terms(text: str) -> int:
    count = 0
    for term in PRESERVE_TERMS.values():
        count += text.count(term)
    return count


def finalize_transformation_results(**context) -> Dict[str, Any]:
    start_time = time.time()
    try:
        transformation_session = context['task_instance'].xcom_pull(task_ids='load_intermediate_data')
        basic_results = context['task_instance'].xcom_pull(task_ids='perform_basic_transformations')
        enhancement_results = context['task_instance'].xcom_pull(task_ids='perform_vllm_enhancement')

        final_content = enhancement_results.get('enhanced_content', basic_results['basic_transformed_content'])
        final_content, fence_fixed = normalize_code_fences(final_content)
        if fence_fixed:
            logger.info("🔧 Финальный контент дополнен закрывающей тройной кавычкой")
        original_content = transformation_session['document_data']['markdown_content']
        validate_markdown_structure(final_content, min_headings=max(1, transformation_session['document_data'].get('metadata', {}).get('sections_count', 1)))
        final_quality = calculate_final_quality(original_content, final_content, basic_results, enhancement_results)

        final_result: Dict[str, Any] = {
            'session_id': transformation_session['session_id'],
            'transformation_completed': True,
            'final_content': final_content,
            'final_quality_score': final_quality,
            'original_length': len(original_content),
            'final_length': len(final_content),
            'chinese_chars_final': count_chinese_characters(final_content),
            'technical_terms_final': count_preserved_terms(final_content),
            'basic_quality': basic_results['basic_quality_score'],
            'enhancement_used': enhancement_results.get('enhancement_successful', False),
            'enhancement_quality': enhancement_results.get('enhancement_quality', 0.0),
            'vllm_chunks_processed': enhancement_results.get('chunks_processed', 0),
            'total_processing_time': time.time() - start_time,
            'ready_for_stage3': final_quality >= 80.0,
        }

        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='finalize_transformation_results',
            processing_time=time.time() - start_time,
            success=True,
        )
        logger.info(f"🎯 Трансформация финализирована: итоговое качество {final_quality:.1f}%")
        return final_result
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='finalize_transformation_results',
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error(f"❌ Ошибка финализации результатов: {e}")
        raise


def calculate_final_quality(original: str, final: str, basic_results: Dict[str, Any], enhancement_results: Dict[str, Any]) -> float:
    try:
        base_quality = basic_results['basic_quality_score']
        enhancement_bonus = 0.0
        if enhancement_results.get('enhancement_successful'):
            enhancement_quality = enhancement_results.get('enhancement_quality', 0.0)
            enhancement_bonus = enhancement_quality * 20

        length_penalty = 0.0
        length_ratio = len(final) / max(len(original), 1)
        if length_ratio < 0.8:
            length_penalty = (0.8 - length_ratio) * 50

        final_quality = base_quality + enhancement_bonus - length_penalty
        return min(100.0, max(0.0, final_quality))
    except Exception:
        return basic_results.get('basic_quality_score', 75.0)


def save_transformed_content(**context) -> Dict[str, Any]:
    start_time = time.time()
    try:
        transformation_session = context['task_instance'].xcom_pull(task_ids='load_intermediate_data')
        final_results = context['task_instance'].xcom_pull(task_ids='finalize_transformation_results')
        original_config = transformation_session['original_config']

        timestamp = original_config.get('timestamp', int(time.time()))
        filename = original_config.get('filename', 'unknown.pdf')
        md_name = f"{timestamp}_{filename.replace('.pdf', '.md')}"

        final_content = final_results['final_content']
        final_quality = final_results['final_quality_score']

        output_dir_env = os.getenv('OUTPUT_FOLDER_ZH', '/app/output/zh')
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        fallback_dir = os.path.join(airflow_home, 'output', 'zh')
        output_dir = output_dir_env

        try:
            os.makedirs(output_dir, exist_ok=True)
        except PermissionError:
            logger.warning(f"Нет прав на создание {output_dir}, используем fallback: {fallback_dir}")
            os.makedirs(fallback_dir, exist_ok=True)
            output_dir = fallback_dir

        output_path = os.path.join(output_dir, md_name)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_content)

        stage3_config: Dict[str, Any] = {
            'markdown_file': output_path,
            'markdown_content': final_content,
            'original_config': original_config,
            'stage2_completed': True,
            'transformation_metadata': {
                'final_quality_score': final_quality,
                'basic_quality_score': final_results['basic_quality'],
                'enhancement_used': final_results['enhancement_used'],
                'enhancement_quality': final_results['enhancement_quality'],
                'vllm_chunks_processed': final_results['vllm_chunks_processed'],
                'chinese_chars_preserved': final_results['chinese_chars_final'],
                'technical_terms_preserved': final_results['technical_terms_final'],
                'total_processing_time': final_results['total_processing_time'],
                'completion_time': datetime.now().isoformat(),
            },
        }

        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='save_transformed_content',
            processing_time=time.time() - start_time,
            success=True,
        )
        logger.info(f"💾 Полностью трансформированный контент сохранен: {output_path}")
        return stage3_config
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='save_transformed_content',
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error(f"❌ Ошибка сохранения трансформированного контента: {e}")
        raise


def notify_transformation_completion(**context) -> None:
    try:
        stage3_config = context['task_instance'].xcom_pull(task_ids='save_transformed_content')
        transformation_metadata = stage3_config['transformation_metadata']

        final_quality = transformation_metadata['final_quality_score']
        basic_quality = transformation_metadata['basic_quality_score']
        enhancement_used = transformation_metadata['enhancement_used']
        vllm_chunks = transformation_metadata['vllm_chunks_processed']
        enhancement_status = "✅ vLLM Enhanced" if enhancement_used else "📝 Basic Only"

        message = f"""
✅ ПОЛНАЯ CONTENT TRANSFORMATION ЗАВЕРШЕНА
📄 Файл: {stage3_config['markdown_file']}
🎯 КАЧЕСТВО ТРАНСФОРМАЦИИ:
- Итоговое качество: {final_quality:.1f}%
- Базовое качество: {basic_quality:.1f}%
- Статус улучшения: {enhancement_status}
🤖 vLLM ИНТЕЛЛЕКТУАЛЬНОЕ УЛУЧШЕНИЕ:
- Обработано чанков: {vllm_chunks}
- Enhancement качество: {transformation_metadata['enhancement_quality']:.3f}
- Использовано: {'✅ Да' if enhancement_used else '❌ Нет'}
🈶 КИТАЙСКАЯ СПЕЦИАЛИЗАЦИЯ:
- Китайских символов сохранено: {transformation_metadata['chinese_chars_preserved']}
- Технических терминов: {transformation_metadata['technical_terms_preserved']}
📊 СТАТИСТИКА:
- Общее время обработки: {transformation_metadata['total_processing_time']:.1f} сек
✅ Готов к передаче на Stage 3 (Translation Pipeline)
"""
        logger.info(message)
        NotificationUtils.send_success_notification(context, stage3_config)
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления о трансформации: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ DAG
# ================================================================================

load_data = PythonOperator(
    task_id='load_intermediate_data',
    python_callable=load_intermediate_data,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

basic_transformations = PythonOperator(
    task_id='perform_basic_transformations',
    python_callable=perform_basic_transformations,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

vllm_enhancement = PythonOperator(
    task_id='perform_vllm_enhancement',
    python_callable=perform_vllm_enhancement,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

finalize_results = PythonOperator(
    task_id='finalize_transformation_results',
    python_callable=finalize_transformation_results,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

save_result = PythonOperator(
    task_id='save_transformed_content',
    python_callable=save_transformed_content,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

notify_completion = PythonOperator(
    task_id='notify_transformation_completion',
    python_callable=notify_transformation_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

load_data >> basic_transformations >> vllm_enhancement >> finalize_results >> save_result >> notify_completion
