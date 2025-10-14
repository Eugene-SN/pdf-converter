#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ Content Transformation - ПОЛНАЯ система трансформации
Исправления:
- Правильный парсинг OpenAI-совместимого ответа от vLLM (choices — список)
- Выход из ретраев после первого 200 OK и валидного парсинга
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
import random
import hashlib
from collections import OrderedDict
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional, Tuple
from requests.adapters import HTTPAdapter
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from translator.vllm_client import (
    build_vllm_headers,
    create_vllm_requests_session,
    get_vllm_api_key as get_vllm_env_api_key,
)

# ✅ logger до любых try/except
logger = logging.getLogger(__name__)

# Утилиты
from shared_utils import (
    SharedUtils, NotificationUtils,
    MetricsUtils, ErrorHandlingUtils,
    ConfigUtils,
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
    'model': os.getenv('VLLM_MODEL_NAME', 'Qwen/Qwen3-30B-A3B-Instruct-2507'),
    'timeout': int(os.getenv('VLLM_STANDARD_TIMEOUT', '150')),
    'max_tokens': 2048,
    'temperature': 0.3,
    'top_p': 0.9,
    'top_k': 40,
    'max_retries': 3,
    'retry_delay': 5,
    'retry_jitter': 0.35,
    'enable_fallback': True,
    'max_concurrent_requests': max(1, int(os.getenv('VLLM_MAX_CONCURRENT', '2'))),
}

# Функция расчета задержки с джиттером для ретраев vLLM
def _compute_vllm_retry_delay(multiplier: float = 1.0) -> float:
    base_delay = max(0.0, VLLM_CONFIG['retry_delay'] * multiplier)
    jitter_ratio = max(0.0, VLLM_CONFIG.get('retry_jitter', 0.0))
    if not jitter_ratio:
        return base_delay
    spread = base_delay * jitter_ratio
    return max(0.1, base_delay + random.uniform(-spread, spread))

# Семафор для ограничения параллелизма запросов к vLLM
_VLLM_SEMAPHORE = threading.Semaphore(VLLM_CONFIG.get('max_concurrent_requests', 1))

# Общая HTTP‑сессия с расширенным пулом соединений
_VLLM_HTTP_POOL = max(4, VLLM_CONFIG['max_concurrent_requests'] * 4)
_VLLM_API_KEY_LOCK = threading.Lock()
_VLLM_SESSION_LOCK = threading.Lock()
_VLLM_API_KEY: Optional[str] = None


def _resolve_vllm_api_key(*, force_refresh: bool = False) -> Optional[str]:
    """Получить текущий API-ключ vLLM с учётом кэша ConfigUtils."""

    global _VLLM_API_KEY
    with _VLLM_API_KEY_LOCK:
        key = ConfigUtils.get_vllm_api_key(refresh=force_refresh)
        if not key:
            key = get_vllm_env_api_key()
        _VLLM_API_KEY = key
        return _VLLM_API_KEY


def _create_vllm_session(api_key: Optional[str]) -> requests.Session:
    session = create_vllm_requests_session(api_key=api_key)
    adapter = HTTPAdapter(pool_connections=_VLLM_HTTP_POOL, pool_maxsize=_VLLM_HTTP_POOL)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def _reset_vllm_session(api_key: Optional[str]) -> None:
    global _VLLM_SESSION
    with _VLLM_SESSION_LOCK:
        try:
            if '_VLLM_SESSION' in globals() and _VLLM_SESSION:
                _VLLM_SESSION.close()
        except Exception:
            pass
        _VLLM_SESSION = _create_vllm_session(api_key)


def _build_vllm_request_headers(
    content_type: Optional[str] = None,
    *,
    force_refresh: bool = False,
) -> Dict[str, str]:
    api_key = _resolve_vllm_api_key(force_refresh=force_refresh)
    return build_vllm_headers(content_type, api_key=api_key)


_reset_vllm_session(_resolve_vllm_api_key())

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


TABLE_ROW_PATTERN = re.compile(r'^\s*\|.*\|\s*$')
TABLE_SEPARATOR_PATTERN = re.compile(r'^\s*\|(?:\s*:?-{3,}:?\s*\|)+\s*$')


def sanitize_table_cell(value: Any) -> str:
    if value is None:
        return ''
    text = str(value).strip()
    if not text:
        return ''
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    if '\n' in text:
        text = '<br>'.join(part.strip() for part in text.split('\n') if part.strip())
    return text


def split_table_cells(line: str) -> List[str]:
    stripped = (line or '').strip()
    if '|' not in stripped:
        return []
    return [cell.strip() for cell in stripped.strip('|').split('|')]


def is_table_separator(line: str) -> bool:
    return bool(TABLE_SEPARATOR_PATTERN.match(line or ''))


def is_markdown_table_row(line: str) -> bool:
    if not TABLE_ROW_PATTERN.match(line or ''):
        return False
    if is_table_separator(line):
        return False
    if (line or '').count('|') < 2:
        return False
    cells = split_table_cells(line)
    if len(cells) < 1:
        return False
    return any(cell for cell in cells)


def trim_trailing_empty_columns(matrix: List[List[str]]) -> List[List[str]]:
    if not matrix:
        return []
    col_count = len(matrix[0])
    last_nonempty = -1
    for col in range(col_count):
        if any((row[col] if col < len(row) else '').strip() for row in matrix):
            last_nonempty = col
    if last_nonempty == -1:
        return []
    trimmed: List[List[str]] = []
    for row in matrix:
        padded_row = list(row[:last_nonempty + 1])
        if len(padded_row) < last_nonempty + 1:
            padded_row.extend([''] * (last_nonempty + 1 - len(padded_row)))
        trimmed.append(padded_row)
    return trimmed


def normalize_table_matrix(matrix: List[List[Any]]) -> List[List[str]]:
    if not matrix:
        return []
    sanitized: List[List[str]] = []
    max_cols = 0
    for row in matrix:
        sanitized_row = [sanitize_table_cell(cell) for cell in row]
        sanitized.append(sanitized_row)
        max_cols = max(max_cols, len(sanitized_row))
    if max_cols == 0:
        return []
    for row in sanitized:
        if len(row) < max_cols:
            row.extend([''] * (max_cols - len(row)))
    sanitized = trim_trailing_empty_columns(sanitized)
    while sanitized and all(not cell.strip() for cell in sanitized[-1]):
        sanitized.pop()
    if not sanitized:
        return []
    if all(not cell.strip() for cell in sanitized[0]) and any(
        any(cell.strip() for cell in row) for row in sanitized[1:]
    ):
        for idx in range(1, len(sanitized)):
            if any(cell.strip() for cell in sanitized[idx]):
                sanitized[0], sanitized[idx] = sanitized[idx], sanitized[0]
                break
    return sanitized


def format_table_lines(matrix: List[List[str]]) -> List[str]:
    if not matrix:
        return []
    col_count = len(matrix[0])
    normalized_rows: List[List[str]] = []
    for row in matrix:
        normalized_rows.append([row[col] if col < len(row) else '' for col in range(col_count)])
    if not normalized_rows:
        return []
    header = normalized_rows[0]
    separator = '| ' + ' | '.join(['---'] * col_count) + ' |'
    body = normalized_rows[1:]
    lines: List[str] = ['| ' + ' | '.join(header) + ' |', separator]
    for row in body:
        lines.append('| ' + ' | '.join(row) + ' |')
    return lines


def count_markdown_table_rows(text: str) -> int:
    if not text:
        return 0
    rows = 0
    for line in text.splitlines():
        if is_markdown_table_row(line):
            rows += 1
    return rows


def _coerce_index(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def build_matrix_from_docling_table(table_payload: Dict[str, Any]) -> List[List[str]]:
    if not isinstance(table_payload, dict):
        return []
    cells = table_payload.get('table_cells') or table_payload.get('cells')
    if not isinstance(cells, list):
        return []
    num_rows = _coerce_index(table_payload.get('num_rows'), 0)
    num_cols = _coerce_index(table_payload.get('num_cols'), 0)
    max_row = num_rows - 1
    max_col = num_cols - 1
    processed_cells: List[Tuple[int, int, int, int, str]] = []
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        start_row = cell.get('start_row_offset_idx')
        if start_row is None:
            start_row = cell.get('row_index') or cell.get('row_idx')
        start_row = _coerce_index(start_row, 0)
        start_col = cell.get('start_col_offset_idx')
        if start_col is None:
            start_col = cell.get('col_index') or cell.get('col_idx')
        start_col = _coerce_index(start_col, 0)
        end_row = cell.get('end_row_offset_idx')
        if end_row is None:
            row_span = cell.get('row_span')
            if row_span is None:
                end_row = start_row
            else:
                end_row = start_row + max(_coerce_index(row_span, 1) - 1, 0)
        end_row = _coerce_index(end_row, start_row)
        end_row = max(end_row, start_row)
        end_col = cell.get('end_col_offset_idx')
        if end_col is None:
            col_span = cell.get('col_span')
            if col_span is None:
                end_col = start_col
            else:
                end_col = start_col + max(_coerce_index(col_span, 1) - 1, 0)
        end_col = _coerce_index(end_col, start_col)
        end_col = max(end_col, start_col)
        max_row = max(max_row, end_row)
        max_col = max(max_col, end_col)
        text = sanitize_table_cell(cell.get('text'))
        processed_cells.append((start_row, end_row, start_col, end_col, text))

    total_rows = max_row + 1
    total_cols = max_col + 1
    if total_rows <= 0 or total_cols <= 0:
        return []
    matrix: List[List[str]] = [['' for _ in range(total_cols)] for _ in range(total_rows)]
    for start_row, end_row, start_col, end_col, text in processed_cells:
        if not text:
            continue
        for row_idx in range(start_row, min(end_row + 1, total_rows)):
            for col_idx in range(start_col, min(end_col + 1, total_cols)):
                if matrix[row_idx][col_idx]:
                    if text not in matrix[row_idx][col_idx]:
                        matrix[row_idx][col_idx] = f"{matrix[row_idx][col_idx]} {text}".strip()
                else:
                    matrix[row_idx][col_idx] = text
    normalized = normalize_table_matrix(matrix)
    return normalized


def _normalize_table_block(table_lines: List[str]) -> List[str]:
    matrix: List[List[str]] = []
    for line in table_lines:
        if is_table_separator(line):
            continue
        cells = split_table_cells(line)
        if cells:
            matrix.append(cells)
    normalized = normalize_table_matrix(matrix)
    if not normalized:
        return [line.rstrip() for line in table_lines]
    return format_table_lines(normalized)


def render_table_markdown(table_entry: Dict[str, Any]) -> str:
    """Преобразует таблицу из payload в Markdown; при ошибке возвращает строку"""
    content = table_entry.get('content')
    if isinstance(content, str):
        stripped = content.strip()
        if is_markdown_table_row(stripped) or '\n' in stripped:
            normalized = _normalize_table_block(content.splitlines())
            return "\n".join(normalized)
        return stripped

    potential_payloads: List[Dict[str, Any]] = []
    seen_payloads: set[int] = set()

    def enqueue_payload(candidate: Any) -> None:
        if not isinstance(candidate, dict):
            return
        candidate_id = id(candidate)
        if candidate_id in seen_payloads:
            return
        seen_payloads.add(candidate_id)
        potential_payloads.append(candidate)

    enqueue_payload(table_entry)
    enqueue_payload(table_entry.get('data'))
    if isinstance(content, dict):
        enqueue_payload(content)
        enqueue_payload(content.get('data'))
    metadata = table_entry.get('metadata')
    if isinstance(metadata, dict):
        enqueue_payload(metadata)
        enqueue_payload(metadata.get('data'))
        enqueue_payload(metadata.get('table_data'))

    for payload in potential_payloads:
        if not isinstance(payload, dict):
            continue
        if payload.get('table_cells') or payload.get('cells'):
            matrix = build_matrix_from_docling_table(payload)
            if matrix:
                return "\n".join(format_table_lines(matrix))

    if isinstance(content, dict):
        table_payload = content if 'table_cells' in content else content.get('data')
        if isinstance(table_payload, dict) and table_payload.get('table_cells'):
            matrix = build_matrix_from_docling_table(table_payload)
            if matrix:
                lines = format_table_lines(matrix)
                return "\n".join(lines)
        data = content.get('data') or content.get('rows')
        if isinstance(data, list) and data:
            matrix: List[List[Any]] = []
            for row in data:
                if isinstance(row, list):
                    matrix.append(row)
                elif isinstance(row, dict):
                    ordered = [row[key] for key in sorted(row.keys())]
                    matrix.append(ordered)
            normalized = normalize_table_matrix(matrix)
            if normalized:
                return "\n".join(format_table_lines(normalized))

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
        table_buffer: List[str] = []
        for line in lines:
            if is_markdown_table_row(line) or is_table_separator(line):
                if is_table_separator(line):
                    continue
                table_buffer.append(line)
                continue
            if table_buffer:
                enhanced_lines.extend(_normalize_table_block(table_buffer))
                table_buffer = []
            enhanced_lines.append(line)
        if table_buffer:
            enhanced_lines.extend(_normalize_table_block(table_buffer))
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
    headers = _build_vllm_request_headers("application/json")
    headers.setdefault("Accept", "application/json")
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
        "stream": False,
        "task_type": "translation",
    }

    for attempt in range(VLLM_CONFIG['max_retries']):
        try:
            logger.info(f"vLLM API вызов (попытка {attempt + 1})")
            with _VLLM_SESSION_LOCK:
                session = _VLLM_SESSION
            response = session.post(
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
                        time.sleep(_compute_vllm_retry_delay())
                        continue
                    return None

                if content:
                    logger.info("✅ vLLM API успешен")
                    stripped = content.strip()
                    _cache_store(cache_key, stripped)
                    return stripped

                logger.warning("vLLM API 200, но пустой content")
                if attempt < VLLM_CONFIG['max_retries'] - 1:
                    time.sleep(_compute_vllm_retry_delay())
                    continue
                return None

            elif response.status_code == 500:
                logger.warning(f"vLLM API 500 (препроцессинг): {response.text[:200]}")
                if attempt < VLLM_CONFIG['max_retries'] - 1:
                    time.sleep(_compute_vllm_retry_delay(2))
                    continue
                logger.error("Все попытки vLLM неудачны (500 ошибка)")
                return None

            elif response.status_code == 401:
                preview = ''
                try:
                    preview = response.text[:200]
                except Exception:
                    preview = ''
                logger.error(f"vLLM API 401 Unauthorized: {preview}")
                refreshed_key = _resolve_vllm_api_key(force_refresh=True)
                _reset_vllm_session(refreshed_key)
                _build_vllm_request_headers(force_refresh=True)
                raise PermissionError("vLLM API unauthorized (401). Check API key configuration.")

            elif response.status_code in (429, 503):
                logger.warning(f"vLLM перегружен ({response.status_code}), повтор через задержку")
                if attempt < VLLM_CONFIG['max_retries'] - 1:
                    time.sleep(_compute_vllm_retry_delay(2))
                    continue
                return None

            else:
                logger.warning(f"vLLM API ошибка: {response.status_code} {response.text[:200]}")

        except PermissionError as auth_error:
            logger.error(f"Критическая ошибка авторизации vLLM: {auth_error}")
            raise
        except Exception as e:
            logger.warning(f"vLLM попытка {attempt + 1} неудачна: {e}")
            if attempt < VLLM_CONFIG['max_retries'] - 1:
                time.sleep(_compute_vllm_retry_delay())
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

        original_tables = count_markdown_table_rows(original)
        enhanced_tables = count_markdown_table_rows(enhanced)
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

        original_tables = count_markdown_table_rows(original)
        transformed_tables = count_markdown_table_rows(transformed)
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
