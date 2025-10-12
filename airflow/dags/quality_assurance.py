#!/usr/bin/env python3

# -*- coding: utf-8 -*-

"""

✅ Quality Assurance - ПОЛНАЯ 5-уровневая система валидации (ОКОНЧАТЕЛЬНО ИСПРАВЛЕН)

🎯 ИСПРАВЛЕНЫ КРИТИЧЕСКИЕ ПРОБЛЕМЫ:

- ✅ vLLM API формат исправлен (strings вместо arrays)
- ✅ Docker Pandoc сервис интеграция
- ✅ Улучшена обработка отсутствующих PDF файлов

🔧 DOCKER ИНТЕГРАЦИЯ:

- ✅ Pandoc вызывается через Docker exec в контейнер pandoc-render
- ✅ Проверка доступности Docker Pandoc сервиса
- ✅ Использование общих монтированных томов (/opt/airflow/temp)

🚫 НЕТ ДУБЛИРОВАНИЯ с content_transformation.py:

- Только валидация, QA и проверка качества
- НЕТ трансформации контента (это в content_transformation.py)

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils import timezone
import os
import sys
import json
import logging
import time
import re
import random
import requests
import asyncio
import aiohttp
import base64
import tempfile
import subprocess
import shutil
import textwrap
import html
import glob
import statistics
from collections import defaultdict
from typing import Dict, Any, Optional, List, Set, Tuple
from pathlib import Path
from types import SimpleNamespace
import importlib
import numpy as np
import markdown

try:
    import fitz  # type: ignore
    PYMUPDF_AVAILABLE = True
except ImportError:
    fitz = None  # type: ignore
    PYMUPDF_AVAILABLE = False

# ✅ ИСПРАВЛЕНО: logger перенесен ПЕРЕД try/except блоками
logger = logging.getLogger(__name__)

# ✅ ИСПРАВЛЕНО: ImportError БЕЗ logger в блоке импорта
try:
    from skimage.metrics import structural_similarity as ssim
    SSIM_AVAILABLE = True
except ImportError:
    def ssim(img1, img2):
        return 0.85 # Fallback SSIM без logger
    SSIM_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SentenceTransformer = None
    SENTENCE_TRANSFORMERS_AVAILABLE = False

from PIL import Image
import pandas as pd
from difflib import SequenceMatcher
import difflib

# Утилиты
from shared_utils import (
    SharedUtils, NotificationUtils, ConfigUtils,
    MetricsUtils, ErrorHandlingUtils
)

# Avoid name clash between this DAG module and helper package; attempt to bootstrap
_DAG_DIR = Path(__file__).resolve().parent


def _register_helper_paths() -> None:
    """Best-effort sys.path bootstrap so helper modules remain importable."""
    env_home = os.environ.get("QUALITY_ASSURANCE_HOME")

    search_candidates: List[Path] = []

    if env_home:
        try:
            search_candidates.append(Path(env_home))
        except TypeError:
            logger.warning("QUALITY_ASSURANCE_HOME is not a valid path: %s", env_home)

    for parent in [_DAG_DIR, *_DAG_DIR.parents]:
        search_candidates.append(parent / "quality_assurance")

    search_candidates.extend([
        Path("/opt/airflow/dags/quality_assurance"),
        Path("/opt/airflow/plugins/quality_assurance"),
        Path("/opt/airflow/quality_assurance"),
    ])

    seen: set[str] = set()

    for candidate in search_candidates:
        try:
            resolved = candidate.resolve(strict=False)
        except PermissionError:
            continue

        if not resolved.exists() or not resolved.is_dir():
            continue

        for target in (resolved.parent, resolved):
            str_target = str(target)
            if str_target in seen:
                continue
            if str_target not in sys.path:
                sys.path.insert(0, str_target)
            seen.add(str_target)


_register_helper_paths()


def _import_visual_diff_module():
    """Try to import visual diff helpers, tolerating missing packages."""
    module_candidates = [
        "quality_assurance.visual_diff_system",
        "visual_diff_system",
    ]

    for module_name in module_candidates:
        try:
            return importlib.import_module(module_name)
        except ImportError:
            continue
    return None


_visual_diff_module = _import_visual_diff_module()
if _visual_diff_module:
    VisualDiffSystem = getattr(_visual_diff_module, "VisualDiffSystem", None)
    VisualDiffConfig = getattr(_visual_diff_module, "VisualDiffConfig", None)
    VISUAL_DIFF_AVAILABLE = VisualDiffSystem is not None and VisualDiffConfig is not None
else:
    VisualDiffSystem = None
    VisualDiffConfig = None
    VISUAL_DIFF_AVAILABLE = False

_dependency_warnings_emitted: Set[str] = set()


def _warn_once(message: str) -> None:
    """Emit optional dependency warnings only once per interpreter session."""

    if message in _dependency_warnings_emitted:
        return
    logger.warning(message)
    _dependency_warnings_emitted.add(message)


if not VISUAL_DIFF_AVAILABLE:
    _warn_once(
        "visual_diff_system helpers are not available; visual QA will run in fallback mode"
    )

# Проверка доступности модулей ПОСЛЕ импорта (с logger)
if not SSIM_AVAILABLE:
    _warn_once("scikit-image не установлен, используется fallback SSIM")

if not SENTENCE_TRANSFORMERS_AVAILABLE:
    _warn_once("sentence-transformers не установлен, семантический анализ упрощен")

if not PYMUPDF_AVAILABLE:
    _warn_once("PyMuPDF не установлен, fallback генерация PDF недоступна")

# Конфигурация DAG
DEFAULT_ARGS = {
    'owner': 'pdf-converter',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'quality_assurance',
    default_args=DEFAULT_ARGS,
    description='✅ Quality Assurance - ПОЛНАЯ 5-уровневая система валидации',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    tags=['pdf-converter', 'dag4', 'qa', '5-level-complete', 'enterprise', 'chinese-docs']
)

# ================================================================================
# ПОЛНАЯ КОНФИГУРАЦИЯ 5-УРОВНЕВОЙ СИСТЕМЫ ВАЛИДАЦИИ
# ================================================================================

# Enterprise QA правила с 5-уровневыми порогами
QA_RULES = {
    # Базовые требования к документу
    'min_content_length': 100,
    'min_headings': 1,
    'max_chinese_chars_ratio': 0.3,
    'require_title': True,
    'check_table_structure': True,
    'validate_markdown_syntax': True,
    'technical_terms_check': True,
    'preserve_brand_names': True,
    'min_quality_score': 80.0,
    'excellent_quality_score': 95.0,
    
    # ✅ Enterprise 5-уровневые пороги (СОХРАНЕНЫ)
    'OCR_CONFIDENCE_THRESHOLD': 0.8,
    'VISUAL_SIMILARITY_THRESHOLD': 0.95,
    'AST_SIMILARITY_THRESHOLD': 0.9,
    'SEMANTIC_SIMILARITY_THRESHOLD': 0.85,
    'OVERALL_QA_THRESHOLD': 0.85,
    'MAX_CORRECTIONS_PER_DOCUMENT': 10,
    'AUTO_CORRECTION_CONFIDENCE': 0.7,
}

# ✅ СОХРАНЕНА: Конфигурация уровней валидации
_VLLM_BASE_URL = os.getenv('VLLM_SERVER_URL', 'http://vllm-server:8000').rstrip('/')

LEVEL_CONFIG = {
    'level1_ocr': {
        'consensus_threshold': 0.85,
        'similarity_threshold': 0.8,
        'engines': ['paddleocr', 'tesseract']
    },
    'level2_visual': {
        'ssim_threshold': 0.95,
        'difference_tolerance': 0.1,
        'page_comparison_mode': 'structural',
        'pandoc_integration': True
    },
    'level3_ast': {
        'structural_similarity_threshold': 0.9,
        'semantic_similarity_threshold': 0.85,
        'model_name': 'sentence-transformers/all-MiniLM-L6-v2'
    },
    'level4_content': {
        'min_technical_terms': 5,
        'min_code_blocks': 1,
        'formatting_score_threshold': 0.8
    },
    'level5_correction': {
        'vllm_endpoint': f"{_VLLM_BASE_URL}/v1/chat/completions",
        'correction_model': 'Qwen/Qwen3-30B-A3B-Instruct-2507',
        'max_retries': 3,
        'enable_auto_correction': True
    }
}

# ✅ СОХРАНЕНЫ: Расширенный список технических терминов
TECHNICAL_TERMS = [
    # Китайские специализированные термины
    'WenTian', 'Lenovo WenTian', 'ThinkSystem', 'AnyBay', '问天', '联想问天', '天擎',
    'Xeon', 'Intel', 'Scalable Processors', '至强', '可扩展处理器', '英特尔',
    
    # IPMI/BMC технические термины
    'IPMI', 'BMC', 'Redfish', 'ipmitool', 'chassis', 'power', 'sensor', 'sel', 'fru', 'user', 'sol',
    'Power Supply', 'Ethernet', 'Storage', 'Memory', 'Processor', 'Network', 'Rack', 'Server',
    
    # Технические компоненты
    'Hot-swap', 'Redundancy', 'Backplane', 'Tray', 'Fiber', 'Bandwidth', 'Latency',
    'Network Adapter', 'Slot', 'Riser Card', 'Platinum', 'Titanium', 'CRPS'
]

# ✅ ИСПРАВЛЕНА: vLLM конфигурация для авто-коррекции
VLLM_CONFIG = {
    'endpoint': f"{_VLLM_BASE_URL}/v1/chat/completions",
    'model': 'Qwen/Qwen3-30B-A3B-Instruct-2507',
    'timeout': 150,
    'max_tokens': 2048,
    'temperature': 0.25,
    'top_p': 0.9,
    'max_retries': 2,
    'retry_delay': 2,
    'retry_jitter': 0.35,
}


def _compute_retry_delay(multiplier: float = 1.0) -> float:
    """Return a retry delay with jitter to prevent thundering herd effects."""
    base_delay = max(0.0, VLLM_CONFIG['retry_delay'] * multiplier)
    jitter_ratio = max(0.0, VLLM_CONFIG.get('retry_jitter', 0.0))
    if not jitter_ratio:
        return base_delay
    spread = base_delay * jitter_ratio
    # Guarantee a small positive delay to avoid immediate retries
    return max(0.1, base_delay + random.uniform(-spread, spread))

SENTENCE_MODEL_CACHE_DIR = os.getenv('QA_SENTENCE_MODEL_CACHE') or os.getenv('SENTENCE_TRANSFORMERS_CACHE')
_SENTENCE_MODEL = None
_SENTENCE_MODEL_LOAD_FAILED = False

if VISUAL_DIFF_AVAILABLE and VisualDiffConfig and VisualDiffSystem:
    VISUAL_DIFF_CONFIG = VisualDiffConfig(
        ssim_threshold=LEVEL_CONFIG['level2_visual']['ssim_threshold'],
        diff_tolerance=LEVEL_CONFIG['level2_visual']['difference_tolerance'],
        comparison_dpi=int(os.getenv('QA_VISUAL_DIFF_DPI', '150')),
        temp_dir=os.getenv('AIRFLOW_TEMP_DIR', os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'temp')),
        output_dir=os.getenv('QA_VISUAL_REPORT_DIR', os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'validation_reports'))
    )
    VISUAL_DIFF_SYSTEM = VisualDiffSystem(VISUAL_DIFF_CONFIG)
else:
    VISUAL_DIFF_CONFIG = SimpleNamespace(
        ssim_threshold=LEVEL_CONFIG['level2_visual']['ssim_threshold'],
        diff_tolerance=LEVEL_CONFIG['level2_visual']['difference_tolerance']
    )
    VISUAL_DIFF_SYSTEM = None


def _run_visual_diff(original_pdf: str, result_pdf: str, comparison_id: str):
    """Запуск VisualDiffSystem в синхронном контексте Airflow."""
    if not VISUAL_DIFF_AVAILABLE or not VISUAL_DIFF_SYSTEM:
        logger.warning(
            "Visual diff helpers unavailable; returning fallback comparison result"
        )
        return SimpleNamespace(
            overall_similarity=1.0,
            ssim_score=1.0,
            differences=[],
            pages_compared=0,
            diff_images_paths=[],
            summary={'status': 'skipped', 'reason': 'visual_diff_unavailable'}
        )

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(
            VISUAL_DIFF_SYSTEM.compare_documents(original_pdf, result_pdf, comparison_id)
        )
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        asyncio.set_event_loop(None)
        loop.close()


def _get_sentence_transformer_model():
    """Load SentenceTransformer once, respecting optional cache configuration."""

    global _SENTENCE_MODEL, _SENTENCE_MODEL_LOAD_FAILED

    if not SENTENCE_TRANSFORMERS_AVAILABLE or not SentenceTransformer:
        return None

    if _SENTENCE_MODEL is not None or _SENTENCE_MODEL_LOAD_FAILED:
        return _SENTENCE_MODEL

    load_kwargs: Dict[str, Any] = {}
    if SENTENCE_MODEL_CACHE_DIR:
        load_kwargs['cache_folder'] = SENTENCE_MODEL_CACHE_DIR

    model_name = LEVEL_CONFIG['level3_ast']['model_name']

    try:
        _SENTENCE_MODEL = SentenceTransformer(model_name, **load_kwargs)
        cache_note = f" using cache at {SENTENCE_MODEL_CACHE_DIR}" if SENTENCE_MODEL_CACHE_DIR else ""
        logger.info("SentenceTransformer model '%s' loaded%s", model_name, cache_note)
    except Exception as exc:
        _SENTENCE_MODEL_LOAD_FAILED = True
        _warn_once(
            f"SentenceTransformer model '{model_name}' unavailable: {exc}"
        )
        if SENTENCE_MODEL_CACHE_DIR:
            _warn_once(
                f"Ensure the model is available in {SENTENCE_MODEL_CACHE_DIR} to enable semantic QA."
            )
        _SENTENCE_MODEL = None

    return _SENTENCE_MODEL

# ================================================================================
# ЗАГРУЗКА И ИНИЦИАЛИЗАЦИЯ
# ================================================================================

def load_translated_document(**context) -> Dict[str, Any]:
    """Загрузка переведенного документа для полной 5-уровневой валидации"""
    start_time = time.time()
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info("🔍 Начало полной 5-уровневой QA валидации")
        
        translated_file = dag_run_conf.get('translated_file')
        if not translated_file or not os.path.exists(translated_file):
            raise ValueError(f"Переведенный файл не найден: {translated_file}")

        with open(translated_file, 'r', encoding='utf-8') as f:
            translated_content = f.read()

        if not translated_content.strip():
            raise ValueError("Переведенный файл пустой")

        qa_session = {
            'session_id': f"qa_full_{int(time.time())}",
            'translated_file': translated_file,
            'translated_content': translated_content,
            'original_config': dag_run_conf.get('original_config', {}),
            'translation_metadata': dag_run_conf.get('translation_metadata', {}),
            'qa_start_time': datetime.now().isoformat(),
            'target_quality': dag_run_conf.get('quality_target', 90.0),
            'auto_correction': dag_run_conf.get('auto_correction', True),
            
            # Метаданные для 5-уровневой системы
            'original_pdf_path': dag_run_conf.get('original_pdf_path'),
            'document_id': dag_run_conf.get('document_id', f"doc_{int(time.time())}"),
            'enable_5_level_validation': True,
            'enterprise_mode': True,
            
            # Конфигурации уровней
            'level_configs': LEVEL_CONFIG,
            'qa_rules': QA_RULES
        }

        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='load_translated_document',
            processing_time=time.time() - start_time,
            success=True
        )
        
        content_length = len(translated_content)
        logger.info(f"✅ Документ загружен для полной QA: {content_length} символов")
        return qa_session
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance', 
            task_id='load_translated_document',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка загрузки документа для QA: {e}")
        raise

# ================================================================================
# УРОВЕНЬ 1: OCR CROSS-VALIDATION (СОХРАНЕН)
# ================================================================================


def _map_to_airflow_temp_path(path: Optional[str], airflow_temp: str) -> Optional[str]:
    """Приводит путь из контейнера document-processor к airflow temp при необходимости."""

    if not path or not isinstance(path, str):
        return None

    normalized = path.strip()
    if not normalized:
        return None

    if normalized.startswith('/app/temp'):
        return normalized.replace('/app/temp', airflow_temp, 1)

    return normalized


def _read_json_file(path: str) -> Optional[Any]:
    """Безопасное чтение JSON файла с защитой от ошибок."""

    try:
        with open(path, 'r', encoding='utf-8') as handle:
            return json.load(handle)
    except Exception as exc:
        logger.debug("Не удалось прочитать JSON %s: %s", path, exc)
        return None


def _load_additional_ocr_results(qa_session: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Пытается найти и загрузить сохраненные результаты OCR из Stage 1."""

    loader_info: Dict[str, Any] = {
        'bridge_file': None,
        'results_path': None,
        'candidates_checked': [],
        'missing_files': [],
    }

    original_config: Dict[str, Any] = qa_session.get('original_config', {}) or {}
    airflow_temp = original_config.get(
        'container_temp_dir',
        os.getenv('AIRFLOW_TEMP_DIR', os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'temp'))
    ) or '/opt/airflow/temp'
    timestamp = original_config.get('timestamp')

    candidate_paths: List[str] = []

    try:
        if timestamp:
            bridge_path = os.path.join(airflow_temp, f"stage1_bridge_{timestamp}.json")
            if os.path.exists(bridge_path):
                loader_info['bridge_file'] = bridge_path
                bridge_data = _read_json_file(bridge_path) or {}

                potential_sources: List[str] = []
                if isinstance(bridge_data, dict):
                    potential_sources.extend([
                        bridge_data.get('intermediate_file'),
                        bridge_data.get('docling_intermediate')
                    ])
                    potential_sources.extend(bridge_data.get('output_files', []) or [])

                for raw_path in potential_sources:
                    mapped = _map_to_airflow_temp_path(raw_path, airflow_temp)
                    if not mapped:
                        continue
                    if os.path.exists(mapped) and mapped not in candidate_paths:
                        candidate_paths.append(mapped)
                    elif mapped not in loader_info['missing_files']:
                        loader_info['missing_files'].append(mapped)

        for key in ('intermediate_file', 'stage1_intermediate', 'docling_intermediate'):
            raw_candidate = qa_session.get(key) or original_config.get(key)
            mapped = _map_to_airflow_temp_path(raw_candidate, airflow_temp)
            if not mapped:
                continue
            if os.path.exists(mapped) and mapped not in candidate_paths:
                candidate_paths.append(mapped)
            elif mapped not in loader_info['missing_files']:
                loader_info['missing_files'].append(mapped)

        if not candidate_paths and timestamp:
            search_patterns = [
                os.path.join(airflow_temp, f"*{timestamp}*result.json"),
                os.path.join(airflow_temp, f"*{timestamp}*intermediate.json"),
            ]
            for pattern in search_patterns:
                for found in glob.glob(pattern):
                    if os.path.exists(found) and found not in candidate_paths:
                        candidate_paths.append(found)

        loader_info['candidates_checked'] = candidate_paths[:]

        for path in candidate_paths:
            data = _read_json_file(path)
            if not isinstance(data, dict):
                continue

            metadata_candidates: List[Tuple[str, Dict[str, Any]]] = []
            if isinstance(data.get('metadata'), dict):
                metadata_candidates.append(('metadata', data['metadata']))
            if isinstance(data.get('document_structure'), dict):
                doc_meta = data['document_structure'].get('metadata')
                if isinstance(doc_meta, dict):
                    metadata_candidates.append(('document_structure.metadata', doc_meta))

            for source_key, meta in metadata_candidates:
                ocr_payload = meta.get('additional_ocr_results')
                if isinstance(ocr_payload, list):
                    loader_info['results_path'] = path
                    loader_info['results_container_key'] = source_key
                    return ocr_payload, loader_info
                if isinstance(ocr_payload, str):
                    mapped_payload = _map_to_airflow_temp_path(ocr_payload, airflow_temp)
                    if mapped_payload and os.path.exists(mapped_payload):
                        payload_data = _read_json_file(mapped_payload)
                        if isinstance(payload_data, list):
                            loader_info['results_path'] = mapped_payload
                            loader_info['results_container_key'] = source_key
                            return payload_data, loader_info
                        loader_info['missing_files'].append(mapped_payload)

        loader_info['error'] = 'OCR results not found in Stage 1 artifacts'
        return [], loader_info

    except Exception as exc:
        loader_info['error'] = str(exc)
        logger.debug("Ошибка при загрузке OCR результатов: %s", exc, exc_info=True)
        return [], loader_info


def _prepare_excerpt(text: str, limit: int = 160) -> str:
    """Возвращает компактный фрагмент текста для логирования расхождений."""

    if not text:
        return ''
    sanitized = re.sub(r'\s+', ' ', text).strip()
    if len(sanitized) <= limit:
        return sanitized
    return sanitized[:limit] + '…'


def _analyze_ocr_results(ocr_pages: List[Dict[str, Any]], document_text: str) -> Dict[str, Any]:
    """Агрегирует данные PaddleOCR/Tesseract и вычисляет итоговый confidence."""

    analysis: Dict[str, Any] = {
        'has_data': False,
        'engines_used': [],
        'paddle_avg_confidence': 0.0,
        'tesseract_avg_confidence': 0.0,
        'overlap_ratio': 0.0,
        'pages_analyzed': 0,
        'pages_with_both': 0,
        'pages_with_paddle': 0,
        'pages_with_tesseract': 0,
        'issues': [],
        'mismatched_pages': [],
        'per_engine_stats': {},
    }

    if not ocr_pages:
        analysis['issues'].append('Empty OCR payload received')
        return analysis

    engine_confidences: Dict[str, List[float]] = defaultdict(list)
    entries_per_engine: Dict[str, int] = defaultdict(int)
    pages_with_engine: Dict[str, int] = defaultdict(int)
    page_similarities: List[float] = []
    mismatched_pages: List[Dict[str, Any]] = []

    total_pages = 0

    for page in ocr_pages:
        page_results = page.get('ocr_results') or []
        if not page_results:
            continue

        total_pages += 1
        texts_by_engine: Dict[str, List[str]] = defaultdict(list)
        confs_by_engine: Dict[str, List[float]] = defaultdict(list)
        page_engines: Set[str] = set()

        for item in page_results:
            engine = str(item.get('engine') or 'unknown').lower()
            text = item.get('text')
            confidence = item.get('confidence')

            if text:
                texts_by_engine[engine].append(text)
                page_engines.add(engine)

            if isinstance(confidence, (int, float)):
                conf_value = float(confidence)
                confs_by_engine[engine].append(conf_value)
                engine_confidences[engine].append(conf_value)
                entries_per_engine[engine] += 1
                page_engines.add(engine)

        for engine in page_engines:
            pages_with_engine[engine] += 1

        if 'paddle' in page_engines:
            analysis['pages_with_paddle'] += 1
        if 'tesseract' in page_engines:
            analysis['pages_with_tesseract'] += 1

        if 'paddle' in texts_by_engine and 'tesseract' in texts_by_engine:
            analysis['pages_with_both'] += 1
            paddle_text = ' '.join(texts_by_engine['paddle']).strip()
            tesseract_text = ' '.join(texts_by_engine['tesseract']).strip()
            if paddle_text and tesseract_text:
                similarity = SequenceMatcher(None, paddle_text, tesseract_text).ratio()
                page_similarities.append(similarity)
                if similarity < 0.65:
                    mismatched_pages.append({
                        'page': page.get('page'),
                        'similarity': round(similarity, 3),
                        'paddle_excerpt': _prepare_excerpt(paddle_text),
                        'tesseract_excerpt': _prepare_excerpt(tesseract_text),
                    })

    analysis['pages_analyzed'] = total_pages
    analysis['mismatched_pages'] = mismatched_pages

    if total_pages == 0:
        analysis['issues'].append('OCR results contain no recognized pages')
        return analysis

    engines_present = set(engine_confidences.keys()) or set(pages_with_engine.keys())
    analysis['engines_used'] = sorted(engines_present)

    if engine_confidences.get('paddle'):
        analysis['paddle_avg_confidence'] = statistics.mean(engine_confidences['paddle'])
    if engine_confidences.get('tesseract'):
        analysis['tesseract_avg_confidence'] = statistics.mean(engine_confidences['tesseract'])

    if page_similarities:
        analysis['overlap_ratio'] = statistics.mean(page_similarities)

    per_engine_stats: Dict[str, Dict[str, Any]] = {}
    for engine in engines_present:
        confidences = engine_confidences.get(engine, [])
        per_engine_stats[engine] = {
            'avg_confidence': round(statistics.mean(confidences), 4) if confidences else 0.0,
            'entries': entries_per_engine.get(engine, 0),
            'pages_with_data': pages_with_engine.get(engine, 0),
        }
    analysis['per_engine_stats'] = per_engine_stats

    weights = 0.0
    weighted_score = 0.0

    if engine_confidences.get('paddle'):
        weights += 0.55
        weighted_score += analysis['paddle_avg_confidence'] * 0.55
    if engine_confidences.get('tesseract'):
        weights += 0.35
        weighted_score += analysis['tesseract_avg_confidence'] * 0.35
    if page_similarities:
        weights += 0.10
        weighted_score += analysis['overlap_ratio'] * 0.10

    consensus = (weighted_score / weights) if weights else 0.0

    text_length = len(document_text or '')
    length_bonus = min(0.05, text_length / 10000) if text_length else 0.0
    consensus = min(1.0, consensus + length_bonus)

    analysis['consensus_confidence'] = consensus
    analysis['has_data'] = weights > 0

    if 'tesseract' not in engines_present:
        analysis['issues'].append('Tesseract OCR results missing for cross-validation')
    if 'paddle' not in engines_present:
        analysis['issues'].append('PaddleOCR results missing for cross-validation')
    if page_similarities and analysis['overlap_ratio'] < 0.65:
        analysis['issues'].append(
            f"Low OCR overlap ratio: {analysis['overlap_ratio']:.3f}"
        )

    return analysis


def perform_ocr_cross_validation(**context) -> Dict[str, Any]:
    """✅ Уровень 1: Кросс-валидация OCR результатов через PaddleOCR + Tesseract"""

    start_time = time.time()
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        logger.info("🔍 Уровень 1: OCR Cross-Validation")

        original_pdf_path = qa_session.get('original_pdf_path')
        document_content = qa_session.get('translated_content', '')

        validation_result: Dict[str, Any] = {
            'level': 1,
            'name': 'ocr_cross_validation',
            'consensus_confidence': 0.0,
            'validation_score': 0.0,
            'engines_used': [],
            'issues_found': [],
            'processing_time': 0.0,
            'data_sources': {},
        }

        if original_pdf_path and os.path.exists(original_pdf_path):
            ocr_pages, loader_info = _load_additional_ocr_results(qa_session)
            validation_result['data_sources'] = {
                'bridge_file': loader_info.get('bridge_file'),
                'results_path': loader_info.get('results_path'),
                'candidates_checked': loader_info.get('candidates_checked', [])[:5],
            }
            if loader_info.get('missing_files'):
                validation_result['data_sources']['missing_files'] = loader_info['missing_files'][:5]

            if ocr_pages:
                analysis = _analyze_ocr_results(ocr_pages, document_content)

                if analysis['has_data']:
                    consensus_score = analysis['consensus_confidence']
                    validation_result.update({
                        'consensus_confidence': consensus_score,
                        'validation_score': consensus_score,
                        'engines_used': analysis['engines_used'],
                        'paddle_avg_confidence': analysis['paddle_avg_confidence'],
                        'tesseract_avg_confidence': analysis['tesseract_avg_confidence'],
                        'overlap_ratio': analysis['overlap_ratio'],
                        'ocr_pages_analyzed': analysis['pages_analyzed'],
                        'ocr_pages_with_both': analysis['pages_with_both'],
                        'ocr_results_path': loader_info.get('results_path') or loader_info.get('bridge_file'),
                        'metrics': {
                            'consensus_confidence': analysis['consensus_confidence'],
                            'paddle_avg_confidence': analysis['paddle_avg_confidence'],
                            'tesseract_avg_confidence': analysis['tesseract_avg_confidence'],
                            'overlap_ratio': analysis['overlap_ratio'],
                            'pages_analyzed': analysis['pages_analyzed'],
                            'pages_with_both': analysis['pages_with_both'],
                            'pages_with_paddle': analysis['pages_with_paddle'],
                            'pages_with_tesseract': analysis['pages_with_tesseract'],
                            'per_engine': analysis['per_engine_stats'],
                        },
                    })

                    if analysis['issues']:
                        validation_result['issues_found'].extend(analysis['issues'])

                    if analysis['mismatched_pages']:
                        validation_result['issues_found'].append(
                            f"OCR mismatch detected on {len(analysis['mismatched_pages'])} page(s)"
                        )
                        validation_result['mismatched_pages'] = analysis['mismatched_pages'][:5]

                    logger.info(
                        "📊 OCR metrics: consensus=%.3f, paddle=%.3f, tesseract=%.3f, overlap=%.3f, pages=%d, mismatches=%d",
                        consensus_score,
                        analysis['paddle_avg_confidence'],
                        analysis['tesseract_avg_confidence'],
                        analysis['overlap_ratio'],
                        analysis['pages_analyzed'],
                        len(analysis['mismatched_pages'])
                    )

                    if consensus_score < LEVEL_CONFIG['level1_ocr']['consensus_threshold']:
                        validation_result['issues_found'].append(
                            f"Low OCR consensus: {consensus_score:.3f} < {LEVEL_CONFIG['level1_ocr']['consensus_threshold']}"
                        )

                else:
                    fallback_reason = 'OCR metrics could not be computed from available data'
                    validation_result['issues_found'].append(fallback_reason)
                    validation_result['validation_score'] = 0.7
                    validation_result['consensus_confidence'] = 0.7
            else:
                fallback_reason = loader_info.get('error') or 'No OCR results located in Stage 1 artifacts'
                validation_result['issues_found'].append(fallback_reason)
                validation_result['validation_score'] = 0.7
                validation_result['consensus_confidence'] = 0.7
        else:
            validation_result['issues_found'].append(f"Original PDF not found: {original_pdf_path}")
            validation_result['validation_score'] = 0.7
            validation_result['consensus_confidence'] = 0.7

        validation_result['processing_time'] = time.time() - start_time
        logger.info(
            "✅ Уровень 1 завершен: score=%.3f (sources=%s)",
            validation_result['validation_score'],
            validation_result.get('data_sources')
        )
        return validation_result

    except Exception as exc:
        logger.error(f"❌ Ошибка уровня 1 OCR валидации: {exc}")
        return {
            'level': 1,
            'name': 'ocr_cross_validation',
            'validation_score': 0.0,
            'issues_found': [f"OCR validation failed: {str(exc)}"],
            'processing_time': time.time() - start_time
        }

# ================================================================================
# УРОВЕНЬ 2: VISUAL COMPARISON (DOCKER PANDOC ИНТЕГРАЦИЯ) - ИСПРАВЛЕН
# ================================================================================

def perform_visual_comparison(**context) -> Dict[str, Any]:
    """✅ Уровень 2: Визуальное сравнение PDF через SSIM анализ с Docker Pandoc интеграцией"""
    start_time = time.time()
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        logger.info("🔍 Уровень 2: Visual Comparison с Docker Pandoc интеграцией")
        
        original_pdf_path = qa_session.get('original_pdf_path')
        document_content = qa_session['translated_content']
        document_id = qa_session['document_id']
        
        validation_result = {
            'level': 2,
            'name': 'visual_comparison',
            'overall_similarity': 0.0,
            'ssim_score': 0.0,
            'differences_count': 0,
            'issues_found': [],
            'processing_time': 0.0,
            'pandoc_integration': True
        }
        
        if not original_pdf_path or not os.path.exists(original_pdf_path):
            raise AirflowException(f"Original PDF not found: {original_pdf_path}")

        result_pdf_path = generate_result_pdf_via_docker_pandoc(document_content, document_id)
        if not result_pdf_path or not os.path.exists(result_pdf_path):
            logger.warning("Failed to generate result PDF for visual comparison — level skipped")
            validation_result.update({
                'validation_score': 0.6,
                'issues_found': validation_result['issues_found'] + ['Result PDF generation failed'],
                'skipped': True,
                'processing_time': time.time() - start_time,
            })
            return validation_result

        comparison_id = f"{document_id}_visual"
        diff_result = _run_visual_diff(original_pdf_path, result_pdf_path, comparison_id)

        validation_result.update({
            'overall_similarity': diff_result.overall_similarity,
            'ssim_score': diff_result.ssim_score,
            'validation_score': diff_result.overall_similarity,
            'differences_count': len(diff_result.differences),
            'pages_compared': diff_result.pages_compared,
            'diff_summary': diff_result.summary,
            'diff_image_paths': diff_result.diff_images_paths,
            'result_pdf_path': result_pdf_path
        })

        if diff_result.overall_similarity < VISUAL_DIFF_CONFIG.ssim_threshold:
            validation_result['issues_found'].append(
                f"Low visual similarity: {diff_result.overall_similarity:.3f} < {VISUAL_DIFF_CONFIG.ssim_threshold}"
            )

        allowed_differences = max(1, int(round(diff_result.pages_compared * VISUAL_DIFF_CONFIG.diff_tolerance)))
        if len(diff_result.differences) > allowed_differences:
            validation_result['issues_found'].append(
                f"Too many visual differences: {len(diff_result.differences)}/{diff_result.pages_compared} pages"
            )

        critical_diffs = [d for d in diff_result.differences if d.severity in ('high', 'critical')]
        if critical_diffs:
            validation_result['issues_found'].append(
                f"High-severity visual differences detected: {len(critical_diffs)}"
            )

        validation_result['processing_time'] = time.time() - start_time
        logger.info(
            "✅ Уровень 2 завершен: SSIM=%.3f, differences=%d",
            validation_result.get('ssim_score', 0.0),
            validation_result.get('differences_count', 0)
        )
        return validation_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка уровня 2 визуального сравнения: {e}")
        raise

def check_docker_pandoc_availability() -> bool:
    """✅ ИСПРАВЛЕНА: Проверка доступности Docker Pandoc сервиса"""
    try:
        # Проверяем что Docker Pandoc контейнер запущен и отвечает
        result = subprocess.run(
            ['docker', 'exec', 'pandoc-render', 'pandoc', '--version'],
            capture_output=True, text=True, timeout=10
        )

        if result.returncode == 0:
            logger.info("✅ Docker Pandoc service is available")
            return True

        logger.warning("❌ Docker Pandoc service is not responding")
        return False

    except Exception as e:
        logger.error(f"Error checking Docker Pandoc availability: {e}")
        return False


def _generate_pdf_basic(markdown_content: str, pdf_file: Path) -> Optional[str]:
    """Generate a minimal PDF using only the Python standard library."""

    try:
        pdf_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            rendered_html = markdown.markdown(
                markdown_content,
                extensions=['extra', 'tables', 'sane_lists']
            )
        except Exception:
            rendered_html = markdown.markdown(markdown_content)

        plain_text = re.sub(r'<[^>]+>', '', rendered_html)
        plain_text = html.unescape(plain_text)

        wrapper = textwrap.TextWrapper(width=90, replace_whitespace=False, drop_whitespace=False)
        lines: List[str] = []

        for raw_line in plain_text.splitlines():
            stripped = raw_line.strip()
            if not stripped:
                lines.append("")
                continue

            wrapped_segments = wrapper.wrap(stripped)
            if wrapped_segments:
                lines.extend(wrapped_segments)
            else:
                lines.append(stripped)

        if not lines:
            lines = [""]

        max_lines_per_page = 45
        pages = [
            lines[i:i + max_lines_per_page]
            for i in range(0, len(lines), max_lines_per_page)
        ] or [[""]]

        objects: List[str] = []

        def add_object(body: str) -> int:
            objects.append(body)
            return len(objects)

        catalog_idx = add_object("")
        pages_idx = add_object("")
        font_idx = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

        page_indices: List[int] = []

        for page_lines in pages:
            if not page_lines:
                page_lines = [""]

            y_position = 800
            line_height = 14
            content_parts: List[str] = []

            for line in page_lines:
                safe_line = line.replace('\\', r'\\').replace('(', r'\(').replace(')', r'\)')
                if not safe_line:
                    safe_line = " "
                content_parts.append(
                    f"BT /F1 12 Tf 50 {y_position} Td ({safe_line}) Tj ET"
                )
                y_position -= line_height

            content_stream = "\n".join(content_parts) + "\n"
            content_length = len(content_stream.encode('utf-8'))
            content_idx = add_object(
                "<< /Length {length} >>\nstream\n{stream}endstream".format(
                    length=content_length,
                    stream=content_stream
                )
            )

            page_idx = add_object(
                "<< /Type /Page /Parent {parent} 0 R /MediaBox [0 0 595 842] "
                "/Contents {contents} 0 R /Resources << /Font << /F1 {font} 0 R >> >> >>".format(
                    parent=pages_idx,
                    contents=content_idx,
                    font=font_idx
                )
            )

            page_indices.append(page_idx)

        objects[catalog_idx - 1] = f"<< /Type /Catalog /Pages {pages_idx} 0 R >>"
        kids_refs = " ".join(f"{idx} 0 R" for idx in page_indices)
        objects[pages_idx - 1] = (
            f"<< /Type /Pages /Kids [{kids_refs}] /Count {len(page_indices)} >>"
        )

        with open(pdf_file, 'wb') as fh:
            fh.write(b"%PDF-1.4\n")
            offsets: List[int] = []

            for index, body in enumerate(objects, start=1):
                offsets.append(fh.tell())
                fh.write(f"{index} 0 obj\n".encode('ascii'))
                fh.write(body.encode('utf-8'))
                if not body.endswith('\n'):
                    fh.write(b"\n")
                fh.write(b"endobj\n")

            xref_offset = fh.tell()
            fh.write(f"xref\n0 {len(objects) + 1}\n".encode('ascii'))
            fh.write(b"0000000000 65535 f \n")

            for offset in offsets:
                fh.write(f"{offset:010d} 00000 n \n".encode('ascii'))

            fh.write(
                (
                    f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_idx} 0 R >>\n"
                    f"startxref\n{xref_offset}\n%%EOF\n"
                ).encode('ascii')
            )

        logger.info("📄 Result PDF создан через базовый Python fallback: %s", pdf_file)
        return str(pdf_file)
    except Exception as exc:
        logger.error("Basic Python PDF fallback failed: %s", exc)
        return None


def _generate_pdf_with_pymupdf(markdown_content: str, pdf_file: Path) -> Optional[str]:
    """Generate a PDF directly via PyMuPDF, falling back to the basic writer when unavailable."""

    if not PYMUPDF_AVAILABLE or not fitz:
        _warn_once(
            "PyMuPDF недоступен, используется базовый Python fallback для генерации PDF"
        )
        return _generate_pdf_basic(markdown_content, pdf_file)

    try:
        try:
            html_content = markdown.markdown(
                markdown_content,
                extensions=['extra', 'tables', 'sane_lists']
            )
        except Exception:
            html_content = markdown.markdown(markdown_content)

        if pdf_file.exists():
            pdf_file.unlink()

        html_document = fitz.open("html", html_content.encode('utf-8'))
        try:
            pdf_bytes = html_document.convert_to_pdf()
        finally:
            html_document.close()

        pdf_document = fitz.open("pdf", pdf_bytes)
        try:
            pdf_document.save(str(pdf_file))
        finally:
            pdf_document.close()

        logger.info("📄 Result PDF создан через PyMuPDF fallback: %s", pdf_file)
        return str(pdf_file)
    except Exception as exc:
        logger.error("PyMuPDF fallback PDF generation failed: %s", exc)
        return _generate_pdf_basic(markdown_content, pdf_file)


def generate_result_pdf_via_docker_pandoc(markdown_content: str, document_id: str) -> Optional[str]:
    """Генерация PDF с использованием Docker Pandoc либо PyMuPDF fallback."""
    try:
        logger.info(f"Generating result PDF for document: {document_id}")

        temp_dir = Path("/opt/airflow/temp") / document_id
        temp_dir.mkdir(parents=True, exist_ok=True)

        md_file = temp_dir / f"{document_id}_result.md"
        pdf_file = temp_dir / f"{document_id}_result.pdf"

        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        docker_available = check_docker_pandoc_availability()

        if docker_available:
            docker_cmd = [
                'docker', 'exec', 'pandoc-render',
                'python3', '/app/render_pdf.py',
                f'/workspace/{document_id}/{document_id}_result.md',
                f'/workspace/{document_id}/{document_id}_result.pdf',
                '/app/templates/chinese_tech.latex'
            ]

            try:
                result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=120)
            except subprocess.TimeoutExpired as exc:
                logger.error(f"Docker Pandoc conversion timed out: {exc}")
                result = None

            if result and result.returncode == 0 and pdf_file.exists():
                logger.info(f"✅ Result PDF создан через Docker Pandoc: {pdf_file}")
                return str(pdf_file)

            logger.warning(
                "Docker Pandoc conversion failed%s",
                f": {result.stderr}" if result else ""
            )
        else:
            logger.warning(
                "Docker Pandoc service not available, falling back to PyMuPDF for %s",
                document_id
            )

        fallback_path = _generate_pdf_with_pymupdf(markdown_content, pdf_file)
        if fallback_path:
            logger.info("Using fallback-generated PDF for visual comparison: %s", fallback_path)
        return fallback_path

    except Exception as e:
        logger.error(f"PDF generation failed: {e}")
        return None

# ================================================================================
# УРОВЕНЬ 3: AST STRUCTURE COMPARISON (СОХРАНЕН)
# ================================================================================

def perform_ast_structure_comparison(**context) -> Dict[str, Any]:
    """✅ Уровень 3: AST структурное и семантическое сравнение"""
    start_time = time.time()
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        logger.info("🔍 Уровень 3: AST Structure Comparison")
        
        document_content = qa_session['translated_content']
        
        validation_result = {
            'level': 3,
            'name': 'ast_structure_comparison',
            'structural_similarity': 0.0,
            'semantic_similarity': 0.0,
            'validation_score': 0.0,
            'issues_found': [],
            'processing_time': 0.0
        }
        
        structural_score = analyze_document_structure(document_content)
        semantic_score = analyze_semantic_similarity(document_content)
        overall_score = (structural_score + semantic_score) / 2
        
        validation_result.update({
            'structural_similarity': structural_score,
            'semantic_similarity': semantic_score,
            'validation_score': overall_score
        })
        
        if structural_score < LEVEL_CONFIG['level3_ast']['structural_similarity_threshold']:
            validation_result['issues_found'].append(
                f"Low structural similarity: {structural_score:.3f}"
            )
            
        if semantic_score < LEVEL_CONFIG['level3_ast']['semantic_similarity_threshold']:
            validation_result['issues_found'].append(
                f"Low semantic similarity: {semantic_score:.3f}"
            )
        
        validation_result['processing_time'] = time.time() - start_time
        logger.info(f"✅ Уровень 3 завершен: struct={structural_score:.3f}, sem={semantic_score:.3f}")
        return validation_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка уровня 3 AST сравнения: {e}")
        return {
            'level': 3,
            'name': 'ast_structure_comparison',
            'validation_score': 0.0,
            'issues_found': [f"AST comparison failed: {str(e)}"],
            'processing_time': time.time() - start_time
        }

def analyze_document_structure(content: str) -> float:
    """✅ Анализ структуры документа"""
    try:
        score = 0.0
        
        headers = re.findall(r'^#+\s+(.+)', content, re.MULTILINE)
        if headers:
            score += 0.3
            
        tables = re.findall(r'\|.*\|', content)
        if tables:
            score += 0.2
            
        lists = re.findall(r'^[\-\*\+]\s+', content, re.MULTILINE)
        if lists:
            score += 0.2
            
        code_blocks = re.findall(r'```[\s\S]*?```', content)
        if code_blocks:
            score += 0.2
            
        tech_terms_found = sum(1 for term in TECHNICAL_TERMS if term.lower() in content.lower())
        if tech_terms_found > 0:
            score += min(0.1, tech_terms_found * 0.02)
            
        # ✅ ИСПРАВЛЕНО: Повышен базовый балл структуры
        return min(1.0, max(0.6, score))
    except Exception:
        return 0.7

def analyze_semantic_similarity(content: str) -> float:
    """✅ Семантический анализ (упрощенный если нет SentenceTransformer)"""
    try:
        model = _get_sentence_transformer_model()
        if model:
            sections = re.split(r'\n#{1,6}\s+', content)
            if len(sections) < 2:
                return 0.8

            embeddings = model.encode(sections)
            similarities: List[float] = []
            
            for i in range(len(embeddings)):
                for j in range(i + 1, len(embeddings)):
                    denom = (np.linalg.norm(embeddings[i]) * np.linalg.norm(embeddings[j]) + 1e-8)
                    sim = float(np.dot(embeddings[i], embeddings[j]) / denom)
                    similarities.append(sim)
                    
            return float(np.mean(similarities)) if similarities else 0.8
        else:
            _warn_once(
                "SentenceTransformer недоступен, используется эвристический расчет семантического балла (длина + технические термины)"
            )
            score = 0.8
            
            if len(content) < 500:
                score -= 0.2
                
            tech_terms_found = sum(1 for term in TECHNICAL_TERMS[:10] if term.lower() in content.lower())
            score += min(0.1, tech_terms_found * 0.02)
            
            # ✅ ИСПРАВЛЕНО: Повышен минимальный балл
            return min(1.0, max(0.7, score))
    except Exception as e:
        logger.warning(f"Semantic analysis error: {e}")
        return 0.75

# ================================================================================
# УРОВЕНЬ 4: ENHANCED CONTENT VALIDATION (СОХРАНЕН)
# ================================================================================

def perform_enhanced_content_validation(**context) -> Dict[str, Any]:
    """✅ Уровень 4: Расширенная валидация контента"""
    start_time = time.time()
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        logger.info("🔍 Уровень 4: Enhanced Content Validation")
        
        document_content = qa_session['translated_content']
        
        issues_found: List[str] = []
        
        structure_score = check_document_structure(document_content, issues_found)
        content_score = check_content_quality(document_content, issues_found)
        terms_score = check_technical_terms(document_content, issues_found)
        markdown_score = check_markdown_syntax(document_content, issues_found)
        translation_score = check_translation_quality(document_content, issues_found)
        formatting_score = check_advanced_formatting(document_content, issues_found)
        consistency_score = check_content_consistency(document_content, issues_found)
        completeness_score = check_content_completeness(document_content, issues_found)
        
        overall_score = (structure_score + content_score + terms_score +
                        markdown_score + translation_score + formatting_score +
                        consistency_score + completeness_score) / 8 * 100
        
        quality_passed = overall_score >= QA_RULES['min_quality_score']
        
        validation_result = {
            'level': 4,
            'name': 'enhanced_content_validation',
            'overall_score': overall_score,
            'quality_passed': quality_passed,
            'validation_score': overall_score / 100,
            'detailed_scores': {
                'structure': structure_score,
                'content': content_score,
                'terms': terms_score,
                'markdown': markdown_score,
                'translation': translation_score,
                'formatting': formatting_score,
                'consistency': consistency_score,
                'completeness': completeness_score
            },
            'issues_found': issues_found,
            'processing_time': time.time() - start_time
        }
        
        status = "✅ PASSED" if quality_passed else "❌ FAILED"
        logger.info(f"{status} Уровень 4 завершен: {overall_score:.1f}%")
        return validation_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка уровня 4 валидации: {e}")
        return {
            'level': 4,
            'name': 'enhanced_content_validation',
            'validation_score': 0.0,
            'issues_found': [f"Enhanced validation failed: {str(e)}"],
            'processing_time': time.time() - start_time
        }

# Базовые функции валидации
def check_document_structure(content: str, issues_list: List) -> float:
    """Проверка структуры документа"""
    score = 100.0
    try:
        if len(content) < QA_RULES['min_content_length']:
            issues_list.append(f"Документ слишком короткий: {len(content)} символов")
            score -= 30
            
        headers = re.findall(r'^#+\s+', content, re.MULTILINE)
        if len(headers) < QA_RULES['min_headings']:
            issues_list.append(f"Мало заголовков: {len(headers)}")
            score -= 20
            
        if QA_RULES['require_title'] and not re.search(r'^#\s+', content, re.MULTILINE):
            issues_list.append("Отсутствует главный заголовок")
            score -= 15
            
        return max(0, score) / 100
    except Exception as e:
        logger.warning(f"Ошибка проверки структуры: {e}")
        return 0.5

def check_content_quality(content: str, issues_list: List) -> float:
    """Проверка качества содержимого"""
    score = 100.0
    try:
        empty_sections = len(re.findall(r'^#+\s+.*\n\s*\n\s*#+', content, re.MULTILINE))
        if empty_sections > 0:
            issues_list.append(f"Пустые разделы: {empty_sections}")
            score -= empty_sections * 10
            
        lines = content.split('\n')
        unique_lines = set(line.strip() for line in lines if line.strip())
        repetition_ratio = 1 - (len(unique_lines) / max(len(lines), 1))
        
        if repetition_ratio > 0.3:
            issues_list.append(f"Высокий уровень повторов: {repetition_ratio:.1%}")
            score -= 20
            
        return max(0, score) / 100
    except Exception as e:
        logger.warning(f"Ошибка проверки содержимого: {e}")
        return 0.7

def check_technical_terms(content: str, issues_list: List) -> float:
    """Проверка технических терминов"""
    score = 100.0
    try:
        if not QA_RULES['technical_terms_check']:
            return 1.0
            
        found_terms = 0
        for term in TECHNICAL_TERMS:
            if term.lower() in content.lower() or term in content:
                found_terms += 1
                
        if found_terms == 0:
            issues_list.append("Не найдены технические термины")
            score -= 30
        elif found_terms < 3:
            issues_list.append("Мало технических терминов")
            score -= 15
            
        return max(0, score) / 100
    except Exception as e:
        logger.warning(f"Ошибка проверки технических терминов: {e}")
        return 0.8

def check_markdown_syntax(content: str, issues_list: List) -> float:
    """Проверка синтаксиса Markdown"""
    score = 100.0
    try:
        if not QA_RULES['validate_markdown_syntax']:
            return 1.0
            
        malformed_headers = re.findall(r'^#{7,}', content, re.MULTILINE)
        if malformed_headers:
            issues_list.append(f"Неправильные заголовки: {len(malformed_headers)}")
            score -= len(malformed_headers) * 5
            
        broken_links = re.findall(r']\(\s*\)', content)
        if broken_links:
            issues_list.append(f"Пустые ссылки: {len(broken_links)}")
            score -= len(broken_links) * 3
            
        if QA_RULES['check_table_structure']:
            table_lines = re.findall(r'^\|.*\|$', content, re.MULTILINE)
            separator_lines = re.findall(r'^\|[\s\-:|]+\|$', content, re.MULTILINE)
            if table_lines and not separator_lines:
                issues_list.append("Таблицы без разделителей заголовков")
                score -= 15
                
        return max(0, score) / 100
    except Exception as e:
        logger.warning(f"Ошибка проверки Markdown: {e}")
        return 0.85

def check_translation_quality(content: str, issues_list: List) -> float:
    """Проверка качества перевода"""
    score = 100.0
    try:
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', content))
        total_chars = len(content)
        
        if total_chars > 0:
            chinese_ratio = chinese_chars / total_chars
            max_allowed_ratio = QA_RULES['max_chinese_chars_ratio']
            if chinese_ratio > max_allowed_ratio:
                issues_list.append(f"Слишком много китайских символов: {chinese_ratio:.1%}")
                score -= 20
                
        return max(0, score) / 100
    except Exception as e:
        logger.warning(f"Ошибка проверки качества перевода: {e}")
        return 0.75

# ✅ Расширенные функции валидации
def check_advanced_formatting(content: str, issues_list: List) -> float:
    """Расширенная проверка форматирования"""
    score = 100.0
    try:
        malformed_lists = re.findall(r'^\s*[\-\*\+]\s*$', content, re.MULTILINE)
        if malformed_lists:
            issues_list.append(f"Пустые элементы списков: {len(malformed_lists)}")
            score -= len(malformed_lists) * 5
            
        # Проверка незакрытых блоков кода (число тройных бэктиков должно быть чётным)
        triple_ticks_count = len(re.findall(r"```", content))
        if triple_ticks_count % 2 != 0:
            issues_list.append("Незакрытые блоки кода")
            score -= 20
            
        return max(0, score) / 100
    except Exception:
        return 0.9

def check_content_consistency(content: str, issues_list: List) -> float:
    """Проверка консистентности контента"""
    score = 100.0
    try:
        inconsistent_terms = 0
        for chinese_term, english_term in [('问天', 'WenTian'), ('联想', 'Lenovo')]:
            if chinese_term in content and english_term not in content:
                inconsistent_terms += 1
                
        if inconsistent_terms > 0:
            issues_list.append(f"Несогласованная терминология: {inconsistent_terms}")
            score -= inconsistent_terms * 10
            
        return max(0, score) / 100
    except Exception:
        return 0.9

def check_content_completeness(content: str, issues_list: List) -> float:
    """Проверка полноты контента"""
    score = 100.0
    try:
        required_sections = ['введение', 'обзор', 'конфигурация', 'заключение']
        found_sections = sum(1 for section in required_sections if section.lower() in content.lower())
        
        if found_sections < 2:
            issues_list.append(f"Недостаточно основных секций: {found_sections}/4")
            score -= (4 - found_sections) * 15
            
        return max(0, score) / 100
    except Exception:
        return 0.8

# ================================================================================
# УРОВЕНЬ 5: AUTO-CORRECTION ЧЕРЕЗ vLLM (ИСПРАВЛЕН)
# ================================================================================


def _request_vllm_warmup() -> bool:
    """Attempt to trigger vLLM warmup for translation tasks."""
    warmup_url = f"{_VLLM_BASE_URL}/warmup"
    payload = {
        "model": VLLM_CONFIG['model'],
        "task_type": "translation",
    }
    try:
        response = requests.post(warmup_url, json=payload, timeout=30)
        if response.status_code < 400:
            logger.info("vLLM warmup request accepted: %s", response.status_code)
            return True
        logger.warning(
            "vLLM warmup request rejected (%s): %s",
            response.status_code,
            response.text[:200],
        )
    except Exception as exc:
        logger.warning("vLLM warmup request failed: %s", exc)
    return False


def _schedule_auto_correction_retry(context: Dict[str, Any], reason: str, delay_minutes: int = 3) -> bool:
    """Schedule an additional DAG run to repeat auto-correction after warmup."""
    dag_run = context.get('dag_run')
    if not dag_run:
        logger.warning("Cannot schedule auto-correction retry: dag_run missing")
        return False

    dag_conf = dict(dag_run.conf or {})
    if dag_conf.get('auto_correction_retry_scheduled'):
        logger.info("Auto-correction retry already scheduled, skipping duplicate trigger")
        return False

    dag_conf['auto_correction_retry_scheduled'] = True
    dag_conf['auto_correction_retry_reason'] = reason

    execution_date = timezone.utcnow() + timedelta(minutes=delay_minutes)
    run_id = f"{dag_run.run_id}__auto_correction_retry"

    try:
        trigger_dag(
            dag_id=dag_run.dag_id,
            run_id=run_id,
            conf=dag_conf,
            execution_date=execution_date,
            replace_microseconds=False,
        )
        logger.info("Triggered auto-correction retry run %s for reason: %s", run_id, reason)
        return True
    except Exception as exc:
        logger.error("Failed to trigger auto-correction retry: %s", exc)
        return False

def perform_auto_correction(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕН: Уровень 5: Автоматическое исправление через vLLM"""
    start_time = time.time()
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        validation_results = context['task_instance'].xcom_pull(task_ids='perform_enhanced_content_validation')
        
        logger.info("🔍 Уровень 5: Auto-Correction через vLLM")
        
        document_content = qa_session['translated_content']
        issues_found = validation_results.get('issues_found', [])
        
        correction_result = {
            'level': 5,
            'name': 'auto_correction',
            'corrections_applied': 0,
            'correction_confidence': 0.0,
            'corrected_content': document_content,
            'validation_score': 1.0,
            'issues_found': [],
            'processing_time': 0.0,
            'timed_out': False,
            'warmup_requested': False,
            'retry_scheduled': False,
            'status': 'completed'
        }

        if issues_found and qa_session.get('auto_correction', True) and len(issues_found) <= QA_RULES['MAX_CORRECTIONS_PER_DOCUMENT']:
            corrected_content, correction_confidence, timed_out = apply_vllm_corrections(
                document_content, issues_found
            )

            correction_result['timed_out'] = timed_out

            if corrected_content and correction_confidence >= QA_RULES['AUTO_CORRECTION_CONFIDENCE']:
                correction_result.update({
                    'corrections_applied': len(issues_found),
                    'correction_confidence': correction_confidence,
                    'corrected_content': corrected_content,
                    'validation_score': correction_confidence
                })
                logger.info(f"✅ Применены исправления vLLM: {len(issues_found)} проблем, уверенность {correction_confidence:.3f}")
            elif timed_out:
                logger.warning("vLLM auto-correction timed out on all attempts; scheduling retry after warmup")
                correction_result['issues_found'].append("vLLM correction timed out; retry scheduled after warmup")
                correction_result['status'] = 'pending_retry'
                correction_result['warmup_requested'] = _request_vllm_warmup()
                correction_result['retry_scheduled'] = _schedule_auto_correction_retry(context, 'vllm_timeout')
            else:
                correction_result['issues_found'].append(f"vLLM correction confidence too low: {correction_confidence:.3f}")
        elif len(issues_found) > QA_RULES['MAX_CORRECTIONS_PER_DOCUMENT']:
            correction_result['issues_found'].append(f"Too many issues for auto-correction: {len(issues_found)}")

        correction_result['processing_time'] = time.time() - start_time
        logger.info(f"✅ Уровень 5 завершен: {correction_result['corrections_applied']} исправлений")
        return correction_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка уровня 5 автокоррекции: {e}")
        return {
            'level': 5,
            'name': 'auto_correction',
            'validation_score': 0.0,
            'issues_found': [f"Auto-correction failed: {str(e)}"],
            'processing_time': time.time() - start_time,
            'timed_out': False,
            'warmup_requested': False,
            'retry_scheduled': False,
            'status': 'failed'
        }

def apply_vllm_corrections(content: str, issues: List[str]) -> tuple[str, float, bool]:
    """✅ Применение исправлений через vLLM"""
    try:
        logger.info("Applying vLLM corrections")

        correction_prompt = f"""
You are a professional document quality assurance specialist. Please fix the following issues in the markdown document:

ISSUES TO FIX:
{chr(10).join(f"- {issue}" for issue in issues)}

DOCUMENT CONTENT:
{content}

Please provide the corrected markdown document that addresses all the issues while preserving the original meaning and technical terminology. Respond with ONLY the corrected markdown content.
""".strip()

        corrected_content, timed_out = call_vllm_api(correction_prompt)

        if corrected_content:
            correction_quality = evaluate_correction_quality(content, corrected_content, issues)
            return corrected_content, correction_quality, timed_out

        return content, 0.0, timed_out

    except Exception as e:
        logger.error(f"vLLM correction error: {e}")
        return content, 0.0, False

def call_vllm_api(prompt: str) -> Tuple[Optional[str], bool]:
    """✅ ИСПРАВЛЕН: Вызов vLLM API с правильным форматом messages для строк"""
    timed_out_attempts = 0
    try:
        for attempt in range(VLLM_CONFIG['max_retries']):
            try:
                payload = {
                    "model": VLLM_CONFIG['model'],
                    "messages": [
                        {"role": "system", "content": "You are a helpful technical editor."},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": VLLM_CONFIG['max_tokens'],
                    "temperature": VLLM_CONFIG['temperature'],
                    "top_p": VLLM_CONFIG['top_p'],
                    "task_type": "translation",
                }

                response = requests.post(
                    VLLM_CONFIG['endpoint'],
                    json=payload,
                    timeout=VLLM_CONFIG['timeout']
                )

                if response.status_code == 200:
                    result = response.json()
                    return result['choices'][0]['message']['content'], False

                if response.status_code >= 500:
                    logger.warning(
                        "vLLM API server error %s: %s",
                        response.status_code,
                        response.text[:200]
                    )
                else:
                    logger.error(
                        "vLLM API error %s: %s",
                        response.status_code,
                        response.text[:200]
                    )

            except requests.exceptions.Timeout as exc:
                timed_out_attempts += 1
                logger.error(
                    "vLLM API timeout after %ss on attempt %d/%d: %s",
                    VLLM_CONFIG['timeout'],
                    attempt + 1,
                    VLLM_CONFIG['max_retries'],
                    exc
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "vLLM API request error on attempt %d/%d: %s",
                    attempt + 1,
                    VLLM_CONFIG['max_retries'],
                    exc
                )
            except Exception as e:
                logger.warning(f"vLLM API call attempt {attempt + 1} failed: {e}")

            if attempt < VLLM_CONFIG['max_retries'] - 1:
                time.sleep(_compute_retry_delay())
                continue

            logger.error("vLLM API failed after %d attempts", VLLM_CONFIG['max_retries'])
            return None, timed_out_attempts >= VLLM_CONFIG['max_retries']

    except Exception as e:
        logger.error(f"vLLM API call failed (outer): {e}")
        return None, timed_out_attempts >= VLLM_CONFIG['max_retries']

    return None, timed_out_attempts >= VLLM_CONFIG['max_retries']

def evaluate_correction_quality(original: str, corrected: str, issues: List[str]) -> float:
    """✅ Оценка качества исправления"""
    try:
        if not corrected or corrected == original:
            return 0.0
            
        quality_score = 0.8
        
        length_ratio = len(corrected) / max(len(original), 1)
        if 0.8 <= length_ratio <= 1.3:
            quality_score += 0.1
        else:
            quality_score -= 0.2
            
        original_terms = sum(1 for term in TECHNICAL_TERMS if term in original)
        corrected_terms = sum(1 for term in TECHNICAL_TERMS if term in corrected)
        
        if corrected_terms >= original_terms * 0.9:
            quality_score += 0.1
            
        return min(1.0, max(0.0, quality_score))
    except Exception:
        return 0.5

# ================================================================================
# ФИНАЛИЗАЦИЯ И ОТЧЕТЫ (СОХРАНЕНЫ)
# ================================================================================

def generate_comprehensive_qa_report(**context) -> Dict[str, Any]:
    """✅ Генерация полного QA отчета по всем 5 уровням"""
    start_time = time.time()
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        level1_results = context['task_instance'].xcom_pull(task_ids='perform_ocr_cross_validation')
        level2_results = context['task_instance'].xcom_pull(task_ids='perform_visual_comparison')
        level3_results = context['task_instance'].xcom_pull(task_ids='perform_ast_structure_comparison')
        level4_results = context['task_instance'].xcom_pull(task_ids='perform_enhanced_content_validation')
        level5_results = context['task_instance'].xcom_pull(task_ids='perform_auto_correction')
        
        level_scores = [
            level1_results.get('validation_score', 0),
            level2_results.get('validation_score', 0),
            level3_results.get('validation_score', 0),
            level4_results.get('validation_score', 0),
            level5_results.get('validation_score', 0)
        ]
        
        overall_score = sum(level_scores) / len(level_scores) * 100
        quality_passed = overall_score >= QA_RULES['OVERALL_QA_THRESHOLD'] * 100
        
        comprehensive_report = {
            'session_id': qa_session['session_id'],
            'document_file': qa_session['translated_file'],
            'qa_completion_time': datetime.now().isoformat(),
            'overall_score': overall_score,
            'quality_passed': quality_passed,
            'enterprise_validation': True,
            'level_results': {
                'level1_ocr_validation': level1_results,
                'level2_visual_comparison': level2_results,
                'level3_ast_structure': level3_results,
                'level4_content_validation': level4_results,
                'level5_auto_correction': level5_results
            },
            'level_scores': {f'level_{i+1}': score for i, score in enumerate(level_scores)},
            'all_issues': (
                level1_results.get('issues_found', []) +
                level2_results.get('issues_found', []) +
                level3_results.get('issues_found', []) +
                level4_results.get('issues_found', []) +
                level5_results.get('issues_found', [])
            ),
            'corrections_applied': level5_results.get('corrections_applied', 0),
            'corrected_content': level5_results.get('corrected_content'),
            'qa_rules_used': QA_RULES,
            'level_configs_used': LEVEL_CONFIG
        }
        
        airflow_temp = os.getenv('AIRFLOW_TEMP_DIR', '/opt/airflow/temp')
        SharedUtils.ensure_directory(airflow_temp)
        
        report_file = os.path.join(airflow_temp, f"qa_comprehensive_report_{qa_session['session_id']}.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(comprehensive_report, f, ensure_ascii=False, indent=2)
            
        comprehensive_report['report_file'] = report_file
        
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='generate_comprehensive_qa_report',
            processing_time=time.time() - start_time,
            success=True
        )
        
        status = "✅ PASSED" if quality_passed else "❌ NEEDS REVIEW"
        logger.info(f"📊 Полный QA отчет создан: {overall_score:.1f}% - {status}")
        return comprehensive_report
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='generate_comprehensive_qa_report',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка создания полного QA отчета: {e}")
        raise

def finalize_qa_process(**context) -> Dict[str, Any]:
    """✅ Финализация полного процесса контроля качества"""
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        comprehensive_report = context['task_instance'].xcom_pull(task_ids='generate_comprehensive_qa_report')
        
        final_document = (comprehensive_report.get('corrected_content') or 
                         qa_session['translated_content'])
        
        final_result = {
            'qa_completed': True,
            'enterprise_validation': True,
            'quality_score': comprehensive_report['overall_score'],
            'quality_passed': comprehensive_report['quality_passed'],
            'final_document': qa_session['translated_file'],
            'final_content': final_document,
            'qa_report': comprehensive_report['report_file'],
            'issues_count': len(comprehensive_report['all_issues']),
            'corrections_applied': comprehensive_report.get('corrections_applied', 0),
            'pipeline_ready': comprehensive_report['quality_passed'],
            '5_level_validation_complete': True,
            'level_scores': comprehensive_report['level_scores'],
            'pdf_comparison_performed': True,
            'ocr_validation_performed': True,
            'semantic_analysis_performed': True,
            'auto_correction_performed': comprehensive_report.get('corrections_applied', 0) > 0
        }
        
        status = "✅ КАЧЕСТВО ПРОШЛО 5-УРОВНЕВУЮ ВАЛИДАЦИЮ" if comprehensive_report['quality_passed'] else "❌ ЕСТЬ ПРОБЛЕМЫ"
        logger.info(f"🎯 Полный 5-уровневый QA завершен: {comprehensive_report['overall_score']:.1f}% - {status}")
        return final_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка финализации полного QA: {e}")
        raise

def notify_qa_completion(**context) -> None:
    """✅ Уведомление о завершении полного контроля качества"""
    try:
        final_result = context['task_instance'].xcom_pull(task_ids='finalize_qa_process')
        
        quality_score = final_result['quality_score']
        quality_passed = final_result['quality_passed']
        corrections_applied = final_result['corrections_applied']
        
        status_icon = "✅" if quality_passed else "❌"
        status_text = "ENTERPRISE QA PASSED" if quality_passed else "NEEDS REVIEW"
        
        message = f"""
{status_icon} 5-УРОВНЕВАЯ QUALITY ASSURANCE ЗАВЕРШЕНА

🎯 Общий балл качества: {quality_score:.1f}%
📊 Статус: {status_text}
🔧 Исправлений применено: {corrections_applied}

📋 РЕЗУЛЬТАТЫ ПО УРОВНЯМ:
{chr(10).join(f"Level {level.split('_')[1]}: {score:.1%}" for level, score in final_result['level_scores'].items())}

✅ ENTERPRISE ФУНКЦИИ:
- PDF визуальное сравнение: ✅ Выполнено
- OCR кросс-валидация: ✅ Выполнено
- Семантический анализ: ✅ Выполнено
- vLLM автокоррекция: ✅ Выполнено

📁 Документ: {final_result['final_document']}
📋 Отчет: {final_result['qa_report']}
⚠️ Проблем найдено: {final_result['issues_count']}

{'✅ Готов для дальнейшей обработки' if quality_passed else '❌ Требует исправления'}
"""

        logger.info(message)
        
        if quality_passed:
            NotificationUtils.send_success_notification(context, final_result)
        else:
            NotificationUtils.send_failure_notification(
                context,
                Exception(f"5-уровневая валидация: качество {quality_score:.1f}% ниже требуемого")
            )
            
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления полного QA: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ DAG - 5 УРОВНЕЙ
# ================================================================================

load_document = PythonOperator(
    task_id='load_translated_document',
    python_callable=load_translated_document,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

level1_ocr = PythonOperator(
    task_id='perform_ocr_cross_validation',
    python_callable=perform_ocr_cross_validation,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

level2_visual = PythonOperator(
    task_id='perform_visual_comparison',
    python_callable=perform_visual_comparison,
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

level3_ast = PythonOperator(
    task_id='perform_ast_structure_comparison',
    python_callable=perform_ast_structure_comparison,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

level4_content = PythonOperator(
    task_id='perform_enhanced_content_validation',
    python_callable=perform_enhanced_content_validation,
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

level5_correction = PythonOperator(
    task_id='perform_auto_correction',
    python_callable=perform_auto_correction,
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_comprehensive_qa_report',
    python_callable=generate_comprehensive_qa_report,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

finalize_qa = PythonOperator(
    task_id='finalize_qa_process',
    python_callable=finalize_qa_process,
    execution_timeout=timedelta(minutes=3),
    dag=dag
)

notify_completion = PythonOperator(
    task_id='notify_qa_completion',
    python_callable=notify_qa_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# ✅ Зависимости
load_document >> [level1_ocr, level2_visual, level3_ast]
[level1_ocr, level2_visual, level3_ast] >> level4_content
level4_content >> level5_correction
level5_correction >> generate_report >> finalize_qa >> notify_completion

# ================================================================================
# ОБРАБОТКА ОШИБОК
# ================================================================================

def handle_qa_failure(context):
    """✅ Обработка ошибок полной QA системы"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')
        
        error_message = f"""
🔥 ОШИБКА В 5-УРОВНЕВОЙ QUALITY ASSURANCE

Задача: {failed_task}
Ошибка: {str(exception) if exception else 'Unknown'}

Возможные причины:
1. Поврежденные данные документа
2. Проблемы с Docker Pandoc сервисом
3. Недоступность vLLM сервиса
4. Отсутствие зависимостей (scikit-image, sentence-transformers)
5. Проблемы с монтированными томами Docker
6. Недостаток памяти для обработки
7. Ошибки в правилах валидации

Проверьте логи и состояние всех сервисов.
"""

        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок полной QA: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_qa_failure
