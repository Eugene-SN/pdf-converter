#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Airflow DAG for Stage 3 translation orchestrated via the translator microservice."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

import requests
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator
from requests.adapters import HTTPAdapter

from shared_utils import ConfigUtils, MetricsUtils, NotificationUtils

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "pdf-converter",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "translation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Stage 3 translation pipeline using the translator microservice",
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    tags=["pdf-converter", "stage3", "translation", "vllm"],
)

_service_urls = ConfigUtils.get_service_urls()
_translator_url_from_env = os.getenv("TRANSLATOR_URL")
_default_translator_url = _service_urls.get("translator") or "http://translator:8003"
_resolved_translator_url = (
    _translator_url_from_env.strip()
    if _translator_url_from_env and _translator_url_from_env.strip()
    else _default_translator_url
)

TRANSLATION_CONFIG: Dict[str, Any] = {
    "service_url": _resolved_translator_url,
    "endpoint": os.getenv("TRANSLATION_ENDPOINT", "/api/v1/translate"),
    "timeout": int(os.getenv("TRANSLATION_TIMEOUT", "300")),
    "max_retries": int(os.getenv("TRANSLATION_MAX_RETRIES", "3")),
    "retry_delay": float(os.getenv("TRANSLATION_RETRY_DELAY", "5")),
    "max_chars_per_chunk": int(os.getenv("TRANSLATION_MAX_CHARS", "3500")),
    "model": os.getenv("VLLM_TRANSLATION_MODEL", "Qwen/Qwen3-30B-A3B-Instruct-2507"),
}

BATCH_CONFIG: Dict[str, int] = {
    "headers": max(1, int(os.getenv("BATCH_SIZE_HEADERS", "4"))),
    "tables": max(1, int(os.getenv("BATCH_SIZE_TABLES", "3"))),
    "commands": max(1, int(os.getenv("BATCH_SIZE_COMMANDS", "2"))),
    "text": max(1, int(os.getenv("BATCH_SIZE_TEXT", "6"))),
    "mixed": max(1, int(os.getenv("BATCH_SIZE_MIXED", "4"))),
    "technical": max(1, int(os.getenv("BATCH_SIZE_TECHNICAL", os.getenv("BATCH_SIZE_TEXT", "6")))),
}

_TRANSLATOR_SESSION = requests.Session()
_TRANSLATOR_ADAPTER = HTTPAdapter(pool_connections=4, pool_maxsize=8)
_TRANSLATOR_SESSION.mount("http://", _TRANSLATOR_ADAPTER)
_TRANSLATOR_SESSION.mount("https://", _TRANSLATOR_ADAPTER)


def _normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def classify_line(line: str) -> str:
    stripped = line.strip()
    if not stripped:
        return "empty"
    if stripped.startswith("#"):
        return "header"
    if stripped.startswith("|"):
        return "table"
    if stripped.startswith("```"):
        return "code"
    if re.search(r"`[^`]+`", stripped):
        return "inline_code"
    if re.search(r"\\b(ipmitool|chassis|power|cli|bios|efi)\\b", stripped, re.IGNORECASE):
        return "command"
    if re.search(r"\\b\\d+(?:\\.\\d+)?\\s*(GB|TB|GHz|MHz|W|%)\\b", stripped):
        return "technical"
    return "text"


def get_batch_size(content_type: str) -> int:
    if content_type == "header":
        return BATCH_CONFIG["headers"]
    if content_type == "table":
        return BATCH_CONFIG["tables"]
    if content_type in {"command", "inline_code"}:
        return BATCH_CONFIG["commands"]
    if content_type == "technical":
        return BATCH_CONFIG["technical"]
    if content_type == "code":
        return 1
    return BATCH_CONFIG["text"]


def build_translation_batches(content: str) -> List[Dict[str, str]]:
    normalized = _normalize_newlines(content)
    lines = normalized.split("\n")
    batches: List[Dict[str, str]] = []
    idx = 0

    while idx < len(lines):
        current_line = lines[idx]
        if not current_line.strip():
            batches.append({"type": "raw", "content": current_line})
            idx += 1
            continue

        content_type = classify_line(current_line)
        batch_limit = get_batch_size(content_type)
        chunk_lines: List[str] = []
        char_count = 0

        while idx < len(lines):
            line = lines[idx]
            if not line.strip():
                break

            line_type = classify_line(line)
            if chunk_lines and line_type != content_type:
                break

            prospective = char_count + len(line) + 1
            if chunk_lines and (len(chunk_lines) >= batch_limit or prospective > TRANSLATION_CONFIG["max_chars_per_chunk"]):
                break

            chunk_lines.append(line)
            char_count = prospective
            idx += 1

            if len(chunk_lines) >= batch_limit:
                break

        if chunk_lines:
            batches.append({
                "type": "translatable",
                "content": "\n".join(chunk_lines),
            })
            continue

        batches.append({"type": "raw", "content": current_line})
        idx += 1

    return batches


def call_translator_with_retries(
    text: str,
    source_lang: str,
    target_lang: str,
    stats: Dict[str, Any],
    chunk_index: int,
    total_chunks: int,
) -> Tuple[Optional[str], int]:
    url = f"{TRANSLATION_CONFIG['service_url'].rstrip('/')}{TRANSLATION_CONFIG['endpoint']}"
    payload = {
        "text": text,
        "source_lang": source_lang,
        "target_lang": target_lang,
    }

    last_error: Optional[Exception] = None

    for attempt in range(1, TRANSLATION_CONFIG["max_retries"] + 1):
        try:
            logger.info(
                "🌐 Отправка чанка %s/%s в translator service (попытка %s)",
                chunk_index + 1,
                total_chunks,
                attempt,
            )
            start = time.time()
            response = _TRANSLATOR_SESSION.post(
                url,
                json=payload,
                timeout=TRANSLATION_CONFIG["timeout"],
            )
            latency = time.time() - start
            stats.setdefault("latencies", []).append(latency)
            stats["api_calls"] += 1

            if response.status_code == 200:
                data = response.json()
                stats["successful_requests"] += 1
                logger.info(
                    "✅ Чанк %s/%s переведен за %.2fs",
                    chunk_index + 1,
                    total_chunks,
                    latency,
                )
                return data.get("translated_content", text), attempt

            stats["failed_requests"] += 1
            last_error = RuntimeError(
                f"Unexpected status {response.status_code}: {response.text[:200]}"
            )
            logger.warning("⚠️ Ошибка перевода чанка %s/%s: %s", chunk_index + 1, total_chunks, last_error)
        except requests.RequestException as exc:
            stats["failed_requests"] += 1
            last_error = exc
            logger.warning(
                "⚠️ Сетевая ошибка при переводе чанка %s/%s: %s",
                chunk_index + 1,
                total_chunks,
                exc,
            )
        stats["retries"] += 1
        if attempt < TRANSLATION_CONFIG["max_retries"]:
            sleep_for = TRANSLATION_CONFIG["retry_delay"] * attempt
            logger.info("⏳ Повтор через %.1fs", sleep_for)
            time.sleep(sleep_for)

    logger.error("❌ Не удалось перевести чанк %s/%s: %s", chunk_index + 1, total_chunks, last_error)
    return None, TRANSLATION_CONFIG["max_retries"]


def count_chinese_characters(text: str) -> int:
    return len(re.findall(r"[\u4e00-\u9fff]", text))


CHINESE_TECH_TERMS: List[str] = [
    "问天",
    "联想问天",
    "天擎",
    "至强",
    "可扩展处理器",
    "英特尔",
    "处理器",
    "内核",
    "线程",
    "睿频",
    "内存",
    "存储",
    "硬盘",
    "固态硬盘",
    "机械硬盘",
    "热插拔",
    "冗余",
    "背板",
    "托架",
    "以太网",
    "光纤",
    "带宽",
    "延迟",
    "网卡",
    "风冷",
    "液冷",
    "散热",
    "风扇",
    "散热器",
    "英寸",
    "机架",
    "插槽",
    "转接卡",
    "电源",
    "铂金",
    "钛金",
    "芯片组",
    "控制器",
    "适配器",
    "光驱",
]


def compute_technical_term_coverage(original: str, translated: str) -> Dict[str, Any]:
    total_occurrences = 0
    remaining_occurrences = 0

    for term in CHINESE_TECH_TERMS:
        original_matches = len(re.findall(re.escape(term), original))
        if original_matches == 0:
            continue

        total_occurrences += original_matches
        translated_matches = len(re.findall(re.escape(term), translated))
        remaining_occurrences += min(original_matches, translated_matches)

    translated_occurrences = max(0, total_occurrences - remaining_occurrences)
    coverage = 1.0
    if total_occurrences > 0:
        coverage = max(0.0, min(1.0, translated_occurrences / total_occurrences))

    return {
        "total": total_occurrences,
        "remaining": remaining_occurrences,
        "translated": translated_occurrences,
        "coverage": coverage,
    }


def validate_translation_quality(original: str, translated: str) -> float:
    try:
        if not original.strip():
            return 100.0

        score = 100.0
        length_ratio = len(translated) / max(len(original), 1)
        if length_ratio < 0.55 or length_ratio > 2.2:
            score -= 15

        original_chinese = count_chinese_characters(original)
        translated_chinese = count_chinese_characters(translated)
        if original_chinese > 0:
            reduction = 1 - (translated_chinese / original_chinese)
            score += max(0.0, reduction) * 25
        elif translated_chinese > 0:
            score -= 25

        structure_delta = abs(translated.count("\n\n") - original.count("\n\n"))
        score -= min(structure_delta * 2, 10)

        return max(0.0, min(100.0, score))
    except Exception:
        return 70.0


def initialize_translation(**context) -> Dict[str, Any]:
    start_time = time.time()
    try:
        dag_run_conf = context["dag_run"].conf or {}
        logger.info(
            "🌐 Инициализация перевода: %s",
            json.dumps(dag_run_conf, indent=2, ensure_ascii=False),
        )

        if dag_run_conf.get("skip_stage3", False):
            raise AirflowSkipException("Stage 3 skipped by orchestrator (no translation required)")

        markdown_file = dag_run_conf.get("markdown_file")
        if not markdown_file or not os.path.exists(markdown_file):
            raise ValueError(f"Markdown файл не найден: {markdown_file}")

        with open(markdown_file, "r", encoding="utf-8") as handle:
            markdown_content = handle.read()

        if not markdown_content.strip():
            raise ValueError("Нет контента для перевода")

        health_url = f"{TRANSLATION_CONFIG['service_url'].rstrip('/')}/health"
        try:
            logger.info("🔍 Проверка доступности translator по %s", health_url)
            response = _TRANSLATOR_SESSION.head(health_url, timeout=5)
            if response.status_code >= 400:
                response = _TRANSLATOR_SESSION.get(health_url, timeout=5)
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Translator health endpoint returned {response.status_code}: {response.text}"
                )
        except requests.RequestException as health_exc:
            raise RuntimeError(
                f"Translator service недоступен по адресу {health_url}"
            ) from health_exc

        original_config = dag_run_conf.get("original_config", {})
        target_language = original_config.get("target_language", dag_run_conf.get("target_language", "ru"))

        translation_session = {
            "session_id": f"translation_{int(time.time())}",
            "markdown_file": markdown_file,
            "markdown_content": markdown_content,
            "source_language": dag_run_conf.get("source_language", "zh-CN"),
            "target_language": target_language,
            "original_config": original_config,
            "lines_total": len(markdown_content.split("\n")),
            "processing_start_time": datetime.now().isoformat(),
        }

        MetricsUtils.record_processing_metrics(
            dag_id="translation_pipeline",
            task_id="initialize_translation",
            processing_time=time.time() - start_time,
            success=True,
        )

        logger.info(
            "✅ Перевод инициализирован: %s (%s строк)",
            target_language,
            translation_session["lines_total"],
        )
        return translation_session
    except Exception as exc:
        MetricsUtils.record_processing_metrics(
            dag_id="translation_pipeline",
            task_id="initialize_translation",
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error("❌ Ошибка инициализации перевода: %s", exc)
        raise


def perform_translation(**context) -> Dict[str, Any]:
    start_time = time.time()
    session = context["task_instance"].xcom_pull(task_ids="initialize_translation")

    source_lang = session.get("source_language", "zh-CN")
    target_language = session.get("target_language", "ru")
    markdown_content = session["markdown_content"]

    stats: Dict[str, Any] = {
        "total_chunks": 0,
        "successful_chunks": 0,
        "failed_chunks": 0,
        "api_calls": 0,
        "successful_requests": 0,
        "failed_requests": 0,
        "retries": 0,
        "latencies": [],
        "errors": [],
    }

    try:
        batches = build_translation_batches(markdown_content)
        logger.info("📦 Подготовлено чанков для перевода: %s", len(batches))

        translated_segments: List[str] = []
        translatable_count = sum(1 for batch in batches if batch["type"] == "translatable")

        chunk_counter = 0
        for index, batch in enumerate(batches):
            if batch["type"] == "raw":
                translated_segments.append(batch["content"])
                continue

            stats["total_chunks"] += 1
            translated, attempts = call_translator_with_retries(
                batch["content"],
                source_lang,
                target_language,
                stats,
                chunk_counter,
                translatable_count,
            )
            chunk_counter += 1

            if translated is None:
                stats["failed_chunks"] += 1
                stats["errors"].append({
                    "chunk_index": index,
                    "content_preview": batch["content"][:120],
                    "source_path": session.get("markdown_file"),
                    "attempts": attempts,
                })
                translated_segments.append(batch["content"])
                continue

            stats["successful_chunks"] += 1
            translated_segments.append(translated)

        if stats["total_chunks"] > 0 and stats["successful_chunks"] == 0:
            raise AirflowException(
                "Перевод не выполнен: ни один чанк не был успешно обработан translator сервисом."
            )

        final_content = "\n".join(translated_segments)
        processing_time = time.time() - start_time
        chinese_remaining = count_chinese_characters(final_content)
        original_chinese = count_chinese_characters(markdown_content)
        quality_score = validate_translation_quality(markdown_content, final_content)

        chinese_ratio = chinese_remaining / max(len(final_content), 1)
        if original_chinese > 0:
            chinese_coverage = max(0.0, min(1.0, 1 - (chinese_remaining / original_chinese)))
        else:
            chinese_coverage = 1.0

        technical_term_stats = compute_technical_term_coverage(markdown_content, final_content)

        average_latency = mean(stats["latencies"]) if stats["latencies"] else 0.0
        max_latency = max(stats["latencies"]) if stats["latencies"] else 0.0

        translation_results = {
            "translated_content": final_content,
            "source_length": len(markdown_content),
            "translated_length": len(final_content),
            "quality_score": quality_score,
            "failed_chunks": stats["errors"],
            "translation_stats": {
                "processing_time_seconds": processing_time,
                "translation_method": "translator_microservice_vllm",
                "chunks_total": stats["total_chunks"],
                "chunks_successful": stats["successful_chunks"],
                "chunks_failed": stats["failed_chunks"],
                "api_calls": stats["api_calls"],
                "api_success": stats["successful_requests"],
                "api_failures": stats["failed_requests"],
                "avg_latency_seconds": round(average_latency, 3),
                "max_latency_seconds": round(max_latency, 3),
                "model": TRANSLATION_CONFIG["model"],
                "service_url": TRANSLATION_CONFIG["service_url"],
                "chinese_chars_original": original_chinese,
                "chinese_chars_remaining": chinese_remaining,
                "chinese_ratio": chinese_ratio,
                "chinese_translation_coverage": chinese_coverage,
                "technical_terms_total": technical_term_stats["total"],
                "technical_terms_remaining": technical_term_stats["remaining"],
                "technical_terms_translated": technical_term_stats["translated"],
                "technical_terms_coverage": technical_term_stats["coverage"],
                "errors": stats["errors"],
            },
        }

        MetricsUtils.record_processing_metrics(
            dag_id="translation_pipeline",
            task_id="perform_translation",
            processing_time=processing_time,
            success=stats["failed_chunks"] == 0,
            chunks_total=stats["total_chunks"],
            api_failures=stats["failed_requests"],
        )

        logger.info(
            "✅ Перевод завершен: %s успешных чанков, %s с ошибками",
            stats["successful_chunks"],
            stats["failed_chunks"],
        )
        return translation_results
    except Exception as exc:
        MetricsUtils.record_processing_metrics(
            dag_id="translation_pipeline",
            task_id="perform_translation",
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error("❌ Ошибка выполнения перевода: %s", exc)
        raise


def save_translation_result(**context) -> Dict[str, Any]:
    start_time = time.time()
    try:
        session = context["task_instance"].xcom_pull(task_ids="initialize_translation")
        translation_results = context["task_instance"].xcom_pull(task_ids="perform_translation") or {}

        original_config = session.get("original_config", {})
        target_language = session.get("target_language", "ru")
        timestamp = original_config.get("timestamp", int(time.time()))
        filename = original_config.get("filename", "document.pdf")

        output_dir = f"/app/output/{target_language}"
        os.makedirs(output_dir, exist_ok=True)

        translated_filename = f"{timestamp}_{filename.replace('.pdf', '.md')}"
        output_path = os.path.join(output_dir, translated_filename)

        translation_stats = translation_results.get("translation_stats", {})
        translated_content = translation_results.get("translated_content", "")
        placeholder_used = False

        if not translated_content:
            placeholder_used = True
            translated_content = (
                "# TRANSLATION PLACEHOLDER\n\n"
                "Перевод недоступен: сервис translator не вернул результат для данного файла."
            )

        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(translated_content)

        chunks_total = translation_stats.get("chunks_total", 0)
        chunks_failed = translation_stats.get("chunks_failed", 0)
        chunks_successful = translation_stats.get("chunks_successful", max(chunks_total - chunks_failed, 0))

        translation_metadata = {
            "target_language": target_language,
            "quality_score": translation_results.get("quality_score", 0.0),
            "translation_method": translation_stats.get("translation_method", "translator_microservice_vllm"),
            "avg_latency_seconds": translation_stats.get("avg_latency_seconds", 0.0),
            "max_latency_seconds": translation_stats.get("max_latency_seconds", 0.0),
            "chunks_total": chunks_total,
            "chunks_successful": chunks_successful,
            "chunks_failed": chunks_failed,
            "model": translation_stats.get("model", TRANSLATION_CONFIG["model"]),
            "service_url": translation_stats.get("service_url", TRANSLATION_CONFIG["service_url"]),
            "chinese_chars_original": translation_stats.get("chinese_chars_original", 0),
            "chinese_chars_remaining": translation_stats.get("chinese_chars_remaining", 0),
            "chinese_ratio": translation_stats.get("chinese_ratio"),
            "chinese_translation_coverage": translation_stats.get("chinese_translation_coverage"),
            "technical_terms_total": translation_stats.get("technical_terms_total", 0),
            "technical_terms_remaining": translation_stats.get("technical_terms_remaining", 0),
            "technical_terms_translated": translation_stats.get("technical_terms_translated", 0),
            "technical_terms_coverage": translation_stats.get("technical_terms_coverage"),
            "completion_time": datetime.now().isoformat(),
            "placeholder_used": placeholder_used,
        }

        stage4_config = {
            "translated_file": output_path,
            "translated_content": translated_content,
            "original_config": original_config,
            "stage3_completed": True,
            "translation_metadata": translation_metadata,
            "failed_chunks": translation_results.get("failed_chunks", translation_stats.get("errors", [])),
            "placeholder_used": placeholder_used,
        }

        MetricsUtils.record_processing_metrics(
            dag_id="translation_pipeline",
            task_id="save_translation_result",
            processing_time=time.time() - start_time,
            success=True,
        )

        logger.info("💾 Перевод сохранен: %s", output_path)
        return stage4_config
    except Exception as exc:
        MetricsUtils.record_processing_metrics(
            dag_id="translation_pipeline",
            task_id="save_translation_result",
            processing_time=time.time() - start_time,
            success=False,
        )
        logger.error("❌ Ошибка сохранения перевода: %s", exc)
        raise


def notify_translation_completion(**context) -> None:
    try:
        stage4_config = context["task_instance"].xcom_pull(task_ids="save_translation_result")
        if not stage4_config:
            raise AirflowException("Нет данных о результате перевода в XCom save_translation_result")

        translation_metadata = stage4_config.get("translation_metadata", {})
        failed_chunks = stage4_config.get("failed_chunks", [])

        chunks_total = translation_metadata.get("chunks_total", 0)
        chunks_failed = translation_metadata.get("chunks_failed", len(failed_chunks))
        chunks_successful = translation_metadata.get(
            "chunks_successful", max(chunks_total - chunks_failed, 0)
        )

        placeholder_used = stage4_config.get("placeholder_used", False)
        status = "успешно" if chunks_failed == 0 and not placeholder_used else "с предупреждениями"
        if placeholder_used:
            status = "завершено с плейсхолдером"

        message_lines = [
            "✅ TRANSLATION PIPELINE завершен",
            f"Статус: {status}",
            "",
            f"🌐 Целевой язык: {translation_metadata.get('target_language', 'n/a')}",
            f"🎯 Качество перевода: {translation_metadata.get('quality_score', 0.0):.1f}%",
            f"⏱️ Средняя задержка vLLM: {translation_metadata.get('avg_latency_seconds', 0.0):.2f} с",
            f"📊 Чанки: {chunks_successful}/{chunks_total} успешных, {chunks_failed} с ошибками",
            f"🤖 Модель: {translation_metadata.get('model', TRANSLATION_CONFIG['model'])}",
            f"📁 Файл: {stage4_config.get('translated_file', 'n/a')}",
        ]

        if failed_chunks:
            message_lines.append(f"❗ Неудачные чанки: {len(failed_chunks)} (см. отчет QA)")

        message = "\n".join(message_lines)
        logger.info(message)
        NotificationUtils.send_success_notification(context, stage4_config)
    except Exception as exc:
        logger.error("❌ Ошибка отправки уведомления: %s", exc)


init_translation = PythonOperator(
    task_id="initialize_translation",
    python_callable=initialize_translation,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

perform_translation_task = PythonOperator(
    task_id="perform_translation",
    python_callable=perform_translation,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

save_result = PythonOperator(
    task_id="save_translation_result",
    python_callable=save_translation_result,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

notify_completion = PythonOperator(
    task_id="notify_translation_completion",
    python_callable=notify_translation_completion,
    trigger_rule="all_done",
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

init_translation >> perform_translation_task >> save_result >> notify_completion


def handle_translation_failure(context: Dict[str, Any]) -> None:
    try:
        failed_task = context["task_instance"].task_id
        exception = context.get("exception")
        error_message = f"""
🔥 ОШИБКА В TRANSLATION PIPELINE

Задача: {failed_task}
Ошибка: {exception}

Проверьте доступность translator service ({TRANSLATION_CONFIG['service_url']}),
настройки модели ({TRANSLATION_CONFIG['model']}) и входные данные Stage 2.
"""
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
    except Exception as exc:
        logger.error("❌ Ошибка в обработчике ошибок: %s", exc)


for task in dag.tasks:
    task.on_failure_callback = handle_translation_failure
