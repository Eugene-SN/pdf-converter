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
from typing import Any, Dict, List, Optional

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
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

TRANSLATION_CONFIG: Dict[str, Any] = {
    "service_url": os.getenv("TRANSLATOR_URL", _service_urls.get("translator", "http://translator:8002")),
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
) -> Optional[str]:
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
                return data.get("translated_content", text)

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
    return None


def count_chinese_characters(text: str) -> int:
    return len(re.findall(r"[\u4e00-\u9fff]", text))


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
            translated = call_translator_with_retries(
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
                })
                translated_segments.append(batch["content"])
                continue

            stats["successful_chunks"] += 1
            translated_segments.append(translated)

        final_content = "\n".join(translated_segments)
        processing_time = time.time() - start_time
        chinese_remaining = count_chinese_characters(final_content)
        quality_score = validate_translation_quality(markdown_content, final_content)

        average_latency = mean(stats["latencies"]) if stats["latencies"] else 0.0
        max_latency = max(stats["latencies"]) if stats["latencies"] else 0.0

        translation_results = {
            "translated_content": final_content,
            "source_length": len(markdown_content),
            "translated_length": len(final_content),
            "quality_score": quality_score,
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
                "chinese_chars_remaining": chinese_remaining,
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
        translation_results = context["task_instance"].xcom_pull(task_ids="perform_translation")

        original_config = session.get("original_config", {})
        target_language = session.get("target_language", "ru")
        timestamp = original_config.get("timestamp", int(time.time()))
        filename = original_config.get("filename", "document.pdf")

        output_dir = f"/app/output/{target_language}"
        os.makedirs(output_dir, exist_ok=True)

        translated_filename = f"{timestamp}_{filename.replace('.pdf', '.md')}"
        output_path = os.path.join(output_dir, translated_filename)

        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(translation_results["translated_content"])

        translation_metadata = {
            "target_language": target_language,
            "quality_score": translation_results["quality_score"],
            "translation_method": translation_results["translation_stats"]["translation_method"],
            "avg_latency_seconds": translation_results["translation_stats"]["avg_latency_seconds"],
            "max_latency_seconds": translation_results["translation_stats"]["max_latency_seconds"],
            "chunks_total": translation_results["translation_stats"]["chunks_total"],
            "chunks_failed": translation_results["translation_stats"]["chunks_failed"],
            "model": translation_results["translation_stats"]["model"],
            "service_url": translation_results["translation_stats"]["service_url"],
            "chinese_chars_remaining": translation_results["translation_stats"]["chinese_chars_remaining"],
            "completion_time": datetime.now().isoformat(),
        }

        stage4_config = {
            "translated_file": output_path,
            "translated_content": translation_results["translated_content"],
            "original_config": original_config,
            "stage3_completed": True,
            "translation_metadata": translation_metadata,
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
        translation_metadata = stage4_config["translation_metadata"]

        message = f"""
✅ TRANSLATION PIPELINE ЗАВЕРШЕН УСПЕШНО

🌐 Целевой язык: {translation_metadata['target_language']}
🎯 Качество перевода: {translation_metadata['quality_score']:.1f}%
⏱️ Средняя задержка vLLM: {translation_metadata['avg_latency_seconds']:.2f} с
📦 Чанков с ошибками: {translation_metadata['chunks_failed']} из {translation_metadata['chunks_total']}
🤖 Модель: {translation_metadata['model']}
📁 Файл: {stage4_config['translated_file']}
"""
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
