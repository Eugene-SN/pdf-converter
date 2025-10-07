#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✅ ИСПРАВЛЕННЫЙ DAG: Document Preprocessing - Финальная версия с Bridge-файлами

ИСПРАВЛЕНИЯ В ВЕРСИИ 4.0:
- ✅ Решена проблема сериализации TableData через правильную интеграцию с document-processor API
- ✅ Убраны прямые импорты Docling из Airflow (правильная архитектура микросервисов)
- ✅ Исправлена проблема master_run_id (автозаполнение из context)
- ✅ Исправлена проблема Permission denied с /app/temp (использование $AIRFLOW_HOME/temp)
- ✅ Добавлен retry при ошибке сериализации таблиц (extract_tables=False)
- ✅ НОВОЕ: Выбор правильного doc_*_intermediate.json из output_files
- ✅ НОВОЕ: Замена префикса пути /app/temp → /opt/airflow/temp  
- ✅ НОВОЕ: Создание bridge-файла stage1_bridge_{timestamp}.json
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import os
import json
import logging
import time
import requests
from typing import Dict, Any, Optional, Tuple

# Импорт исправленных утилит
from shared_utils import (
    SharedUtils, NotificationUtils, ConfigUtils,
    MetricsUtils, ErrorHandlingUtils
)

# Настройка логирования
logger = logging.getLogger(__name__)

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
    'document_preprocessing',
    default_args=DEFAULT_ARGS,
    description='DAG 1: Исправленный процессор конвертации PDF в Markdown через document-processor API',
    schedule_interval=None,
    max_active_runs=3,
    catchup=False,
    tags=['pdf-converter', 'dag1', 'chinese-docs', 'production', 'v4.0-fixed']
)

# Конфигурация сервисов
DOCUMENT_PROCESSOR_URL = os.getenv('DOCUMENT_PROCESSOR_URL', 'http://document-processor:8001')

# Конфигурация для китайских документов
CHINESE_DOC_CONFIG = {
    'ocr_languages': 'chi_sim,chi_tra,eng',
    'ocr_confidence_threshold': 0.75,
    'chinese_header_patterns': [
        r'^[第章节]\s*[一二三四五六七八九十\d]+\s*[章节]',
        r'^[一二三四五六七八九十]+[、．]',
        r'^\d+[、．]\s*[\u4e00-\u9fff]',
    ],
    'tech_terms': {
        '问天': 'WenTian',
        '联想问天': 'Lenovo WenTian',
        '天擎': 'ThinkSystem',
        '至强': 'Xeon',
        '可扩展处理器': 'Scalable Processors',
        '英特尔': 'Intel',
        '处理器': 'Processor',
        '内核': 'Core',
        '线程': 'Thread',
        '内存': 'Memory',
        '存储': 'Storage',
        '以太网': 'Ethernet',
        '机架': 'Rack',
        '插槽': 'Slot',
        '电源': 'Power Supply'
    }
}

def validate_input_file(**context) -> Dict[str, Any]:
    """✅ Валидация входного файла с автозаполнением master_run_id"""
    start_time = time.time()
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"📋 Получена конфигурация: {json.dumps(dag_run_conf, indent=2, ensure_ascii=False)}")

        # Обязательные параметры (master_run_id убран из обязательных!)
        required_params = ['input_file', 'filename', 'timestamp']
        missing_params = [param for param in required_params if not dag_run_conf.get(param)]
        if missing_params:
            raise ValueError(f"Отсутствуют обязательные параметры: {missing_params}")

        # ✅ ИСПРАВЛЕНИЕ: Автозаполнение master_run_id при отсутствии
        master_run_id = dag_run_conf.get('master_run_id') or context['dag_run'].run_id
        if 'master_run_id' not in dag_run_conf:
            logger.info(f"🆔 master_run_id автозаполнен из context: {master_run_id}")
            dag_run_conf['master_run_id'] = master_run_id

        # Валидация файла
        input_file = dag_run_conf['input_file']
        # НОРМАЛИЗАЦИЯ: хостовый префикс -> контейнерный
        if isinstance(input_file, str) and input_file.startswith('/mnt/storage/apps/pdf-converter/'):
            input_file = input_file.replace('/mnt/storage/apps/pdf-converter/', '/app/', 1)
        # Сохранить нормализованный путь в конфиг
        dag_run_conf['input_file'] = input_file

        # Валидация файла по контейнерному пути
        if not SharedUtils.validate_input_file(input_file):
            raise ValueError(f"Некорректный файл: {input_file}")

        # Анализ китайского документа
        file_info = analyze_chinese_document(input_file)

        # Обогащенная конфигурация
        enriched_config = {
            **dag_run_conf,
            **file_info,
            'chinese_doc_analysis': file_info,
            'processing_mode': 'chinese_optimized',
            'validation_timestamp': datetime.now().isoformat()
        }

        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='validate_input_file',
            processing_time=time.time() - start_time,
            success=True
        )

        logger.info(f"✅ Входной файл валидирован: {dag_run_conf['filename']}")
        return enriched_config

    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='validate_input_file',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка валидации: {e}")
        raise

def analyze_chinese_document(file_path: str) -> Dict[str, Any]:
    """Быстрый анализ китайского документа"""
    try:
        file_size = os.path.getsize(file_path)
        file_hash = SharedUtils.calculate_file_hash(file_path)

        # Базовый анализ
        estimated_pages = max(1, file_size // 102400)  # ~100KB на страницу
        has_chinese_text = False

        # Попытка быстрого анализа через PyMuPDF если доступен
        try:
            import fitz
            doc = fitz.open(file_path)
            estimated_pages = doc.page_count

            # Проверка первых страниц на китайский текст
            for page_num in range(min(2, doc.page_count)):
                page = doc[page_num]
                text = page.get_text()[:500]  # Первые 500 символов
                chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
                if chinese_chars > 5:
                    has_chinese_text = True
                    break
            doc.close()
        except Exception:
            pass  # Fallback к базовому анализу

        return {
            'file_hash': file_hash,
            'file_size_bytes': file_size,
            'file_size_mb': file_size / (1024 * 1024),
            'estimated_pages': estimated_pages,
            'has_chinese_text': has_chinese_text,
            'recommended_ocr': not has_chinese_text,
            'processing_complexity': 'high' if file_size > 50*1024*1024 else 'medium'
        }

    except Exception as e:
        logger.warning(f"Анализ документа не удался: {e}")
        return {
            'file_hash': 'unknown',
            'file_size_bytes': 0,
            'file_size_mb': 0.0,
            'estimated_pages': 1,
            'has_chinese_text': True,
            'recommended_ocr': True,
            'processing_complexity': 'medium'
        }

def process_document_with_api(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕННАЯ обработка через document-processor API с выбором правильного intermediate файла"""
    start_time = time.time()
    config = context['task_instance'].xcom_pull(task_ids='validate_input_file')

    try:
        input_file = config['input_file']
        timestamp = config['timestamp']
        filename = config['filename']

        logger.info(f"🔄 Отправка в document-processor API: {DOCUMENT_PROCESSOR_URL}/process")

        # ✅ ИСПРАВЛЕНИЕ: Безопасная временная директория (НЕ /app/temp!)
        airflow_temp = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'temp')
        os.makedirs(airflow_temp, exist_ok=True)

        # Формирование опций для API
        use_ocr = bool(config.get("enable_ocr", False))
        recommended_ocr = config.get("chinese_doc_analysis", {}).get("recommended_ocr")
        if recommended_ocr is True and not use_ocr:
            logger.info("🔁 OCR включен по рекомендации анализа документа")
            use_ocr = True
        elif recommended_ocr is False and use_ocr:
            logger.info("ℹ️ OCR оставлен включенным согласно входной конфигурации")

        api_options = {
            "extract_tables": bool(config.get("extract_tables", True)),
            "extract_images": bool(config.get("extract_images", True)),
            "extract_formulas": bool(config.get("extract_formulas", True)),
            "use_ocr": use_ocr,
            "ocr_languages": config.get("ocr_languages", "eng,chi_sim"),
            "high_quality_ocr": bool(config.get("high_quality_ocr", True)),
            "preserve_layout": bool(config.get("preserve_structure", True)),
            "enable_chunking": False
        }

        def call_api(options: Dict[str, Any], attempt: int = 1) -> requests.Response:
            """Вызов API document-processor с опциями"""
            with open(input_file, 'rb') as f:
                files = {'file': (os.path.basename(input_file), f, 'application/pdf')}
                data = {'options': json.dumps(options, ensure_ascii=False)}

                logger.info(f"📤 Попытка {attempt}: отправка файла {filename} с опциями: {options}")

                return requests.post(
                    f"{DOCUMENT_PROCESSOR_URL}/process",
                    files=files,
                    data=data,
                    timeout=60*15  # 15 минут таймаут
                )

        def perform_request(options: Dict[str, Any], attempt_start: int = 1) -> Tuple[requests.Response, Dict[str, Any], int]:
            """Выполнить запрос и при необходимости повторить без таблиц."""
            resp_local = call_api(options, attempt=attempt_start)
            effective_options = dict(options)
            next_attempt = attempt_start

            if (
                resp_local.status_code == 500
                and "TableData is not JSON serializable" in resp_local.text
                and effective_options.get("extract_tables", True)
            ):
                logger.warning(
                    "⚠️ Обнаружена ошибка сериализации TableData. Повторный вызов без извлечения таблиц."
                )
                effective_options["extract_tables"] = False
                next_attempt += 1
                resp_local = call_api(effective_options, attempt=next_attempt)

            return resp_local, effective_options, next_attempt

        # ✅ ИСПРАВЛЕНИЕ: Первый вызов с полными опциями
        resp, effective_api_options, last_attempt = perform_request(api_options, attempt_start=1)

        # Проверка ответа API
        if resp.status_code != 200:
            error_msg = f"API вернул {resp.status_code}: {resp.text}"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg, "original_config": config}

        # Парсинг JSON ответа
        try:
            resp_json = resp.json()
        except Exception as json_error:
            error_msg = f"Некорректный JSON от API: {json_error}"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg, "original_config": config}

        if not resp_json.get("success", False):
            error_msg = f"API сообщил об ошибке: {resp_json.get('message') or resp_json.get('error')}"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg, "original_config": config}

        sections_count = int(resp_json.get("sections_count", 0) or 0)
        markdown_payload = (resp_json.get("markdown_content") or resp_json.get("raw_text") or "").strip()
        if sections_count == 0 and not markdown_payload:
            if not effective_api_options.get("use_ocr"):
                logger.warning(
                    "⚠️ Docling вернул пустую структуру без OCR. Повторная попытка API с OCR включенным."
                )
                ocr_retry_options = {**effective_api_options, "use_ocr": True}
                resp, effective_api_options, last_attempt = perform_request(
                    ocr_retry_options, attempt_start=last_attempt + 1
                )

                if resp.status_code != 200:
                    error_msg = f"API вернул {resp.status_code}: {resp.text}"
                    logger.error(f"❌ {error_msg}")
                    return {"success": False, "error": error_msg, "original_config": config}

                try:
                    resp_json = resp.json()
                except Exception as json_error:
                    error_msg = f"Некорректный JSON от API (fallback OCR): {json_error}"
                    logger.error(f"❌ {error_msg}")
                    return {"success": False, "error": error_msg, "original_config": config}

                if not resp_json.get("success", False):
                    error_msg = (
                        "API сообщил об ошибке при OCR fallback: "
                        f"{resp_json.get('message') or resp_json.get('error')}"
                    )
                    logger.error(f"❌ {error_msg}")
                    return {"success": False, "error": error_msg, "original_config": config}

                sections_count = int(resp_json.get("sections_count", 0) or 0)
                markdown_payload = (
                    (resp_json.get("markdown_content") or resp_json.get("raw_text") or "").strip()
                )

                if sections_count == 0 and not markdown_payload:
                    error_msg = (
                        "Docling вернул пустую структуру даже после OCR fallback"
                    )
                    logger.error(f"❌ {error_msg}")
                    return {"success": False, "error": error_msg, "original_config": config}
            else:
                error_msg = (
                    "Docling вернул пустую структуру (sections=0, markdown missing); "
                    "pipeline настроен на цифровые PDF без OCR"
                )
                logger.error(f"❌ {error_msg}")
                return {"success": False, "error": error_msg, "original_config": config}

        # ✅ НОВОЕ: Правильный выбор intermediate файла из output_files
        def map_to_airflow_temp(path: str) -> str:
            """Замена префикса /app/temp → /opt/airflow/temp"""
            if path.startswith("/app/temp"):
                return path.replace("/app/temp", airflow_temp, 1)
            return path

        # 1) Предпочесть doc_*_intermediate.json из output_files
        intermediate_file = resp_json.get("intermediate_file")
        output_files = resp_json.get("output_files", []) or []
        doc_intermediate = None

        for path in output_files:
            if isinstance(path, str) and path.endswith("_intermediate.json"):
                doc_intermediate = path
                break

        if doc_intermediate:
            mapped_path = map_to_airflow_temp(doc_intermediate)
            if os.path.exists(mapped_path):
                intermediate_file = mapped_path
                logger.info(f"✅ Выбран полноценный intermediate файл: {intermediate_file}")

        # 2) Если артефакт не найден, использовать минимальный безопасный
        if not intermediate_file or not os.path.exists(intermediate_file):
            local_intermediate = os.path.join(airflow_temp, f"preprocessing_{timestamp}.json")
            safe_payload = {
                "title": resp_json.get("document_id", filename.replace('.pdf', '')),
                "pages_count": resp_json.get("pages_count", 0),
                "markdown_content": resp_json.get("markdown_content", ""),  # вдруг сервис вернул
                "raw_text": resp_json.get("raw_text", ""),
                "api_response": resp_json
            }

            with open(local_intermediate, "w", encoding="utf-8") as f:
                json.dump(safe_payload, f, ensure_ascii=False, indent=2)
            intermediate_file = local_intermediate
            logger.info(f"📁 Создан безопасный промежуточный файл: {intermediate_file}")

        # ✅ НОВОЕ: 3) Сохранить bridge-файл для оркестратора
        bridge_file = os.path.join(airflow_temp, f"stage1_bridge_{timestamp}.json")
        try:
            with open(bridge_file, "w", encoding="utf-8") as f:
                json.dump({
                    "intermediate_file": intermediate_file,
                    "docling_intermediate": doc_intermediate,  # исходный путь от API
                    "output_files": output_files
                }, f, ensure_ascii=False, indent=2)
            logger.info(f"🌉 Создан bridge-файл: {bridge_file}")
        except Exception as e:
            logger.warning(f"Не удалось записать bridge-файл Stage1: {e}")

        processing_time = resp_json.get("processing_time", time.time() - start_time)
        pages = resp_json.get("pages_count", 0)

        # Обновляем финальные настройки OCR на основе успешного ответа
        use_ocr = bool(effective_api_options.get("use_ocr", False))
        config['enable_ocr'] = use_ocr

        result = {
            "success": True,
            "document_info": {
                "title": filename.replace('.pdf', ''),
                "total_pages": pages,
                "processing_time": processing_time,
                "status": "success"
            },
            "intermediate_file": intermediate_file,
            "original_config": config,
            "processing_stats": {
                "pages_processed": pages,
                "ocr_used": use_ocr,
                "processing_time_seconds": processing_time,
                "tables_extracted": effective_api_options.get("extract_tables", True),
                "api_attempts": last_attempt,
                "ocr_fallback_used": use_ocr and not api_options.get("use_ocr", False)
            },
            "api_response": resp_json,  # Полный ответ API для отладки
            "output_files": output_files  # ✅ НОВОЕ: Добавлено для прозрачности
        }

        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='process_document_with_api',
            processing_time=processing_time,
            pages_count=pages,
            success=True
        )

        logger.info(f"✅ Документ обработан через API за {processing_time:.2f}с")
        return result

    except requests.RequestException as req_error:
        error_msg = f"Сетевая ошибка при обращении к API: {req_error}"
        logger.error(f"❌ {error_msg}")
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='process_document_with_api',
            processing_time=time.time() - start_time,
            success=False
        )
        return {"success": False, "error": error_msg, "original_config": config}

    except Exception as e:
        error_msg = f"Критическая ошибка обработки: {str(e)}"
        logger.error(f"❌ {error_msg}")
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='process_document_with_api',
            processing_time=time.time() - start_time,
            success=False
        )
        return {"success": False, "error": error_msg, "original_config": config}

def prepare_for_next_stage(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Подготовка данных для следующего DAG с проверенным путем"""
    start_time = time.time()
    try:
        result = context['task_instance'].xcom_pull(task_ids='process_document_with_api')
        if not result.get('success'):
            raise AirflowException(f"Обработка документа не удалась: {result.get('error')}")

        # Проверка промежуточного файла
        intermediate_file = result.get('intermediate_file')
        if not intermediate_file or not os.path.exists(intermediate_file):
            raise AirflowException("Промежуточный файл не найден после Stage 1")

        # Конфигурация для следующего DAG
        next_stage_config = {
            'intermediate_file': intermediate_file,
            'original_config': result['original_config'],
            'dag1_metadata': {
                **result.get('processing_stats', {}),
                'completion_time': datetime.now().isoformat()
            },
            'dag1_completed': True,
            'ready_for_transformation': True,
            'chinese_document': True,
            'output_files': result.get('output_files', [])  # ✅ НОВОЕ: Передаем для фолбэка
        }

        # ✅ НОВОЕ: дублируем в bridge для оркестратора (на случай прямого чтения)
        airflow_temp = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'temp')
        timestamp = result['original_config']['timestamp']
        bridge_file = os.path.join(airflow_temp, f"stage1_bridge_{timestamp}.json")
        try:
            with open(bridge_file, "w", encoding="utf-8") as f:
                json.dump({
                    "intermediate_file": intermediate_file,
                    "output_files": result.get('output_files', [])
                }, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='prepare_for_next_stage',
            processing_time=time.time() - start_time,
            success=True
        )

        logger.info("✅ Данные подготовлены для следующего DAG")
        return next_stage_config

    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='prepare_for_next_stage',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка подготовки: {e}")
        raise

def notify_completion(**context) -> None:
    """Уведомление о завершении"""
    try:
        result = context['task_instance'].xcom_pull(task_ids='process_document_with_api')
        next_config = context['task_instance'].xcom_pull(task_ids='prepare_for_next_stage')

        if result and result.get('success'):
            stats = result.get('processing_stats', {})
            message = f"""
✅ DOCUMENT PREPROCESSING ЗАВЕРШЕН УСПЕШНО
📄 Файл: {result['original_config']['filename']}
📊 Страниц: {stats.get('pages_processed', 'N/A')}
⏱️ Время: {stats.get('processing_time_seconds', 0):.2f}с
🔍 OCR: {'Да' if stats.get('ocr_used') else 'Нет'}
📋 Таблицы: {'Да' if stats.get('tables_extracted') else 'Нет'}
📁 Файл: {next_config.get('intermediate_file', 'N/A')}
✅ Готов для следующей стадии
"""
            NotificationUtils.send_success_notification(context, result)
        else:
            error = result.get('error', 'Unknown error') if result else 'No result'
            message = f"""
❌ DOCUMENT PREPROCESSING ЗАВЕРШЕН С ОШИБКОЙ
📄 Файл: {result['original_config']['filename'] if result else 'Unknown'}
❌ Ошибка: {error}
⏰ Время: {datetime.now().isoformat()}
"""
            NotificationUtils.send_failure_notification(context, Exception(error))

        logger.info(message)

    except Exception as e:
        logger.error(f"❌ Ошибка уведомления: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ================================================================================

validate_input = PythonOperator(
    task_id='validate_input_file',
    python_callable=validate_input_file,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# ✅ ИСПРАВЛЕНО: task_id изменен для ясности (теперь вызывает API, а не Docling напрямую)
process_document = PythonOperator(
    task_id='process_document_with_api',  # Переименовано для ясности
    python_callable=process_document_with_api,
    execution_timeout=timedelta(hours=1),
    dag=dag
)

prepare_next = PythonOperator(
    task_id='prepare_for_next_stage',
    python_callable=prepare_for_next_stage,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

notify_task = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Определение зависимостей
validate_input >> process_document >> prepare_next >> notify_task

def handle_processing_failure(context):
    """✅ Улучшенная обработка ошибок"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')

        error_message = f"""
🔥 КРИТИЧЕСКАЯ ОШИБКА В DOCUMENT PREPROCESSING v4.0
Задача: {failed_task}
Ошибка: {str(exception) if exception else 'Unknown'}
Возможные причины и решения:
1. TableData сериализация → повторите с extract_tables=false
2. Permission denied → проверьте директорию /opt/airflow/temp
3. API недоступен → проверьте document-processor:8001/health
4. Неверный файл → проверьте PDF и его размер
"""

        logger.error(error_message)
        ErrorHandlingUtils.handle_processing_error(context, exception, failed_task)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка в обработчике: {e}")

# Применение обработчика ошибок
for task in dag.tasks:
    task.on_failure_callback = handle_processing_failure
