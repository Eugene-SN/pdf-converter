#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Исправленный shared_utils.py с улучшенной обработкой ошибок и метриками
и добавленными утилитами для vLLM (Qwen-VL формат сообщений, health-check, логирование).
"""

import os
import json
import logging
import hashlib
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from pathlib import Path

# Настройка логирования
logger = logging.getLogger(__name__)


class SharedUtils:
    """Общие утилиты для обработки файлов и валидации"""

    @staticmethod
    def validate_input_file(file_path: str) -> bool:
        """Валидация входного PDF файла"""
        try:
            if not file_path or not isinstance(file_path, str):
                logger.error("Некорректный путь к файлу")
                return False

            # Нормализуем возможный хост-путь к контейнерному префиксу
            # /mnt/storage/apps/pdf-converter/* -> /app/*
            try:
                if isinstance(file_path, str) and file_path.startswith('/mnt/storage/apps/pdf-converter/'):
                    file_path = file_path.replace('/mnt/storage/apps/pdf-converter/', '/app/', 1)
            except Exception:
                pass

            path = Path(file_path)

            # Проверка существования файла
            if not path.exists():
                logger.error(f"Файл не существует: {file_path}")
                return False

            # Проверка что это файл, а не директория
            if not path.is_file():
                logger.error(f"Путь указывает не на файл: {file_path}")
                return False

            # Проверка расширения
            if path.suffix.lower() != '.pdf':
                logger.error(f"Неподдерживаемое расширение файла: {path.suffix}")
                return False

            # Проверка размера файла
            file_size = path.stat().st_size
            if file_size == 0:
                logger.error("Файл пустой")
                return False
            if file_size > 500 * 1024 * 1024:  # 500MB
                logger.error(f"Файл слишком большой: {file_size / (1024*1024):.2f} MB")
                return False

            # Базовая проверка PDF заголовка
            try:
                with open(file_path, 'rb') as f:
                    header = f.read(8)
                    if not header.startswith(b'%PDF-'):
                        logger.error("Файл не является корректным PDF")
                        return False
            except Exception as e:
                logger.error(f"Ошибка чтения файла: {e}")
                return False

            logger.info(f"Файл прошел валидацию: {file_path} ({file_size / (1024*1024):.2f} MB)")
            return True
        except Exception as e:
            logger.error(f"Ошибка валидации файла {file_path}: {e}")
            return False

    @staticmethod
    def calculate_file_hash(file_path: str, algorithm: str = 'md5') -> str:
        """Вычисление хеша файла"""
        try:
            hash_algo = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                # Читаем файл частями для экономии памяти
                for chunk in iter(lambda: f.read(8192), b''):
                    hash_algo.update(chunk)
            return hash_algo.hexdigest()
        except Exception as e:
            logger.error(f"Ошибка вычисления хеша файла {file_path}: {e}")
            return 'error'

    @staticmethod
    def ensure_directory(directory_path: str) -> bool:
        """Создание директории если она не существует"""
        try:
            Path(directory_path).mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Не удалось создать директорию {directory_path}: {e}")
            return False


class ConfigUtils:
    """Утилиты для работы с конфигурацией"""

    @staticmethod
    def get_processing_paths() -> Dict[str, str]:
        # По умолчанию используем контейнерные пути, переопределяемые через env
        base_path = os.getenv('PROCESSING_BASE_PATH', '/app')
        paths = {
            # Приоритет: UPLOAD_FOLDER -> INPUT_DIR -> /app/input
            'input_pdf': os.getenv('UPLOAD_FOLDER') or os.getenv('INPUT_DIR', os.path.join(base_path, 'input')),
            # Temp под управлением Airflow: AIRFLOW_TEMP_DIR -> /opt/airflow/temp
            'temp_dir': os.getenv('AIRFLOW_TEMP_DIR', os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'temp')),
            # Унифицированный выход: OUTPUT_DIR -> /app/output/zh (stage2 по умолчанию zh)
            'output_md': os.getenv('OUTPUT_DIR', os.path.join(base_path, 'output/zh')),
            'logs': os.path.join(base_path, 'logs'),
            'work_dir': os.getenv('WORK_DIR', '/tmp/document_processor'),
        }

        # Создаем директории
        for _path_type, path in paths.items():
            SharedUtils.ensure_directory(path)

        return paths

    @staticmethod
    def get_service_urls() -> Dict[str, str]:
        """Получение URL сервисов"""
        return {
            'document_processor': os.getenv('DOCUMENT_PROCESSOR_URL', 'http://document-processor:8001'),
            'translator': os.getenv('TRANSLATOR_URL', 'http://translator:8002'),
            'quality_assurance': os.getenv('QUALITY_ASSURANCE_URL', 'http://quality-assurance:8002'),
            'airflow': os.getenv('AIRFLOW_URL', 'http://localhost:8080'),
        }


class MetricsUtils:
    """Утилиты для записи метрик"""

    @staticmethod
    def record_processing_metrics(
        dag_id: str,
        task_id: str,
        processing_time: float,
        success: bool,
        pages_count: int = 0,
        file_size_mb: float = 0.0,
        **kwargs
    ) -> None:
        """Запись метрик обработки"""
        try:
            metrics_data: Dict[str, Any] = {
                'dag_id': dag_id,
                'task_id': task_id,
                'processing_time_seconds': processing_time,
                'pages_processed': pages_count,
                'file_size_mb': file_size_mb,
                'success': success,
                'timestamp': datetime.now().isoformat(),
                **kwargs,
            }

            logger.info(f"📊 Метрики: {metrics_data}")

            # Опционально: отправка в StatsD/Prometheus
            statsd_host = os.getenv('STATSD_HOST')
            if statsd_host:
                MetricsUtils._send_to_statsd(metrics_data)
        except Exception as e:
            logger.error(f"Ошибка записи метрик: {e}")

    @staticmethod
    def _send_to_statsd(metrics_data: Dict[str, Any]) -> None:
        """Отправка метрик в StatsD (опционально)"""
        try:
            import socket

            statsd_host = os.getenv('STATSD_HOST', 'localhost')
            statsd_port = int(os.getenv('STATSD_PORT', 8125))

            # Простая отправка UDP пакета
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            # Формируем метрики в StatsD формате
            dag_id = metrics_data.get('dag_id', 'unknown')
            task_id = metrics_data.get('task_id', 'unknown')
            processing_time = metrics_data.get('processing_time_seconds', 0)
            success = 1 if metrics_data.get('success') else 0

            metrics = [
                f"pdf_converter.{dag_id}.{task_id}.processing_time:{processing_time}|ms",
                f"pdf_converter.{dag_id}.{task_id}.success:{success}|c",
                f"pdf_converter.{dag_id}.{task_id}.pages:{metrics_data.get('pages_processed', 0)}|g",
            ]

            for metric in metrics:
                sock.sendto(metric.encode(), (statsd_host, statsd_port))
            sock.close()
        except Exception as e:
            logger.warning(f"Не удалось отправить метрики в StatsD: {e}")


class NotificationUtils:
    """Утилиты для отправки уведомлений"""

    @staticmethod
    def send_success_notification(context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Отправка уведомления об успешном завершении"""
        try:
            message = f"""
✅ УСПЕШНОЕ ЗАВЕРШЕНИЕ
DAG: {context.get('dag_run', {}).dag_id if hasattr(context.get('dag_run', {}), 'dag_id') else 'unknown'}
Task: {context.get('task_instance', {}).task_id if hasattr(context.get('task_instance', {}), 'task_id') else 'unknown'}
Время: {datetime.now().isoformat()}
Результат: {result.get('message', 'Обработка завершена')}
"""
            logger.info(message)
            # Опционально: отправка в Slack/Teams/Email
            NotificationUtils._send_external_notification(message, 'success')
        except Exception as e:
            logger.error(f"Ошибка отправки успешного уведомления: {e}")

    @staticmethod
    def send_failure_notification(context: Dict[str, Any], exception: Exception) -> None:
        """Отправка уведомления об ошибке"""
        try:
            message = f"""
❌ ОШИБКА ВЫПОЛНЕНИЯ
DAG: {context.get('dag_run', {}).dag_id if hasattr(context.get('dag_run', {}), 'dag_id') else 'unknown'}
Task: {context.get('task_instance', {}).task_id if hasattr(context.get('task_instance', {}), 'task_id') else 'unknown'}
Время: {datetime.now().isoformat()}
Ошибка: {str(exception)}
"""
            logger.error(message)
            # Опционально: отправка в Slack/Teams/Email
            NotificationUtils._send_external_notification(message, 'error')
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления об ошибке: {e}")

    @staticmethod
    def _send_external_notification(message: str, notification_type: str) -> None:
        """Отправка внешнего уведомления (Slack, Teams, etc.)"""
        try:
            webhook_url = os.getenv('NOTIFICATION_WEBHOOK_URL')
            if not webhook_url:
                return

            import requests  # локальный импорт для необязательной зависимости

            payload = {
                'text': message,
                'type': notification_type,
                'timestamp': datetime.now().isoformat(),
            }

            response = requests.post(
                webhook_url,
                json=payload,
                timeout=10,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                logger.info("Внешнее уведомление отправлено успешно")
            else:
                logger.warning(f"Внешнее уведомление не отправлено: {response.status_code}")
        except Exception as e:
            logger.warning(f"Ошибка отправки внешнего уведомления: {e}")


class ErrorHandlingUtils:
    """Утилиты для обработки ошибок"""

    @staticmethod
    def handle_processing_error(
        context: Dict[str, Any],
        error: Exception,
        stage: str = "unknown"
    ) -> None:
        """Централизованная обработка ошибок"""
        try:
            error_info: Dict[str, Any] = {
                'stage': stage,
                'error_type': type(error).__name__,
                'error_message': str(error),
                'timestamp': datetime.now().isoformat(),
            }

            # Определяем тип ошибки для принятия решений
            if "TableData is not JSON serializable" in str(error):
                error_info['category'] = 'serialization_error'
                error_info['suggested_action'] = 'disable_table_extraction'
                logger.warning("Обнаружена ошибка сериализации таблиц - рекомендуется повторить без extract_tables")
            elif "Permission denied" in str(error):
                error_info['category'] = 'permission_error'
                error_info['suggested_action'] = 'check_file_permissions'
                logger.error("Ошибка прав доступа к файлу или директории")
            elif "No such file or directory" in str(error):
                error_info['category'] = 'file_not_found'
                error_info['suggested_action'] = 'verify_file_path'
                logger.error("Файл или директория не найдены")
            elif "Connection" in str(error) or "timeout" in str(error).lower():
                error_info['category'] = 'network_error'
                error_info['suggested_action'] = 'retry_with_backoff'
                logger.error("Сетевая ошибка или таймаут")
            else:
                error_info['category'] = 'general_error'
                error_info['suggested_action'] = 'investigate_logs'
                logger.error(f"Ошибка обработки [{stage}]: {error_info}")

            # Запись метрик об ошибке
            MetricsUtils.record_processing_metrics(
                dag_id=context.get('dag_run', {}).dag_id if hasattr(context.get('dag_run', {}), 'dag_id') else 'unknown',
                task_id=context.get('task_instance', {}).task_id if hasattr(context.get('task_instance', {}), 'task_id') else 'unknown',
                processing_time=0,
                success=False,
                error_category=error_info['category'],
                error_type=error_info['error_type']
            )

            # Отправка уведомления
            NotificationUtils.send_failure_notification(context, error)
        except Exception as e:
            logger.critical(f"Критическая ошибка в обработчике ошибок: {e}")


class VLLMUtils:
    """Утилиты для работы с vLLM API и форматированием сообщений Qwen-VL"""

    @staticmethod
    def format_qwen_vl_message(prompt: str, role: str = "user") -> Dict[str, Any]:
        """
        Формирует корректное мультимодальное сообщение для Qwen2.5-VL
        в OpenAI-совместимом формате vLLM: content как список частей.
        """
        return {
            "role": role,
            "content": [{"type": "text", "text": prompt}],
        }

    @staticmethod
    def is_vllm_available(endpoint: str, timeout: float = 5.0) -> bool:
        """
        Проверка доступности vLLM сервиса.
        Сначала пробуем /health (если проксировано), затем /v1/models.
        """
        try:
            import requests  # локальный импорт для необязательной зависимости

            base = endpoint
            # отбрасываем путь до /v1/...
            if '/v1/' in base:
                base = base.split('/v1/')

            # Попытка /health
            try:
                resp = requests.get(f"{base}/health", timeout=timeout)
                if resp.status_code == 200:
                    return True
            except Exception:
                pass

            # Попытка /v1/models
            try:
                resp = requests.get(f"{base}/v1/models", timeout=timeout)
                return resp.status_code == 200
            except Exception:
                return False
        except Exception:
            return False

    @staticmethod
    def log_vllm_error(response: Any, context: str = "") -> None:
        """
        Логирование ошибок vLLM с учетом частых кодов и подсказок по причинам.
        """
        try:
            status = getattr(response, "status_code", None)
            text_preview = ""
            try:
                text_preview = response.text[:200] if hasattr(response, "text") else ""
            except Exception:
                text_preview = ""

            if status == 500:
                logger.error(f"vLLM 500 в {context}: internal error (возможна ошибка препроцессинга payload). {text_preview}")
            elif status == 503:
                logger.warning(f"vLLM 503 в {context}: service overloaded/unavailable. {text_preview}")
            elif status == 429:
                logger.warning(f"vLLM 429 в {context}: rate limited. {text_preview}")
            else:
                logger.error(f"vLLM {status} в {context}: {text_preview}")
        except Exception as e:
            logger.error(f"Не удалось залогировать ошибку vLLM ({context}): {e}")


def _initialize_shared_utils() -> bool:
    """Инициализация модуля shared_utils"""
    try:
        # Проверяем доступность необходимых директорий
        paths = ConfigUtils.get_processing_paths()
        logger.info(f"Инициализированы пути обработки: {list(paths.keys())}")

        # Проверяем конфигурацию сервисов
        services = ConfigUtils.get_service_urls()
        logger.info(f"Настроены URL сервисов: {list(services.keys())}")
        return True
    except Exception as e:
        logger.error(f"Ошибка инициализации shared_utils: {e}")
        return False


# Выполняем инициализацию при импорте
_initialization_success = _initialize_shared_utils()
if not _initialization_success:
    logger.warning("shared_utils инициализирован с предупреждениями")
