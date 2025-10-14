#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flask Document Processor Gateway
================================

Используется как точка входа для загрузки PDF-файлов и делегирует
конвертацию специализированному сервису `document-processor`, где
развёрнут Docling. Если сервис недоступен, применяется упрощённый
fallback-процесс.
"""

import os
import json
import logging
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import uuid
import tempfile
import shutil

import requests
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
from werkzeug.exceptions import BadRequest

# Fallback библиотеки
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация Flask приложения
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB максимум

# Конфигурация
WORK_DIR = os.getenv('WORK_DIR', '/tmp/document_processor')
DOCUMENT_PROCESSOR_URL = os.getenv('DOCUMENT_PROCESSOR_URL', 'http://document-processor:8001').rstrip('/')
DOCUMENT_PROCESSOR_TIMEOUT = float(os.getenv('DOCUMENT_PROCESSOR_TIMEOUT', '120'))
DOCUMENT_PROCESSOR_HEALTH_TIMEOUT = float(os.getenv('DOCUMENT_PROCESSOR_HEALTH_TIMEOUT', '5'))
ALLOWED_EXTENSIONS = {'pdf'}

# Убеждаемся что рабочая директория существует
os.makedirs(WORK_DIR, exist_ok=True)


class DocumentProcessorUnavailable(RuntimeError):
    """Выбрасывается при недоступности сервиса document-processor."""


class DocumentProcessorError(RuntimeError):
    """Выбрасывается, когда document-processor вернул ошибку."""

    def __init__(self, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message


def allowed_file(filename: str) -> bool:
    """Проверка допустимого расширения файла"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def extract_processing_options(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """Извлечение и валидация опций обработки из формы"""
    try:
        if 'options' in form_data:
            if isinstance(form_data['options'], str):
                options = json.loads(form_data['options'])
            else:
                options = form_data['options']
        else:
            options = {}

        # Значения по умолчанию с валидацией типов
        processed_options = {
            'extract_tables': bool(options.get('extract_tables', True)),
            'extract_images': bool(options.get('extract_images', True)),
            'extract_formulas': bool(options.get('extract_formulas', True)),
            'use_ocr': bool(options.get('use_ocr', False)),
            'ocr_languages': str(options.get('ocr_languages', 'eng,chi_sim')),
            'high_quality_ocr': bool(options.get('high_quality_ocr', True)),
            'preserve_layout': bool(options.get('preserve_layout', True)),
            'enable_chunking': bool(options.get('enable_chunking', False))
        }

        logger.info(f"Обработанные опции: {processed_options}")
        return processed_options

    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON опций: {e}")
        raise BadRequest("Некорректный JSON в поле options")
    except Exception as e:
        logger.error(f"Ошибка обработки опций: {e}")
        # Возвращаем безопасные значения по умолчанию
        return {
            'extract_tables': True,
            'extract_images': True,
            'extract_formulas': True,
            'use_ocr': False,
            'ocr_languages': 'eng,chi_sim',
            'high_quality_ocr': True,
            'preserve_layout': True,
            'enable_chunking': False
        }


def check_document_processor_health() -> bool:
    """Проверяет доступность document-processor по /health."""
    health_url = f"{DOCUMENT_PROCESSOR_URL}/health"
    try:
        response = requests.get(health_url, timeout=DOCUMENT_PROCESSOR_HEALTH_TIMEOUT)
        if response.status_code == 200:
            return True
        logger.warning(
            "Документ-процессор ответил статусом %s: %s",
            response.status_code,
            response.text[:200]
        )
        return False
    except requests.RequestException as exc:
        logger.warning("Document-processor недоступен по %s: %s", health_url, exc)
        return False


DOCUMENT_PROCESSOR_AVAILABLE = check_document_processor_health()
if not DOCUMENT_PROCESSOR_AVAILABLE:
    logger.warning(
        "Docling недоступен через document-processor (%s) - будет использован fallback режим",
        DOCUMENT_PROCESSOR_URL,
    )


def forward_to_document_processor(file_path: str, filename: str, options: Dict[str, Any]) -> Dict[str, Any]:
    """Отправляет PDF в document-processor и возвращает результат."""
    process_url = f"{DOCUMENT_PROCESSOR_URL}/process"
    try:
        with open(file_path, 'rb') as file_stream:
            files = {'file': (filename, file_stream, 'application/pdf')}
            data = {'options': json.dumps(options, ensure_ascii=False)}
            response = requests.post(
                process_url,
                files=files,
                data=data,
                timeout=DOCUMENT_PROCESSOR_TIMEOUT
            )
    except requests.RequestException as exc:
        raise DocumentProcessorUnavailable(
            f"Не удалось подключиться к document-processor по адресу {process_url}: {exc}"
        ) from exc

    if response.status_code >= 500:
        raise DocumentProcessorUnavailable(
            f"document-processor вернул статус {response.status_code}: {response.text[:200]}"
        )

    if response.status_code >= 400:
        raise DocumentProcessorError(response.status_code, response.text)

    try:
        return response.json()
    except ValueError as exc:
        raise DocumentProcessorError(
            response.status_code,
            f"Некорректный JSON от document-processor: {response.text[:200]}"
        ) from exc


def process_with_fallback(file_path: str, reason: str) -> Dict[str, Any]:
    """Fallback обработка PDF без Docling"""
    start_time = time.time()

    try:
        filename = Path(file_path).stem
        file_size = os.path.getsize(file_path)

        # Пытаемся извлечь базовую информацию через PyMuPDF
        basic_content = f"# {filename}\n\n"
        pages_count = 1

        if PYMUPDF_AVAILABLE:
            try:
                doc = fitz.open(file_path)
                pages_count = doc.page_count

                # Извлекаем текст из первых нескольких страниц
                text_content = ""
                for page_num in range(min(5, pages_count)):  # Максимум 5 страниц
                    page = doc[page_num]
                    text_content += page.get_text()
                    if len(text_content) > 5000:  # Ограничение размера
                        text_content = text_content[:5000] + "...\n[Содержимое обрезано]"
                        break

                if text_content.strip():
                    basic_content += text_content
                else:
                    basic_content += "Документ обработан в fallback режиме (текст не извлечен).\n"

                doc.close()
            except Exception as e:
                logger.warning(f"Не удалось извлечь текст через PyMuPDF: {e}")
                basic_content += "Документ обработан в базовом fallback режиме.\n"
        else:
            basic_content += "Документ обработан в базовом fallback режиме (PyMuPDF недоступен).\n"

        basic_content += f"\nРазмер файла: {file_size / (1024*1024):.2f} MB\n"
        basic_content += f"Предполагаемое количество страниц: {pages_count}\n"

        # Подготовка промежуточного файла
        work_id = str(uuid.uuid4())
        intermediate_file = os.path.join(WORK_DIR, f"{work_id}_intermediate.json")

        intermediate_data = {
            'title': filename,
            'pages_count': pages_count,
            'markdown_content': basic_content,
            'raw_text': basic_content,
            'metadata': {
                'fallback_mode': True,
                'processing_timestamp': datetime.now().isoformat(),
                'file_size_bytes': file_size,
                'fallback_reason': reason
            },
            'work_id': work_id
        }

        with open(intermediate_file, 'w', encoding='utf-8') as f:
            json.dump(intermediate_data, f, ensure_ascii=False, indent=2)

        processing_time = time.time() - start_time

        return {
            'success': True,
            'document_id': work_id,
            'pages_count': pages_count,
            'processing_time': processing_time,
            'intermediate_file': intermediate_file,
            'output_files': [intermediate_file],
            'metadata': {
                'title': filename,
                'pages': pages_count,
                'processing_mode': 'fallback',
                'fallback_reason': reason
            }
        }

    except Exception as e:
        logger.error(f"Ошибка fallback обработки: {e}")
        raise


@app.route('/health', methods=['GET'])
def health_check():
    """Проверка состояния сервиса"""
    global DOCUMENT_PROCESSOR_AVAILABLE
    DOCUMENT_PROCESSOR_AVAILABLE = check_document_processor_health()
    return jsonify({
        'status': 'healthy',
        'service': 'document-processor-gateway',
        'version': '4.0',
        'document_processor_available': DOCUMENT_PROCESSOR_AVAILABLE,
        'pymupdf_available': PYMUPDF_AVAILABLE,
        'document_processor_url': DOCUMENT_PROCESSOR_URL,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/process', methods=['POST'])
def process_document():
    """Основной эндпоинт для обработки документов"""

    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'Файл не передан в запросе',
            'message': 'Требуется файл в поле "file"'
        }), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'Пустое имя файла',
            'message': 'Файл не выбран'
        }), 400

    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'error': 'Неподдерживаемый формат файла',
            'message': f'Поддерживаются только: {", ".join(ALLOWED_EXTENSIONS)}'
        }), 400

    temp_dir = tempfile.mkdtemp(prefix='docproc_', dir=WORK_DIR)
    temp_file = None

    try:
        filename = secure_filename(file.filename)
        temp_file = os.path.join(temp_dir, filename)
        file.save(temp_file)

        logger.info(f"Получен файл для обработки: {filename} ({os.path.getsize(temp_file)} байт)")

        options = extract_processing_options(request.form.to_dict())

        try:
            result = forward_to_document_processor(temp_file, filename, options)
            logger.info(
                "Документ обработан через document-processor: %s",
                result.get('document_id')
            )
            global DOCUMENT_PROCESSOR_AVAILABLE
            DOCUMENT_PROCESSOR_AVAILABLE = True
            return jsonify(result)
        except DocumentProcessorError as e:
            logger.error(f"document-processor вернул ошибку: {e}")
            return jsonify({
                'success': False,
                'error': 'document_processor_error',
                'message': str(e)
            }), 502
        except DocumentProcessorUnavailable as e:
            logger.warning(f"Document-processor недоступен: {e}")
            DOCUMENT_PROCESSOR_AVAILABLE = False
            fallback_result = process_with_fallback(
                temp_file,
                reason='document_processor_unavailable'
            )
            return jsonify(fallback_result)

    except BadRequest as e:
        logger.error(f"Ошибка валидации запроса: {e}")
        return jsonify({
            'success': False,
            'error': 'Ошибка валидации запроса',
            'message': str(e)
        }), 400

    except Exception as e:
        logger.error(f"Критическая ошибка обработки: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': 'Внутренняя ошибка сервера',
            'message': f'Document processing failed: {str(e)}'
        }), 500

    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception as e:
                logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")

        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"Не удалось удалить временную директорию {temp_dir}: {e}")


@app.route('/download/<file_id>', methods=['GET'])
def download_file(file_id: str):
    """Скачивание обработанных файлов"""
    try:
        file_pattern = f"{file_id}_intermediate.json"
        file_path = os.path.join(WORK_DIR, file_pattern)

        if not os.path.exists(file_path):
            return jsonify({
                'success': False,
                'error': 'Файл не найден',
                'message': f'Файл с ID {file_id} не существует или был удален'
            }), 404

        return send_file(
            file_path,
            as_attachment=True,
            download_name=f"document_{file_id}.json",
            mimetype='application/json'
        )

    except Exception as e:
        logger.error(f"Ошибка скачивания файла {file_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Ошибка скачивания',
            'message': str(e)
        }), 500


@app.errorhandler(413)
def file_too_large(e):
    return jsonify({
        'success': False,
        'error': 'Файл слишком большой',
        'message': 'Максимальный размер файла: 500MB'
    }), 413


@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f"Внутренняя ошибка сервера: {e}")
    return jsonify({
        'success': False,
        'error': 'Внутренняя ошибка сервера',
        'message': 'Произошла неожиданная ошибка'
    }), 500


if __name__ == '__main__':
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', 8001))
    debug = os.getenv('DEBUG', 'false').lower() == 'true'

    logger.info(f"Запуск Document Processor Gateway на {host}:{port}")
    logger.info(f"Рабочая директория: {WORK_DIR}")
    logger.info(f"Document-processor URL: {DOCUMENT_PROCESSOR_URL}")
    logger.info(f"Document-processor доступен: {DOCUMENT_PROCESSOR_AVAILABLE}")
    logger.info(f"PyMuPDF доступен: {PYMUPDF_AVAILABLE}")

    app.run(host=host, port=port, debug=debug, threaded=True)
