#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flask Document Processor API
Исправленная версия для решения проблемы сериализации TableData и других объектов Docling
"""

import os
import json
import logging
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import uuid
import tempfile
import shutil

from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
from werkzeug.exceptions import BadRequest, InternalServerError

# Импорты для обработки PDF
try:
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.datamodel.document import Document
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    logging.warning("Docling не установлен - будет использован fallback режим")

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
ALLOWED_EXTENSIONS = {'pdf'}

# Убеждаемся что рабочая директория существует
os.makedirs(WORK_DIR, exist_ok=True)

class DoclingTableDataSerializer:
    """
    Класс для сериализации объектов Docling в JSON-совместимые структуры
    """
    
    @staticmethod
    def serialize_table_data(table_data) -> Dict[str, Any]:
        """Сериализация TableData в словарь"""
        try:
            if hasattr(table_data, 'table_cells'):
                # Извлекаем данные ячеек таблицы
                cells = []
                for cell in table_data.table_cells:
                    cell_data = {
                        'row_span': getattr(cell, 'row_span', 1),
                        'col_span': getattr(cell, 'col_span', 1),
                        'start_row_offset_idx': getattr(cell, 'start_row_offset_idx', 0),
                        'end_row_offset_idx': getattr(cell, 'end_row_offset_idx', 0),
                        'start_col_offset_idx': getattr(cell, 'start_col_offset_idx', 0),
                        'end_col_offset_idx': getattr(cell, 'end_col_offset_idx', 0),
                        'text': getattr(cell, 'text', ''),
                        'bbox': DoclingTableDataSerializer.serialize_bbox(getattr(cell, 'bbox', None))
                    }
                    cells.append(cell_data)
                
                return {
                    'type': 'table',
                    'table_cells': cells,
                    'num_rows': getattr(table_data, 'num_rows', 0),
                    'num_cols': getattr(table_data, 'num_cols', 0),
                    'bbox': DoclingTableDataSerializer.serialize_bbox(getattr(table_data, 'bbox', None))
                }
            else:
                # Fallback для неизвестной структуры
                return {
                    'type': 'table',
                    'raw_data': str(table_data),
                    'serialization_error': 'Unknown table structure'
                }
        except Exception as e:
            logger.error(f"Ошибка сериализации TableData: {e}")
            return {
                'type': 'table',
                'error': str(e),
                'raw_data': str(table_data)[:500]  # Ограниченный размер для безопасности
            }
    
    @staticmethod
    def serialize_bbox(bbox) -> Optional[Dict[str, float]]:
        """Сериализация bbox в словарь"""
        if bbox is None:
            return None
        try:
            return {
                'l': float(getattr(bbox, 'l', 0)),
                't': float(getattr(bbox, 't', 0)),
                'r': float(getattr(bbox, 'r', 0)),
                'b': float(getattr(bbox, 'b', 0))
            }
        except Exception:
            return None
    
    @staticmethod
    def serialize_docling_object(obj) -> Any:
        """Универсальный сериализатор для объектов Docling"""
        if obj is None:
            return None
        
        # Проверяем тип объекта
        obj_type = type(obj).__name__
        
        if 'TableData' in obj_type:
            return DoclingTableDataSerializer.serialize_table_data(obj)
        elif hasattr(obj, '__dict__'):
            # Общий подход для объектов с атрибутами
            result = {'type': obj_type}
            for key, value in obj.__dict__.items():
                if not key.startswith('_'):  # Пропускаем приватные атрибуты
                    result[key] = DoclingTableDataSerializer.serialize_docling_object(value)
            return result
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
            # Итерируемые объекты (списки, кортежи и т.д.)
            return [DoclingTableDataSerializer.serialize_docling_object(item) for item in obj]
        else:
            # Примитивные типы или строки
            try:
                json.dumps(obj)  # Проверка на JSON-сериализуемость
                return obj
            except (TypeError, ValueError):
                return str(obj)

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

def process_with_docling(file_path: str, options: Dict[str, Any]) -> Dict[str, Any]:
    """Обработка PDF с использованием Docling"""
    start_time = time.time()
    
    try:
        # Настройка пайплайна Docling
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = options.get('use_ocr', False)
        pipeline_options.do_table_structure = options.get('extract_tables', True)
        pipeline_options.generate_page_images = options.get('extract_images', True)
        
        # Создание конвертера
        converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
        
        logger.info(f"Запуск конвертации Docling (OCR: {pipeline_options.do_ocr})")
        
        # Конвертация
        result = converter.convert(file_path)
        document = result.document
        
        # Извлечение данных с безопасной сериализацией
        markdown_content = document.export_to_markdown()
        
        # Безопасная сериализация метаданных документа
        metadata = {}
        if hasattr(document, 'meta') and document.meta:
            for key, value in document.meta.__dict__.items():
                if not key.startswith('_'):
                    metadata[key] = DoclingTableDataSerializer.serialize_docling_object(value)
        
        # Безопасная сериализация основного содержимого
        content_data = []
        if hasattr(document, 'main_text') and document.main_text:
            for item in document.main_text:
                content_data.append(DoclingTableDataSerializer.serialize_docling_object(item))
        
        # Подготовка промежуточного файла
        work_id = str(uuid.uuid4())
        intermediate_file = os.path.join(WORK_DIR, f"{work_id}_intermediate.json")
        
        intermediate_data = {
            'title': getattr(document, 'title', '') or Path(file_path).stem,
            'pages_count': len(document.pages) if hasattr(document, 'pages') else 1,
            'markdown_content': markdown_content,
            'raw_text': markdown_content,  # Для совместимости
            'metadata': metadata,
            'content_data': content_data,
            'processing_options': options,
            'processing_timestamp': datetime.now().isoformat(),
            'docling_version': '2.0+',
            'work_id': work_id
        }
        
        # Сохранение промежуточного файла
        with open(intermediate_file, 'w', encoding='utf-8') as f:
            json.dump(intermediate_data, f, ensure_ascii=False, indent=2, default=str)
        
        processing_time = time.time() - start_time
        
        return {
            'success': True,
            'document_id': work_id,
            'pages_count': intermediate_data['pages_count'],
            'processing_time': processing_time,
            'intermediate_file': intermediate_file,
            'output_files': [intermediate_file],
            'metadata': {
                'title': intermediate_data['title'],
                'pages': intermediate_data['pages_count'],
                'processing_mode': 'docling',
                'ocr_used': options.get('use_ocr', False)
            }
        }
        
    except Exception as e:
        logger.error(f"Ошибка обработки Docling: {e}")
        logger.error(traceback.format_exc())
        
        # Если ошибка связана с сериализацией таблиц, пробуем без таблиц
        if "TableData" in str(e) and options.get('extract_tables', True):
            logger.warning("Повторная попытка обработки без извлечения таблиц")
            options_no_tables = {**options, 'extract_tables': False}
            return process_with_docling(file_path, options_no_tables)
        
        raise

def process_with_fallback(file_path: str, options: Dict[str, Any]) -> Dict[str, Any]:
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
                'file_size_bytes': file_size
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
                'fallback_reason': 'docling_unavailable' if not DOCLING_AVAILABLE else 'docling_error'
            }
        }
        
    except Exception as e:
        logger.error(f"Ошибка fallback обработки: {e}")
        raise

@app.route('/health', methods=['GET'])
def health_check():
    """Проверка состояния сервиса"""
    return jsonify({
        'status': 'healthy',
        'service': 'document-processor',
        'version': '4.0',
        'docling_available': DOCLING_AVAILABLE,
        'pymupdf_available': PYMUPDF_AVAILABLE,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/process', methods=['POST'])
def process_document():
    """Основной эндпоинт для обработки документов"""
    
    # Валидация запроса
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
    
    # Создание временного файла
    temp_dir = tempfile.mkdtemp(prefix='docproc_', dir=WORK_DIR)
    temp_file = None
    
    try:
        # Сохранение загруженного файла
        filename = secure_filename(file.filename)
        temp_file = os.path.join(temp_dir, filename)
        file.save(temp_file)
        
        logger.info(f"Получен файл для обработки: {filename} ({os.path.getsize(temp_file)} байт)")
        
        # Извлечение опций обработки
        options = extract_processing_options(request.form.to_dict())
        
        # Выбор метода обработки
        if DOCLING_AVAILABLE:
            try:
                result = process_with_docling(temp_file, options)
                logger.info(f"Документ обработан через Docling: {result['document_id']}")
            except Exception as e:
                logger.warning(f"Docling обработка не удалась, переключаемся на fallback: {e}")
                result = process_with_fallback(temp_file, options)
        else:
            logger.info("Используется fallback обработка (Docling недоступен)")
            result = process_with_fallback(temp_file, options)
        
        return jsonify(result)
        
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
        # Очистка временных файлов
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
        # Поиск файла по ID
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
    # Настройка для production
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', 8001))
    debug = os.getenv('DEBUG', 'false').lower() == 'true'
    
    logger.info(f"Запуск Document Processor API на {host}:{port}")
    logger.info(f"Рабочая директория: {WORK_DIR}")
    logger.info(f"Docling доступен: {DOCLING_AVAILABLE}")
    logger.info(f"PyMuPDF доступен: {PYMUPDF_AVAILABLE}")
    
    app.run(host=host, port=port, debug=debug, threaded=True)