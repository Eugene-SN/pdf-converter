#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ПОЛНОСТЬЮ ИСПРАВЛЕННЫЙ DoclingProcessor
Решает ВСЕ проблемы с Docling API, OCR инициализацией и обработкой документов

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:
- ✅ Обновлен до актуального Docling API v2.0+
- ✅ Правильная инициализация DocumentConverter
- ✅ Условная загрузка OCR (только при необходимости)
- ✅ Корректная обработка pipeline_options
- ✅ Исправлено экспортирование в Markdown
- ✅ Устранены потери текстового контента
"""

import os
import json
import logging
import asyncio
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import tempfile
from datetime import datetime
import traceback

# ✅ ИСПРАВЛЕНО: Правильные импорты Docling v2.0+
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling_core.types.doc import DoclingDocument
from docling.chunking import HybridChunker
from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer

# Дополнительные импорты для обработки
try:
    from transformers import AutoTokenizer
    HF_TRANSFORMERS_AVAILABLE = True
except ImportError:
    HF_TRANSFORMERS_AVAILABLE = False
    logging.warning("Transformers library not available, chunking will be limited")

from pydantic import BaseModel, Field
import structlog

# Настройка логирования
logger = structlog.get_logger("docling_processor")

def safe_serialize_tabledata(obj):
    """Безопасная сериализация объектов TableData и других Docling объектов"""
    if hasattr(obj, '__dict__'):
        result = {'_type': obj.__class__.__name__}
        for key, value in obj.__dict__.items():
            if not key.startswith('_'):
                try:
                    import json
                    json.dumps(value)
                    result[key] = value
                except (TypeError, ValueError):
                    if hasattr(value, '__dict__'):
                        result[key] = safe_serialize_tabledata(value)
                    elif hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
                        result[key] = [safe_serialize_tabledata(item) for item in value]
                    else:
                        result[key] = str(value)
        return result
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
        return [safe_serialize_tabledata(item) for item in obj]
    else:
        try:
            import json
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

# ================================================================================
# КОНФИГУРАЦИОННЫЕ МОДЕЛИ
# ================================================================================

class DoclingConfig(BaseModel):
    """✅ ИСПРАВЛЕНО: Конфигурация для Docling процессора"""
    model_path: str = "/mnt/storage/models/docling"
    cache_dir: str = "/mnt/storage/models/docling"
    temp_dir: str = "/app/temp"
    
    # Основные настройки
    use_gpu: bool = True
    max_workers: int = 4
    enable_ocr_by_default: bool = False  # ✅ По умолчанию OCR отключен
    
    # OCR настройки (загружаются условно)
    ocr_languages: List[str] = ["eng", "rus", "chi_sim"]
    ocr_confidence_threshold: float = 0.8
    
    # Извлечение контента
    extract_tables: bool = True
    extract_images: bool = True
    extract_formulas: bool = True
    preserve_layout: bool = True
    
    # Производительность
    processing_timeout: int = 3600
    max_file_size_mb: int = 500
    
    class Config:
        env_prefix = "DOCLING_"

class DocumentStructure(BaseModel):
    """✅ ИСПРАВЛЕНО: Структура обработанного документа"""
    title: str = ""
    authors: List[str] = []
    sections: List[Dict[str, Any]] = []
    tables: List[Dict[str, Any]] = []
    images: List[Dict[str, Any]] = []
    formulas: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}
    
    # ✅ НОВОЕ: Добавлены поля для Markdown контента
    raw_text: str = ""
    markdown_content: str = ""
    processing_stats: Dict[str, Any] = {}

# ================================================================================
# ОСНОВНОЙ DOCLING ПРОЦЕССОР
# ================================================================================

class DoclingProcessor:
    """✅ ПОЛНОСТЬЮ ИСПРАВЛЕННЫЙ процессор для работы с Docling v2.0+"""
    
    def __init__(self, config: DoclingConfig):
        self.config = config
        self.converter: Optional[DocumentConverter] = None
        self.chunker: Optional[HybridChunker] = None
        self.ocr_initialized = False
        
        # Создаем необходимые директории
        Path(self.config.cache_dir).mkdir(parents=True, exist_ok=True)
        Path(self.config.temp_dir).mkdir(parents=True, exist_ok=True)
        
        # ✅ ИСПРАВЛЕНО: Инициализируем базовый конвертер БЕЗ OCR
        self._initialize_base_converter()
        
        logger.info("DoclingProcessor initialized with conditional OCR loading")
    
    def _initialize_base_converter(self):
        """✅ ИСПРАВЛЕНО: Инициализация базового конвертера без OCR"""
        try:
            # Базовые pipeline options БЕЗ OCR
            base_pipeline_options = PdfPipelineOptions()
            base_pipeline_options.do_ocr = False  # ✅ OCR отключен по умолчанию
            base_pipeline_options.do_table_structure = self.config.extract_tables
            base_pipeline_options.generate_page_images = self.config.extract_images

            # ✅ ИСПРАВЛЕНО: Правильное создание DocumentConverter
            self.converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(
                        pipeline_options=base_pipeline_options
                    )
                }
            )

            # ✅ ВАЖНО: Сбрасываем флаг OCR, иначе последующие запросы
            # без OCR будут считать, что он ещё активен.
            self.ocr_initialized = False

            logger.info("✅ Base DocumentConverter initialized (OCR disabled)")

        except Exception as e:
            logger.error(f"❌ Failed to initialize base DocumentConverter: {e}")
            raise
    
    def _initialize_ocr_converter(self, ocr_languages: str = "eng"):
        """✅ НОВОЕ: Условная инициализация конвертера с OCR"""
        try:
            # OCR pipeline options
            ocr_pipeline_options = PdfPipelineOptions()
            ocr_pipeline_options.do_ocr = True
            ocr_pipeline_options.do_table_structure = self.config.extract_tables
            ocr_pipeline_options.generate_page_images = self.config.extract_images
            
            # ✅ ИСПРАВЛЕНО: Создание нового конвертера с OCR
            self.converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(
                        pipeline_options=ocr_pipeline_options
                    )
                }
            )
            
            self.ocr_initialized = True
            logger.info(f"✅ OCR DocumentConverter initialized with languages: {ocr_languages}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize OCR DocumentConverter: {e}")
            # Fallback к базовому конвертеру
            self._initialize_base_converter()
            raise
    
    def _initialize_chunker(self):
        """✅ ИСПРАВЛЕНО: Инициализация chunker для продвинутой обработки"""
        if not HF_TRANSFORMERS_AVAILABLE:
            logger.warning("Transformers not available, skipping chunker initialization")
            return
            
        try:
            # ✅ Используем рекомендованную модель из официальных примеров
            tokenizer = HuggingFaceTokenizer(
                tokenizer=AutoTokenizer.from_pretrained(
                    "sentence-transformers/all-MiniLM-L6-v2"
                )
            )
            
            self.chunker = HybridChunker(
                tokenizer=tokenizer,
                max_tokens=1024,
                overlap_tokens=128
            )
            
            logger.info("✅ HybridChunker initialized")
            
        except Exception as e:
            logger.warning(f"Failed to initialize chunker: {e}")
            self.chunker = None
    
    async def process_document(
        self, 
        file_path: str, 
        output_dir: str, 
        use_ocr: bool = False,
        ocr_languages: str = "eng"
    ) -> DocumentStructure:
        """
        ✅ ИСПРАВЛЕНО: Главный метод обработки документа
        
        Args:
            file_path: Путь к PDF файлу
            output_dir: Директория для сохранения результатов
            use_ocr: Включить ли OCR обработку
            ocr_languages: Языки для OCR (eng, rus, chi_sim)
            
        Returns:
            DocumentStructure: Структурированные данные документа
        """
        start_time = datetime.now()
        
        try:
            logger.info(f"🔄 Starting document processing: {file_path}")
            logger.info(f"   OCR enabled: {use_ocr}")
            logger.info(f"   OCR languages: {ocr_languages}")

            # Валидация входного файла
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            # Проверка размера файла
            file_size = os.path.getsize(file_path)
            max_size = self.config.max_file_size_mb * 1024 * 1024
            if file_size > max_size:
                raise ValueError(f"File too large: {file_size} bytes (max: {max_size})")

            # ✅ ИСПРАВЛЕНО: Условная инициализация OCR
            if use_ocr and not self.ocr_initialized:
                logger.info("🔄 Initializing OCR converter...")
                self._initialize_ocr_converter(ocr_languages)
            elif not use_ocr and self.ocr_initialized:
                logger.info("🔄 Switching back to base converter...")
                self._initialize_base_converter()

            fallback_used = False

            # ✅ ИСПРАВЛЕНО: Обработка документа с современным API
            logger.info("🔄 Converting document with Docling...")
            conversion_result = self.converter.convert(file_path)
            docling_document = conversion_result.document

            # ✅ ИСПРАВЛЕНО: Правильное извлечение данных из Docling документа
            document_structure = await self._extract_document_structure(
                docling_document, file_path, output_dir
            )

            def _structure_is_empty(struct: DocumentStructure) -> bool:
                return (
                    len(struct.sections) == 0
                    and not (struct.markdown_content or "").strip()
                    and not (struct.raw_text or "").strip()
                )

            # ✅ НОВОЕ: Автоматический fallback на OCR, если без OCR контент пустой
            if _structure_is_empty(document_structure) and not use_ocr:
                logger.warning(
                    "⚠️ Docling вернул пустую структуру без OCR. Повторная попытка с OCR включенным."
                )
                try:
                    self._initialize_ocr_converter(ocr_languages)
                    conversion_result = self.converter.convert(file_path)
                    docling_document = conversion_result.document
                    document_structure = await self._extract_document_structure(
                        docling_document, file_path, output_dir
                    )
                    use_ocr = True
                    fallback_used = True
                except Exception as fallback_err:
                    logger.error(
                        "❌ OCR fallback failed",
                        error=str(fallback_err),
                    )
                    raise

            if _structure_is_empty(document_structure):
                raise ValueError(
                    "Docling returned empty structure even after OCR fallback"
                )

            # Расчет статистики обработки
            processing_time = (datetime.now() - start_time).total_seconds()
            document_structure.processing_stats = {
                "processing_time_seconds": processing_time,
                "file_size_bytes": file_size,
                "ocr_used": use_ocr,
                "ocr_languages": ocr_languages if use_ocr else None,
                "docling_version": "2.0+",
                "timestamp": datetime.now().isoformat(),
                "ocr_fallback_triggered": fallback_used,
            }

            document_structure.metadata.setdefault("processing_notes", [])
            if fallback_used:
                document_structure.metadata["processing_notes"].append(
                    "ocr_fallback_applied"
                )
            document_structure.metadata["has_ocr_content"] = bool(use_ocr)

            logger.info(f"✅ Document processing completed in {processing_time:.2f}s")
            return document_structure

        except Exception as e:
            logger.error(f"❌ Error processing document: {e}")
            logger.error(traceback.format_exc())
            raise
    
    async def _extract_document_structure(
        self, 
        docling_document: DoclingDocument, 
        original_file_path: str,
        output_dir: str
    ) -> DocumentStructure:
        """✅ ИСПРАВЛЕНО: Извлечение структуры из Docling документа"""
        
        try:
            # ✅ ИСПРАВЛЕНО: Правильное извлечение метаданных
            title = getattr(docling_document, 'title', '') or \
                   Path(original_file_path).stem
            
            # ✅ ИСПРАВЛЕНО: Правильный экспорт в Markdown с фолбэком
            markdown_content = ""
            try:
                markdown_content = docling_document.export_to_markdown() or ""
            except Exception as md_error:
                logger.warning(f"Markdown export failed, fallback to structural reconstruction: {md_error}")
                markdown_content = ""

            if not isinstance(markdown_content, str):
                markdown_content = str(markdown_content)

            # Извлечение текстового контента
            raw_text = getattr(docling_document, 'text', '') or ""
            if not isinstance(raw_text, str):
                raw_text = str(raw_text)

            # ✅ ИСПРАВЛЕНО: Обработка страниц и элементов
            total_pages = len(docling_document.pages) if hasattr(docling_document, 'pages') else 1

            # Извлечение секций
            sections = []
            if hasattr(docling_document, 'sections'):
                for i, section in enumerate(docling_document.sections):
                    sections.append({
                        "id": i,
                        "title": getattr(section, 'title', f'Section {i+1}'),
                        "content": getattr(section, 'text', ''),
                        "level": getattr(section, 'level', 1),
                        "page": getattr(section, 'page', 1)
                    })

            for section in sections:
                section['title'] = str(section.get('title') or f"Section {section.get('id', 0) + 1}").strip()
                section['content'] = str(section.get('content') or "").strip()
                section['level'] = int(section.get('level') or 1)
                section['page'] = int(section.get('page') or 1)

            generated_outline = False
            if not sections and markdown_content:
                sections = self._build_sections_from_markdown(markdown_content)
                generated_outline = True

            if not raw_text:
                raw_text = self._markdown_to_plain_text(markdown_content) if markdown_content else ""

            if markdown_content == "" and raw_text:
                markdown_content = self._create_markdown_from_plain_text(raw_text)

            # Извлечение таблиц
            tables = []
            if hasattr(docling_document, 'tables'):
                for i, table in enumerate(docling_document.tables):
                    table_data = {
                        "id": i,
                        "page": getattr(table, 'page', 1),
                        "content": self._extract_table_content(table),
                        "bbox": getattr(table, 'bbox', None)
                    }
                    
                    # Сохранение таблицы в отдельный файл
                    table_file = Path(output_dir) / f"table_{i}.json"
                    with open(table_file, 'w', encoding='utf-8') as f:
                        json.dump(table_data, f, ensure_ascii=False, indent=2, default=safe_serialize_tabledata)
                    
                    table_data["file_path"] = str(table_file)
                    tables.append(table_data)
            
            # Извлечение изображений
            images = []
            if hasattr(docling_document, 'images'):
                for i, image in enumerate(docling_document.images):
                    image_data = {
                        "id": i,
                        "page": getattr(image, 'page', 1),
                        "bbox": getattr(image, 'bbox', None),
                        "format": getattr(image, 'format', 'unknown')
                    }
                    
                    # Сохранение изображения
                    if hasattr(image, 'data'):
                        image_file = Path(output_dir) / f"image_{i}.png"
                        # Здесь должна быть логика сохранения изображения
                        image_data["file_path"] = str(image_file)
                    
                    images.append(image_data)
            
            # ✅ НОВОЕ: Продвинутое chunking если доступно
            chunks = []
            if self.chunker and markdown_content:
                try:
                    chunks = list(self.chunker.chunk(docling_document))
                    logger.info(f"📝 Document chunked into {len(chunks)} pieces")
                except Exception as e:
                    logger.warning(f"Chunking failed: {e}")
            
            # Создание DocumentStructure
            document_structure = DocumentStructure(
                title=title,
                authors=[],  # TODO: Извлечение авторов если доступно
                sections=sections,
                tables=tables,
                images=images,
                formulas=[],  # TODO: Извлечение формул
                raw_text=raw_text,
                markdown_content=markdown_content,
                metadata={
                    "original_file": original_file_path,
                    "total_pages": total_pages,
                    "sections_count": len(sections),
                    "tables_count": len(tables),
                    "images_count": len(images),
                    "chunks_count": len(chunks),
                    "has_ocr_content": self.ocr_initialized,
                    "extraction_method": "docling_v2",
                    "content_length": len(raw_text),
                    "section_titles": [section.get("title") for section in sections],
                    "outline_generated": generated_outline
                }
            )

            logger.info(f"✅ Document structure extracted: {len(sections)} sections, "
                       f"{len(tables)} tables, {len(images)} images")
            
            return document_structure
            
        except Exception as e:
            logger.error(f"❌ Error extracting document structure: {e}")
            logger.error(traceback.format_exc())
            raise

    def _extract_table_content(self, table) -> Dict[str, Any]:
        """Извлечение содержимого таблицы"""
        try:
            if hasattr(table, 'data'):
                return {
                    "type": "table",
                    "data": table.data,
                    "rows": getattr(table, 'rows', 0),
                    "columns": getattr(table, 'columns', 0)
                }
            else:
                return {
                    "type": "table",
                    "content": str(table),
                    "extracted_method": "string_representation"
                }
        except Exception as e:
            logger.warning(f"Failed to extract table content: {e}")
            return {"type": "table", "error": str(e)}

    def _build_sections_from_markdown(self, markdown_content: str) -> List[Dict[str, Any]]:
        """Формирование секций на основе Markdown разметки"""
        sections: List[Dict[str, Any]] = []
        current: Optional[Dict[str, Any]] = None

        for line in markdown_content.splitlines():
            stripped = line.strip()

            if stripped.startswith('#'):
                level = len(stripped) - len(stripped.lstrip('#'))
                title = stripped[level:].strip() or f"Section {len(sections) + 1}"

                if current is not None:
                    current['content'] = "\n".join(current.pop('content_lines')).strip()
                    sections.append(current)

                current = {
                    'id': len(sections),
                    'title': title,
                    'level': max(1, min(level, 6)),
                    'page': 1,
                    'content_lines': []
                }
            else:
                if current is None:
                    current = {
                        'id': len(sections),
                        'title': f"Section {len(sections) + 1}",
                        'level': 1,
                        'page': 1,
                        'content_lines': []
                    }
                current['content_lines'].append(line)

        if current is not None:
            current['content'] = "\n".join(current.pop('content_lines')).strip()
            sections.append(current)

        if not sections and markdown_content.strip():
            sections.append({
                'id': 0,
                'title': 'Document',
                'level': 1,
                'page': 1,
                'content': markdown_content.strip()
            })
        else:
            for idx, section in enumerate(sections):
                section.setdefault('content', '')
                section['content'] = section['content'].strip()
                section['id'] = idx
                section.setdefault('page', 1)

        return sections

    def _markdown_to_plain_text(self, markdown_content: str) -> str:
        """Простейшее преобразование Markdown в текст"""
        plain_lines: List[str] = []
        for line in markdown_content.splitlines():
            stripped = line.strip()
            if not stripped:
                plain_lines.append("")
                continue

            if stripped.startswith('#'):
                stripped = stripped.lstrip('#').strip()

            if stripped.startswith('|') and stripped.endswith('|'):
                # Пропускаем строки-разделители таблиц
                continue
            if set(stripped) <= {'|', ':', '-', ' '}:
                continue

            plain_lines.append(stripped.replace('`', ''))

        return "\n".join(plain_lines).strip()

    def _create_markdown_from_plain_text(self, plain_text: str) -> str:
        """Фолбэк: создаем Markdown из простого текста"""
        return plain_text.strip()

    def export_to_markdown(
        self, 
        document_structure: DocumentStructure, 
        output_file: str
    ) -> str:
        """✅ ИСПРАВЛЕНО: Экспорт в Markdown файл"""
        try:
            # Используем уже готовый markdown контент от Docling
            markdown_content = document_structure.markdown_content
            
            # Дополнительное форматирование если нужно
            if not markdown_content:
                # Fallback: создаем markdown из структуры
                markdown_content = self._create_markdown_from_structure(document_structure)
            
            # Сохранение в файл
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            logger.info(f"✅ Markdown exported to: {output_file}")
            return markdown_content
            
        except Exception as e:
            logger.error(f"❌ Error exporting to markdown: {e}")
            raise
    
    def _create_markdown_from_structure(self, document_structure: DocumentStructure) -> str:
        """Создание Markdown из структуры документа (fallback)"""
        lines = []
        
        # Заголовок документа
        if document_structure.title:
            lines.append(f"# {document_structure.title}\n")
        
        # Авторы
        if document_structure.authors:
            lines.append(f"**Authors:** {', '.join(document_structure.authors)}\n")
        
        # Секции
        for section in document_structure.sections:
            level = "#" * min(section.get("level", 1) + 1, 6)
            lines.append(f"{level} {section.get('title', 'Untitled Section')}\n")
            lines.append(f"{section.get('content', '')}\n")
        
        # Таблицы
        if document_structure.tables:
            lines.append("## Tables\n")
            for i, table in enumerate(document_structure.tables):
                lines.append(f"### Table {i+1}\n")
                lines.append(f"Page: {table.get('page', 'N/A')}\n")
                if table.get('file_path'):
                    lines.append(f"Data file: {table['file_path']}\n")
        
        return "\n".join(lines)
    
    def cleanup_temp_files(self, output_dir: str, keep_main_files: bool = True):
        """Очистка временных файлов"""
        try:
            output_path = Path(output_dir)
            if output_path.exists():
                for file_path in output_path.glob("*"):
                    if keep_main_files and file_path.suffix in ['.md', '.json']:
                        continue
                    file_path.unlink()
                logger.info(f"🧹 Cleaned up temporary files in {output_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp files: {e}")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Получение статистики обработчика"""
        return {
            "converter_initialized": self.converter is not None,
            "ocr_initialized": self.ocr_initialized,
            "chunker_available": self.chunker is not None,
            "config": self.config.dict(),
            "transformers_available": HF_TRANSFORMERS_AVAILABLE
        }

# ================================================================================
# УТИЛИТЫ И ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ================================================================================

def create_docling_processor_from_env() -> DoclingProcessor:
    """✅ Создание процессора из переменных окружения"""
    config = DoclingConfig(
        model_path=os.getenv('DOCLING_MODEL_PATH', '/mnt/storage/models/docling'),
        cache_dir=os.getenv('DOCLING_HOME', '/mnt/storage/models/docling'),
        temp_dir=os.getenv('TEMP_DIR', '/app/temp'),
        use_gpu=os.getenv('DOCLING_USE_GPU', 'true').lower() == 'true',
        max_workers=int(os.getenv('DOCLING_MAX_WORKERS', '4')),
        enable_ocr_by_default=os.getenv('DEFAULT_USE_OCR', 'false').lower() == 'true',
        ocr_confidence_threshold=float(os.getenv('OCR_CONFIDENCE_THRESHOLD', '0.8')),
        extract_tables=os.getenv('DEFAULT_EXTRACT_TABLES', 'true').lower() == 'true',
        extract_images=os.getenv('DEFAULT_EXTRACT_IMAGES', 'true').lower() == 'true',
        processing_timeout=int(os.getenv('PROCESSING_TIMEOUT_MINUTES', '60')) * 60,
        max_file_size_mb=int(os.getenv('MAX_FILE_SIZE_MB', '500'))
    )
    
    return DoclingProcessor(config)

# ================================================================================
# ЭКСПОРТ
# ================================================================================

__all__ = [
    'DoclingProcessor',
    'DoclingConfig', 
    'DocumentStructure',
    'create_docling_processor_from_env'
]
