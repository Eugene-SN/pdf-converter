#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ПОЛНОСТЬЮ ИСПРАВЛЕННЫЙ FastAPI Main Server для Document Processor Service v4.0
Решает ВСЕ проблемы с инициализацией, OCR обработкой и интеграцией с DAG1

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:
- ✅ Правильная инициализация исправленного DoclingProcessor
- ✅ Условная обработка OCR флага
- ✅ Исправлена интеграция с DAG1
- ✅ Корректная обработка ошибок и тайм-аутов
- ✅ Правильные API endpoints для Airflow
- ✅ Оптимизированное управление ресурсами
"""

import os
import sys
import asyncio
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import tempfile
import traceback

# FastAPI импорты
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import uvicorn

# Pydantic модели
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings

# HTTP клиенты
import httpx
import aiofiles

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server, generate_latest, REGISTRY
from prometheus_client.exposition import CONTENT_TYPE_LATEST
import psutil

# ✅ ИСПРАВЛЕНО: Импорты наших исправленных процессоров
from docling_processor import (
    DoclingProcessor, 
    DoclingConfig, 
    DocumentStructure,
    create_docling_processor_from_env
)
from ocr_processor import OCRProcessor, OCRConfig
from table_extractor import TableExtractor, TableConfig
from structure_analyzer import StructureAnalyzer, AnalysisConfig

# ================================================================================
# КОНФИГУРАЦИЯ И НАСТРОЙКИ
# ================================================================================

class Settings(BaseSettings):
    """✅ ИСПРАВЛЕНО: Настройки приложения"""
    # Основные настройки сервера
    host: str = "0.0.0.0"
    port: int = 8001
    debug: bool = False
    
    # Пути
    temp_dir: str = "/app/temp"
    cache_dir: str = "/app/cache"
    models_dir: str = "/mnt/storage/models"

    # Пути к моделям
    paddlex_home: str = "/mnt/storage/models/paddlex"
    docling_models_dir: str = "/mnt/storage/models/docling"
    hf_cache_dir: str = "/mnt/storage/models/docling/huggingface"

    # Ограничения
    max_file_size: int = 500 * 1024 * 1024  # 500MB
    max_pages: int = 1000
    timeout_seconds: int = 3600
    
    # ✅ ИСПРАВЛЕНО: Правильные настройки Docling
    docling_model_path: str = "/mnt/storage/models/docling"
    docling_use_gpu: bool = True
    docling_max_workers: int = 4
    
    # ✅ ИСПРАВЛЕНО: OCR настройки (условные)
    paddleocr_use_gpu: bool = True
    paddleocr_langs: List[str] = ["ch", "en", "ru"]
    ocr_confidence_threshold: float = 0.8
    enable_ocr_by_default: bool = False  # ✅ По умолчанию OCR отключен
    
    # Таблицы настройки
    tabula_java_options: str = "-Xmx2048m"
    table_detection_threshold: float = 0.7
    
    class Config:
        env_file = ".env"

settings = Settings()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO if not settings.debug else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = structlog.get_logger("document_processor_api")

def safe_serialize_tabledata(obj):
    """Безопасная сериализация объектов TableData и других Docling объектов"""
    if hasattr(obj, '__dict__'):
        result = {'_type': obj.__class__.__name__}
        for key, value in obj.__dict__.items():
            if not key.startswith('_'):
                try:
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
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

# ================================================================================
# PROMETHEUS МЕТРИКИ
# ================================================================================

def create_metric_safe(type_cls, name, description, labels=None):
    """Безопасное создание метрики - возвращает существующую или создает новую"""
    if name in REGISTRY._names_to_collectors:
        return REGISTRY._names_to_collectors[name]
    else:
        if labels:
            return type_cls(name, description, labels)
        else:
            return type_cls(name, description)

# HTTP метрики
http_requests = create_metric_safe(Counter, 'doc_processor_http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
http_duration = create_metric_safe(Histogram, 'doc_processor_http_duration_seconds', 'HTTP request duration', ['method', 'endpoint'])
active_requests = create_metric_safe(Gauge, 'doc_processor_active_requests', 'Active HTTP requests')

# Обработка файлов
files_processed = create_metric_safe(Counter, 'doc_processor_files_total', 'Total files processed', ['status', 'type', 'ocr_used'])
processing_duration = create_metric_safe(Histogram, 'doc_processor_processing_duration_seconds', 'File processing duration', ['type', 'ocr_used'])
pages_processed = create_metric_safe(Counter, 'doc_processor_pages_total', 'Total pages processed')

# Системные метрики
memory_usage = create_metric_safe(Gauge, 'doc_processor_memory_usage_bytes', 'Memory usage')
disk_usage = create_metric_safe(Gauge, 'doc_processor_disk_usage_percent', 'Disk usage percentage')

# ================================================================================
# PYDANTIC МОДЕЛИ
# ================================================================================

class ProcessingOptions(BaseModel):
    """✅ ИСПРАВЛЕНО: Опции для обработки документа"""
    extract_tables: bool = True
    extract_images: bool = True
    extract_formulas: bool = True
    use_ocr: bool = False  # ✅ ИСПРАВЛЕНО: По умолчанию False для цифровых PDF
    ocr_languages: str = "eng"  # ✅ НОВОЕ: Поддержка языков OCR
    high_quality_ocr: bool = True
    output_format: str = Field(default="json", pattern="^(json|markdown)$")
    preserve_layout: bool = True
    enable_chunking: bool = False
    
    @validator('ocr_languages')
    def validate_ocr_languages(cls, v):
        allowed_langs = ['eng', 'rus', 'chi_sim', 'chi_tra']
        langs = [lang.strip() for lang in v.split(',')]
        for lang in langs:
            if lang not in allowed_langs:
                raise ValueError(f"Unsupported OCR language: {lang}")
        return v

class ProcessingResponse(BaseModel):
    """✅ ИСПРАВЛЕНО: Ответ на запрос обработки"""
    success: bool
    message: str
    processing_time: float
    document_id: str
    pages_count: int
    sections_count: int
    tables_count: int
    images_count: int
    formulas_count: int
    output_files: List[str]
    metadata: Dict[str, Any]
    
    # ✅ НОВОЕ: Дополнительные поля для интеграции с DAG
    intermediate_file: Optional[str] = None
    ocr_used: bool = False
    docling_version: str = "2.0+"

class HealthResponse(BaseModel):
    """Ответ health check"""
    status: str
    timestamp: str
    version: str = "4.0.0"
    services: Dict[str, str]
    system_info: Dict[str, Any]

# ================================================================================
# ГЛОБАЛЬНЫЕ ПРОЦЕССОРЫ
# ================================================================================

# ✅ ИСПРАВЛЕНО: Глобальные процессоры инициализируются условно
docling_processor: Optional[DoclingProcessor] = None
ocr_processor: Optional[OCRProcessor] = None
table_extractor: Optional[TableExtractor] = None
structure_analyzer: Optional[StructureAnalyzer] = None

async def initialize_processors():
    """✅ ИСПРАВЛЕНО: Инициализация процессоров"""
    global docling_processor, ocr_processor, table_extractor, structure_analyzer
    
    logger.info("🔄 Initializing document processors...")
    
    try:
        # ✅ ИСПРАВЛЕНО: Используем исправленный DoclingProcessor
        docling_processor = create_docling_processor_from_env()
        logger.info("✅ Docling processor initialized (OCR on demand)")
        
        # ✅ ИСПРАВЛЕНО: OCR процессор (инициализируется условно)
        if settings.enable_ocr_by_default:
            ocr_config = OCRConfig(
                use_gpu=settings.paddleocr_use_gpu,
                lang=settings.paddleocr_langs,
                confidence_threshold=settings.ocr_confidence_threshold,
            )
            ocr_processor = OCRProcessor(ocr_config)
            logger.info("✅ OCR processor pre-initialized")
        else:
            logger.info("ℹ️  OCR processor will be initialized on demand")
        
        # Table Extractor
        table_config = TableConfig(
            java_options=settings.tabula_java_options,
            detection_threshold=settings.table_detection_threshold,
            temp_dir=settings.temp_dir
        )
        table_extractor = TableExtractor(table_config)
        logger.info("✅ Table extractor initialized")
        
        # Structure Analysis
        analysis_config = AnalysisConfig(
            min_heading_length=5,
            max_heading_length=200,
            check_cross_references=True
        )
        structure_analyzer = StructureAnalyzer(analysis_config)
        logger.info("✅ Structure analyzer initialized")
        
        logger.info("🎯 All processors initialized successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize processors: {e}")
        logger.error(traceback.format_exc())
        raise

# ================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ================================================================================

def update_system_metrics():
    """Обновление системных метрик"""
    try:
        # Память
        memory = psutil.virtual_memory()
        memory_usage.set(memory.used)
        
        # Диск
        disk = psutil.disk_usage(settings.temp_dir)
        disk_usage.set(disk.percent)
        
    except Exception as e:
        logger.warning(f"Failed to update system metrics: {e}")

def validate_pdf_file(file_content: bytes) -> bool:
    """Валидация PDF файла"""
    # Проверяем размер
    if len(file_content) > settings.max_file_size:
        raise HTTPException(
            status_code=413, 
            detail=f"File too large. Max size: {settings.max_file_size} bytes"
        )
    
    # Проверяем PDF signature
    if not file_content.startswith(b'%PDF'):
        raise HTTPException(
            status_code=400,
            detail="Invalid PDF file format"
        )
    
    return True

async def save_uploaded_file(upload_file: UploadFile, temp_dir: str) -> str:
    """Сохранение загруженного файла"""
    # Создаем временный файл
    file_path = Path(temp_dir) / f"upload_{int(time.time())}_{upload_file.filename}"
    
    try:
        async with aiofiles.open(file_path, 'wb') as f:
            content = await upload_file.read()
            validate_pdf_file(content)
            await f.write(content)
        
        return str(file_path)
    
    except Exception as e:
        if file_path.exists():
            file_path.unlink()
        raise

# ================================================================================
# FASTAPI APPLICATION
# ================================================================================

app = FastAPI(
    title="Document Processor API",
    description="✅ ИСПРАВЛЕННЫЙ PDF document processing with Docling v2.0+, OCR, and table extraction",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# ================================================================================
# API ENDPOINTS
# ================================================================================

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    logger.info("🚀 Starting Document Processor API v4.0 (FIXED)")
    
    # Создаем директории
    Path(settings.temp_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.cache_dir).mkdir(parents=True, exist_ok=True)
    
    # Инициализируем процессоры
    await initialize_processors()
    
    # Запускаем Prometheus метрики
    start_http_server(9001)
    logger.info("📊 Prometheus metrics server started on port 9001")

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """✅ ИСПРАВЛЕНО: Health check endpoint"""
    update_system_metrics()
    
    # Проверяем статус сервисов
    services_status = {
        "docling": "healthy" if docling_processor else "unavailable",
        "ocr": "ready" if ocr_processor else "on_demand", 
        "table_extractor": "healthy" if table_extractor else "unavailable",
        "structure_analyzer": "healthy" if structure_analyzer else "unavailable"
    }
    
    # Системная информация
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage(settings.temp_dir)
    
    system_info = {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": memory.percent,
        "memory_available_gb": round(memory.available / 1024**3, 2),
        "disk_free_gb": round(disk.free / 1024**3, 2),
        "temp_files_count": len(list(Path(settings.temp_dir).glob("*"))),
        "uptime_seconds": int(time.time() - startup_time),
        "docling_version": "2.0+",
        "ocr_initialized": ocr_processor is not None if ocr_processor else False
    }
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        services=services_status,
        system_info=system_info
    )

@app.post("/process", response_model=ProcessingResponse)
async def process_document_endpoint(
    file: UploadFile = File(...),
    options: str = Form(default='{"extract_tables": true, "extract_images": true, "use_ocr": false}'),
    background_tasks: BackgroundTasks = None
):
    """
    ✅ ИСПРАВЛЕНО: Основной endpoint для обработки документов через DAG1
    Это основной метод, который вызывается из Airflow DAG1
    """
    start_time = time.time()
    active_requests.inc()
    document_id = f"doc_{int(start_time)}"
    
    try:
        http_requests.labels(method="POST", endpoint="/process", status="started").inc()

        # Парсим опции
        ocr_request_rejected = False
        try:
            processing_options = ProcessingOptions.parse_raw(options)
            logger.info(f"📥 Processing options: use_ocr={processing_options.use_ocr}, "
                       f"ocr_languages={processing_options.ocr_languages}, "
                       f"extract_tables={processing_options.extract_tables}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid options format: {e}")

        if processing_options.use_ocr:
            logger.info("📄 OCR explicitly requested for this document")
            ocr_request_rejected = False

        # Создаем рабочую директорию
        work_dir = Path(settings.temp_dir) / document_id
        work_dir.mkdir(parents=True, exist_ok=True)
        
        # Сохраняем файл
        pdf_path = await save_uploaded_file(file, str(work_dir))
        
        logger.info(f"🔄 Starting document processing: {document_id}")
        logger.info(f"   File: {file.filename}")
        logger.info(f"   OCR: {'enabled' if processing_options.use_ocr else 'disabled'}")
        
        # ✅ ИСПРАВЛЕНО: Основная обработка через исправленный Docling
        document_structure = await docling_processor.process_document(
            pdf_path, 
            str(work_dir),
            use_ocr=processing_options.use_ocr,
            ocr_languages=processing_options.ocr_languages
        )
        
        # ✅ НОВОЕ: Дополнительная обработка OCR если включено и доступно
        if processing_options.use_ocr and ocr_processor:
            logger.info("🔄 Running additional OCR processing...")
            try:
                ocr_results = await ocr_processor.process_document_pages(
                    pdf_path, str(work_dir)
                )
                document_structure.metadata["additional_ocr_results"] = ocr_results
                logger.info(f"✅ Additional OCR completed: {len(ocr_results)} pages")
            except Exception as ocr_error:
                logger.warning(f"Additional OCR failed: {ocr_error}")
                document_structure.metadata["ocr_error"] = str(ocr_error)
        
        # Улучшенное извлечение таблиц
        if processing_options.extract_tables and table_extractor:
            try:
                enhanced_tables = await table_extractor.extract_tables_from_pdf(
                    pdf_path, str(work_dir)
                )
                # Объединяем с результатами Docling
                document_structure.tables.extend(enhanced_tables)
                logger.info(f"✅ Enhanced table extraction: +{len(enhanced_tables)} tables")
            except Exception as table_error:
                logger.warning(f"Enhanced table extraction failed: {table_error}")
        
        # Структурный анализ
        if structure_analyzer:
            try:
                analysis_result = await structure_analyzer.analyze_document_structure(
                    document_structure
                )
                document_structure.metadata["structure_analysis"] = analysis_result
                logger.info("✅ Document structure analysis completed")
            except Exception as analysis_error:
                logger.warning(f"Structure analysis failed: {analysis_error}")
        
        # ✅ ИСПРАВЛЕНО: Подготовка данных для DAG1->DAG2 передачи
        processing_time = time.time() - start_time
        
        # Создаем промежуточный файл для DAG2
        intermediate_data = {
            "title": document_structure.title,
            "pages_count": document_structure.metadata.get("total_pages", 0),
            "markdown_content": document_structure.markdown_content,
            "raw_text": document_structure.raw_text,
            "sections": document_structure.sections,
            "tables": [{"id": i, "page": getattr(t, 'page', 1), "content": t} for i, t in enumerate(document_structure.tables)],
            "images": document_structure.images,
            "metadata": {
                **document_structure.metadata,
                "processing_time_seconds": processing_time,
                "ocr_used": processing_options.use_ocr,
                "ocr_languages": processing_options.ocr_languages if processing_options.use_ocr else None,
                "docling_version": "2.0+",
                "processed_by": "document_processor_v4.0",
                "timestamp": datetime.now().isoformat(),
                "ocr_request_rejected": ocr_request_rejected
            }
        }
        
        # Сохраняем промежуточный результат для DAG2
        intermediate_file = work_dir / f"{document_id}_intermediate.json"
        with open(intermediate_file, 'w', encoding='utf-8') as f:
            json.dump(
                intermediate_data,
                f,
                ensure_ascii=False,
                indent=2,
                default=safe_serialize_tabledata,
            )
        
        # Также сохраняем основной результат
        result_file = work_dir / f"{document_id}_result.json"
        result_data = {
            "document_structure": document_structure.dict(),
            "processing_stats": document_structure.processing_stats
        }
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, ensure_ascii=False, indent=2)
        
        # ✅ ИСПРАВЛЕНО: Метрики
        processing_duration.labels(
            type="document", 
            ocr_used=str(processing_options.use_ocr)
        ).observe(processing_time)
        
        pages_processed.inc(document_structure.metadata.get("total_pages", 0))
        
        files_processed.labels(
            status="success", 
            type="pdf", 
            ocr_used=str(processing_options.use_ocr)
        ).inc()
        
        http_requests.labels(method="POST", endpoint="/process", status="success").inc()
        
        # ✅ ИСПРАВЛЕНО: Ответ для DAG1
        response = ProcessingResponse(
            success=True,
            message="Document processed successfully",
            processing_time=processing_time,
            document_id=document_id,
            pages_count=document_structure.metadata.get("total_pages", 0),
            sections_count=len(document_structure.sections),
            tables_count=len(document_structure.tables),
            images_count=len(document_structure.images),
            formulas_count=len(document_structure.formulas),
            output_files=[str(result_file), str(intermediate_file)],
            metadata=document_structure.metadata,
            # ✅ НОВОЕ: Поля для интеграции с DAG
            intermediate_file=str(intermediate_file),
            ocr_used=processing_options.use_ocr,
            docling_version="2.0+"
        )
        
        logger.info(f"✅ Document processing completed: {document_id} in {processing_time:.2f}s")
        
        # Планируем очистку временных файлов через 1 час
        if background_tasks:
            background_tasks.add_task(
                docling_processor.cleanup_temp_files, 
                str(work_dir), 
                keep_main_files=True
            )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        files_processed.labels(
            status="error", 
            type="pdf", 
            ocr_used="unknown"
        ).inc()
        
        http_requests.labels(method="POST", endpoint="/process", status="error").inc()
        
        logger.error(f"❌ Error processing document {document_id}: {e}")
        logger.error(traceback.format_exc())
        
        raise HTTPException(
            status_code=500,
            detail=f"Document processing failed: {str(e)}"
        )
    
    finally:
        active_requests.dec()

@app.post("/convert")
async def convert_document_alias(
    file: UploadFile = File(...),
    options: str = Form(default='{"extract_tables": true, "extract_images": true, "use_ocr": false}')
):
    """✅ Alias для /process (совместимость с существующими вызовами)"""
    return await process_document_endpoint(file, options)

@app.post("/markdown")
async def convert_to_markdown(
    file: UploadFile = File(...),
    options: str = Form(default='{"extract_tables": true, "extract_images": true, "use_ocr": false}')
):
    """✅ ИСПРАВЛЕНО: Конвертация PDF в Markdown формат"""
    # Сначала обрабатываем документ
    result = await process_document_endpoint(file, options)
    
    # Находим промежуточный файл
    intermediate_file = result.intermediate_file
    if not intermediate_file or not os.path.exists(intermediate_file):
        raise HTTPException(status_code=500, detail="Intermediate file not found")
    
    # Загружаем данные
    with open(intermediate_file, 'r', encoding='utf-8') as f:
        document_data = json.load(f)
    
    # Создаем Markdown файл
    work_dir = Path(intermediate_file).parent
    md_file = work_dir / f"{result.document_id}.md"
    
    # Записываем Markdown контент
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(document_data.get('markdown_content', '# Document\n\nNo content available.'))
    
    return FileResponse(
        path=str(md_file),
        filename=f"{result.document_id}.md",
        media_type="text/markdown"
    )

@app.get("/status")
async def get_detailed_status():
    """✅ Подробный статус всех компонентов"""
    processor_stats = {}
    if docling_processor:
        processor_stats = docling_processor.get_processing_stats()
    
    return {
        "service": "document-processor",
        "version": "4.0.0", 
        "timestamp": datetime.now().isoformat(),
        "status": "healthy",
        "processors": {
            "docling": {
                "available": bool(docling_processor),
                "version": "2.0+",
                "stats": processor_stats
            },
            "ocr": {
                "available": bool(ocr_processor),
                "initialized": ocr_processor.is_initialized() if ocr_processor else False,
                "on_demand": not settings.enable_ocr_by_default
            },
            "table_extractor": {"available": bool(table_extractor)},
            "structure_analyzer": {"available": bool(structure_analyzer)}
        },
        "settings": {
            "max_file_size_mb": settings.max_file_size / 1024 / 1024,
            "timeout_seconds": settings.timeout_seconds,
            "temp_dir": settings.temp_dir,
            "enable_ocr_by_default": settings.enable_ocr_by_default,
            "supported_ocr_languages": settings.paddleocr_langs
        }
    }

@app.get("/metrics")
async def get_metrics():
    """Prometheus метрики endpoint"""
    from fastapi import Response
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ================================================================================
# MAIN
# ================================================================================

startup_time = time.time()

if __name__ == "__main__":
    logger.info(f"🚀 Starting FIXED Document Processor API on {settings.host}:{settings.port}")
    
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        log_level="info" if not settings.debug else "debug",
        access_log=True,
        reload=settings.debug
    )
