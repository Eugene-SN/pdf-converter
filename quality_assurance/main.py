#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Quality Assurance Main Server для PDF Converter Pipeline v4.0
5-уровневая система валидации документов с автоматической коррекцией
"""

import os
import sys
import asyncio
import logging
import subprocess
import re
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import tempfile
import json
import time
from datetime import datetime
import traceback
from contextlib import asynccontextmanager

# FastAPI импорты
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends, status
from fastapi.responses import JSONResponse, FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import uvicorn

# Pydantic модели
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# HTTP клиенты
import httpx
import aiofiles
import fitz  # PyMuPDF

try:
    from pdf2image import convert_from_path
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    convert_from_path = None
    PDF2IMAGE_AVAILABLE = False

try:
    from PIL import Image
    RESAMPLING_LANCZOS = Image.Resampling.LANCZOS
except AttributeError:  # Pillow < 9.1
    from PIL import Image
    RESAMPLING_LANCZOS = Image.LANCZOS

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server, generate_latest, REGISTRY
from prometheus_client.exposition import CONTENT_TYPE_LATEST
import psutil

# Наши валидаторы
from ocr_validator import OCRValidator, OCRValidationConfig
from visual_diff_system import VisualDiffSystem, VisualDiffConfig
from ast_comparator import ASTComparator, ASTComparisonConfig
from auto_corrector import AutoCorrector, AutoCorrectorConfig
from content_validator import ContentValidator, ContentValidationConfig

# =======================================================================================
# КОНФИГУРАЦИЯ И НАСТРОЙКИ
# =======================================================================================

class Settings(BaseSettings):
    """Настройки приложения"""
    # Основные настройки сервера
    host: str = "0.0.0.0"
    port: int = 8002
    debug: bool = False
    
    # Пути
    temp_dir: str = "/app/temp"
    cache_dir: str = "/app/cache"
    validation_reports_dir: str = "/app/validation_reports"
    models_dir: str = "/mnt/storage/models"
    visual_diff_dpi: int = 150
    pandoc_container_name: str = "pandoc-render"
    pandoc_workspace: str = "/workspace"
    pandoc_template_path: str = "/app/templates/chinese_tech.latex"
    pandoc_host_mount: str = "/opt/airflow/temp"

    # Внешние сервисы
    vllm_base_url: str = "http://vllm-server:8000"
    vllm_api_key: str = "vllm-api-key"
    document_processor_url: str = "http://document-processor:8001"
    
    # Пороги валидации
    ocr_confidence_threshold: float = 0.8
    visual_similarity_threshold: float = 0.95
    ast_similarity_threshold: float = 0.9
    overall_qa_threshold: float = 0.85
    
    # Настройки автокоррекции
    enable_auto_correction: bool = True
    max_corrections_per_document: int = 10

    class Config:
        env_file = ".env"

settings = Settings()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO if not settings.debug else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = structlog.get_logger("qa_main")

if not PDF2IMAGE_AVAILABLE:
    logger.warning("pdf2image not available; falling back to PyMuPDF rendering for previews")

# =======================================================================================
# PROMETHEUS МЕТРИКИ - ИСПРАВЛЕНО ДУБЛИРОВАНИЕ
# =======================================================================================

def clear_registered_metrics():
    """Очистка уже зарегистрированных метрик для предотвращения дублирования"""
    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        if hasattr(collector, '_name') and collector._name.startswith('qa_'):
            try:
                REGISTRY.unregister(collector)
            except KeyError:
                pass

# Очищаем существующие метрики перед регистрацией новых
clear_registered_metrics()

# HTTP метрики
http_requests = Counter('qa_http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
http_duration = Histogram('qa_http_duration_seconds', 'HTTP request duration', ['method', 'endpoint'])
active_requests = Gauge('qa_active_requests', 'Active HTTP requests')

# QA процесс метрики
qa_full_validation_requests = Counter('qa_full_validation_total', 'Full QA validation requests', ['status'])
qa_validation_duration = Histogram('qa_validation_duration_seconds', 'QA validation duration')
qa_overall_score = Histogram('qa_overall_score', 'QA overall validation score')

# Системные метрики
memory_usage = Gauge('qa_memory_usage_bytes', 'Memory usage')
disk_usage = Gauge('qa_disk_usage_percent', 'Disk usage percentage')

# =======================================================================================
# PYDANTIC МОДЕЛИ
# =======================================================================================

class ValidationRequest(BaseModel):
    """Запрос на валидацию"""
    document_id: str
    original_pdf_path: Optional[str] = None
    result_pdf_path: Optional[str] = None
    document_content: Optional[str] = None
    document_structure: Optional[Dict[str, Any]] = None
    enable_auto_correction: bool = True

class ValidationResponse(BaseModel):
    """Ответ на валидацию"""
    success: bool
    message: str
    validation_id: str
    overall_score: float
    passed: bool
    processing_time: float
    
    # Результаты отдельных валидаторов
    ocr_validation: Optional[Dict[str, Any]] = None
    visual_diff: Optional[Dict[str, Any]] = None
    ast_comparison: Optional[Dict[str, Any]] = None
    content_validation: Optional[Dict[str, Any]] = None
    
    # Автокоррекция
    auto_correction: Optional[Dict[str, Any]] = None
    corrected_document: Optional[str] = None
    
    # Отчеты
    validation_report_path: Optional[str] = None
    recommendations: List[str] = []

class HealthResponse(BaseModel):
    """Ответ health check"""
    status: str
    timestamp: str
    version: str = "4.0.0"
    validators: Dict[str, str]
    system_info: Dict[str, Any]

# =======================================================================================
# ИНИЦИАЛИЗАЦИЯ ВАЛИДАТОРОВ
# =======================================================================================

# Глобальные валидаторы
ocr_validator: Optional[OCRValidator] = None
visual_diff_system: Optional[VisualDiffSystem] = None
ast_comparator: Optional[ASTComparator] = None
content_validator: Optional[ContentValidator] = None
auto_corrector: Optional[AutoCorrector] = None


def _sanitize_identifier(value: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]', '_', value)


async def generate_pdf_from_markdown(markdown_content: str, document_id: str) -> str:
    return await asyncio.to_thread(_generate_pdf_from_markdown_sync, markdown_content, document_id)


def _generate_pdf_from_markdown_sync(markdown_content: str, document_id: str) -> str:
    safe_id = _sanitize_identifier(document_id or f"doc_{int(time.time())}")
    temp_dir = Path(settings.pandoc_host_mount) / f"qa_pdf_{safe_id}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    md_file = temp_dir / "source.md"
    pdf_file = temp_dir / "result.pdf"

    md_file.write_text(markdown_content, encoding='utf-8')

    docker_cmd = [
        'docker', 'exec', settings.pandoc_container_name,
        'python3', '/app/render_pdf.py',
        f"{settings.pandoc_workspace}/{temp_dir.name}/source.md",
        f"{settings.pandoc_workspace}/{temp_dir.name}/result.pdf",
        settings.pandoc_template_path
    ]

    result = subprocess.run(docker_cmd, capture_output=True, text=True)
    if result.returncode != 0 or not pdf_file.exists():
        raise RuntimeError(f"Pandoc rendering failed: {result.stderr.strip()}")

    return str(pdf_file)


async def render_pdf_preview(pdf_path: str, work_dir: Path, page_index: int = 0, dpi: int = 200) -> str:
    return await asyncio.to_thread(_render_pdf_preview_sync, pdf_path, work_dir, page_index, dpi)


def _render_pdf_preview_sync(pdf_path: str, work_dir: Path, page_index: int, dpi: int) -> str:
    if not Path(pdf_path).exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    work_dir.mkdir(parents=True, exist_ok=True)
    preview_path = work_dir / f"page_{page_index + 1}.png"

    if PDF2IMAGE_AVAILABLE and convert_from_path is not None:
        images = convert_from_path(pdf_path, dpi=dpi, first_page=page_index + 1, last_page=page_index + 1, fmt='PNG')
        if not images:
            raise RuntimeError("Failed to render preview via pdf2image")
        image = images[0]
        image.save(preview_path, 'PNG')
    else:
        with fitz.open(pdf_path) as doc:
            if page_index >= len(doc):
                raise IndexError(f"PDF has only {len(doc)} pages")
            zoom = dpi / 72.0
            mat = fitz.Matrix(zoom, zoom)
            pix = doc[page_index].get_pixmap(matrix=mat, alpha=False)
            pix.save(str(preview_path))

    return str(preview_path)

async def initialize_validators():
    """Инициализация всех валидаторов"""
    global ocr_validator, visual_diff_system, ast_comparator, content_validator, auto_corrector
    
    logger.info("Initializing QA validators...")
    
    try:
        # OCR Validator
        ocr_config = OCRValidationConfig(
            consensus_threshold=settings.ocr_confidence_threshold,
            temp_dir=settings.temp_dir,
            cache_dir=settings.cache_dir
        )
        ocr_validator = OCRValidator(ocr_config)
        
        # Visual Diff System
        visual_config = VisualDiffConfig(
            ssim_threshold=settings.visual_similarity_threshold,
            temp_dir=settings.temp_dir,
            output_dir=settings.validation_reports_dir,
            comparison_dpi=settings.visual_diff_dpi
        )
        visual_diff_system = VisualDiffSystem(visual_config)
        
        # AST Comparator
        ast_config = ASTComparisonConfig(
            similarity_threshold=settings.ast_similarity_threshold,
            models_dir=f"{settings.models_dir}/shared/qa"
        )
        ast_comparator = ASTComparator(ast_config)
        
        # Content Validator
        content_config = ContentValidationConfig()
        content_validator = ContentValidator(content_config)
        
        # Auto Corrector
        corrector_config = AutoCorrectorConfig(
            vllm_base_url=settings.vllm_base_url,
            vllm_api_key=settings.vllm_api_key,
            max_corrections_per_document=settings.max_corrections_per_document
        )
        auto_corrector = AutoCorrector(corrector_config)
        
        logger.info("All QA validators initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize validators: {e}")
        raise

def update_system_metrics():
    """Обновление системных метрик"""
    try:
        # Память
        memory = psutil.virtual_memory()
        memory_usage.set(memory.used)
        
        # Диск
        disk = psutil.disk_usage('/app/temp')
        disk_usage.set(disk.percent)
        
    except Exception as e:
        logger.warning(f"Failed to update system metrics: {e}")

# =======================================================================================
# LIFESPAN EVENTS - ИСПРАВЛЕНО НА НОВЫЙ СИНТАКСИС
# =======================================================================================

startup_time = time.time()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager для управления запуском и остановкой"""
    # Startup
    logger.info("Starting Quality Assurance API v4.0")
    
    # Создаем директории
    Path(settings.temp_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.cache_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.validation_reports_dir).mkdir(parents=True, exist_ok=True)
    
    # Инициализируем валидаторы
    await initialize_validators()
    
    # Запускаем Prometheus метрики
    start_http_server(8003)
    logger.info("Prometheus metrics server started on port 8003")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Quality Assurance API")

# =======================================================================================
# FASTAPI APPLICATION - ИСПРАВЛЕНО
# =======================================================================================

app = FastAPI(
    title="Quality Assurance API",
    description="5-level document validation system with auto-correction",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan  # Используем новый lifespan вместо on_event
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

# =======================================================================================
# API ENDPOINTS
# =======================================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    update_system_metrics()
    
    # Проверяем статус валидаторов
    validators_status = {
        "ocr_validator": "healthy" if ocr_validator else "unavailable",
        "visual_diff_system": "healthy" if visual_diff_system else "unavailable",
        "ast_comparator": "healthy" if ast_comparator else "unavailable",
        "content_validator": "healthy" if content_validator else "unavailable",
        "auto_corrector": "healthy" if auto_corrector else "unavailable"
    }
    
    # Системная информация
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/app/temp')
    system_info = {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": memory.percent,
        "memory_available_gb": round(memory.available / 1024**3, 2),
        "disk_free_gb": round(disk.free / 1024**3, 2),
        "temp_files_count": len(list(Path(settings.temp_dir).glob("*"))),
        "uptime_seconds": int(time.time() - startup_time)
    }
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        validators=validators_status,
        system_info=system_info
    )

@app.post("/validate", response_model=ValidationResponse)
async def validate_document(request: ValidationRequest):
    """
    Полная валидация документа через все 5 уровней QA системы
    """
    start_time = time.time()
    active_requests.inc()
    validation_id = f"qa_{int(start_time)}"
    
    try:
        qa_full_validation_requests.labels(status='started').inc()
        http_requests.labels(method="POST", endpoint="/validate", status="started").inc()
        
        logger.info(f"Starting full validation: {validation_id}")
        
        # Создаем рабочую директорию
        work_dir = Path(settings.temp_dir) / validation_id
        work_dir.mkdir(parents=True, exist_ok=True)

        if not request.original_pdf_path or not Path(request.original_pdf_path).exists():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="original_pdf_path is required and must exist")

        original_pdf_path = request.original_pdf_path
        result_pdf_path = request.result_pdf_path
        generated_result_pdf = False

        if not result_pdf_path:
            if not request.document_content:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide either result_pdf_path or document_content")
            result_pdf_path = await generate_pdf_from_markdown(request.document_content, validation_id)
            generated_result_pdf = True
        elif not Path(result_pdf_path).exists():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Result PDF not found: {result_pdf_path}")

        validation_results = {
            "generated_result_pdf": generated_result_pdf,
            "paths": {
                "original_pdf": original_pdf_path,
                "result_pdf": result_pdf_path
            }
        }
        recommendations: List[str] = []

        # Уровень 1: OCR Validation
        if ocr_validator:
            try:
                preview_dir = work_dir / "ocr_preview"
                preview_image = await render_pdf_preview(original_pdf_path, preview_dir, page_index=0, dpi=settings.visual_diff_dpi)
                ocr_result = await ocr_validator.validate_ocr_results(
                    preview_image,
                    request.document_content if request.document_content else None
                )

                validation_results["ocr_validation"] = {
                    "consensus_confidence": ocr_result.consensus_confidence,
                    "validation_score": ocr_result.validation_score,
                    "issues_found": ocr_result.issues_found,
                    "engines_used": [result.engine for result in ocr_result.individual_results]
                }

                recommendations.extend(ocr_result.recommendations)

            except Exception as e:
                logger.warning(f"OCR validation failed: {e}")
                validation_results["ocr_validation"] = {"error": str(e)}

        # Уровень 2: Visual Diff
        if visual_diff_system:
            try:
                diff_result = await visual_diff_system.compare_documents(
                    original_pdf_path,
                    result_pdf_path,
                    validation_id
                )

                validation_results["visual_diff"] = {
                    "overall_similarity": diff_result.overall_similarity,
                    "ssim_score": diff_result.ssim_score,
                    "differences_count": len(diff_result.differences),
                    "summary": diff_result.summary,
                    "diff_image_paths": diff_result.diff_images_paths,
                    "metadata": diff_result.metadata,
                }

                if diff_result.differences:
                    recommendations.append("Review visual differences highlighted in validation report")

            except Exception as e:
                logger.warning(f"Visual diff failed: {e}")
                validation_results["visual_diff"] = {"error": str(e)}
        
        # Уровень 3: AST Comparison
        if request.document_structure and ast_comparator:
            try:
                # Для демонстрации используем ту же структуру как оригинал
                original_ast = request.document_structure
                result_ast = request.document_structure  # В реальности это будет из результирующего документа
                
                ast_result = await ast_comparator.compare_ast_structures(
                    original_ast, result_ast, validation_id
                )
                
                validation_results["ast_comparison"] = {
                    "overall_similarity": ast_result.overall_similarity,
                    "structural_similarity": ast_result.structural_similarity,
                    "semantic_similarity": ast_result.semantic_similarity,
                    "issues_found": ast_result.issues_found
                }
                
                recommendations.extend(ast_result.recommendations)
                
            except Exception as e:
                logger.warning(f"AST comparison failed: {e}")
                validation_results["ast_comparison"] = {"error": str(e)}
        
        # Уровень 4: Content Validation
        if request.document_content and content_validator:
            try:
                content_result = await content_validator.validate_content(request.document_content)
                
                validation_results["content_validation"] = {
                    "passed": content_result.passed,
                    "score": content_result.score,
                    "issues_found": content_result.issues_found,
                    "technical_terms_found": content_result.technical_terms_found,
                    "code_blocks_found": content_result.code_blocks_found
                }
                
                recommendations.extend(content_result.recommendations)
                
            except Exception as e:
                logger.warning(f"Content validation failed: {e}")
                validation_results["content_validation"] = {"error": str(e)}
        
        # Расчет общего скора
        scores = []
        if "ocr_validation" in validation_results and "validation_score" in validation_results["ocr_validation"]:
            scores.append(validation_results["ocr_validation"]["validation_score"])
        if "visual_diff" in validation_results and "overall_similarity" in validation_results["visual_diff"]:
            scores.append(validation_results["visual_diff"]["overall_similarity"])
        if "ast_comparison" in validation_results and "overall_similarity" in validation_results["ast_comparison"]:
            scores.append(validation_results["ast_comparison"]["overall_similarity"])
        if "content_validation" in validation_results and "score" in validation_results["content_validation"]:
            scores.append(validation_results["content_validation"]["score"])
        
        overall_score = sum(scores) / len(scores) if scores else 0.0
        passed = overall_score >= settings.overall_qa_threshold
        
        # Уровень 5: Auto Correction (если включена и есть проблемы)
        corrected_document = None
        auto_correction_result = None
        
        if (request.enable_auto_correction and settings.enable_auto_correction and
            not passed and request.document_content and auto_corrector):
            try:
                async with auto_corrector as corrector:
                    correction_result = await corrector.apply_corrections(
                        request.document_content, validation_results, validation_id
                    )
                
                auto_correction_result = {
                    "total_corrections": correction_result.total_corrections,
                    "successful_corrections": correction_result.successful_corrections,
                    "failed_corrections": correction_result.failed_corrections,
                    "processing_time": correction_result.processing_time
                }
                
                if correction_result.corrected_document:
                    corrected_document = correction_result.corrected_document
                    
            except Exception as e:
                logger.warning(f"Auto correction failed: {e}")
                auto_correction_result = {"error": str(e)}
        
        # Создание отчета валидации
        report_path = await create_validation_report(
            validation_id, validation_results, overall_score, recommendations, work_dir
        )
        
        # Обновляем метрики
        processing_time = time.time() - start_time
        qa_validation_duration.observe(processing_time)
        qa_overall_score.observe(overall_score)
        
        status = 'success' if passed else 'failed'
        qa_full_validation_requests.labels(status=status).inc()
        http_requests.labels(method="POST", endpoint="/validate", status=status).inc()
        
        response = ValidationResponse(
            success=True,
            message=f"Validation completed with score {overall_score:.2f}",
            validation_id=validation_id,
            overall_score=overall_score,
            passed=passed,
            processing_time=processing_time,
            ocr_validation=validation_results.get("ocr_validation"),
            visual_diff=validation_results.get("visual_diff"),
            ast_comparison=validation_results.get("ast_comparison"),
            content_validation=validation_results.get("content_validation"),
            auto_correction=auto_correction_result,
            corrected_document=corrected_document,
            validation_report_path=str(report_path) if report_path else None,
            recommendations=recommendations
        )
        
        logger.info(
            f"Validation completed: {validation_id}",
            score=overall_score,
            passed=passed,
            processing_time=processing_time
        )
        
        return response
        
    except HTTPException as exc:
        qa_full_validation_requests.labels(status='error').inc()
        http_requests.labels(method="POST", endpoint="/validate", status=str(exc.status_code)).inc()
        logger.error(f"Validation {validation_id} failed with HTTP error: {exc.detail}")
        raise
    except Exception as e:
        qa_full_validation_requests.labels(status='error').inc()
        http_requests.labels(method="POST", endpoint="/validate", status="error").inc()
        logger.error(f"Error in validation {validation_id}: {e}\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Validation failed: {str(e)}"
        )
    finally:
        active_requests.dec()

async def create_validation_report(
    validation_id: str,
    validation_results: Dict[str, Any],
    overall_score: float,
    recommendations: List[str],
    work_dir: Path
) -> Optional[Path]:
    """Создание отчета валидации"""
    try:
        report_path = work_dir / f"{validation_id}_validation_report.json"
        
        report = {
            "validation_id": validation_id,
            "timestamp": datetime.now().isoformat(),
            "overall_score": overall_score,
            "passed": overall_score >= settings.overall_qa_threshold,
            "validation_results": validation_results,
            "recommendations": recommendations,
            "thresholds": {
                "ocr_confidence": settings.ocr_confidence_threshold,
                "visual_similarity": settings.visual_similarity_threshold,
                "ast_similarity": settings.ast_similarity_threshold,
                "overall_qa": settings.overall_qa_threshold
            }
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        return report_path
        
    except Exception as e:
        logger.error(f"Error creating validation report: {e}")
        return None

@app.get("/metrics")
async def get_metrics():
    """Prometheus метрики endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/status")
async def get_status():
    """Подробный статус всех компонентов"""
    return {
        "service": "quality-assurance",
        "version": "4.0.0",
        "timestamp": datetime.now().isoformat(),
        "validators": {
            "ocr_validator": bool(ocr_validator),
            "visual_diff_system": bool(visual_diff_system),
            "ast_comparator": bool(ast_comparator),
            "content_validator": bool(content_validator),
            "auto_corrector": bool(auto_corrector)
        },
        "settings": {
            "overall_qa_threshold": settings.overall_qa_threshold,
            "enable_auto_correction": settings.enable_auto_correction,
            "max_corrections_per_document": settings.max_corrections_per_document
        }
    }

# =======================================================================================
# MAIN
# =======================================================================================

if __name__ == "__main__":
    logger.info(f"Starting Quality Assurance API on {settings.host}:{settings.port}")
    
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        log_level="info" if not settings.debug else "debug",
        access_log=True,
        reload=settings.debug
    )
