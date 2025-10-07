#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visual Diff System для PDF Converter Pipeline v4.0
Визуальное сравнение оригинального PDF и результирующего документа
"""

import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path
import tempfile
import json
from dataclasses import dataclass
from datetime import datetime

# Обработка изображений и визуальное сравнение
import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont
try:
    RESAMPLING_LANCZOS = Image.Resampling.LANCZOS
except AttributeError:
    RESAMPLING_LANCZOS = Image.LANCZOS
from skimage import metrics
from skimage.color import rgb2gray
from skimage.transform import resize

# PDF обработка
import fitz  # PyMuPDF
try:
    from pdf2image import convert_from_path
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    convert_from_path = None
    PDF2IMAGE_AVAILABLE = False
import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge

# =======================================================================================
# КОНФИГУРАЦИЯ И МЕТРИКИ
# =======================================================================================

logger = structlog.get_logger("visual_diff_system")

if not PDF2IMAGE_AVAILABLE:
    logger.warning("pdf2image не установлен, используется fallback на PyMuPDF рендеринг")

# Prometheus метрики
visual_diff_requests = Counter('visual_diff_requests_total', 'Visual diff requests', ['status'])
visual_diff_duration = Histogram('visual_diff_duration_seconds', 'Visual diff processing duration')
ssim_scores = Histogram('ssim_scores', 'SSIM similarity scores')
visual_differences_found = Counter('visual_differences_found', 'Visual differences detected', ['severity'])

@dataclass
class VisualDiffConfig:
    """Конфигурация визуального сравнения"""
    # SSIM параметры
    ssim_threshold: float = 0.95
    ssim_window_size: int = 7
    
    # Визуальные различия
    diff_tolerance: float = 0.1
    highlight_differences: bool = True
    
    # Разрешение для сравнения
    comparison_dpi: int = 150
    max_image_size: Tuple[int, int] = (2000, 2000)
    
    # Цвета для выделения
    diff_color_added: Tuple[int, int, int] = (0, 255, 0)  # Зеленый
    diff_color_removed: Tuple[int, int, int] = (255, 0, 0)  # Красный
    diff_color_changed: Tuple[int, int, int] = (255, 255, 0)  # Желтый
    
    # Директории
    temp_dir: str = "/app/temp"
    output_dir: str = "/app/validation_reports"

# =======================================================================================
# КЛАССЫ ДАННЫХ
# =======================================================================================

@dataclass
class VisualDifference:
    """Визуальное различие"""
    type: str  # "added", "removed", "changed"
    bbox: Tuple[int, int, int, int]  # x, y, width, height
    severity: str  # "low", "medium", "high", "critical"
    confidence: float
    description: str
    page_number: int

@dataclass
class VisualDiffResult:
    """Результат визуального сравнения"""
    overall_similarity: float
    ssim_score: float
    differences: List[VisualDifference]
    pages_compared: int
    processing_time: float
    diff_images_paths: List[str]
    summary: Dict[str, Any]
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

# =======================================================================================
# VISUAL DIFF SYSTEM КЛАСС
# =======================================================================================

class VisualDiffSystem:
    """Система визуального сравнения документов"""
    
    def __init__(self, config: Optional[VisualDiffConfig] = None):
        self.config = config or VisualDiffConfig()
        self.logger = structlog.get_logger("visual_diff_system")
        
        # Создаем директории
        Path(self.config.temp_dir).mkdir(parents=True, exist_ok=True)
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
    
    async def compare_documents(
        self,
        original_pdf_path: str,
        result_pdf_path: str,
        comparison_id: str
    ) -> VisualDiffResult:
        """
        Основное сравнение документов
        
        Args:
            original_pdf_path: Путь к оригинальному PDF
            result_pdf_path: Путь к результирующему PDF
            comparison_id: Идентификатор сравнения
            
        Returns:
            VisualDiffResult: Результат сравнения
        """
        start_time = datetime.now()
        
        try:
            visual_diff_requests.labels(status='started').inc()
            
            # Проверяем файлы
            for path in [original_pdf_path, result_pdf_path]:
                if not Path(path).exists():
                    raise FileNotFoundError(f"PDF file not found: {path}")
            
            # Конвертируем PDF в изображения
            original_images = await self._pdf_to_images(original_pdf_path, f"{comparison_id}_orig")
            result_images = await self._pdf_to_images(result_pdf_path, f"{comparison_id}_result")
            
            # Сравниваем страницы
            differences = []
            ssim_scores_list = []
            diff_images_paths = []
            
            max_pages = min(len(original_images), len(result_images))
            
            for page_num in range(max_pages):
                page_diff = await self._compare_page_images(
                    original_images[page_num],
                    result_images[page_num], 
                    page_num + 1,
                    comparison_id
                )
                
                differences.extend(page_diff["differences"])
                ssim_scores_list.append(page_diff["ssim"])
                
                if page_diff["diff_image_path"]:
                    diff_images_paths.append(page_diff["diff_image_path"])
            
            # Предупреждаем о разном количестве страниц
            if len(original_images) != len(result_images):
                differences.append(VisualDifference(
                    type="changed",
                    bbox=(0, 0, 0, 0),
                    severity="high",
                    confidence=1.0,
                    description=f"Page count mismatch: original={len(original_images)}, result={len(result_images)}",
                    page_number=0
                ))
            
            # Расчет общих метрик
            overall_ssim = np.mean(ssim_scores_list) if ssim_scores_list else 0.0
            overall_similarity = self._calculate_overall_similarity(differences, overall_ssim)
            
            # Обновляем метрики
            processing_time = (datetime.now() - start_time).total_seconds()
            visual_diff_duration.observe(processing_time)
            ssim_scores.observe(overall_ssim)
            
            # Подсчет различий по типам
            diff_summary = {"added": 0, "removed": 0, "changed": 0}
            severity_summary = {"low": 0, "medium": 0, "high": 0, "critical": 0}
            
            for diff in differences:
                diff_summary[diff.type] = diff_summary.get(diff.type, 0) + 1
                severity_summary[diff.severity] = severity_summary.get(diff.severity, 0) + 1
                visual_differences_found.labels(severity=diff.severity).inc()
            
            visual_diff_requests.labels(status='success').inc()
            
            result = VisualDiffResult(
                overall_similarity=overall_similarity,
                ssim_score=overall_ssim,
                differences=differences,
                pages_compared=max_pages,
                processing_time=processing_time,
                diff_images_paths=diff_images_paths,
                summary={
                    "total_differences": len(differences),
                    "by_type": diff_summary,
                    "by_severity": severity_summary,
                    "pages_compared": max_pages,
                    "pages_original": len(original_images),
                    "pages_result": len(result_images)
                },
                metadata={
                    "comparison_id": comparison_id,
                    "ssim_threshold": self.config.ssim_threshold,
                    "diff_tolerance": self.config.diff_tolerance
                }
            )
            
            self.logger.info(
                f"Visual comparison completed",
                comparison_id=comparison_id,
                similarity=overall_similarity,
                differences=len(differences),
                processing_time=processing_time
            )
            
            return result
            
        except Exception as e:
            visual_diff_requests.labels(status='error').inc()
            self.logger.error(f"Visual diff error: {e}")
            raise
    
    async def _pdf_to_images(self, pdf_path: str, prefix: str) -> List[str]:
        """Конвертация PDF в изображения"""
        try:
            temp_dir = Path(self.config.temp_dir) / f"images_{prefix}"
            temp_dir.mkdir(parents=True, exist_ok=True)

            image_paths: List[str] = []

            if PDF2IMAGE_AVAILABLE:
                images = convert_from_path(
                    pdf_path,
                    dpi=self.config.comparison_dpi,
                    fmt='PNG'
                )

                for idx, image in enumerate(images, start=1):
                    if image.size[0] > self.config.max_image_size[0] or image.size[1] > self.config.max_image_size[1]:
                        image.thumbnail(self.config.max_image_size, RESAMPLING_LANCZOS)
                    image_path = temp_dir / f"page_{idx}.png"
                    image.save(image_path, 'PNG')
                    image_paths.append(str(image_path))
            else:
                with fitz.open(pdf_path) as doc:
                    for idx, page in enumerate(doc, start=1):
                        zoom = self.config.comparison_dpi / 72.0
                        mat = fitz.Matrix(zoom, zoom)
                        pix = page.get_pixmap(matrix=mat, alpha=False)
                        image_path = temp_dir / f"page_{idx}.png"
                        pix.save(str(image_path))
                        image_paths.append(str(image_path))

            if not image_paths:
                raise RuntimeError("no images rendered from PDF")

            return image_paths

        except Exception as e:
            self.logger.error(f"Error converting PDF to images: {e}")
            raise
    
    async def _compare_page_images(
        self,
        original_path: str,
        result_path: str,
        page_number: int,
        comparison_id: str
    ) -> Dict[str, Any]:
        """Сравнение изображений двух страниц"""
        try:
            # Загружаем изображения
            img1 = cv2.imread(original_path)
            img2 = cv2.imread(result_path)
            
            if img1 is None or img2 is None:
                raise Exception(f"Cannot load images: {original_path}, {result_path}")
            
            # Приводим к одному размеру
            if img1.shape != img2.shape:
                height = min(img1.shape[0], img2.shape[0])
                width = min(img1.shape[1], img2.shape[1])
                img1 = cv2.resize(img1, (width, height))
                img2 = cv2.resize(img2, (width, height))
            
            # Конвертируем в grayscale для SSIM
            gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
            gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
            
            # Рассчитываем SSIM
            ssim_score = metrics.structural_similarity(
                gray1, gray2,
                win_size=self.config.ssim_window_size,
                full=True
            )[0]
            
            # Находим различия
            differences = await self._find_visual_differences(
                img1, img2, gray1, gray2, page_number
            )
            
            # Создаем изображение с выделенными различиями
            diff_image_path = None
            if self.config.highlight_differences and differences:
                diff_image_path = await self._create_diff_image(
                    img1, img2, differences, page_number, comparison_id
                )
            
            return {
                "ssim": ssim_score,
                "differences": differences,
                "diff_image_path": diff_image_path
            }
            
        except Exception as e:
            self.logger.error(f"Error comparing page images: {e}")
            return {"ssim": 0.0, "differences": [], "diff_image_path": None}
    
    async def _find_visual_differences(
        self,
        img1: np.ndarray,
        img2: np.ndarray,
        gray1: np.ndarray,
        gray2: np.ndarray,
        page_number: int
    ) -> List[VisualDifference]:
        """Поиск визуальных различий между изображениями"""
        differences = []
        
        try:
            # Рассчитываем абсолютную разность
            diff = cv2.absdiff(gray1, gray2)
            
            # Применяем пороговое значение
            threshold_value = int(255 * self.config.diff_tolerance)
            _, thresh = cv2.threshold(diff, threshold_value, 255, cv2.THRESH_BINARY)
            
            # Находим контуры различий
            contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            for contour in contours:
                # Фильтруем слишком маленькие различия
                area = cv2.contourArea(contour)
                if area < 100:  # Минимальная площадь различия
                    continue
                
                # Получаем bounding box
                x, y, w, h = cv2.boundingRect(contour)
                
                # Определяем тип различия
                diff_type = self._classify_difference_type(img1, img2, x, y, w, h)
                
                # Определяем серьезность
                severity = self._assess_difference_severity(area, w, h, img1.shape)
                
                # Рассчитываем confidence
                region_diff = diff[y:y+h, x:x+w]
                confidence = np.mean(region_diff) / 255.0
                
                differences.append(VisualDifference(
                    type=diff_type,
                    bbox=(x, y, w, h),
                    severity=severity,
                    confidence=confidence,
                    description=f"{diff_type.title()} region on page {page_number}",
                    page_number=page_number
                ))
            
            return differences
            
        except Exception as e:
            self.logger.error(f"Error finding visual differences: {e}")
            return []
    
    def _classify_difference_type(
        self,
        img1: np.ndarray,
        img2: np.ndarray,
        x: int, y: int, w: int, h: int
    ) -> str:
        """Классификация типа различия"""
        try:
            # Извлекаем области
            region1 = img1[y:y+h, x:x+w]
            region2 = img2[y:y+h, x:x+w]
            
            # Рассчитываем средние яркости
            brightness1 = np.mean(cv2.cvtColor(region1, cv2.COLOR_BGR2GRAY))
            brightness2 = np.mean(cv2.cvtColor(region2, cv2.COLOR_BGR2GRAY))
            
            # Если в одном изображении область почти черная/белая
            if brightness1 < 30 and brightness2 > 200:
                return "added"
            elif brightness1 > 200 and brightness2 < 30:
                return "removed"
            else:
                return "changed"
                
        except:
            return "changed"
    
    def _assess_difference_severity(
        self,
        area: float,
        width: int,
        height: int,
        image_shape: Tuple[int, int, int]
    ) -> str:
        """Оценка серьезности различия"""
        # Рассчитываем процент от общей площади
        total_area = image_shape[0] * image_shape[1]
        area_percent = area / total_area
        
        # Классификация по размеру
        if area_percent > 0.1:  # Более 10% изображения
            return "critical"
        elif area_percent > 0.05:  # 5-10%
            return "high"
        elif area_percent > 0.01:  # 1-5%
            return "medium"
        else:
            return "low"
    
    async def _create_diff_image(
        self,
        img1: np.ndarray,
        img2: np.ndarray,
        differences: List[VisualDifference],
        page_number: int,
        comparison_id: str
    ) -> str:
        """Создание изображения с выделенными различиями"""
        try:
            # Создаем composite изображение
            height, width = img1.shape[:2]
            diff_img = np.zeros((height, width * 2 + 10, 3), dtype=np.uint8)
            
            # Копируем оригинальные изображения
            diff_img[:, :width] = img1
            diff_img[:, width + 10:] = img2
            
            # Выделяем различия на обеих изображениях
            for diff in differences:
                x, y, w, h = diff.bbox
                color = self._get_diff_color(diff.type)
                
                # Прямоугольники на обеих изображениях
                cv2.rectangle(diff_img, (x, y), (x + w, y + h), color, 2)
                cv2.rectangle(diff_img, (x + width + 10, y), (x + w + width + 10, y + h), color, 2)
                
                # Подпись серьезности
                if diff.severity in ["high", "critical"]:
                    cv2.putText(diff_img, diff.severity.upper(), (x, y - 5), 
                               cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1)
            
            # Сохраняем изображение
            output_path = Path(self.config.output_dir) / f"{comparison_id}_page_{page_number}_diff.png"
            cv2.imwrite(str(output_path), diff_img)
            
            return str(output_path)
            
        except Exception as e:
            self.logger.error(f"Error creating diff image: {e}")
            return ""
    
    def _get_diff_color(self, diff_type: str) -> Tuple[int, int, int]:
        """Получение цвета для типа различия"""
        colors = {
            "added": self.config.diff_color_added,
            "removed": self.config.diff_color_removed,
            "changed": self.config.diff_color_changed
        }
        return colors.get(diff_type, (255, 255, 255))  # Белый по умолчанию
    
    def _calculate_overall_similarity(
        self,
        differences: List[VisualDifference],
        ssim_score: float
    ) -> float:
        """Расчет общего показателя сходства"""
        try:
            # Базовое значение от SSIM
            similarity = ssim_score
            
            # Штрафы за различия по серьезности
            penalty_weights = {
                "low": 0.01,
                "medium": 0.05,
                "high": 0.1,
                "critical": 0.2
            }
            
            total_penalty = 0.0
            for diff in differences:
                penalty = penalty_weights.get(diff.severity, 0.05)
                total_penalty += penalty * diff.confidence
            
            # Применяем штрафы
            similarity = max(0.0, similarity - total_penalty)
            
            return min(1.0, similarity)
            
        except:
            return ssim_score

# =======================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =======================================================================================

def create_visual_diff_system(config: Optional[VisualDiffConfig] = None) -> VisualDiffSystem:
    """Фабричная функция для создания системы визуального сравнения"""
    return VisualDiffSystem(config)

async def compare_pdf_documents(
    original_pdf: str,
    result_pdf: str,
    comparison_id: str,
    config: Optional[VisualDiffConfig] = None
) -> VisualDiffResult:
    """
    Высокоуровневая функция для сравнения PDF документов
    
    Args:
        original_pdf: Путь к оригинальному PDF
        result_pdf: Путь к результирующему PDF
        comparison_id: Идентификатор сравнения
        config: Конфигурация системы
        
    Returns:
        VisualDiffResult: Результат сравнения
    """
    system = create_visual_diff_system(config)
    return await system.compare_documents(original_pdf, result_pdf, comparison_id)

# =======================================================================================
# ОСНОВНОЙ БЛОК ДЛЯ ТЕСТИРОВАНИЯ
# =======================================================================================

if __name__ == "__main__":
    # Пример использования
    async def main():
        config = VisualDiffConfig()
        system = VisualDiffSystem(config)
        
        original_pdf = "/app/temp/original.pdf"
        result_pdf = "/app/temp/result.pdf"
        comparison_id = "test_comparison"
        
        if Path(original_pdf).exists() and Path(result_pdf).exists():
            result = await system.compare_documents(original_pdf, result_pdf, comparison_id)
            
            print(f"Overall similarity: {result.overall_similarity:.2f}")
            print(f"SSIM score: {result.ssim_score:.2f}")
            print(f"Differences found: {len(result.differences)}")
            print(f"Pages compared: {result.pages_compared}")
            
            if result.differences:
                print("Differences by severity:")
                for severity, count in result.summary["by_severity"].items():
                    if count > 0:
                        print(f"  {severity}: {count}")
        else:
            print(f"Test files not found: {original_pdf}, {result_pdf}")
    
    asyncio.run(main())
