#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OCR Validator для PDF Converter Pipeline v4.0
Кросс-валидация OCR результатов для достижения максимальной точности
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
import difflib
import statistics

# OCR движки
import pytesseract
from paddleocr import PaddleOCR

# Обработка изображений
import cv2
import numpy as np
from PIL import Image

# Текстовый анализ
import jieba
from textdistance import levenshtein, jaccard
from langdetect import detect

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge

# =======================================================================================
# КОНФИГУРАЦИЯ И МЕТРИКИ
# =======================================================================================

logger = structlog.get_logger("ocr_validator")

# Prometheus метрики
ocr_validation_requests = Counter('ocr_validation_requests_total', 'OCR validation requests', ['status'])
ocr_validation_duration = Histogram('ocr_validation_duration_seconds', 'OCR validation duration')
ocr_consensus_score = Histogram('ocr_consensus_score', 'OCR consensus confidence score')
ocr_engines_used = Counter('ocr_engines_used_total', 'OCR engines usage', ['engine'])

@dataclass
class OCRValidationConfig:
    """Конфигурация OCR валидации"""
    # Настройки консенсуса
    consensus_threshold: float = 0.85
    min_engines: int = 2
    max_engines: int = 3
    
    # PaddleOCR настройки
    paddle_use_gpu: bool = True
    paddle_lang: str = "ch"
    paddle_use_angle_cls: bool = True
    
    # Tesseract настройки
    tesseract_config: str = "--oem 3 --psm 6"
    tesseract_lang: str = "chi_sim+eng+rus"
    
    # Валидация текста
    min_confidence: float = 0.7
    similarity_threshold: float = 0.8
    chinese_mode: bool = True
    
    # Директории
    temp_dir: str = "/app/temp"
    cache_dir: str = "/app/cache"

# =======================================================================================
# КЛАССЫ ДАННЫХ
# =======================================================================================

@dataclass
class OCRResult:
    """Результат OCR от одного движка"""
    engine: str
    text: str
    confidence: float
    processing_time: float
    word_count: int
    language: str
    bbox_info: Optional[List[Dict]] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

@dataclass
class ValidationResult:
    """Результат валидации OCR"""
    consensus_text: str
    consensus_confidence: float
    individual_results: List[OCRResult]
    validation_score: float
    issues_found: List[str]
    recommendations: List[str]
    processing_time: float
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

# =======================================================================================
# OCR VALIDATOR КЛАСС
# =======================================================================================

class OCRValidator:
    """Главный класс для валидации OCR результатов"""
    
    def __init__(self, config: Optional[OCRValidationConfig] = None):
        self.config = config or OCRValidationConfig()
        self.logger = structlog.get_logger("ocr_validator")
        
        # Инициализируем OCR движки
        self._initialize_engines()
        
        # Создаем директории
        Path(self.config.temp_dir).mkdir(parents=True, exist_ok=True)
        Path(self.config.cache_dir).mkdir(parents=True, exist_ok=True)
    
    def _initialize_engines(self):
        """Инициализация OCR движков"""
        try:
            # PaddleOCR
            self.paddle_ocr = PaddleOCR(
                use_angle_cls=self.config.paddle_use_angle_cls,
                lang=self.config.paddle_lang
            )
            self.logger.info("PaddleOCR initialized")
            
            # Tesseract (проверяем доступность)
            try:
                pytesseract.get_tesseract_version()
                self.tesseract_available = True
                self.logger.info("Tesseract OCR available")
            except Exception as e:
                self.tesseract_available = False
                self.logger.warning(f"Tesseract not available: {e}")
                
        except Exception as e:
            self.logger.error(f"Error initializing OCR engines: {e}")
            raise
    
    async def validate_ocr_results(
        self, 
        image_path: str,
        reference_ocr: Optional[str] = None,
        expected_language: str = "zh-CN"
    ) -> ValidationResult:
        """
        Валидация OCR результатов через кросс-проверку движков
        
        Args:
            image_path: Путь к изображению
            reference_ocr: Эталонный OCR результат (из Document Processor)
            expected_language: Ожидаемый язык документа
            
        Returns:
            ValidationResult: Результат валидации
        """
        start_time = datetime.now()
        
        try:
            ocr_validation_requests.labels(status='started').inc()
            
            # Проверяем существование файла
            if not Path(image_path).exists():
                raise FileNotFoundError(f"Image file not found: {image_path}")
            
            # Получаем результаты от разных движков
            ocr_results = []
            
            # PaddleOCR результат
            paddle_result = await self._run_paddle_ocr(image_path)
            if paddle_result:
                ocr_results.append(paddle_result)
                ocr_engines_used.labels(engine='paddleocr').inc()
            
            # Tesseract результат
            if self.tesseract_available:
                tesseract_result = await self._run_tesseract_ocr(image_path)
                if tesseract_result:
                    ocr_results.append(tesseract_result)
                    ocr_engines_used.labels(engine='tesseract').inc()
            
            # Добавляем reference OCR если предоставлен
            if reference_ocr:
                ref_result = OCRResult(
                    engine="reference",
                    text=reference_ocr,
                    confidence=1.0,
                    processing_time=0.0,
                    word_count=len(reference_ocr.split()),
                    language=expected_language
                )
                ocr_results.append(ref_result)
                ocr_engines_used.labels(engine='reference').inc()
            
            if not ocr_results:
                raise Exception("No OCR results obtained")
            
            # Строим консенсус
            validation_result = await self._build_consensus(ocr_results, expected_language)
            
            # Обновляем метрики
            processing_time = (datetime.now() - start_time).total_seconds()
            validation_result.processing_time = processing_time
            
            ocr_validation_duration.observe(processing_time)
            ocr_consensus_score.observe(validation_result.consensus_confidence)
            ocr_validation_requests.labels(status='success').inc()
            
            self.logger.info(
                f"OCR validation completed",
                consensus_confidence=validation_result.consensus_confidence,
                engines_used=len(ocr_results),
                processing_time=processing_time
            )
            
            return validation_result
            
        except Exception as e:
            ocr_validation_requests.labels(status='error').inc()
            self.logger.error(f"OCR validation error: {e}")
            raise
    
    async def _run_paddle_ocr(self, image_path: str) -> Optional[OCRResult]:
        """Запуск PaddleOCR"""
        try:
            start_time = datetime.now()
            
            # Загружаем изображение
            image = cv2.imread(image_path)
            if image is None:
                raise Exception(f"Cannot load image: {image_path}")
            
            # Запускаем OCR
            results = self.paddle_ocr.ocr(image, cls=True)
            
            if not results or not results[0]:
                return None
            
            # Извлекаем текст и confidence
            text_parts = []
            confidences = []
            bbox_info = []
            
            for line in results[0]:
                if len(line) >= 2:
                    bbox, (text, confidence) = line[0], line[1]
                    if confidence >= self.config.min_confidence:
                        text_parts.append(text)
                        confidences.append(confidence)
                        bbox_info.append({
                            'bbox': bbox,
                            'text': text,
                            'confidence': confidence
                        })
            
            if not text_parts:
                return None
            
            full_text = ' '.join(text_parts)
            avg_confidence = statistics.mean(confidences)
            
            # Определяем язык
            try:
                language = detect(full_text)
            except:
                language = "unknown"
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return OCRResult(
                engine="paddleocr",
                text=full_text,
                confidence=avg_confidence,
                processing_time=processing_time,
                word_count=len(full_text.split()),
                language=language,
                bbox_info=bbox_info
            )
            
        except Exception as e:
            self.logger.warning(f"PaddleOCR failed: {e}")
            return None
    
    async def _run_tesseract_ocr(self, image_path: str) -> Optional[OCRResult]:
        """Запуск Tesseract OCR"""
        try:
            start_time = datetime.now()
            
            # Загружаем изображение
            image = Image.open(image_path)
            
            # Запускаем OCR
            text = pytesseract.image_to_string(
                image, 
                lang=self.config.tesseract_lang,
                config=self.config.tesseract_config
            )
            
            if not text.strip():
                return None
            
            # Получаем данные о confidence (если доступно)
            try:
                data = pytesseract.image_to_data(
                    image, 
                    lang=self.config.tesseract_lang,
                    config=self.config.tesseract_config,
                    output_type=pytesseract.Output.DICT
                )
                
                confidences = [int(conf) for conf in data['conf'] if int(conf) > 0]
                avg_confidence = statistics.mean(confidences) / 100.0 if confidences else 0.5
                
            except:
                avg_confidence = 0.5  # Дефолтное значение
            
            # Определяем язык
            try:
                language = detect(text)
            except:
                language = "unknown"
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return OCRResult(
                engine="tesseract",
                text=text.strip(),
                confidence=avg_confidence,
                processing_time=processing_time,
                word_count=len(text.split()),
                language=language
            )
            
        except Exception as e:
            self.logger.warning(f"Tesseract OCR failed: {e}")
            return None
    
    async def _build_consensus(
        self, 
        ocr_results: List[OCRResult],
        expected_language: str
    ) -> ValidationResult:
        """Построение консенсуса из результатов OCR"""
        
        if not ocr_results:
            raise Exception("No OCR results to build consensus from")
        
        issues_found = []
        recommendations = []
        
        # Если только один результат
        if len(ocr_results) == 1:
            result = ocr_results[0]
            return ValidationResult(
                consensus_text=result.text,
                consensus_confidence=result.confidence,
                individual_results=ocr_results,
                validation_score=result.confidence,
                issues_found=["Only one OCR result available"],
                recommendations=["Consider using multiple OCR engines for better accuracy"],
                processing_time=0.0
            )
        
        # Анализируем сходство между результатами
        similarity_scores = []
        texts = [result.text for result in ocr_results]
        
        # Попарное сравнение всех текстов
        for i in range(len(texts)):
            for j in range(i + 1, len(texts)):
                similarity = self._calculate_text_similarity(texts[i], texts[j])
                similarity_scores.append(similarity)
        
        avg_similarity = statistics.mean(similarity_scores)
        
        # Выбираем лучший результат как консенсус
        best_result = max(ocr_results, key=lambda r: r.confidence)
        
        # Если сходство низкое, понижаем confidence
        if avg_similarity < self.config.similarity_threshold:
            consensus_confidence = best_result.confidence * avg_similarity
            issues_found.append(f"Low similarity between OCR results: {avg_similarity:.2f}")
            recommendations.append("Manual review recommended due to inconsistent OCR results")
        else:
            consensus_confidence = best_result.confidence
        
        # Проверяем язык
        detected_languages = [r.language for r in ocr_results if r.language != "unknown"]
        if detected_languages and all(lang != expected_language[:2] for lang in detected_languages):
            issues_found.append(f"Language mismatch: expected {expected_language}, got {detected_languages}")
            consensus_confidence *= 0.9
        
        # Проверяем длину текста
        text_lengths = [r.word_count for r in ocr_results]
        if max(text_lengths) / min(text_lengths) > 2.0:  # Большая разница в длине
            issues_found.append("Significant length difference between OCR results")
            recommendations.append("Check image quality and OCR parameters")
            consensus_confidence *= 0.95
        
        # Финальная оценка валидации
        validation_score = consensus_confidence * avg_similarity
        
        return ValidationResult(
            consensus_text=best_result.text,
            consensus_confidence=consensus_confidence,
            individual_results=ocr_results,
            validation_score=validation_score,
            issues_found=issues_found,
            recommendations=recommendations,
            processing_time=0.0,
            metadata={
                "similarity_scores": similarity_scores,
                "avg_similarity": avg_similarity,
                "best_engine": best_result.engine,
                "detected_languages": detected_languages
            }
        )
    
    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Расчет сходства между двумя текстами"""
        try:
            if not text1.strip() or not text2.strip():
                return 0.0
            
            # Используем несколько метрик
            
            # 1. Levenshtein distance (normalized)
            levenshtein_sim = 1.0 - (levenshtein.distance(text1, text2) / max(len(text1), len(text2)))
            
            # 2. Jaccard similarity для слов
            words1 = set(text1.split())
            words2 = set(text2.split())
            jaccard_sim = jaccard.similarity(words1, words2)
            
            # 3. SequenceMatcher для более точного сравнения
            sequence_sim = difflib.SequenceMatcher(None, text1, text2).ratio()
            
            # Для китайского текста используем jieba
            if self.config.chinese_mode and any('\u4e00' <= char <= '\u9fff' for char in text1 + text2):
                # Токенизация китайского текста
                tokens1 = set(jieba.cut(text1))
                tokens2 = set(jieba.cut(text2))
                chinese_sim = len(tokens1 & tokens2) / len(tokens1 | tokens2) if tokens1 | tokens2 else 0.0
                
                # Средневзвешенное с учетом китайской токенизации
                return (levenshtein_sim * 0.3 + jaccard_sim * 0.2 + sequence_sim * 0.3 + chinese_sim * 0.2)
            else:
                # Средневзвешенное для обычного текста
                return (levenshtein_sim * 0.4 + jaccard_sim * 0.3 + sequence_sim * 0.3)
                
        except Exception as e:
            self.logger.warning(f"Error calculating text similarity: {e}")
            return 0.0

# =======================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =======================================================================================

def create_ocr_validator(config: Optional[OCRValidationConfig] = None) -> OCRValidator:
    """Фабричная функция для создания OCR Validator"""
    return OCRValidator(config)

async def validate_ocr_from_document_processor(
    image_path: str,
    reference_text: str,
    config: Optional[OCRValidationConfig] = None
) -> ValidationResult:
    """
    Высокоуровневая функция для валидации OCR из Document Processor
    
    Args:
        image_path: Путь к изображению страницы
        reference_text: OCR текст от Document Processor
        config: Конфигурация валидатора
        
    Returns:
        ValidationResult: Результат валидации
    """
    validator = create_ocr_validator(config)
    return await validator.validate_ocr_results(image_path, reference_text)

# =======================================================================================
# ОСНОВНОЙ БЛОК ДЛЯ ТЕСТИРОВАНИЯ
# =======================================================================================

if __name__ == "__main__":
    # Пример использования
    async def main():
        config = OCRValidationConfig()
        validator = OCRValidator(config)
        
        # Тестовое изображение
        image_path = "/app/temp/test_page.png"
        reference_text = "测试文档 Test Document"
        
        if Path(image_path).exists():
            result = await validator.validate_ocr_results(image_path, reference_text)
            
            print(f"Consensus confidence: {result.consensus_confidence:.2f}")
            print(f"Validation score: {result.validation_score:.2f}")
            print(f"Engines used: {len(result.individual_results)}")
            print(f"Issues found: {len(result.issues_found)}")
            
            if result.issues_found:
                print("Issues:")
                for issue in result.issues_found:
                    print(f"  - {issue}")
        else:
            print(f"Test image not found: {image_path}")
    
    asyncio.run(main())