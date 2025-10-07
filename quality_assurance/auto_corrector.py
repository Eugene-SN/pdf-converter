#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Auto Corrector для PDF Converter Pipeline v4.0
Автоматическое исправление обнаруженных проблем в документах
"""

import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Union
import json
import re
from dataclasses import dataclass
from datetime import datetime

# HTTP клиенты для взаимодействия с vLLM
import httpx
import aiohttp

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge

# =======================================================================================
# КОНФИГУРАЦИЯ И МЕТРИКИ
# =======================================================================================

logger = structlog.get_logger("auto_corrector")

# Prometheus метрики
correction_requests = Counter('correction_requests_total', 'Auto correction requests', ['correction_type', 'status'])
correction_duration = Histogram('correction_duration_seconds', 'Auto correction duration', ['correction_type'])
corrections_applied = Counter('corrections_applied_total', 'Total corrections applied', ['correction_type'])

@dataclass
class AutoCorrectorConfig:
    """Конфигурация автокоррекции"""
    # vLLM сервер настройки
    vllm_base_url: str = "http://vllm-server:8000"
    vllm_api_key: str = "vllm-api-key"
    
    # Пороги для применения коррекций
    ocr_confidence_threshold: float = 0.8
    visual_similarity_threshold: float = 0.95
    ast_similarity_threshold: float = 0.9
    
    # Настройки коррекции
    max_corrections_per_document: int = 10
    enable_aggressive_corrections: bool = False
    
    # Типы коррекций
    enable_ocr_correction: bool = True
    enable_structure_correction: bool = True
    enable_translation_correction: bool = True
    enable_formatting_correction: bool = True
    
    # Директории
    temp_dir: str = "/app/temp"
    cache_dir: str = "/app/cache"

@dataclass
class CorrectionAction:
    """Действие по коррекции"""
    type: str  # "ocr", "structure", "translation", "formatting"
    description: str
    original_content: str
    corrected_content: str
    confidence: float
    applied: bool = False
    error_message: Optional[str] = None

@dataclass
class CorrectionResult:
    """Результат автокоррекции"""
    total_corrections: int
    successful_corrections: int
    failed_corrections: int
    corrections_applied: List[CorrectionAction]
    corrected_document: Optional[str] = None
    processing_time: float = 0.0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

# =======================================================================================
# AUTO CORRECTOR КЛАСС
# =======================================================================================

class AutoCorrector:
    """Система автоматической коррекции документов"""
    
    def __init__(self, config: Optional[AutoCorrectorConfig] = None):
        self.config = config or AutoCorrectorConfig()
        self.logger = structlog.get_logger("auto_corrector")
        
        # HTTP клиент для vLLM
        self.http_client = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.http_client = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.http_client:
            await self.http_client.close()
    
    async def apply_corrections(
        self,
        document_content: str,
        validation_results: Dict[str, Any],
        correction_id: str
    ) -> CorrectionResult:
        """
        Применение автоматических коррекций к документу
        
        Args:
            document_content: Содержимое документа для коррекции
            validation_results: Результаты валидации из всех систем QA
            correction_id: Идентификатор коррекции
            
        Returns:
            CorrectionResult: Результат коррекции
        """
        start_time = datetime.now()
        
        try:
            correction_requests.labels(correction_type='combined', status='started').inc()
            
            corrections_to_apply = []
            
            # OCR коррекции
            if self.config.enable_ocr_correction:
                ocr_corrections = await self._generate_ocr_corrections(
                    document_content, validation_results.get("ocr_validation", {})
                )
                corrections_to_apply.extend(ocr_corrections)
            
            # Структурные коррекции
            if self.config.enable_structure_correction:
                structure_corrections = await self._generate_structure_corrections(
                    document_content, validation_results.get("ast_comparison", {})
                )
                corrections_to_apply.extend(structure_corrections)
            
            # Коррекции перевода
            if self.config.enable_translation_correction:
                translation_corrections = await self._generate_translation_corrections(
                    document_content, validation_results.get("content_validation", {})
                )
                corrections_to_apply.extend(translation_corrections)
            
            # Форматирование
            if self.config.enable_formatting_correction:
                formatting_corrections = await self._generate_formatting_corrections(
                    document_content, validation_results.get("visual_diff", {})
                )
                corrections_to_apply.extend(formatting_corrections)
            
            # Ограничиваем количество коррекций
            corrections_to_apply = corrections_to_apply[:self.config.max_corrections_per_document]
            
            # Применяем коррекции
            corrected_document = document_content
            successful_corrections = 0
            failed_corrections = 0
            
            for correction in corrections_to_apply:
                try:
                    if correction.confidence >= 0.7:  # Применяем только уверенные коррекции
                        corrected_document = await self._apply_single_correction(
                            corrected_document, correction
                        )
                        correction.applied = True
                        successful_corrections += 1
                        corrections_applied.labels(correction_type=correction.type).inc()
                    else:
                        correction.applied = False
                        correction.error_message = "Confidence too low"
                        failed_corrections += 1
                        
                except Exception as e:
                    correction.applied = False
                    correction.error_message = str(e)
                    failed_corrections += 1
                    self.logger.warning(f"Failed to apply correction: {e}")
            
            # Финальная проверка через vLLM
            if successful_corrections > 0:
                corrected_document = await self._final_review_correction(
                    document_content, corrected_document, correction_id
                )
            
            # Результат
            processing_time = (datetime.now() - start_time).total_seconds()
            correction_duration.labels(correction_type='combined').observe(processing_time)
            
            result = CorrectionResult(
                total_corrections=len(corrections_to_apply),
                successful_corrections=successful_corrections,
                failed_corrections=failed_corrections,
                corrections_applied=corrections_to_apply,
                corrected_document=corrected_document if successful_corrections > 0 else None,
                processing_time=processing_time,
                metadata={
                    "correction_id": correction_id,
                    "original_length": len(document_content),
                    "corrected_length": len(corrected_document),
                    "correction_ratio": successful_corrections / len(corrections_to_apply) if corrections_to_apply else 0
                }
            )
            
            correction_requests.labels(correction_type='combined', status='success').inc()
            
            self.logger.info(
                f"Auto correction completed",
                correction_id=correction_id,
                total_corrections=len(corrections_to_apply),
                successful=successful_corrections,
                failed=failed_corrections,
                processing_time=processing_time
            )
            
            return result
            
        except Exception as e:
            correction_requests.labels(correction_type='combined', status='error').inc()
            self.logger.error(f"Auto correction error: {e}")
            raise
    
    async def _generate_ocr_corrections(
        self,
        document_content: str,
        ocr_validation: Dict[str, Any]
    ) -> List[CorrectionAction]:
        """Генерация коррекций OCR ошибок"""
        corrections = []
        
        try:
            if not ocr_validation or ocr_validation.get("consensus_confidence", 1.0) >= self.config.ocr_confidence_threshold:
                return corrections
            
            issues = ocr_validation.get("issues_found", [])
            
            for issue in issues:
                if "similarity" in issue.lower():
                    # Проблема с согласованностью OCR результатов
                    correction = CorrectionAction(
                        type="ocr",
                        description="Fix OCR inconsistencies using vLLM",
                        original_content=document_content,
                        corrected_content="",  # Будет заполнено при применении
                        confidence=0.8
                    )
                    corrections.append(correction)
                    break  # Одна коррекция OCR за раз
            
            return corrections
            
        except Exception as e:
            self.logger.error(f"Error generating OCR corrections: {e}")
            return []
    
    async def _generate_structure_corrections(
        self,
        document_content: str,
        ast_comparison: Dict[str, Any]
    ) -> List[CorrectionAction]:
        """Генерация коррекций структуры документа"""
        corrections = []
        
        try:
            if not ast_comparison or ast_comparison.get("overall_similarity", 1.0) >= self.config.ast_similarity_threshold:
                return corrections
            
            issues = ast_comparison.get("issues_found", [])
            
            for issue in issues:
                if "heading" in issue.lower() and "missing" in issue.lower():
                    correction = CorrectionAction(
                        type="structure",
                        description="Restore missing headings structure",
                        original_content=document_content,
                        corrected_content="",
                        confidence=0.7
                    )
                    corrections.append(correction)
                
                elif "level" in issue.lower():
                    correction = CorrectionAction(
                        type="structure", 
                        description="Fix heading level hierarchy",
                        original_content=document_content,
                        corrected_content="",
                        confidence=0.8
                    )
                    corrections.append(correction)
            
            return corrections[:2]  # Максимум 2 структурные коррекции
            
        except Exception as e:
            self.logger.error(f"Error generating structure corrections: {e}")
            return []
    
    async def _generate_translation_corrections(
        self,
        document_content: str,
        content_validation: Dict[str, Any]
    ) -> List[CorrectionAction]:
        """Генерация коррекций перевода"""
        corrections = []
        
        try:
            # Ищем проблемы с техническими терминами
            technical_terms = re.findall(r'[A-Z]{2,}[A-Za-z0-9_-]*', document_content)
            
            if len(technical_terms) < 5:  # Слишком мало технических терминов
                correction = CorrectionAction(
                    type="translation",
                    description="Restore technical terminology",
                    original_content=document_content,
                    corrected_content="",
                    confidence=0.6
                )
                corrections.append(correction)
            
            # Проверяем наличие IPMI/BMC команд
            if "ipmi" not in document_content.lower() and "bmc" not in document_content.lower():
                correction = CorrectionAction(
                    type="translation",
                    description="Restore IPMI/BMC command references",
                    original_content=document_content,
                    corrected_content="",
                    confidence=0.7
                )
                corrections.append(correction)
            
            return corrections[:1]  # Одна коррекция перевода
            
        except Exception as e:
            self.logger.error(f"Error generating translation corrections: {e}")
            return []
    
    async def _generate_formatting_corrections(
        self,
        document_content: str,
        visual_diff: Dict[str, Any]
    ) -> List[CorrectionAction]:
        """Генерация коррекций форматирования"""
        corrections = []
        
        try:
            # Проверяем базовое Markdown форматирование
            issues = []
            
            # Проверяем заголовки
            if not re.search(r'^#{1,6}\s+.+$', document_content, re.MULTILINE):
                issues.append("No markdown headings found")
            
            # Проверяем таблицы
            if '|' not in document_content and 'table' in document_content.lower():
                issues.append("Tables not in markdown format")
            
            # Проверяем код блоки
            if 'ipmi' in document_content.lower() and '```' not in document_content:
                issues.append("Code blocks not formatted")
            
            for issue in issues:
                correction = CorrectionAction(
                    type="formatting",
                    description=f"Fix markdown formatting: {issue}",
                    original_content=document_content,
                    corrected_content="",
                    confidence=0.9
                )
                corrections.append(correction)
            
            return corrections[:2]  # Максимум 2 коррекции форматирования
            
        except Exception as e:
            self.logger.error(f"Error generating formatting corrections: {e}")
            return []
    
    async def _apply_single_correction(
        self,
        document_content: str,
        correction: CorrectionAction
    ) -> str:
        """Применение одной коррекции через vLLM"""
        try:
            if not self.http_client:
                self.http_client = aiohttp.ClientSession()
            
            # Формируем промпт для коррекции
            system_prompt = self._get_correction_prompt(correction.type, correction.description)
            
            # Запрос к vLLM
            async with self.http_client.post(
                f"{self.config.vllm_base_url}/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.config.vllm_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "Qwen/Qwen2.5-32B-Instruct",
                    "messages": [
                        {"role": "system", "content": "You are a helpful technical editor."}, 
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 4096
                }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    corrected_content = result["choices"][0]["message"]["content"]
                    correction.corrected_content = corrected_content
                    return corrected_content
                else:
                    raise Exception(f"vLLM request failed: {response.status}")
                    
        except Exception as e:
            self.logger.error(f"Error applying correction: {e}")
            raise
    
    def _get_correction_prompt(self, correction_type: str, description: str) -> str:
        """Получение промпта для коррекции определенного типа"""
        
        base_prompt = """You are an expert document corrector specializing in technical documentation.

CRITICAL RULES:
1. PRESERVE all technical commands, API calls, and parameter names
2. MAINTAIN original document structure and formatting
3. OUTPUT only the corrected document content
4. DO NOT add explanations or comments

"""
        
        type_specific = {
            "ocr": """
TASK: Fix OCR recognition errors while preserving technical content.
- Correct obvious character recognition mistakes
- Fix spacing and punctuation errors
- Maintain all IPMI, BMC, Redfish commands exactly as they are
- Keep Chinese text in Chinese, English text in English
""",
            "structure": """
TASK: Fix document structure and heading hierarchy.
- Ensure proper markdown heading levels (# ## ### etc.)
- Maintain logical document flow
- Preserve all content while improving organization
- Keep technical sections properly structured
""",
            "translation": """
TASK: Restore missing technical terminology and improve translation quality.
- Add back missing technical terms (IPMI, BMC, API names)
- Improve translation consistency
- Preserve all command syntax and technical parameters
- Maintain mixed language content where appropriate
""",
            "formatting": """
TASK: Fix markdown formatting issues.
- Convert tables to proper markdown table format
- Wrap technical commands in code blocks (```)
- Fix heading formatting (# ## ###)
- Preserve all content while improving presentation
"""
        }
        
        return base_prompt + type_specific.get(correction_type, type_specific["formatting"])
    
    async def _final_review_correction(
        self,
        original_content: str,
        corrected_content: str,
        correction_id: str
    ) -> str:
        """Финальный обзор и валидация коррекций"""
        try:
            # Простая проверка - не потерялся ли контент значительно
            original_length = len(original_content.split())
            corrected_length = len(corrected_content.split())
            
            # Если потерялось более 30% контента, возвращаем оригинал
            if corrected_length < original_length * 0.7:
                self.logger.warning(f"Correction {correction_id} removed too much content, reverting")
                return original_content
            
            return corrected_content
            
        except Exception as e:
            self.logger.error(f"Error in final review: {e}")
            return corrected_content

# =======================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =======================================================================================

def create_auto_corrector(config: Optional[AutoCorrectorConfig] = None) -> AutoCorrector:
    """Фабричная функция для создания Auto Corrector"""
    return AutoCorrector(config)

async def apply_document_corrections(
    document_content: str,
    validation_results: Dict[str, Any],
    correction_id: str,
    config: Optional[AutoCorrectorConfig] = None
) -> CorrectionResult:
    """
    Высокоуровневая функция для применения автокоррекций
    
    Args:
        document_content: Содержимое документа
        validation_results: Результаты валидации
        correction_id: Идентификатор коррекции
        config: Конфигурация корректора
        
    Returns:
        CorrectionResult: Результат коррекции
    """
    async with create_auto_corrector(config) as corrector:
        return await corrector.apply_corrections(document_content, validation_results, correction_id)

# =======================================================================================
# ОСНОВНОЙ БЛОК ДЛЯ ТЕСТИРОВАНИЯ
# =======================================================================================

if __name__ == "__main__":
    # Пример использования
    async def main():
        config = AutoCorrectorConfig()
        
        document_content = """
# 测试文档
这是一个测试文档，包含一些OCR错误和格式问题。

## 1PM1 Commands
以下是一些1PM1命令的示例：
- ipmitool power status
- ipmitool sensor list
        """
        
        validation_results = {
            "ocr_validation": {
                "consensus_confidence": 0.75,
                "issues_found": ["Low similarity between OCR results: 0.65"]
            },
            "ast_comparison": {
                "overall_similarity": 0.85,
                "issues_found": ["2 headings have different levels"]
            }
        }
        
        async with create_auto_corrector(config) as corrector:
            result = await corrector.apply_corrections(
                document_content, validation_results, "test_correction"
            )
            
            print(f"Total corrections: {result.total_corrections}")
            print(f"Successful: {result.successful_corrections}")
            print(f"Failed: {result.failed_corrections}")
            
            if result.corrected_document:
                print("Corrected document length:", len(result.corrected_document))
    
    # asyncio.run(main())  # Закомментировано для избежания ошибок без vLLM сервера
    print("Auto corrector module loaded successfully")