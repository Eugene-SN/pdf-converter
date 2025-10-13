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
from dataclasses import dataclass, field
from datetime import datetime

# HTTP клиенты для взаимодействия с vLLM
import httpx
import aiohttp
from aiohttp import ClientTimeout

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge

from translator.vllm_client import (
    build_vllm_headers,
    create_vllm_aiohttp_session,
    get_vllm_api_key,
    get_vllm_server_url,
)

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
    vllm_base_url: str = field(default_factory=get_vllm_server_url)
    vllm_api_key: Optional[str] = field(default_factory=get_vllm_api_key)
    # Централизованная модель vLLM (совпадает с TRANSLATION_MODEL в translator)
    vllm_model: str = field(
        default_factory=lambda: os.getenv(
            "VLLM_MODEL_NAME", "Qwen/Qwen3-30B-A3B-Instruct-2507"
        )
    )
    vllm_connect_timeout: float = 10.0
    vllm_request_timeout: float = 300.0
    vllm_max_retries: int = 1
    vllm_retry_backoff_seconds: float = 2.0
    
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

    def _build_client_timeout(self) -> ClientTimeout:
        """Создаем таймауты для долгих запросов к vLLM."""
        return ClientTimeout(
            total=self.config.vllm_request_timeout + self.config.vllm_connect_timeout,
            connect=self.config.vllm_connect_timeout,
            sock_connect=self.config.vllm_connect_timeout,
            sock_read=self.config.vllm_request_timeout,
        )

    async def __aenter__(self):
        """Async context manager entry"""
        self.http_client = create_vllm_aiohttp_session(
            timeout=self._build_client_timeout(),
            api_key=self.config.vllm_api_key,
            base_url=self.config.vllm_base_url,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.http_client:
            await self.http_client.close()

    async def _ensure_http_client(self) -> aiohttp.ClientSession:
        """Гарантирует наличие активного HTTP клиента."""
        if self.http_client is None or self.http_client.closed:
            self.http_client = create_vllm_aiohttp_session(
                timeout=self._build_client_timeout(),
                api_key=self.config.vllm_api_key,
                base_url=self.config.vllm_base_url,
            )
        return self.http_client
    
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
            client = await self._ensure_http_client()

            # Формируем промпт для коррекции
            system_prompt = self._get_correction_prompt(correction.type, correction.description)
            prompt, excerpt_truncated = self._build_user_prompt(document_content, correction)

            self.logger.info(
                "sending_vllm_correction_request",
                correction_type=correction.type,
                prompt_length=len(prompt),
                excerpt_truncated=excerpt_truncated,
            )

            attempts = max(1, int(self.config.vllm_max_retries) + 1)
            last_exception: Optional[Exception] = None
            failure_status = "failed"
            request_timeout = self._build_client_timeout()
            loop = asyncio.get_running_loop()

            for attempt in range(1, attempts + 1):
                attempt_started = loop.time()
                try:
                    async with client.post(
                        f"{self.config.vllm_base_url}/v1/chat/completions",
                        headers=build_vllm_headers(
                            "application/json", api_key=self.config.vllm_api_key
                        ),
                        json={
                            "model": self.config.vllm_model,
                            "prompt": prompt,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.1,
                        "max_tokens": 4096
                    },
                    timeout=request_timeout,
                    ) as response:
                        duration = loop.time() - attempt_started
                        if response.status == 200:
                            result = await response.json(content_type=None)
                            choices = result.get("choices", []) if isinstance(result, dict) else []
                            message = choices[0].get("message", {}) if choices else {}
                            corrected_content = message.get("content", "")

                            if not corrected_content.strip():
                                self.logger.warning(
                                    "empty_vllm_response",
                                    correction_type=correction.type,
                                    duration=duration,
                                    attempt=attempt,
                                )
                                correction.error_message = "Empty response from vLLM"
                                correction.corrected_content = document_content
                                correction_requests.labels(correction_type=correction.type, status="failed").inc()
                                return document_content

                            self.logger.info(
                                "vllm_correction_completed",
                                correction_type=correction.type,
                                duration=duration,
                                attempt=attempt,
                                response_length=len(corrected_content),
                            )
                            correction.corrected_content = corrected_content
                            correction_requests.labels(correction_type=correction.type, status="completed").inc()
                            return corrected_content

                        error_body = await response.text()
                        self.logger.error(
                            "vllm_request_failed",
                            status=response.status,
                            body_preview=error_body[:500],
                            correction_type=correction.type,
                            duration=duration,
                            attempt=attempt,
                        )
                        last_exception = Exception(f"vLLM request failed with status {response.status}")
                        failure_status = "failed"
                except asyncio.TimeoutError as timeout_error:
                    duration = loop.time() - attempt_started
                    self.logger.error(
                        "vllm_request_timeout",
                        correction_type=correction.type,
                        timeout=self.config.vllm_request_timeout,
                        duration=duration,
                        attempt=attempt,
                    )
                    last_exception = timeout_error
                    failure_status = "timeout"
                except aiohttp.ClientError as client_error:
                    duration = loop.time() - attempt_started
                    self.logger.error(
                        "vllm_client_error",
                        correction_type=correction.type,
                        error=str(client_error),
                        duration=duration,
                        attempt=attempt,
                    )
                    last_exception = client_error
                    failure_status = "failed"

                if attempt < attempts and self.config.vllm_retry_backoff_seconds > 0:
                    await asyncio.sleep(self.config.vllm_retry_backoff_seconds)

            if last_exception:
                correction.error_message = str(last_exception)
            correction_requests.labels(correction_type=correction.type, status=failure_status).inc()
            return document_content

        except Exception as e:
            self.logger.error("unexpected_vllm_error", correction_type=correction.type, error=str(e))
            correction.error_message = str(e)
            correction_requests.labels(correction_type=correction.type, status="failed").inc()
            return document_content

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

    def _build_user_prompt(
        self,
        document_content: str,
        correction: CorrectionAction,
        max_excerpt_length: int = 2000
    ) -> Tuple[str, bool]:
        """Сборка пользовательского промпта с учетом ограничения длины выдержки"""

        excerpt_source = correction.original_content or document_content
        excerpt = (excerpt_source or "").strip()
        excerpt_truncated = False

        if len(excerpt) > max_excerpt_length:
            excerpt = excerpt[:max_excerpt_length].rstrip()
            excerpt_truncated = True

        prompt_lines = [
            "Apply the requested correction to the provided document excerpt.",
            f"Correction request: {correction.description.strip()}",
            "Document excerpt:",
            "```",
            excerpt,
            "```",
            "Return only the corrected excerpt without additional commentary.",
        ]

        if excerpt_truncated:
            prompt_lines.append("(Note: The excerpt was truncated for processing.)")

        prompt = "\n".join(prompt_lines)
        return prompt, excerpt_truncated
    
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