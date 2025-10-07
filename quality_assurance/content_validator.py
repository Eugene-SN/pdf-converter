#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Content Validator для PDF Converter Pipeline v4.0
Валидация содержимого документа на соответствие техническим требованиям
"""

import re
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass
import structlog
from prometheus_client import Counter, Histogram

logger = structlog.get_logger("content_validator")

# Prometheus метрики
content_validation_requests = Counter('content_validation_requests_total', 'Content validation requests', ['status'])
content_validation_duration = Histogram('content_validation_duration_seconds', 'Content validation duration')

@dataclass
class ContentValidationConfig:
    """Конфигурация валидации содержимого"""
    # Технические термины которые должны сохраняться
    required_technical_terms: List[str] = None
    
    # Минимальные требования
    min_technical_terms: int = 5
    min_code_blocks: int = 1
    
    def __post_init__(self):
        if self.required_technical_terms is None:
            self.required_technical_terms = [
                'IPMI', 'BMC', 'Redfish', 'ipmitool', 'API', 'HTTP', 'REST',
                'JSON', 'XML', 'SNMP', 'SSH', 'Telnet', 'CLI', 'GUI',
                'firmware', 'BIOS', 'UEFI', 'sensor', 'temperature',
                'voltage', 'power', 'fan', 'CPU', 'memory', 'storage'
            ]

@dataclass
class ContentValidationResult:
    """Результат валидации содержимого"""
    passed: bool
    score: float
    issues_found: List[str]
    recommendations: List[str]
    technical_terms_found: int
    code_blocks_found: int
    processing_time: float
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

class ContentValidator:
    """Валидатор содержимого документа"""
    
    def __init__(self, config: Optional[ContentValidationConfig] = None):
        self.config = config or ContentValidationConfig()
        self.logger = structlog.get_logger("content_validator")
    
    async def validate_content(self, document_content: str, document_type: str = "technical") -> ContentValidationResult:
        """Валидация содержимого документа"""
        import time
        start_time = time.time()
        
        try:
            content_validation_requests.labels(status='started').inc()
            
            issues_found = []
            recommendations = []
            
            # Проверка технических терминов
            technical_terms_found = self._count_technical_terms(document_content)
            if technical_terms_found < self.config.min_technical_terms:
                issues_found.append(f"Too few technical terms found: {technical_terms_found} (expected >= {self.config.min_technical_terms})")
                recommendations.append("Check if technical terminology was properly preserved during processing")
            
            # Проверка блоков кода
            code_blocks_found = len(re.findall(r'```[\s\S]*?```', document_content))
            if code_blocks_found < self.config.min_code_blocks and 'command' in document_content.lower():
                issues_found.append(f"Too few code blocks found: {code_blocks_found} (expected >= {self.config.min_code_blocks})")
                recommendations.append("Technical commands should be wrapped in code blocks")
            
            # Проверка структуры markdown
            if not re.search(r'^#{1,6}\s+.+$', document_content, re.MULTILINE):
                issues_found.append("No markdown headings found")
                recommendations.append("Document should have proper markdown heading structure")
            
            # Расчет общего скора
            score = 1.0
            if issues_found:
                score -= 0.2 * len(issues_found)
            score = max(0.0, score)
            
            processing_time = time.time() - start_time
            content_validation_duration.observe(processing_time)
            content_validation_requests.labels(status='success').inc()
            
            return ContentValidationResult(
                passed=len(issues_found) == 0,
                score=score,
                issues_found=issues_found,
                recommendations=recommendations,
                technical_terms_found=technical_terms_found,
                code_blocks_found=code_blocks_found,
                processing_time=processing_time
            )
            
        except Exception as e:
            content_validation_requests.labels(status='error').inc()
            logger.error(f"Content validation error: {e}")
            raise
    
    def _count_technical_terms(self, content: str) -> int:
        """Подсчет технических терминов в документе"""
        count = 0
        content_lower = content.lower()
        
        for term in self.config.required_technical_terms:
            if term.lower() in content_lower:
                count += 1
        
        return count