#!/usr/bin/env python3
"""
Structure Analyzer для PDF Converter Pipeline v4.0
Анализ структуры документов и объединение результатов разных процессоров
"""

import os
import logging
from typing import Dict, Any, List, Optional, Set
import json
import traceback
import re
from dataclasses import dataclass

# Импорты для работы с текстом
from collections import defaultdict, Counter
import unicodedata

logger = logging.getLogger(__name__)

@dataclass
class AnalysisConfig:
    """Конфигурация анализатора структуры"""
    min_heading_length: int = 5
    max_heading_length: int = 200
    check_cross_references: bool = True
    enable_layout_analysis: bool = True
    merge_blocks: bool = True
    heading_detection_threshold: float = 0.7


class StructureAnalyzer:
    """
    Анализатор структуры документов
    """
    
    def __init__(self, config: AnalysisConfig = None):
        """
        Инициализация анализатора структуры
        
        Args:
            config: Конфигурация анализатора
        """
        self.config = config or AnalysisConfig()
        
        # Параметры из конфигурации
        self.enable_layout_analysis = self.config.enable_layout_analysis
        self.merge_blocks = self.config.merge_blocks
        self.heading_detection_threshold = self.config.heading_detection_threshold
        self.min_heading_length = self.config.min_heading_length
        self.max_heading_length = self.config.max_heading_length
        self.check_cross_references = self.config.check_cross_references
        
        # Паттерны для определения заголовков
        self.heading_patterns = [
            r'^[IVX]{1,4}\.?\s+',  # Римские цифры
            r'^\d+\.?\s+',  # Арабские цифры
            r'^\d+\.\d+\.?\s+',  # Нумерация типа 1.1
            r'^\d+\.\d+\.\d+\.?\s+',  # Нумерация типа 1.1.1
            r'^[A-Za-zА-Яа-я]+\.?\s+',  # Буквенная нумерация
            r'^Глава\s+\d+',  # Главы
            r'^Раздел\s+\d+',  # Разделы
            r'^Часть\s+\d+'  # Части
        ]
        
        logger.info("Structure Analyzer инициализирован")
    
    async def analyze_document_structure(self, document_structure) -> Dict[str, Any]:
        """
        Анализ структуры документа (для совместимости с main.py)
        
        Args:
            document_structure: Структура документа от Docling
            
        Returns:
            Dict: Результат анализа структуры
        """
        return self.analyze_document(document_structure)
    
    def analyze_document(self, docling_result: Dict[str, Any], 
                        ocr_result: List[Dict[str, Any]] = None,
                        table_result: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Анализ структуры документа на основе результатов всех процессоров
        
        Args:
            docling_result: Результаты Docling
            ocr_result: Результаты OCR (по страницам)
            table_result: Результаты извлечения таблиц
            
        Returns:
            Dict: Структурированный анализ документа
        """
        try:
            logger.info("Начинаем структурный анализ документа")
            
            # Если входные данные - это DocumentStructure объект, преобразуем в dict
            if hasattr(docling_result, '__dict__'):
                docling_dict = {
                    "filename": getattr(docling_result, 'title', 'unknown'),
                    "page_count": len(getattr(docling_result, 'sections', [])),
                    "text": "",
                    "markdown": "",
                    "pages": [],
                    "tables": getattr(docling_result, 'tables', []),
                    "figures": getattr(docling_result, 'images', []),
                    "metadata": getattr(docling_result, 'metadata', {})
                }
                
                # Конвертируем секции в страницы для совместимости
                for i, section in enumerate(getattr(docling_result, 'sections', [])):
                    page_data = {
                        "page_number": i + 1,
                        "text": section.get('content', ''),
                        "elements": [{
                            "type": "heading",
                            "text": section.get('title', ''),
                            "bbox": None
                        }],
                        "tables": [],
                        "figures": []
                    }
                    docling_dict["pages"].append(page_data)
                
                docling_result = docling_dict
            
            analysis = {
                "document_info": {
                    "filename": docling_result.get("filename", "unknown"),
                    "total_pages": docling_result.get("page_count", 0),
                    "total_text_blocks": 0,
                    "total_tables": len(docling_result.get("tables", [])),
                    "total_figures": len(docling_result.get("figures", [])),
                    "languages_detected": [],
                    "processing_methods": ["docling"]
                },
                "structure": {
                    "headings": [],
                    "paragraphs": [],
                    "lists": [],
                    "tables": [],
                    "figures": [],
                    "footnotes": [],
                    "headers_footers": []
                },
                "content_analysis": {
                    "reading_order": [],
                    "logical_sections": [],
                    "cross_references": [],
                    "document_outline": []
                },
                "quality_metrics": {
                    "text_extraction_confidence": 0.0,
                    "structure_detection_confidence": 0.0,
                    "table_extraction_quality": 0.0
                }
            }
            
            # Анализ текстовых блоков из Docling
            if docling_result:
                analysis = self._analyze_docling_structure(analysis, docling_result)
            
            # Дополнение OCR результатами
            if ocr_result:
                analysis = self._integrate_ocr_results(analysis, ocr_result)
            
            # Интеграция таблиц
            if table_result:
                analysis = self._integrate_table_results(analysis, table_result)
            
            # Структурный анализ
            if self.enable_layout_analysis:
                analysis = self._perform_layout_analysis(analysis)
            
            # Определение порядка чтения
            analysis = self._determine_reading_order(analysis)
            
            # Расчет метрик качества
            analysis = self._calculate_quality_metrics(analysis, docling_result, ocr_result, table_result)
            
            logger.info("Структурный анализ завершен")
            return analysis
            
        except Exception as e:
            logger.error(f"Ошибка структурного анализа: {e}")
            logger.error(traceback.format_exc())
            return self._create_empty_analysis(docling_result.get("filename", "unknown") if isinstance(docling_result, dict) else "unknown")
    
    def _analyze_docling_structure(self, analysis: Dict[str, Any], docling_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Анализ структуры из результатов Docling
        """
        try:
            analysis["document_info"]["processing_methods"].append("docling")
            
            # Обработка страниц
            for page in docling_result.get("pages", []):
                page_num = page.get("page_number", 0)
                
                for element in page.get("elements", []):
                    element_type = element.get("type", "").lower()
                    text = element.get("text", "").strip()
                    bbox = element.get("bbox")
                    
                    if not text:
                        continue
                    
                    # Классификация элементов
                    if "heading" in element_type or "title" in element_type or self._is_heading(text):
                        analysis["structure"]["headings"].append({
                            "text": text,
                            "page": page_num,
                            "bbox": bbox,
                            "type": element_type,
                            "level": self._determine_heading_level(text),
                            "source": "docling"
                        })
                    elif "paragraph" in element_type or "text" in element_type:
                        analysis["structure"]["paragraphs"].append({
                            "text": text,
                            "page": page_num,
                            "bbox": bbox,
                            "type": element_type,
                            "word_count": len(text.split()),
                            "source": "docling"
                        })
                    elif "list" in element_type:
                        analysis["structure"]["lists"].append({
                            "text": text,
                            "page": page_num,
                            "bbox": bbox,
                            "type": element_type,
                            "items": self._extract_list_items(text),
                            "source": "docling"
                        })
                
                analysis["document_info"]["total_text_blocks"] += len(page.get("elements", []))
            
            return analysis
            
        except Exception as e:
            logger.error(f"Ошибка анализа Docling структуры: {e}")
            return analysis
    
    def _integrate_ocr_results(self, analysis: Dict[str, Any], ocr_result: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Интеграция результатов OCR в структурный анализ
        """
        try:
            analysis["document_info"]["processing_methods"].append("paddleocr")
            
            for page_result in ocr_result:
                page_num = page_result.get("page_num", 0)
                
                # Добавляем текстовые блоки из OCR, если они не пересекаются с Docling
                for text_block in page_result.get("text_blocks", []):
                    text = text_block.get("text", "").strip()
                    
                    if text and not self._text_already_captured(text, analysis):
                        if self._is_heading(text):
                            analysis["structure"]["headings"].append({
                                "text": text,
                                "page": page_num + 1,
                                "bbox": text_block.get("bbox"),
                                "confidence": text_block.get("confidence", 0.0),
                                "level": self._determine_heading_level(text),
                                "source": "ocr"
                            })
                        else:
                            analysis["structure"]["paragraphs"].append({
                                "text": text,
                                "page": page_num + 1,
                                "bbox": text_block.get("bbox"),
                                "confidence": text_block.get("confidence", 0.0),
                                "word_count": len(text.split()),
                                "source": "ocr"
                            })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Ошибка интеграции OCR результатов: {e}")
            return analysis
    
    def _integrate_table_results(self, analysis: Dict[str, Any], table_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Интеграция результатов извлечения таблиц
        """
        try:
            methods_used = table_result.get("methods_used", [])
            if methods_used:
                analysis["document_info"]["processing_methods"].extend(methods_used)
            
            # Добавляем таблицы из извлекателя таблиц
            for table in table_result.get("tables", []):
                analysis["structure"]["tables"].append({
                    "table_id": table.get("table_id", ""),
                    "method": table.get("method", ""),
                    "page": table.get("page"),
                    "rows": table.get("rows", 0),
                    "columns": table.get("columns", 0),
                    "data": table.get("data", []),
                    "bbox": table.get("bbox"),
                    "confidence": table.get("confidence"),
                    "source": "table_extractor"
                })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Ошибка интеграции результатов таблиц: {e}")
            return analysis
    
    def _perform_layout_analysis(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Анализ макета документа
        """
        try:
            # Группировка элементов по страницам
            pages_content = defaultdict(list)
            
            for heading in analysis["structure"]["headings"]:
                pages_content[heading["page"]].append({"type": "heading", "data": heading})
            
            for paragraph in analysis["structure"]["paragraphs"]:
                pages_content[paragraph["page"]].append({"type": "paragraph", "data": paragraph})
            
            for table in analysis["structure"]["tables"]:
                if table.get("page"):
                    pages_content[table["page"]].append({"type": "table", "data": table})
            
            # Создание логических секций
            logical_sections = []
            current_section = None
            
            for page_num in sorted(pages_content.keys()):
                page_elements = pages_content[page_num]
                
                # Сортировка элементов по вертикальной позиции (если есть bbox)
                page_elements.sort(key=lambda x: self._get_vertical_position(x))
                
                for element in page_elements:
                    if element["type"] == "heading":
                        # Начинаем новую секцию
                        if current_section:
                            logical_sections.append(current_section)
                        
                        current_section = {
                            "title": element["data"]["text"],
                            "level": element["data"].get("level", 1),
                            "page_start": page_num,
                            "page_end": page_num,
                            "elements": [element]
                        }
                    elif current_section:
                        # Добавляем элемент к текущей секции
                        current_section["elements"].append(element)
                        current_section["page_end"] = page_num
            
            # Добавляем последнюю секцию
            if current_section:
                logical_sections.append(current_section)
            
            analysis["content_analysis"]["logical_sections"] = logical_sections
            
            return analysis
            
        except Exception as e:
            logger.error(f"Ошибка анализа макета: {e}")
            return analysis
    
    # Остальные методы остаются такими же, как в оригинальном файле
    def _determine_reading_order(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Определение порядка чтения документа"""
        reading_order = []
        
        # Объединяем все текстовые элементы
        all_elements = []
        
        for heading in analysis["structure"]["headings"]:
            all_elements.append({
                "type": "heading",
                "text": heading["text"],
                "page": heading["page"],
                "bbox": heading.get("bbox"),
                "order_weight": 1
            })
        
        for paragraph in analysis["structure"]["paragraphs"]:
            all_elements.append({
                "type": "paragraph", 
                "text": paragraph["text"],
                "page": paragraph["page"],
                "bbox": paragraph.get("bbox"),
                "order_weight": 2
            })
        
        # Сортировка по странице, затем по порядку
        all_elements.sort(key=lambda x: (x["page"], x["order_weight"]))
        
        analysis["content_analysis"]["reading_order"] = all_elements
        return analysis
    
    def _calculate_quality_metrics(self, analysis: Dict[str, Any], docling_result, ocr_result, table_result) -> Dict[str, Any]:
        """Расчет метрик качества"""
        metrics = analysis["quality_metrics"]
        
        # Простые метрики по умолчанию
        metrics["text_extraction_confidence"] = 0.9
        metrics["structure_detection_confidence"] = 0.8
        metrics["table_extraction_quality"] = 0.7
        
        return analysis
    
    def _is_heading(self, text: str) -> bool:
        """Определение, является ли текст заголовком"""
        try:
            text = text.strip()
            
            # Проверка длины
            if len(text) < self.min_heading_length or len(text) > self.max_heading_length:
                return False
            
            # Проверка паттернов заголовков
            for pattern in self.heading_patterns:
                if re.match(pattern, text, re.IGNORECASE):
                    return True
            
            # Дополнительные эвристики
            if len(text) < 100 and text.isupper():
                return True
            
            if text.endswith(':') and len(text) < 150:
                return True
            
            return False
            
        except Exception:
            return False
    
    def _determine_heading_level(self, text: str) -> int:
        """Определение уровня заголовка"""
        try:
            text = text.strip()
            
            # Римские цифры - уровень 1
            if re.match(r'^[IVX]{1,4}\.?\s+', text):
                return 1
            # Простые цифры - уровень 2  
            elif re.match(r'^\d+\.?\s+', text):
                return 2
            # Нумерация типа 1.1 - уровень 3
            elif re.match(r'^\d+\.\d+\.?\s+', text):
                return 3
            # По умолчанию
            else:
                return 2
                
        except Exception:
            return 2
    
    def _extract_list_items(self, text: str) -> List[str]:
        """Извлечение элементов списка из текста"""
        try:
            items = []
            for line in text.split('\n'):
                line = line.strip()
                if line:
                    # Удаляем маркеры списка
                    clean_line = re.sub(r'^[•\-\*\d+\.]\s*', '', line)
                    if clean_line:
                        items.append(clean_line)
            return items
        except Exception:
            return [text]
    
    def _text_already_captured(self, text: str, analysis: Dict[str, Any]) -> bool:
        """Проверка, был ли текст уже захвачен"""
        text_lower = text.lower().strip()
        
        # Проверяем заголовки
        for heading in analysis["structure"]["headings"]:
            if heading["text"].lower().strip() == text_lower:
                return True
        
        # Проверяем параграфы  
        for paragraph in analysis["structure"]["paragraphs"]:
            if paragraph["text"].lower().strip() == text_lower:
                return True
        
        return False
    
    def _get_vertical_position(self, element: Dict[str, Any]) -> float:
        """Получение вертикальной позиции элемента для сортировки"""
        try:
            bbox = element.get("data", {}).get("bbox")
            if bbox and isinstance(bbox, dict):
                return bbox.get("y1", 0.0)
            elif bbox and isinstance(bbox, (list, tuple)) and len(bbox) >= 2:
                return bbox[1]  # y координата
            else:
                return 0.0
        except Exception:
            return 0.0
    
    def _create_empty_analysis(self, filename: str) -> Dict[str, Any]:
        """Создание пустого анализа при ошибке"""
        return {
            "document_info": {
                "filename": filename,
                "total_pages": 0,
                "total_text_blocks": 0,
                "total_tables": 0,
                "total_figures": 0,
                "languages_detected": [],
                "processing_methods": []
            },
            "structure": {
                "headings": [],
                "paragraphs": [],
                "lists": [],
                "tables": [],
                "figures": [],
                "footnotes": [],
                "headers_footers": []
            },
            "content_analysis": {
                "reading_order": [],
                "logical_sections": [],
                "cross_references": [],
                "document_outline": []
            },
            "quality_metrics": {
                "text_extraction_confidence": 0.0,
                "structure_detection_confidence": 0.0,
                "table_extraction_quality": 0.0
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Проверка работоспособности анализатора структуры"""
        try:
            return {
                "status": "healthy",
                "enable_layout_analysis": self.enable_layout_analysis,
                "merge_blocks": self.merge_blocks,
                "heading_detection_threshold": self.heading_detection_threshold,
                "available_patterns": len(self.heading_patterns)
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }


def create_structure_analyzer(config: AnalysisConfig = None) -> StructureAnalyzer:
    """Фабричная функция для создания анализатора структуры"""
    return StructureAnalyzer(config)