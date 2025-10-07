#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Table Extractor для PDF Converter Pipeline v4.0
Извлечение таблиц из PDF с использованием Tabula-Py и Camelot
ИСПРАВЛЕНА ОШИБКА: edge_tol,row_tol cannot be used with flavor='lattice'
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
import subprocess

# Библиотеки для извлечения таблиц
import tabula
import camelot
import pandas as pd

# PDF обработка
import fitz  # PyMuPDF
from pdf2image import convert_from_path

# Обработка изображений
import cv2
import numpy as np
from PIL import Image, ImageDraw

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge

# =======================================================================================
# КОНФИГУРАЦИЯ И МЕТРИКИ
# =======================================================================================

logger = structlog.get_logger("table_extractor")

# Prometheus метрики
table_extraction_requests = Counter('table_extraction_requests_total', 'Table extraction requests', ['engine', 'status'])
table_extraction_duration = Histogram('table_extraction_duration_seconds', 'Table extraction duration', ['engine'])
tables_extracted = Counter('tables_extracted_total', 'Total tables extracted', ['engine', 'quality'])

@dataclass
class TableConfig:
    """Конфигурация извлечения таблиц"""
    # ✅ УСКОРЕНИЕ: уменьшены ресурсы
    java_options: str = "-Xmx1024m"  # Уменьшено с 2048m
    
    # Thresholds для определения качества таблиц
    detection_threshold: float = 0.6  # Понижен порог
    min_rows: int = 2
    min_cols: int = 2
    max_empty_cells_ratio: float = 0.5
    
    # Директории
    temp_dir: str = "/app/temp"
    cache_dir: str = "/app/cache"
    
    # ✅ УСКОРЕНИЕ: оптимизированные настройки
    tabula_pages: str = "all"
    tabula_lattice: bool = True
    tabula_stream: bool = False  # Отключаем медленный stream
    camelot_flavor: str = "lattice"  # Только lattice
    
    # ✅ ИСПРАВЛЕНО: правильные параметры только для stream
    camelot_edge_tol: int = 500  # Используется только для stream
    camelot_row_tol: int = 2     # Используется только для stream

# =======================================================================================
# КЛАССЫ ДАННЫХ
# =======================================================================================

@dataclass
class ExtractedTable:
    """Извлеченная таблица"""
    id: int
    page: int
    engine: str  # "tabula", "camelot", "hybrid"
    confidence: float
    rows: int
    columns: int
    bbox: Optional[Tuple[float, float, float, float]]
    data: List[List[str]]
    headers: List[str]
    file_path: Optional[str] = None
    quality_score: float = 0.0
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

@dataclass
class TableExtractionResult:
    """Результат извлечения всех таблиц"""
    total_tables: int
    successful_extractions: int
    failed_extractions: int
    tables: List[ExtractedTable]
    processing_time: float
    engines_used: Dict[str, int]
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

# =======================================================================================
# TABLE EXTRACTOR КЛАСС
# =======================================================================================

class TableExtractor:
    """Главный класс для извлечения таблиц из PDF"""

    def __init__(self, config: Optional[TableConfig] = None):
        self.config = config or TableConfig()
        self.logger = structlog.get_logger("table_extractor")

        # Создаем необходимые директории
        Path(self.config.temp_dir).mkdir(parents=True, exist_ok=True)
        Path(self.config.cache_dir).mkdir(parents=True, exist_ok=True)

        # Настраиваем Java для Tabula
        os.environ["JAVA_OPTS"] = self.config.java_options

    async def extract_tables_from_pdf(
        self,
        pdf_path: str,
        output_dir: str
    ) -> List[ExtractedTable]:
        """
        Основной метод извлечения таблиц из PDF
        
        Args:
            pdf_path: Путь к PDF файлу
            output_dir: Директория для сохранения результатов
            
        Returns:
            List[ExtractedTable]: Список извлеченных таблиц
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Starting table extraction from: {pdf_path}")

            # Проверяем файл
            if not Path(pdf_path).exists():
                raise FileNotFoundError(f"PDF file not found: {pdf_path}")

            # Создаем рабочую директорию
            work_dir = Path(output_dir) / "tables"
            work_dir.mkdir(parents=True, exist_ok=True)

            # Получаем информацию о PDF
            pdf_info = await self._get_pdf_info(pdf_path)

            all_tables = []
            table_counter = 0
            engines_used = {"tabula": 0, "camelot": 0, "hybrid": 0}

            # Извлекаем таблицы по страницам
            for page_num in range(1, pdf_info["pages"] + 1):
                # Tabula extraction
                tabula_tables = await self._extract_with_tabula(
                    pdf_path, page_num, str(work_dir)
                )

                # Camelot extraction
                camelot_tables = await self._extract_with_camelot(
                    pdf_path, page_num, str(work_dir)
                )

                # Объединяем и выбираем лучшие результаты
                page_tables = await self._merge_extraction_results(
                    tabula_tables, camelot_tables, page_num, table_counter
                )

                # Обновляем счетчики
                for table in page_tables:
                    table_counter += 1
                    table.id = table_counter
                    engines_used[table.engine] += 1

                    # Сохраняем таблицу в CSV
                    csv_path = await self._save_table_to_csv(
                        table, str(work_dir)
                    )
                    table.file_path = csv_path

                all_tables.extend(page_tables)

            # Обновляем метрики
            processing_time = (datetime.now() - start_time).total_seconds()
            table_extraction_duration.labels(engine="combined").observe(processing_time)
            
            for engine, count in engines_used.items():
                if count > 0:
                    tables_extracted.labels(engine=engine, quality="good").inc(count)

            self.logger.info(
                f"Table extraction completed",
                total_tables=len(all_tables),
                processing_time=processing_time,
                engines_used=engines_used
            )

            return all_tables

        except Exception as e:
            table_extraction_requests.labels(engine="combined", status="error").inc()
            self.logger.error(f"Error in table extraction: {e}")
            raise

    async def _get_pdf_info(self, pdf_path: str) -> Dict[str, Any]:
        """Получение информации о PDF"""
        try:
            doc = fitz.open(pdf_path)
            info = {
                "pages": doc.page_count,
                "metadata": doc.metadata,
                "has_text": any(page.get_text() for page in doc),
                "file_size": Path(pdf_path).stat().st_size
            }
            doc.close()
            return info
        except Exception as e:
            self.logger.error(f"Error getting PDF info: {e}")
            return {"pages": 1, "metadata": {}, "has_text": True, "file_size": 0}

    async def _extract_with_tabula(
        self,
        pdf_path: str,
        page_num: int,
        output_dir: str
    ) -> List[ExtractedTable]:
        """Извлечение таблиц с помощью Tabula-Py"""
        tables = []
        
        try:
            table_extraction_requests.labels(engine="tabula", status="started").inc()
            start_time = datetime.now()

            # Пробуем lattice mode (для таблиц с границами)
            try:
                lattice_dfs = tabula.read_pdf(
                    pdf_path,
                    pages=page_num,
                    lattice=True,
                    multiple_tables=True,
                    pandas_options={'header': None}
                )

                for i, df in enumerate(lattice_dfs):
                    if not df.empty and self._validate_dataframe(df):
                        table = ExtractedTable(
                            id=0,  # Будет установлен позже
                            page=page_num,
                            engine="tabula",
                            confidence=0.8,  # Tabula lattice достаточно надежен
                            rows=len(df),
                            columns=len(df.columns),
                            bbox=None,
                            data=df.values.tolist(),
                            headers=df.columns.tolist() if df.columns.notna().any() else [],
                            quality_score=self._calculate_table_quality(df)
                        )
                        tables.append(table)

            except Exception as e:
                self.logger.debug(f"Tabula lattice failed for page {page_num}: {e}")

            # ✅ УСКОРЕНИЕ: убираем stream mode если lattice работает
            # if not tables:  # Только если lattice не дал результатов
            #     try:
            #         # Stream mode отключен для ускорения
            #     except Exception as e:
            #         self.logger.debug(f"Tabula stream failed for page {page_num}: {e}")

            # Метрики
            duration = (datetime.now() - start_time).total_seconds()
            table_extraction_duration.labels(engine="tabula").observe(duration)
            
            if tables:
                table_extraction_requests.labels(engine="tabula", status="success").inc()
            else:
                table_extraction_requests.labels(engine="tabula", status="no_tables").inc()

            return tables

        except Exception as e:
            table_extraction_requests.labels(engine="tabula", status="error").inc()
            self.logger.error(f"Tabula extraction error on page {page_num}: {e}")
            return []

    async def _extract_with_camelot(
        self,
        pdf_path: str,
        page_num: int,
        output_dir: str
    ) -> List[ExtractedTable]:
        """✅ ИСПРАВЛЕНО: Извлечение таблиц с помощью Camelot"""
        tables = []
        
        try:
            table_extraction_requests.labels(engine="camelot", status="started").inc()
            start_time = datetime.now()

            # ✅ ИСПРАВЛЕНО: Пробуем lattice flavor БЕЗ edge_tol и row_tol
            try:
                lattice_tables = camelot.read_pdf(
                    pdf_path,
                    pages=str(page_num),
                    flavor='lattice'
                    # ❌ УБРАНО: edge_tol=self.config.camelot_edge_tol,
                    # ❌ УБРАНО: row_tol=self.config.camelot_row_tol
                )

                for camelot_table in lattice_tables:
                    df = camelot_table.df
                    if not df.empty and self._validate_dataframe(df):
                        # Получаем bbox если доступен
                        bbox = None
                        if hasattr(camelot_table, '_bbox'):
                            bbox = camelot_table._bbox

                        table = ExtractedTable(
                            id=0,
                            page=page_num,
                            engine="camelot",
                            confidence=camelot_table.accuracy / 100.0 if hasattr(camelot_table, 'accuracy') else 0.7,
                            rows=len(df),
                            columns=len(df.columns),
                            bbox=bbox,
                            data=df.values.tolist(),
                            headers=df.iloc[0].tolist() if len(df) > 0 else [],
                            quality_score=self._calculate_table_quality(df),
                            metadata={
                                "accuracy": getattr(camelot_table, 'accuracy', 0),
                                "whitespace": getattr(camelot_table, 'whitespace', 0),
                                "order": getattr(camelot_table, 'order', 0)
                            }
                        )
                        tables.append(table)

            except Exception as e:
                self.logger.debug(f"Camelot lattice failed for page {page_num}: {e}")

            # ✅ ИСПРАВЛЕНО: stream flavor С правильными параметрами (только если lattice не дал результатов)
            if not tables:
                try:
                    stream_tables = camelot.read_pdf(
                        pdf_path,
                        pages=str(page_num),
                        flavor='stream',
                        row_tol=self.config.camelot_row_tol  # ✅ Только для stream
                    )

                    for camelot_table in stream_tables:
                        df = camelot_table.df
                        if not df.empty and self._validate_dataframe(df):
                            table = ExtractedTable(
                                id=0,
                                page=page_num,
                                engine="camelot",
                                confidence=camelot_table.accuracy / 100.0 if hasattr(camelot_table, 'accuracy') else 0.5,
                                rows=len(df),
                                columns=len(df.columns),
                                bbox=None,
                                data=df.values.tolist(),
                                headers=df.iloc[0].tolist() if len(df) > 0 else [],
                                quality_score=self._calculate_table_quality(df)
                            )
                            tables.append(table)

                except Exception as e:
                    self.logger.debug(f"Camelot stream failed for page {page_num}: {e}")

            # Метрики
            duration = (datetime.now() - start_time).total_seconds()
            table_extraction_duration.labels(engine="camelot").observe(duration)
            
            if tables:
                table_extraction_requests.labels(engine="camelot", status="success").inc()
            else:
                table_extraction_requests.labels(engine="camelot", status="no_tables").inc()

            return tables

        except Exception as e:
            table_extraction_requests.labels(engine="camelot", status="error").inc()
            self.logger.error(f"Camelot extraction error on page {page_num}: {e}")
            return []

    async def _merge_extraction_results(
        self,
        tabula_tables: List[ExtractedTable],
        camelot_tables: List[ExtractedTable],
        page_num: int,
        table_counter: int
    ) -> List[ExtractedTable]:
        """Объединение результатов разных движков"""
        if not tabula_tables and not camelot_tables:
            return []

        # Если только один движок дал результаты
        if not tabula_tables:
            return camelot_tables
        if not camelot_tables:
            return tabula_tables

        # Если оба движка дали результаты, выбираем лучшие
        merged_tables = []

        # Простая стратегия: берем таблицы с лучшим quality_score
        all_tables = tabula_tables + camelot_tables
        all_tables.sort(key=lambda t: t.quality_score, reverse=True)

        # Убираем дубликаты (таблицы с похожими размерами)
        for table in all_tables:
            is_duplicate = False
            for existing in merged_tables:
                if (abs(table.rows - existing.rows) <= 1 and
                    abs(table.columns - existing.columns) <= 1):
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                merged_tables.append(table)

        # Помечаем лучшие таблицы как hybrid если использовались оба движка
        if len(merged_tables) > 0 and len(tabula_tables) > 0 and len(camelot_tables) > 0:
            merged_tables[0].engine = "hybrid"

        return merged_tables

    def _validate_dataframe(self, df: pd.DataFrame) -> bool:
        """Валидация DataFrame таблицы"""
        try:
            # Основные проверки
            if df.empty:
                return False
                
            if len(df) < self.config.min_rows:
                return False
                
            if len(df.columns) < self.config.min_cols:
                return False

            # Проверка на слишком много пустых ячеек
            total_cells = len(df) * len(df.columns)
            empty_cells = df.isnull().sum().sum()
            if empty_cells / total_cells > self.config.max_empty_cells_ratio:
                return False

            return True

        except Exception:
            return False

    def _calculate_table_quality(self, df: pd.DataFrame) -> float:
        """Расчет качества таблицы"""
        try:
            if df.empty:
                return 0.0

            # Базовый балл
            quality = 0.5

            # Бонус за размер
            if len(df) >= 3:
                quality += 0.1
            if len(df.columns) >= 3:
                quality += 0.1

            # Штраф за пустые ячейки
            total_cells = len(df) * len(df.columns)
            empty_cells = df.isnull().sum().sum()
            empty_ratio = empty_cells / total_cells
            quality -= empty_ratio * 0.3

            # Бонус за наличие заголовков
            first_row = df.iloc[0] if len(df) > 0 else pd.Series()
            if not first_row.isnull().all():
                quality += 0.1

            # Бонус за разнообразие данных
            unique_values = df.nunique().mean()
            if unique_values > 1:
                quality += 0.1

            return max(0.0, min(1.0, quality))

        except Exception:
            return 0.5

    async def _save_table_to_csv(
        self,
        table: ExtractedTable,
        output_dir: str
    ) -> str:
        """Сохранение таблицы в CSV файл"""
        try:
            filename = f"table_{table.page}_{table.id}_{table.engine}.csv"
            file_path = Path(output_dir) / filename

            # Создаем DataFrame
            df = pd.DataFrame(table.data)

            # Устанавливаем заголовки если есть
            if table.headers:
                df.columns = table.headers[:len(df.columns)]

            # Сохраняем в CSV
            df.to_csv(file_path, index=False, encoding='utf-8')

            return str(file_path)

        except Exception as e:
            self.logger.error(f"Error saving table to CSV: {e}")
            return ""

# =======================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =======================================================================================

def create_table_extractor(config: Optional[TableConfig] = None) -> TableExtractor:
    """Фабричная функция для создания Table Extractor"""
    return TableExtractor(config)

async def extract_tables_from_pdf(
    pdf_path: str,
    output_dir: str,
    config: Optional[TableConfig] = None
) -> TableExtractionResult:
    """
    Высокоуровневая функция для извлечения таблиц
    
    Args:
        pdf_path: Путь к PDF файлу
        output_dir: Директория для результатов
        config: Конфигурация экстрактора
        
    Returns:
        TableExtractionResult: Результат извлечения
    """
    start_time = datetime.now()
    extractor = create_table_extractor(config)
    tables = await extractor.extract_tables_from_pdf(pdf_path, output_dir)
    
    processing_time = (datetime.now() - start_time).total_seconds()
    
    engines_used = {}
    for table in tables:
        engine = table.engine
        engines_used[engine] = engines_used.get(engine, 0) + 1

    return TableExtractionResult(
        total_tables=len(tables),
        successful_extractions=len(tables),
        failed_extractions=0,
        tables=tables,
        processing_time=processing_time,
        engines_used=engines_used
    )

# =======================================================================================
# ОСНОВНОЙ БЛОК ДЛЯ ТЕСТИРОВАНИЯ
# =======================================================================================

if __name__ == "__main__":
    # Пример использования
    async def main():
        config = TableConfig()
        extractor = TableExtractor(config)

        # Тестовый файл
        pdf_path = "/app/temp/test_document.pdf"
        output_dir = "/app/temp/output"

        if Path(pdf_path).exists():
            result = await extract_tables_from_pdf(pdf_path, output_dir, config)

            print(f"Tables extracted: {result.total_tables}")
            print(f"Processing time: {result.processing_time:.2f}s")
            print(f"Engines used: {result.engines_used}")

            for table in result.tables:
                print(f"Table {table.id}: {table.rows}x{table.columns} from {table.engine}")
        else:
            print(f"Test file not found: {pdf_path}")

    asyncio.run(main())