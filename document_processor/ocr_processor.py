#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ИСПРАВЛЕННЫЙ OCR Processor для PDF Converter Pipeline v4.0
✅ УСТРАНЕНЫ ПРОБЛЕМЫ:
- Условная инициализация OCR только при необходимости
- Правильная обработка use_ocr флага
- Избежание загрузки моделей при use_ocr=False
"""

from typing import List, Dict, Any, Union, Optional
import logging
from pathlib import Path

# ✅ ИСПРАВЛЕНО: Условные импорты OCR движков
OCR_AVAILABLE = {}

try:
    import pytesseract
    OCR_AVAILABLE['tesseract'] = True
except ImportError:
    OCR_AVAILABLE['tesseract'] = False

try:
    from paddleocr import PaddleOCR
    OCR_AVAILABLE['paddleocr'] = True
except ImportError:
    OCR_AVAILABLE['paddleocr'] = False

# Для PDF -> изображения
try:
    from pdf2image import convert_from_path
    import numpy as np
    from PIL import Image
    PDF_CONVERSION_AVAILABLE = True
except ImportError:
    PDF_CONVERSION_AVAILABLE = False

logger = logging.getLogger(__name__)


class OCRConfig:
    def __init__(
        self,
        use_gpu: bool = False,
        lang: Union[str, List[str]] = "ch",
        confidence_threshold: float = 0.8,
        enable_tesseract_cv: bool = True,
        pdf_dpi: int = 200,
    ):
        self.use_gpu = use_gpu
        # Поддерживаем как строку, так и список языков, а также строку "ch,en,ru"
        if isinstance(lang, str):
            if "," in lang:
                self.lang = [l.strip() for l in lang.split(",") if l.strip()]
            else:
                self.lang = [lang.strip()]
        else:
            self.lang = lang or ["ch"]
        self.confidence_threshold = float(confidence_threshold)
        self.enable_tesseract_cv = bool(enable_tesseract_cv)
        self.pdf_dpi = int(pdf_dpi)

    def __repr__(self):
        return (
            f"OCRConfig(use_gpu={self.use_gpu}, "
            f"lang={self.lang}, "
            f"confidence_threshold={self.confidence_threshold}, "
            f"enable_tesseract_cv={self.enable_tesseract_cv}, "
            f"pdf_dpi={self.pdf_dpi})"
        )


class OCRProcessor:
    """✅ ИСПРАВЛЕННЫЙ OCR Processor с условной инициализацией"""
    
    def __init__(self, config: Optional[OCRConfig] = None):
        self.config = config or OCRConfig()
        logger.info("Initializing OCRProcessor with %s", self.config)

        # ✅ ИСПРАВЛЕНО: НЕ инициализируем OCR движки в __init__
        self.ocr_engines: Dict[str, Any] = {}
        self.supported_langs: List[str] = []
        self.engines_initialized = False
        
        # Карта языков для Tesseract
        self.tesseract_lang_map = {
            "ch": "chi_sim",
            "en": "eng",
            "ru": "rus",
        }
        
        logger.info("✅ OCRProcessor создан БЕЗ автоматической инициализации движков")

    def _initialize_engines_if_needed(self):
        """✅ ИСПРАВЛЕНО: Ленивая инициализация OCR движков"""
        if self.engines_initialized:
            return
        
        logger.info("🔄 Инициализируем OCR движки по требованию...")
        
        for lang in self.config.lang:
            if not OCR_AVAILABLE['paddleocr']:
                logger.warning(f"PaddleOCR недоступен для языка {lang}")
                continue
                
            try:
                logger.info("Initializing PaddleOCR for language: %s", lang)
                # PaddleOCR сам скачает модели при первой инициализации, если их нет
                engine = PaddleOCR(
                    lang=lang,
                    use_textline_orientation=True,
                    show_log=False  # ✅ Отключаем избыточное логирование
                )
                self.ocr_engines[lang] = engine
                self.supported_langs.append(lang)
                logger.info("PaddleOCR for %s initialized", lang)
            except Exception as e:
                logger.error("Failed to initialize PaddleOCR for %s: %s", lang, e)

        if not self.ocr_engines:
            raise RuntimeError("No PaddleOCR engines initialized for requested languages")
        
        self.engines_initialized = True
        logger.info("✅ OCR движки инициализированы: %s", list(self.ocr_engines.keys()))

    def _resolve_lang(self, lang: Optional[str]) -> str:
        """Возвращает корректный язык, инициализированный в движке."""
        if not self.engines_initialized:
            self._initialize_engines_if_needed()
            
        if not lang:
            return self.supported_langs[0] if self.supported_langs else "ch"
        if lang not in self.ocr_engines:
            fallback = self.supported_langs[0] if self.supported_langs else "ch"
            logger.warning("Requested lang '%s' not initialized, fallback to '%s'", lang, fallback)
            return fallback
        return lang

    def ocr_image(self, image_path: str, lang: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        ✅ ИСПРАВЛЕНО: OCR по пути до изображения с ленивой инициализацией
        """
        try:
            # ✅ Инициализируем движки только при первом вызове
            if not self.engines_initialized:
                self._initialize_engines_if_needed()
            
            eff_lang = self._resolve_lang(lang)
            engine = self.ocr_engines[eff_lang]

            result = engine.ocr(image_path)
            output: List[Dict[str, Any]] = []

            # result — список строк [(bbox, (text, conf)), ...]
            if result and len(result) > 0 and result[0]:
                for line in result[0]:
                    try:
                        bbox, (text, conf) = line
                        if conf >= self.config.confidence_threshold:
                            output.append(
                                {
                                    "text": text,
                                    "confidence": float(conf),
                                    "bbox": bbox,
                                    "engine": "paddle",
                                    "language": eff_lang,
                                }
                            )
                    except Exception as ie:
                        logger.debug("Skip malformed OCR line: %s (%s)", line, ie)

            # Кросс-валидация Tesseract (опционально)
            if (self.config.enable_tesseract_cv and 
                eff_lang in self.tesseract_lang_map and 
                OCR_AVAILABLE['tesseract']):
                try:
                    t_lang = self.tesseract_lang_map[eff_lang]
                    t_text = pytesseract.image_to_string(image_path, lang=t_lang).strip()
                    if t_text:
                        output.append(
                            {
                                "text": t_text,
                                "confidence": 0.9,  # эвристика
                                "bbox": None,
                                "engine": "tesseract",
                                "language": eff_lang,
                            }
                        )
                except Exception as te:
                    logger.debug("Tesseract failed for %s: %s", eff_lang, te)

            return output

        except Exception as e:
            logger.error("Error during OCR on %s (lang=%s): %s", image_path, lang, e)
            return []

    async def process_document_pages(
        self, 
        pdf_path: str, 
        work_dir: str, 
        lang: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        ✅ ИСПРАВЛЕНО: Совместимость с main.py с условной инициализацией OCR
        """
        results: List[Dict[str, Any]] = []
        
        if not PDF_CONVERSION_AVAILABLE:
            logger.error("pdf2image не установлен - невозможно конвертировать PDF в изображения")
            return results
        
        # ✅ Инициализируем OCR движки только при первом вызове
        if not self.engines_initialized:
            self._initialize_engines_if_needed()
            
        eff_lang = self._resolve_lang(lang)

        try:
            Path(work_dir).mkdir(parents=True, exist_ok=True)
            images = convert_from_path(pdf_path, self.config.pdf_dpi)
            
            for idx, pil_img in enumerate(images, start=1):
                img_path = Path(work_dir) / f"page_{idx}.png"
                pil_img.save(str(img_path))

                page_ocr = self.ocr_image(str(img_path), eff_lang)
                results.append(
                    {
                        "page": idx,
                        "language": eff_lang,
                        "ocr_results": page_ocr,
                        "image_path": str(img_path),
                        "text_count": len(page_ocr),
                    }
                )
        except Exception as e:
            logger.error("Error in process_document_pages: %s", e)

        return results

    def ocr_image_multilang(
        self, 
        image_path: str, 
        langs: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """OCR по нескольким языкам. Возвращает словарь lang -> список блоков."""
        # ✅ Инициализируем OCR движки только при первом вызове
        if not self.engines_initialized:
            self._initialize_engines_if_needed()
            
        langs = langs or self.supported_langs
        results: Dict[str, List[Dict[str, Any]]] = {}
        
        for l in langs:
            if l in self.supported_langs:
                results[l] = self.ocr_image(image_path, l)
            else:
                logger.debug("Language %s not initialized, skip", l)
        return results

    @staticmethod
    def create_dummy_processor() -> 'OCRProcessor':
        """✅ Фабричный метод для создания фиктивного процессора без инициализации OCR"""
        logger.info("Создаем фиктивный OCR процессор (OCR отключен)")
        return OCRProcessor()

    def is_initialized(self) -> bool:
        """Проверка, инициализированы ли OCR движки"""
        return self.engines_initialized

    def get_available_engines(self) -> Dict[str, bool]:
        """Получение информации о доступных OCR движках"""
        return OCR_AVAILABLE.copy()