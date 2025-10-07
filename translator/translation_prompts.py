#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Translation Prompts для Qwen/Qwen3-30B-A3B-Instruct-2507
ОБНОВЛЕНО: Оптимизированные промпты для новой A3B архитектуры без think-блоков
"""

from typing import Dict, List, Any, Optional
from enum import Enum

# =======================================================================================
# БАЗОВЫЕ НАСТРОЙКИ ДЛЯ QWEN3-30B-A3B-INSTRUCT-2507
# =======================================================================================

class ModelConfig:
    """Конфигурация для новой модели"""
    MODEL_NAME = "Qwen/Qwen3-30B-A3B-Instruct-2507"
    MODEL_ALIAS = "Qwen3-30B-A3B-Instruct"
    
    # Оптимальные параметры для A3B архитектуры
    DEFAULT_TEMPERATURE = 0.2      # Снижено для более стабильного вывода
    DEFAULT_MAX_TOKENS = 4096      # A3B лучше работает с умеренными размерами
    DEFAULT_TOP_P = 0.8            # Более консервативный sampling
    DEFAULT_TOP_K = 40             # Оптимизировано для A3B
    
    # Параметры специфичные для translation
    TRANSLATION_TEMPERATURE = 0.1  # Очень низкая для переводов
    TRANSLATION_MAX_TOKENS = 6144  # Увеличено для длинных документов
    
    # A3B специфичные настройки
    USE_SYSTEM_PROMPT = True       # A3B хорошо следует system prompts
    ENABLE_JSON_MODE = False       # Не нужен для translation задач
    STOP_SEQUENCES = ["<|endoftext|>", "<|im_end|>"]

# =======================================================================================
# ОСНОВНЫЕ ПРОМПТЫ ДЛЯ ПЕРЕВОДА - ОПТИМИЗИРОВАНЫ ДЛЯ A3B
# =======================================================================================

class TranslationPrompts:
    """Промпты для перевода, оптимизированные для Qwen3-30B-A3B без think-блоков"""
    
    @staticmethod
    def technical_document_system_prompt() -> str:
        """Системный промпт для технических документов - упрощён для A3B"""
        return """You are an expert technical translator specializing in IT documentation, server management, and hardware manuals.

CORE PRINCIPLES:
1. Preserve all technical terms, commands, and code blocks EXACTLY as written
2. Maintain markdown formatting structure perfectly
3. Translate descriptive text while keeping technical content unchanged
4. Ensure natural, fluent target language for explanatory content

CRITICAL REQUIREMENTS:
• Keep ALL technical terms unchanged: IPMI, BMC, API, REST, JSON, XML, HTTP, HTTPS, SSH, CLI
• Preserve command syntax: ipmitool, curl commands, file paths, parameters
• Maintain code blocks (```), tables, and formatting exactly
• Keep English technical terminology in Chinese translations when appropriate
• Preserve numbered lists, bullet points, and heading hierarchy

OUTPUT FORMAT: Provide ONLY the translated document without explanations or commentary."""

    @staticmethod
    def get_translation_prompt(
        source_language: str,
        target_language: str,
        preserve_technical_terms: bool = True,
        document_type: str = "technical"
    ) -> str:
        """
        Генерация промпта для конкретной языковой пары
        Упрощено для A3B - убраны think-блоки и лишние инструкции
        """
        
        # Маппинг языков для более естественных инструкций
        language_map = {
            "english": "English",
            "chinese": "Chinese",
            "russian": "Russian",
            "spanish": "Spanish",
            "french": "French",
            "german": "German"
        }
        
        source_lang = language_map.get(source_language, source_language.title())
        target_lang = language_map.get(target_language, target_language.title())
        
        base_prompt = f"""Translate the following {source_lang} document to {target_lang}.

TRANSLATION REQUIREMENTS:
• Maintain all markdown syntax (headers, lists, tables, code blocks)
• Preserve technical terminology and command syntax exactly
• Keep code blocks unchanged (```bash, ```json, etc.)
• Translate only descriptive and explanatory text
• Ensure natural {target_lang} flow for translated content"""

        if preserve_technical_terms:
            base_prompt += f"""
• Keep these terms in English: IPMI, BMC, Redfish, API, REST, JSON, XML, HTTP, HTTPS, SSH, CLI, URL, ID
• Preserve all file paths, commands, and configuration parameters
• Maintain error codes and status messages unchanged"""

        if document_type == "technical":
            base_prompt += f"""

TECHNICAL DOCUMENT SPECIFICS:
• Server management commands stay in English
• Configuration file examples unchanged
• Network protocols and standards in original language
• Hardware specifications preserve English terminology"""

        base_prompt += f"""

IMPORTANT: Output ONLY the translated document. No explanations or additional text."""
        
        return base_prompt

    @staticmethod
    def get_batch_translation_prompt(
        source_language: str,
        target_language: str,
        chunk_context: Optional[str] = None
    ) -> str:
        """Промпт для batch перевода больших документов"""
        
        language_map = {
            "english": "English",
            "chinese": "Chinese", 
            "russian": "Russian"
        }
        
        source_lang = language_map.get(source_language, source_language.title())
        target_lang = language_map.get(target_language, target_language.title())
        
        prompt = f"""Translate this {source_lang} document section to {target_lang}.

CONSISTENCY REQUIREMENTS:
• Maintain terminology consistent with the full document
• Preserve technical terms and formatting exactly
• Keep the same translation style throughout
• Handle section breaks and references appropriately"""

        if chunk_context:
            prompt += f"""

DOCUMENT CONTEXT:
This section is part of: {chunk_context}
Maintain consistency with the overall document theme and terminology."""

        prompt += f"""

OUTPUT: Provide only the translated section content."""
        
        return prompt

    @staticmethod
    def get_quality_check_prompt(target_language: str) -> str:
        """Промпт для проверки качества перевода"""
        
        language_map = {"chinese": "Chinese", "russian": "Russian", "english": "English"}
        target_lang = language_map.get(target_language, target_language.title())
        
        return f"""Review this {target_lang} translation for accuracy and quality.

CHECK FOR:
• Natural {target_lang} language flow
• Preserved technical terminology
• Correct markdown formatting
• Complete translation (no missing sections)
• Consistent terminology usage

RESPOND WITH: Only "APPROVED" if translation meets all criteria, or list specific issues requiring correction."""

# =======================================================================================
# СПЕЦИАЛИЗИРОВАННЫЕ ПРОМПТЫ ДЛЯ АВТОКОРРЕКЦИИ (QA ЭТАП)
# =======================================================================================

class QACorrectionPrompts:
    """Промпты для автокоррекции в QA этапе - адаптированы для A3B"""
    
    @staticmethod
    def ocr_correction_prompt(target_language: str = "chinese") -> str:
        """Коррекция OCR ошибок - упрощённый промпт для A3B"""
        
        return f"""Fix OCR recognition errors in this technical document while preserving all formatting.

COMMON OCR ERRORS TO FIX:
• Character substitutions (O→0, l→1, m→rn, etc.)
• Missing spaces between words
• Broken technical terms and commands
• Incorrect punctuation and symbols

PRESERVE UNCHANGED:
• All code blocks and command syntax
• Technical terminology and parameters
• Markdown formatting structure
• Table layouts and alignments

OUTPUT: Provide only the corrected document text."""

    @staticmethod
    def structure_correction_prompt() -> str:
        """Коррекция структуры документа"""
        
        return f"""Restore proper document structure and hierarchy.

FIX THESE ISSUES:
• Incorrect heading levels (# ## ### ####)
• Missing section breaks
• Broken numbered lists and bullet points
• Disrupted table formatting
• Lost cross-references

MAINTAIN:
• Original content and meaning
• Technical accuracy
• Markdown syntax consistency

OUTPUT: Provide the document with corrected structure only."""

    @staticmethod
    def translation_improvement_prompt(target_language: str) -> str:
        """Улучшение качества перевода"""
        
        language_names = {"chinese": "Chinese", "russian": "Russian"}
        lang_name = language_names.get(target_language, target_language.title())
        
        return f"""Improve the {lang_name} translation quality while maintaining technical accuracy.

IMPROVEMENTS TO MAKE:
• More natural {lang_name} language flow
• Better terminology consistency
• Clearer explanations of technical concepts
• Improved readability and comprehension

NEVER CHANGE:
• Technical terms, commands, or code
• Markdown formatting structure
• Document organization
• Core technical meaning

OUTPUT: Provide only the improved translation."""

    @staticmethod
    def formatting_correction_prompt() -> str:
        """Коррекция форматирования markdown"""
        
        return f"""Fix markdown formatting issues in this document.

CORRECTIONS NEEDED:
• Restore proper code block syntax (```language)
• Fix table formatting and alignment
• Correct heading hierarchy
• Repair broken links and references
• Fix list formatting (numbered and bullet)

PRESERVE:
• All content and text exactly
• Technical accuracy
• Document structure and flow

OUTPUT: Provide the document with corrected formatting only."""

# =======================================================================================
# УТИЛИТЫ ДЛЯ РАБОТЫ С ПРОМПТАМИ
# =======================================================================================

class PromptBuilder:
    """Конструктор промптов для различных сценариев"""
    
    @staticmethod
    def build_conversation(
        system_prompt: str,
        user_content: str,
        model_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Построение запроса в формате OpenAI для A3B модели"""
        
        # Дефолтные параметры для A3B
        default_config = {
            "model": ModelConfig.MODEL_ALIAS,
            "temperature": ModelConfig.TRANSLATION_TEMPERATURE,
            "max_tokens": ModelConfig.TRANSLATION_MAX_TOKENS,
            "top_p": ModelConfig.DEFAULT_TOP_P,
            "top_k": ModelConfig.DEFAULT_TOP_K,
            "stop": ModelConfig.STOP_SEQUENCES
        }
        
        if model_config:
            default_config.update(model_config)
        
        return {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            **default_config
        }
    
    @staticmethod
    def build_simple_request(
        prompt: str,
        content: str,
        temperature: float = 0.1
    ) -> Dict[str, Any]:
        """Упрощённый запрос для быстрых задач"""
        
        combined_prompt = f"{prompt}\n\nContent to process:\n{content}"
        
        return {
            "model": ModelConfig.MODEL_ALIAS,
            "messages": [
                {"role": "user", "content": combined_prompt}
            ],
            "temperature": temperature,
            "max_tokens": ModelConfig.DEFAULT_MAX_TOKENS,
            "stop": ModelConfig.STOP_SEQUENCES
        }

# =======================================================================================
# ЭКСПОРТ
# =======================================================================================

__all__ = [
    'ModelConfig',
    'TranslationPrompts',
    'QACorrectionPrompts', 
    'PromptBuilder'
]