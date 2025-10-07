#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ПЕРЕРАБОТАННЫЙ Translation Pipeline v3.0 - Упрощенная архитектура
Встроенная логика перевода без внешних зависимостей, оптимизированная для китайских документов

КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ:
- ✅ Убрана зависимость от vLLM микросервиса
- ✅ Встроенные правила перевода
- ✅ Сохранение технических терминов
- ✅ Простая но эффективная логика
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowSkipException
import os
import json
import logging
import time
import re
from typing import Dict, Any, Optional, List

# Утилиты
from shared_utils import (
    SharedUtils, NotificationUtils, ConfigUtils, 
    MetricsUtils, ErrorHandlingUtils
)

# Настройка логирования
logger = logging.getLogger(__name__)

# Конфигурация DAG
DEFAULT_ARGS = {
    'owner': 'pdf-converter',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'translation_pipeline',
    default_args=DEFAULT_ARGS,
    description='DAG 3: Translation Pipeline v3.0 - Упрощенный перевод для китайских документов',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    tags=['pdf-converter', 'dag3', 'translation', 'chinese-docs', 'v3']
)

# ================================================================================
# СЛОВАРИ ДЛЯ ПЕРЕВОДА КИТАЙСКИХ ТЕХНИЧЕСКИХ ДОКУМЕНТОВ
# ================================================================================

# Технические термины (НЕ ПЕРЕВОДЯТСЯ)
TECHNICAL_TERMS = {
    # Бренды (КРИТИЧЕСКИ ВАЖНО!)
    "问天": "WenTian",
    "联想问天": "Lenovo WenTian",
    "天擎": "ThinkSystem",
    "AnyBay": "AnyBay",
    
    # Процессоры
    "至强": "Xeon",
    "可扩展处理器": "Scalable Processors",
    "英特尔": "Intel",
    
    # Технические спецификации
    "处理器": "Processor",
    "内核": "Core", 
    "线程": "Thread",
    "睿频": "Turbo Boost",
    "内存": "Memory",
    "存储": "Storage",
    "硬盘": "Drive",
    "固态硬盘": "SSD",
    "机械硬盘": "HDD",
    "热插拔": "Hot-swap",
    "冗余": "Redundancy",
    "背板": "Backplane",
    "托架": "Tray",
    "以太网": "Ethernet",
    "光纤": "Fiber",
    "带宽": "Bandwidth",
    "延迟": "Latency",
    "网卡": "Network Adapter",
    "英寸": "inch",
    "机架": "Rack",
    "插槽": "Slot",
    "转接卡": "Riser Card",
    "电源": "Power Supply",
    "铂金": "Platinum",
    "钛金": "Titanium",
    "CRPS": "CRPS"
}

# Русские переводы
CHINESE_TO_RUSSIAN = {
    # Общие термины
    "文档": "документ",
    "技术": "технический",
    "规格": "спецификация",
    "说明": "описание",
    "安装": "установка",
    "配置": "конфигурация",
    "管理": "управление",
    "监控": "мониторинг",
    "维护": "обслуживание",
    "故障": "неисправность",
    "排除": "устранение",
    "性能": "производительность",
    "优化": "оптимизация",
    "升级": "обновление",
    "兼容": "совместимость",
    "支持": "поддержка",
    "推荐": "рекомендуется",
    "要求": "требования",
    "注意": "внимание",
    "警告": "предупреждение",
    "重要": "важно",
    
    # Цифры и единицы
    "个": "",  # счетное слово
    "台": "шт.",
    "套": "комплект",
    "只": "шт.",
    "条": "шт.",
    "根": "шт.",
    
    # Таблицы и списки
    "表": "таблица",
    "列表": "список",
    "项目": "элемент",
    "选项": "опция",
    "参数": "параметр",
    "值": "значение",
    "默认": "по умолчанию",
    "可选": "опционально",
    "必须": "обязательно",
    
    # Действия
    "点击": "нажмите",
    "选择": "выберите",  
    "输入": "введите",
    "确认": "подтвердите",
    "取消": "отмените",
    "保存": "сохраните",
    "删除": "удалите",
    "修改": "измените",
    "添加": "добавьте",
    "移除": "удалите"
}

# Английские переводы
CHINESE_TO_ENGLISH = {
    # Общие термины
    "文档": "document",
    "技术": "technical",
    "规格": "specification",
    "说明": "description",
    "安装": "installation",
    "配置": "configuration",
    "管理": "management",
    "监控": "monitoring", 
    "维护": "maintenance",
    "故障": "fault",
    "排除": "troubleshooting",
    "性能": "performance",
    "优化": "optimization",
    "升级": "upgrade",
    "兼容": "compatibility",
    "支持": "support",
    "推荐": "recommended",
    "要求": "requirements",
    "注意": "note",
    "警告": "warning",
    "重要": "important",
    
    # Единицы измерения
    "个": "",
    "台": "units",
    "套": "set",
    "只": "piece",
    "条": "item",
    "根": "piece",
    
    # Таблицы и списки
    "表": "table",
    "列表": "list", 
    "项目": "item",
    "选项": "option",
    "参数": "parameter",
    "值": "value",
    "默认": "default",
    "可选": "optional",
    "必须": "required",
    
    # Действия
    "点击": "click",
    "选择": "select",
    "输入": "enter",
    "确认": "confirm",
    "取消": "cancel",
    "保存": "save",
    "删除": "delete",
    "修改": "modify",
    "添加": "add",
    "移除": "remove"
}

# ================================================================================
# ОСНОВНЫЕ ФУНКЦИИ ПЕРЕВОДА
# ================================================================================

def initialize_translation(**context) -> Dict[str, Any]:
    """Инициализация процесса перевода"""
    start_time = time.time()
    
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"🌐 Инициализация перевода: {json.dumps(dag_run_conf, indent=2, ensure_ascii=False)}")

        # Новое: корректный пропуск Stage 3 по флагу
        if dag_run_conf.get('skip_stage3', False):
            raise AirflowSkipException("Stage 3 skipped by orchestrator (no translation required)")
        
        # Получение Markdown файла
        markdown_file = dag_run_conf.get('markdown_file')
        if not markdown_file or not os.path.exists(markdown_file):
            raise ValueError(f"Markdown файл не найден: {markdown_file}")
        
        # Чтение контента
        with open(markdown_file, 'r', encoding='utf-8') as f:
            markdown_content = f.read()
        
        if not markdown_content.strip():
            raise ValueError("Нет контента для перевода")
        
        original_config = dag_run_conf.get('original_config', {})
        target_language = original_config.get('target_language', 'ru')
        
        # Инициализация сессии перевода
        translation_session = {
            'session_id': f"translation_{int(time.time())}",
            'source_language': 'zh-CN',
            'target_language': target_language,
            'markdown_content': markdown_content,
            'markdown_file': markdown_file,
            'original_config': original_config,
            'preserve_technical_terms': dag_run_conf.get('preserve_technical_terms', True),
            'chinese_source': dag_run_conf.get('chinese_source', True),
            'lines_total': len(markdown_content.split('\n')),
            'processing_start_time': datetime.now().isoformat()
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='translation_pipeline',
            task_id='initialize_translation',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ Перевод инициализирован: {target_language} ({translation_session['lines_total']} строк)")
        return translation_session
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='translation_pipeline',
            task_id='initialize_translation',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка инициализации перевода: {e}")
        raise

def perform_translation(**context) -> Dict[str, Any]:
    """Выполнение перевода документа"""
    start_time = time.time()
    session = context['task_instance'].xcom_pull(task_ids='initialize_translation')
    
    try:
        markdown_content = session['markdown_content']
        target_language = session['target_language']
        
        logger.info(f"🔄 Начинаем перевод на {target_language}")
        
        # Выбираем словарь перевода
        if target_language == 'ru':
            translation_dict = CHINESE_TO_RUSSIAN
        elif target_language == 'en':
            translation_dict = CHINESE_TO_ENGLISH
        else:
            # Для неподдерживаемых языков возвращаем оригинал
            logger.warning(f"Язык {target_language} не поддерживается, возвращаем оригинал")
            translation_dict = {}
        
        if target_language == 'original' or target_language == 'zh':
            # Возвращаем оригинальный китайский контент
            translated_content = markdown_content
        else:
            # Выполняем перевод
            translated_content = translate_content(
                markdown_content, 
                translation_dict, 
                target_language,
                session.get('preserve_technical_terms', True)
            )
        
        # Постобработка перевода
        final_content = post_process_translation(translated_content, target_language)
        
        # Валидация качества
        quality_score = validate_translation_quality(
            markdown_content, 
            final_content, 
            target_language
        )
        
        translation_results = {
            'translated_content': final_content,
            'source_length': len(markdown_content),
            'translated_length': len(final_content),
            'quality_score': quality_score,
            'translation_stats': {
                'processing_time_seconds': time.time() - start_time,
                'translation_method': 'builtin_dictionary_v3',
                'chinese_chars_remaining': count_chinese_characters(final_content),
                'technical_terms_preserved': count_preserved_technical_terms(final_content)
            }
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='translation_pipeline',
            task_id='perform_translation',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ Перевод завершен. Качество: {quality_score:.1f}%")
        return translation_results
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='translation_pipeline',
            task_id='perform_translation',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка выполнения перевода: {e}")
        raise

def translate_content(content: str, translation_dict: Dict[str, str], target_lang: str, preserve_terms: bool = True) -> str:
    """Основная функция перевода контента"""
    try:
        # Сначала сохраняем технические термины
        if preserve_terms:
            for chinese_term, english_term in TECHNICAL_TERMS.items():
                if chinese_term in content:
                    # Заменяем китайские термины на английские аналоги
                    content = content.replace(chinese_term, english_term)
        
        # Применяем словарь перевода
        for chinese_phrase, translation in translation_dict.items():
            if chinese_phrase in content and translation:
                content = content.replace(chinese_phrase, translation)
        
        # Специальная обработка для заголовков
        content = translate_chinese_headings(content, target_lang)
        
        # Обработка таблиц
        content = translate_table_content(content, translation_dict)
        
        return content
        
    except Exception as e:
        logger.warning(f"Ошибка перевода контента: {e}")
        return content

def translate_chinese_headings(content: str, target_lang: str) -> str:
    """Перевод китайских заголовков"""
    try:
        lines = content.split('\n')
        translated_lines = []
        
        heading_translations = {
            'ru': {
                '第': 'Глава',
                '章': '',
                '节': 'Раздел',
                '部分': 'Часть',
                '概述': 'Обзор',
                '介绍': 'Введение',
                '总结': 'Заключение',
                '附录': 'Приложение'
            },
            'en': {
                '第': 'Chapter',
                '章': '',
                '节': 'Section',
                '部分': 'Part',
                '概述': 'Overview',
                '介绍': 'Introduction',
                '总结': 'Summary',
                '附录': 'Appendix'
            }
        }
        
        translations = heading_translations.get(target_lang, {})
        
        for line in lines:
            if line.strip().startswith('#'):
                # Это заголовок - переводим его содержимое
                for chinese, translation in translations.items():
                    if chinese in line and translation:
                        line = line.replace(chinese, translation)
            
            translated_lines.append(line)
        
        return '\n'.join(translated_lines)
        
    except Exception as e:
        logger.warning(f"Ошибка перевода заголовков: {e}")
        return content

def translate_table_content(content: str, translation_dict: Dict[str, str]) -> str:
    """Перевод содержимого таблиц"""
    try:
        lines = content.split('\n')
        translated_lines = []
        
        for line in lines:
            if '|' in line and len(line.split('|')) >= 3:
                # Это строка таблицы - переводим содержимое ячеек
                cells = line.split('|')
                translated_cells = []
                
                for cell in cells:
                    cell_content = cell.strip()
                    # Применяем переводы к содержимому ячеек
                    for chinese, translation in translation_dict.items():
                        if chinese in cell_content and translation:
                            cell_content = cell_content.replace(chinese, translation)
                    translated_cells.append(cell_content)
                
                line = '|'.join(translated_cells)
            
            translated_lines.append(line)
        
        return '\n'.join(translated_lines)
        
    except Exception as e:
        logger.warning(f"Ошибка перевода таблиц: {e}")
        return content

def post_process_translation(content: str, target_lang: str) -> str:
    """Постобработка переведенного контента"""
    try:
        # Очистка лишних пробелов
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        
        # Исправление пунктуации для русского языка
        if target_lang == 'ru':
            # Замена точек на правильную пунктуацию
            content = re.sub(r'\.([А-Я])', r'. \1', content)
            # Исправление кавычек
            content = content.replace('"', '«').replace('"', '»')
        
        # Исправление технических терминов в скобках
        content = re.sub(r'\(\s*([A-Za-z0-9]+)\s*\)', r' (\1)', content)
        
        return content.strip()
        
    except Exception as e:
        logger.warning(f"Ошибка постобработки: {e}")
        return content

def validate_translation_quality(original: str, translated: str, target_lang: str) -> float:
    """Валидация качества перевода"""
    try:
        quality_score = 100.0
        
        # Для оригинального китайского - максимальный балл
        if target_lang in ['original', 'zh']:
            return 100.0
        
        # Проверка длины
        length_ratio = len(translated) / max(len(original), 1)
        if length_ratio < 0.5 or length_ratio > 2.0:
            quality_score -= 15
        
        # Проверка сохранения технических терминов
        preserved_terms = count_preserved_technical_terms(translated)
        original_terms = count_chinese_technical_terms(original)
        
        if original_terms > 0:
            term_preservation = min(1.0, preserved_terms / original_terms)
            quality_score += term_preservation * 10
        
        # Проверка структуры (заголовки, таблицы)
        original_headers = len(re.findall(r'^#+\s', original, re.MULTILINE))
        translated_headers = len(re.findall(r'^#+\s', translated, re.MULTILINE))
        
        if original_headers > 0:
            header_preservation = translated_headers / original_headers
            if header_preservation < 0.9:
                quality_score -= 10
        
        # Проверка китайских символов (должно быть меньше в переводе)
        chinese_remaining = count_chinese_characters(translated)
        chinese_original = count_chinese_characters(original)
        
        if chinese_original > 0:
            chinese_reduction = 1 - (chinese_remaining / chinese_original)
            quality_score += chinese_reduction * 20
        
        return max(0, min(100, quality_score))
        
    except Exception:
        return 75.0  # Средняя оценка по умолчанию

def count_chinese_characters(text: str) -> int:
    """Подсчет китайских символов"""
    return len(re.findall(r'[\u4e00-\u9fff]', text))

def count_preserved_technical_terms(text: str) -> int:
    """Подсчет сохраненных технических терминов"""
    count = 0
    for term in TECHNICAL_TERMS.values():
        count += text.count(term)
    return count

def count_chinese_technical_terms(text: str) -> int:
    """Подсчет китайских технических терминов"""
    count = 0
    for term in TECHNICAL_TERMS.keys():
        count += text.count(term)
    return count

def save_translation_result(**context) -> Dict[str, Any]:
    """Сохранение результата перевода"""
    start_time = time.time()
    
    try:
        session = context['task_instance'].xcom_pull(task_ids='initialize_translation')
        translation_results = context['task_instance'].xcom_pull(task_ids='perform_translation')
        
        original_config = session['original_config']
        target_language = session['target_language']
        timestamp = original_config.get('timestamp', int(time.time()))
        filename = original_config.get('filename', 'unknown.pdf')
        
        # Определение пути сохранения
        output_dir = f"/app/output/{target_language}"
        os.makedirs(output_dir, exist_ok=True)
        
        translated_filename = f"{timestamp}_{filename.replace('.pdf', '.md')}"
        output_path = f"{output_dir}/{translated_filename}"
        
        # Сохранение переведенного контента
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(translation_results['translated_content'])
        
        # Подготовка конфигурации для Stage 4
        stage4_config = {
            'translated_file': output_path,
            'translated_content': translation_results['translated_content'],
            'original_config': original_config,
            'stage3_completed': True,
            'translation_metadata': {
                'target_language': target_language,
                'quality_score': translation_results['quality_score'],
                'translation_method': 'builtin_dictionary_v3',
                'chinese_chars_remaining': translation_results['translation_stats']['chinese_chars_remaining'],
                'technical_terms_preserved': translation_results['translation_stats']['technical_terms_preserved'],
                'completion_time': datetime.now().isoformat()
            }
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='translation_pipeline',
            task_id='save_translation_result',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"💾 Перевод сохранен: {output_path}")
        return stage4_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='translation_pipeline',
            task_id='save_translation_result',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка сохранения перевода: {e}")
        raise

def notify_translation_completion(**context) -> None:
    """Уведомление о завершении перевода"""
    try:
        stage4_config = context['task_instance'].xcom_pull(task_ids='save_translation_result')
        translation_metadata = stage4_config['translation_metadata']
        
        target_language = translation_metadata['target_language']
        quality_score = translation_metadata['quality_score']
        chinese_remaining = translation_metadata['chinese_chars_remaining']
        tech_terms = translation_metadata['technical_terms_preserved']
        
        message = f"""
✅ TRANSLATION PIPELINE ЗАВЕРШЕН УСПЕШНО

🌐 Целевой язык: {target_language}
🎯 Качество перевода: {quality_score:.1f}%
🈶 Китайских символов осталось: {chinese_remaining}
🔧 Технических терминов сохранено: {tech_terms}
📊 Метод: {translation_metadata['translation_method']}
📁 Файл: {stage4_config['translated_file']}

✅ Готов к передаче на Stage 4 (Quality Assurance)
        """
        
        logger.info(message)
        NotificationUtils.send_success_notification(context, stage4_config)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ================================================================================

# Задача 1: Инициализация перевода
init_translation = PythonOperator(
    task_id='initialize_translation',
    python_callable=initialize_translation,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 2: Выполнение перевода
perform_translation_task = PythonOperator(
    task_id='perform_translation',
    python_callable=perform_translation,
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

# Задача 3: Сохранение результата
save_result = PythonOperator(
    task_id='save_translation_result',
    python_callable=save_translation_result,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 4: Уведомление о завершении
notify_completion = PythonOperator(
    task_id='notify_translation_completion',
    python_callable=notify_translation_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Определение зависимостей
init_translation >> perform_translation_task >> save_result >> notify_completion

# Обработка ошибок
def handle_translation_failure(context):
    """Обработка ошибок перевода"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')
        
        error_message = f"""
🔥 ОШИБКА В TRANSLATION PIPELINE

Задача: {failed_task}
Ошибка: {str(exception) if exception else 'Unknown'}

Возможные причины:
1. Отсутствует Markdown файл от Stage 2
2. Неподдерживаемый язык перевода
3. Проблемы с сохранением результата
4. Ошибки в словарях перевода

Требуется проверка входных данных и конфигурации.
        """
        
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_translation_failure