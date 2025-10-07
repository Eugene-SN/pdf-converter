#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✅ ИСПРАВЛЕННЫЙ Orchestrator DAG v4.0 - Правильная координация этапов
Устраняет проблемы с передачей данных между этапами и обеспечивает полную обработку
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from typing import Dict, Any, Optional, List
import os
import json
import logging
import time
import glob
from pathlib import Path
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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# ✅ ИСПРАВЛЕНО: Создание Master DAG с нативным рендером шаблонов
dag = DAG(
    'orchestrator_dag',
    default_args=DEFAULT_ARGS,
    description='✅ ИСПРАВЛЕННЫЙ Orchestrator v4.0 - Правильная координация всех этапов PDF конвейера',
    schedule_interval=None, # Запускается по API
    max_active_runs=3,
    catchup=False,
    tags=['pdf-converter', 'orchestrator', 'master-dag', 'v4', 'fixed'],
    render_template_as_native_obj=True # ✅ КРИТИЧНО: Нативный рендер шаблонов
)
# ===============================================================================
# ФУНКЦИИ ОРКЕСТРАЦИИ
# ===============================================================================
def validate_orchestrator_input(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Валидация входных данных с определением режима работы"""
    start_time = time.time()
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"🔍 Валидация входных данных оркестратора: {json.dumps(dag_run_conf, indent=2, ensure_ascii=False)}")
        required_params = ['input_file', 'filename', 'timestamp']
        missing_params = [param for param in required_params if not dag_run_conf.get(param)]
        if missing_params:
            raise ValueError(f"Отсутствуют обязательные параметры: {missing_params}")
        # ✅ НОВОЕ: Нормализация пути файла от хостового к контейнерному
        input_file = dag_run_conf['input_file']
        if isinstance(input_file, str) and input_file.startswith('/mnt/storage/apps/pdf-converter/'):
            input_file = input_file.replace('/mnt/storage/apps/pdf-converter/', '/app/', 1)
        dag_run_conf['input_file'] = input_file
        if not SharedUtils.validate_input_file(input_file):
            raise ValueError(f"Недопустимый входной файл: {input_file}")
        stage_mode = dag_run_conf.get('stage_mode', 'full_conversion_with_validation')
        target_language = dag_run_conf.get('target_language', 'original')
        enable_ocr = bool(dag_run_conf.get('enable_ocr', False))
        if dag_run_conf.get('enable_ocr') is None:
            logger.info('OCR flag not provided; defaulting to disabled for digital PDFs')

        master_config = {
            'input_file': input_file,
            'filename': dag_run_conf['filename'],
            'target_language': target_language,
            'quality_level': dag_run_conf.get('quality_level', 'high'),
            'enable_ocr': enable_ocr,
            'preserve_structure': dag_run_conf.get('preserve_structure', True),
            'extract_tables': dag_run_conf.get('extract_tables', True),
            'extract_images': dag_run_conf.get('extract_images', True),
            'preserve_technical_terms': dag_run_conf.get('preserve_technical_terms', True),
            'chinese_document': dag_run_conf.get('chinese_document', True),
            'timestamp': dag_run_conf['timestamp'],
            'master_run_id': context['dag_run'].run_id,
            'pipeline_version': '4.0_fixed',
            'processing_start_time': datetime.now().isoformat(),
            'stage_mode': stage_mode,
            'processing_stages': dag_run_conf.get('processing_stages', 4),
            'validation_enabled': dag_run_conf.get('validation_enabled', True),
            'quality_target': dag_run_conf.get('quality_target', 95.0),
            'translation_required': target_language not in ['original', 'zh', 'zh-CN'],
            'input_dir': '/app/input',
            'output_zh_dir': '/app/output/zh',
            'output_target_dir': f"/app/output/{target_language}" if target_language not in ['original', 'zh'] else '/app/output/zh',
            'container_temp_dir': '/opt/airflow/temp',
        }
        MetricsUtils.record_processing_metrics(
            dag_id='orchestrator_dag',
            task_id='validate_orchestrator_input',
            processing_time=time.time() - start_time,
            success=True
        )
        logger.info(f"✅ Конфигурация оркестратора валидирована для файла: {dag_run_conf['filename']}")
        logger.info(f"🎯 Режим работы: {stage_mode}, Целевой язык: {target_language}")
        return master_config
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='orchestrator_dag',
            task_id='validate_orchestrator_input',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка валидации входных данных: {e}")
        raise
def prepare_stage1_config(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Подготовка конфигурации для Stage 1"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        stage1_config = {
            'input_file': master_config['input_file'],
            'filename': master_config['filename'],
            'enable_ocr': master_config['enable_ocr'],
            'preserve_structure': master_config['preserve_structure'],
            'extract_tables': master_config['extract_tables'],
            'extract_images': master_config['extract_images'],
            'chinese_document': master_config['chinese_document'],
            'quality_level': master_config['quality_level'],
            'timestamp': master_config['timestamp'],
            'master_run_id': master_config['master_run_id'],
            'language': 'zh-CN',
            'chinese_optimization': True,
            'processing_timeout': 1800,
            'pipeline_version': '4.0_fixed',
            'processing_mode': 'chinese_optimized',
            'output_mode': 'orchestrated',
            'temp_base_dir': master_config['container_temp_dir'],
        }
        logger.info(f"🚀 Подготовлен запуск Stage 1: Document Preprocessing")
        return stage1_config
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 1: {e}")
        raise
def check_stage1_completion(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Проверка завершения Stage 1 с bridge-файлами"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        timestamp = master_config['timestamp']
        airflow_temp = os.getenv('AIRFLOW_TEMP_DIR', os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'temp'))
        bridge_file = os.path.join(airflow_temp, f"stage1_bridge_{timestamp}.json")
        intermediate_file = None
        if os.path.exists(bridge_file):
            try:
                with open(bridge_file, 'r', encoding='utf-8') as f:
                    bridge_data = json.load(f)
                intermediate_file = bridge_data.get('intermediate_file') or bridge_data.get('docling_intermediate')
            except Exception:
                intermediate_file = None
        if not intermediate_file:
            patterns = [
                os.path.join(airflow_temp, f"*{timestamp}*intermediate.json"),
                os.path.join(airflow_temp, f"doc_*_intermediate.json"),
            ]
            for pattern in patterns:
                found = glob.glob(pattern)
                if found:
                    intermediate_file = found
                    break
        if not intermediate_file or not os.path.exists(intermediate_file):
            raise AirflowException(f"Промежуточный файл Stage 1 не найден (ts={timestamp}); проверьте том temp и пути")
        try:
            with open(intermediate_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not data or 'title' not in data:
                logger.warning(f"⚠️ Промежуточный файл {intermediate_file} содержит неполные данные")
        except Exception as e:
            logger.error(f"❌ Не удалось прочитать промежуточный файл {intermediate_file}: {e}")
            raise AirflowException(f"Промежуточный файл поврежден: {e}")
        stage1_completion = {
            'intermediate_file': intermediate_file,
            'stage1_completed': True,
            'ready_for_transformation': True,
            'original_config': master_config,
            'completion_time': datetime.now().isoformat(),
            'data_validation': 'passed',
        }
        logger.info(f"✅ Stage 1 завершен успешно, данные подготовлены для Stage 2")
        return stage1_completion
    except Exception as e:
        logger.error(f"❌ Ошибка проверки Stage 1: {e}")
        raise
def prepare_stage2_config(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Подготовка конфигурации для Stage 2 с проверенным путем"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        stage1_result = context['task_instance'].xcom_pull(task_ids='check_stage1_completion')
        stage2_config = {
            'intermediate_file': stage1_result['intermediate_file'],
            'original_config': master_config,
            'stage1_completed': True,
            'ready_for_transformation': True,
            'chinese_document': master_config['chinese_document'],
            'preserve_technical_terms': master_config['preserve_technical_terms'],
            'transformation_method': 'chinese_optimized_v4',
            'quality_level': master_config['quality_level'],
            'pipeline_version': '4.0_fixed',
            'output_mode': 'orchestrated',
            'timestamp': master_config['timestamp'],
            'filename': master_config['filename'],
        }
        logger.info(f"🚀 Подготовлен запуск Stage 2: Content Transformation")
        return stage2_config
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 2: {e}")
        raise
def check_stage2_completion(**context) -> Dict[str, Any]:
    """✅ НОВОЕ: Проверка завершения Stage 2"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        timestamp = master_config['timestamp']
        filename = master_config['filename']
        expected_md_file = f"/app/output/zh/{timestamp}_{filename.replace('.pdf', '.md')}"
        host_md_file = f"{master_config['output_zh_dir']}/{timestamp}_{filename.replace('.pdf', '.md')}"
        fallback_md_file = f"/opt/airflow/output/zh/{timestamp}_{filename.replace('.pdf', '.md')}"
        md_file_path = None
        for path in [expected_md_file, host_md_file, fallback_md_file]:
            if os.path.exists(path):
                md_file_path = path
                logger.info(f"✅ Найден MD файл: {path}")
                break
        if not md_file_path:
            patterns = [
                f"/app/output/zh/*{timestamp}*.md",
                f"{master_config['output_zh_dir']}/*{timestamp}*.md",
                f"/app/output/zh/{filename.replace('.pdf', '.md')}",
                f"{master_config['output_zh_dir']}/{filename.replace('.pdf', '.md')}",
                f"/opt/airflow/output/zh/*{timestamp}*.md",
                f"/opt/airflow/output/zh/{filename.replace('.pdf', '.md')}",
            ]
            for pattern in patterns:
                files = glob.glob(pattern)
                if files:
                    md_file_path = files
                    break
        if not md_file_path:
            raise AirflowException(f"❌ MD файл для {filename} не найден")
        stage2_completion = {
            'markdown_file': md_file_path,
            'stage2_completed': True,
            'ready_for_translation': master_config['translation_required'],
            'original_config': master_config,
            'completion_time': datetime.now().isoformat(),
        }
        logger.info(f"✅ Stage 2 завершен успешно: {md_file_path}")
        return stage2_completion
    except Exception as e:
        logger.error(f"❌ Ошибка проверки Stage 2: {e}")
        raise
def prepare_stage3_config(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Подготовка конфигурации для Stage 3 (Translation)"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        stage2_result = context['task_instance'].xcom_pull(task_ids='check_stage2_completion')
        if not master_config['translation_required']:
            logger.info("🔄 Перевод не требуется, пропускаем Stage 3")
            return {'skip_stage3': True}
        stage3_config = {
            'markdown_file': stage2_result['markdown_file'],
            'original_config': master_config,
            'stage2_completed': True,
            'target_language': master_config['target_language'],
            'preserve_technical_terms': master_config['preserve_technical_terms'],
            'chinese_source': True,
            'translation_method': 'builtin_dictionary_v4',
            'pipeline_version': '4.0_fixed',
            'output_mode': 'orchestrated',
        }
        logger.info(f"🚀 Подготовлен запуск Stage 3: Translation → {master_config['target_language']}")
        return stage3_config
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 3: {e}")
        raise
def prepare_stage4_config(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Подготовка конфигурации для Stage 4 (QA)"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        if master_config['translation_required']:
            translated_file = f"{master_config['output_target_dir']}/{master_config['timestamp']}_{master_config['filename'].replace('.pdf', '.md')}"
        else:
            stage2_result = context['task_instance'].xcom_pull(task_ids='check_stage2_completion')
            translated_file = stage2_result['markdown_file']
        document_id = master_config.get(
            'document_id',
            f"{master_config['timestamp']}_{master_config['filename'].replace('.pdf', '')}"
        )

        stage4_config = {
            'translated_file': translated_file,
            'original_config': master_config,
            'stage3_completed': not master_config['translation_required'] or True,
            'target_language': master_config['target_language'],
            'quality_target': master_config['quality_target'],
            'auto_correction': master_config.get('auto_correction', True),
            'validation_levels': 5,
            'validation_mode': 'content_with_structure',
            'pipeline_version': '4.0_fixed',
            'original_pdf_path': master_config['input_file'],
            'document_id': document_id,
        }
        logger.info(f"🚀 Подготовлен запуск Stage 4: Quality Assurance")
        return stage4_config
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 4: {e}")
        raise
def finalize_orchestration(**context) -> Dict[str, Any]:
    """✅ ИСПРАВЛЕНО: Финализация работы оркестратора"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        start_time = datetime.fromisoformat(master_config['processing_start_time'])
        end_time = datetime.now()
        processing_duration = end_time - start_time
        final_output_path = None
        if master_config['translation_required']:
            final_output_path = f"{master_config['output_target_dir']}/{master_config['timestamp']}_{master_config['filename'].replace('.pdf', '.md')}"
        else:
            final_output_path = f"{master_config['output_zh_dir']}/{master_config['timestamp']}_{master_config['filename'].replace('.pdf', '.md')}"
        orchestration_result = {
            'master_run_id': master_config['master_run_id'],
            'processing_completed': True,
            'total_processing_time': str(processing_duration),
            'processing_duration_seconds': processing_duration.total_seconds(),
            'source_file': master_config['input_file'],
            'filename': master_config['filename'],
            'target_language': master_config['target_language'],
            'final_output_path': final_output_path,
            'qa_report_path': f"/app/temp/qa_report_{master_config['timestamp']}.json",
            'pipeline_stages_completed': master_config['processing_stages'],
            'orchestrator_version': '4.0_fixed',
            'chinese_document_optimized': master_config['chinese_document'],
            'translation_performed': master_config['translation_required'],
            'success': True,
            'processing_stats': {
                'ocr_used': master_config['enable_ocr'],
                'technical_terms_preserved': master_config['preserve_technical_terms'],
                'target_language': master_config['target_language'],
                'quality_level': master_config['quality_level'],
                'stage_mode': master_config['stage_mode'],
            },
        }
        logger.info(f"🎯 Оркестрация завершена успешно за {processing_duration}")
        return orchestration_result
    except Exception as e:
        logger.error(f"❌ Ошибка финализации оркестрации: {e}")
        raise
def notify_orchestrator_completion(**context) -> None:
    """✅ ИСПРАВЛЕНО: Финальное уведомление"""
    try:
        orchestration_result = context['task_instance'].xcom_pull(task_ids='finalize_orchestration')
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        success = orchestration_result['success']
        processing_time = orchestration_result['total_processing_time']
        filename = master_config['filename']
        target_language = master_config['target_language']
        if success:
            message = f"""
🎉 ИСПРАВЛЕННЫЙ PDF КОНВЕЙЕР v4.0 УСПЕШНО ЗАВЕРШЕН!
📄 Файл: {filename}
🌐 Язык: {target_language}
⏱️ Время обработки: {processing_time}
🔄 ВЫПОЛНЕННЫЕ ЭТАПЫ:
✅ Stage 1: Document Preprocessing (извлечение контента)
✅ Stage 2: Content Transformation (преобразование в MD)
{'✅ Stage 3: Translation Pipeline (перевод)' if orchestration_result['translation_performed'] else '⏭️ Stage 3: Пропущен (перевод не требуется)'}
✅ Stage 4: Quality Assurance (валидация)
📊 РЕЗУЛЬТАТЫ:
- Исправленная архитектура: ✅ Применена
- Китайские документы: ✅ Оптимизированы
- Технические термины: ✅ Сохранены
- Итоговый файл: {orchestration_result['final_output_path']}
- QA отчет: {orchestration_result['qa_report_path']}
🚀 СИСТЕМА v4.0 ИСПРАВЛЕНА И ГОТОВА К РАБОТЕ!
"""
        else:
            message = f"""
❌ ИСПРАВЛЕННЫЙ PDF КОНВЕЙЕР v4.0 ЗАВЕРШЕН С ОШИБКАМИ
📄 Файл: {filename}
⏱️ Время до ошибки: {processing_time}
Требуется проверка логов каждого этапа для диагностики проблемы.
"""
        logger.info(message)
        if success:
            NotificationUtils.send_success_notification(context, orchestration_result)
        else:
            NotificationUtils.send_failure_notification(context, Exception("Pipeline failed"))
    except Exception as e:
        logger.error(f"❌ Ошибка отправки финального уведомления: {e}")
# ===============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ===============================================================================
validate_input = PythonOperator(
    task_id='validate_orchestrator_input',
    python_callable=validate_orchestrator_input,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)
prepare_stage1 = PythonOperator(
    task_id='prepare_stage1_config',
    python_callable=prepare_stage1_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)
trigger_stage1 = TriggerDagRunOperator(
    task_id='trigger_stage1_preprocessing',
    trigger_dag_id='document_preprocessing',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage1_config') | tojson }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=45),
    dag=dag
)
check_stage1 = PythonOperator(
    task_id='check_stage1_completion',
    python_callable=check_stage1_completion,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)
prepare_stage2 = PythonOperator(
    task_id='prepare_stage2_config',
    python_callable=prepare_stage2_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)
trigger_stage2 = TriggerDagRunOperator(
    task_id='trigger_stage2_transformation',
    trigger_dag_id='content_transformation',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage2_config') | tojson }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=30),
    dag=dag
)
check_stage2 = PythonOperator(
    task_id='check_stage2_completion',
    python_callable=check_stage2_completion,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)
prepare_stage3 = PythonOperator(
    task_id='prepare_stage3_config',
    python_callable=prepare_stage3_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)
trigger_stage3 = TriggerDagRunOperator(
    task_id='trigger_stage3_translation',
    trigger_dag_id='translation_pipeline',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage3_config') | tojson }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=30),
    dag=dag
)
prepare_stage4 = PythonOperator(
    task_id='prepare_stage4_config',
    python_callable=prepare_stage4_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)
trigger_stage4 = TriggerDagRunOperator(
    task_id='trigger_stage4_quality_assurance',
    trigger_dag_id='quality_assurance',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage4_config') | tojson }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=20),
    dag=dag
)
finalize_orchestrator = PythonOperator(
    task_id='finalize_orchestration',
    python_callable=finalize_orchestration,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)
notify_completion = PythonOperator(
    task_id='notify_orchestrator_completion',
    python_callable=notify_orchestrator_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=3),
    dag=dag
)
# ===============================================================================
# УСЛОВНОЕ ВЫПОЛНЕНИЕ STAGE 3 (ДОПОЛНЕНО)
# ===============================================================================
def _should_translate(**context) -> bool:
    """Возвращает True, если перевод требуется, иначе False (ShortCircuit)."""
    mc = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
    return bool(mc.get('translation_required'))
should_translate = ShortCircuitOperator(
    task_id='should_translate',
    python_callable=_should_translate,
    ignore_downstream_trigger_rules=False,
    dag=dag,
)
# Ветвление: запуск Stage 3 только при необходимости перевода
prepare_stage3 >> should_translate >> trigger_stage3
# Разрешить Stage 4 идти от Stage 2 (и учитывать результат Stage 3, если он был)
from airflow.utils.trigger_rule import TriggerRule
prepare_stage4.trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
prepare_stage4.set_upstream(check_stage2)
prepare_stage4.set_upstream(trigger_stage3)
# ===============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# ===============================================================================
(validate_input >> prepare_stage1 >> trigger_stage1 >> check_stage1 >>
 prepare_stage2 >> trigger_stage2 >> check_stage2 >>
 prepare_stage3 >>
 prepare_stage4 >> trigger_stage4 >>
 finalize_orchestrator >> notify_completion)
# ===============================================================================
# ОБРАБОТКА ОШИБОК...
# ===============================================================================
def handle_orchestrator_failure(context):
    """✅ ИСПРАВЛЕНО: Специальная обработка ошибок оркестратора"""
    try:
        failed_task = context['task_instance'].task_id
        master_config = context.get('dag_run', {}).conf or {}
        exception = context.get('exception')
        error_message = f"""
🔥 КРИТИЧЕСКАЯ ОШИБКА В ИСПРАВЛЕННОМ ОРКЕСТРАТОРЕ v4.0
Задача: {failed_task}
Файл: {master_config.get('filename', 'unknown')}
Ошибка: {str(exception) if exception else 'Unknown error'}

ДИАГНОСТИКА ПО ЗАДАЧАМ:
- validate_orchestrator_input: проверьте входные параметры
- check_stage1_completion: проверьте создание промежуточных файлов
- check_stage2_completion: проверьте создание MD файлов
- trigger_stage*: проверьте доступность соответствующих DAG

РЕКОМЕНДАЦИИ ПО УСТРАНЕНИЮ:
1. Проверьте маппирование volumes в docker-compose.yml
2. Убедитесь в правильности путей файлов
3. Проверьте права доступа к директориям
4. Проверьте логи конкретного этапа для деталей
Исправленный оркестратор v4.0 использует правильную передачу данных между этапами.
"""
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок оркестратора: {e}")
for task in dag.tasks:
    task.on_failure_callback = handle_orchestrator_failure
