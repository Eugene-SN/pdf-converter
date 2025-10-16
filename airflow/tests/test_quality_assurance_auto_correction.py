import datetime
import importlib
import logging
import math
import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DAGS_ROOT = PROJECT_ROOT / "airflow" / "dags"
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))


def _install_airflow_stubs() -> None:
    if "airflow" in sys.modules:
        return

    airflow_module = types.ModuleType("airflow")
    airflow_root = Path(__file__).resolve().parent.parent
    airflow_module.__path__ = [str(airflow_root)]

    class DAG:  # pragma: no cover - simple placeholder for tests
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.tasks = []

        def add_task(self, task):  # pragma: no cover - testing stub
            self.tasks.append(task)

    airflow_module.DAG = DAG
    sys.modules["airflow"] = airflow_module

    exceptions_module = types.ModuleType("airflow.exceptions")

    class AirflowException(Exception):  # pragma: no cover - testing stub
        pass

    exceptions_module.AirflowException = AirflowException
    sys.modules["airflow.exceptions"] = exceptions_module
    airflow_module.exceptions = exceptions_module

    operators_module = types.ModuleType("airflow.operators")
    python_module = types.ModuleType("airflow.operators.python")

    class _OperatorBase:  # pragma: no cover - testing stub
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            dag = kwargs.get("dag")
            if hasattr(dag, "add_task"):
                dag.add_task(self)

        def __rshift__(self, other):
            return other

        def __rrshift__(self, other):
            return self

        def __lshift__(self, other):
            return other

        def __rlshift__(self, other):
            return self

        def set_upstream(self, other):  # pragma: no cover - testing stub
            return None

        def set_downstream(self, other):  # pragma: no cover - testing stub
            return None

    class PythonOperator(_OperatorBase):  # pragma: no cover - testing stub
        pass

    class ShortCircuitOperator(_OperatorBase):  # pragma: no cover
        pass

    python_module.PythonOperator = PythonOperator
    python_module.ShortCircuitOperator = ShortCircuitOperator
    operators_module.python = python_module
    airflow_module.operators = operators_module
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.python"] = python_module

    trigger_dagrun_module = types.ModuleType("airflow.operators.trigger_dagrun")

    class TriggerDagRunOperator(_OperatorBase):  # pragma: no cover - testing stub
        pass

    trigger_dagrun_module.TriggerDagRunOperator = TriggerDagRunOperator
    sys.modules["airflow.operators.trigger_dagrun"] = trigger_dagrun_module

    api_module = types.ModuleType("airflow.api")
    common_module = types.ModuleType("airflow.api.common")
    experimental_module = types.ModuleType("airflow.api.common.experimental")
    trigger_module = types.ModuleType("airflow.api.common.experimental.trigger_dag")

    def trigger_dag(*args, **kwargs):  # pragma: no cover - testing stub
        return None

    trigger_module.trigger_dag = trigger_dag
    experimental_module.trigger_dag = trigger_module
    sys.modules["airflow.api"] = api_module
    sys.modules["airflow.api.common"] = common_module
    sys.modules["airflow.api.common.experimental"] = experimental_module
    sys.modules["airflow.api.common.experimental.trigger_dag"] = trigger_module
    airflow_module.api = api_module

    utils_module = types.ModuleType("airflow.utils")
    timezone_namespace = types.SimpleNamespace(utcnow=datetime.datetime.utcnow)
    utils_module.timezone = timezone_namespace
    sys.modules["airflow.utils"] = utils_module
    airflow_module.utils = utils_module

    trigger_rule_module = types.ModuleType("airflow.utils.trigger_rule")

    class TriggerRule:  # pragma: no cover - testing stub
        NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
        NONE_FAILED = "none_failed"
        ALL_SUCCESS = "all_success"

    trigger_rule_module.TriggerRule = TriggerRule
    sys.modules["airflow.utils.trigger_rule"] = trigger_rule_module

    if "requests" not in sys.modules:
        requests_module = types.ModuleType("requests")

        class _RequestsTimeout(Exception):  # pragma: no cover - testing stub
            pass

        class _RequestsRequestException(Exception):  # pragma: no cover
            pass

        requests_module.exceptions = types.SimpleNamespace(
            Timeout=_RequestsTimeout,
            RequestException=_RequestsRequestException,
        )

        def _unpatched_post(*args, **kwargs):  # pragma: no cover - testing stub
            raise RuntimeError("requests.post stub called without monkeypatch")

        requests_module.post = _unpatched_post
        sys.modules["requests"] = requests_module

    if "aiohttp" not in sys.modules:
        aiohttp_module = types.ModuleType("aiohttp")
        sys.modules["aiohttp"] = aiohttp_module

    if "numpy" not in sys.modules:
        numpy_module = types.ModuleType("numpy")

        def _dot(vec_a, vec_b):  # pragma: no cover - testing stub
            return sum(a * b for a, b in zip(vec_a, vec_b))

        def _mean(values):  # pragma: no cover - testing stub
            values = list(values)
            return sum(values) / len(values) if values else 0.0

        numpy_module.dot = _dot
        numpy_module.mean = _mean
        numpy_module.linalg = types.SimpleNamespace(
            norm=lambda vec: math.sqrt(sum(val * val for val in vec))
        )
        sys.modules["numpy"] = numpy_module

    if "markdown" not in sys.modules:
        markdown_module = types.ModuleType("markdown")

        def _markdown(text, *args, **kwargs):  # pragma: no cover - testing stub
            return text

        markdown_module.markdown = _markdown
        sys.modules["markdown"] = markdown_module

    if "PIL" not in sys.modules:
        pil_module = types.ModuleType("PIL")
        image_module = types.ModuleType("PIL.Image")

        class _StubImage:  # pragma: no cover - testing stub
            @staticmethod
            def open(*args, **kwargs):
                return None

        pil_module.Image = _StubImage
        image_module.Image = _StubImage
        image_module.open = _StubImage.open
        sys.modules["PIL"] = pil_module
        sys.modules["PIL.Image"] = image_module

    if "pandas" not in sys.modules:
        pandas_module = types.ModuleType("pandas")

        class _DataFrame:  # pragma: no cover - testing stub
            pass

        pandas_module.DataFrame = _DataFrame
        pandas_module.Series = _DataFrame
        pandas_module.read_csv = lambda *args, **kwargs: _DataFrame()
        sys.modules["pandas"] = pandas_module


_install_airflow_stubs()

from airflow.dags.shared_utils import ConfigUtils

quality_assurance = importlib.import_module("airflow.dags.quality_assurance")
orchestrator = importlib.import_module("airflow.dags.orchestrator_dag")


class DummyTaskInstance:
    def __init__(self, mapping):
        self._mapping = mapping

    def xcom_pull(self, *, task_ids):
        return self._mapping[task_ids]


def test_call_vllm_api_requires_api_key(monkeypatch, tmp_path, caplog):
    ConfigUtils._SECRET_CACHE.clear()
    monkeypatch.delenv("VLLM_API_KEY", raising=False)

    quality_assurance.VLLM_CONFIG['max_retries'] = 1
    quality_assurance.VLLM_CONFIG['api_key'] = None

    api_calls = []

    def fail_if_called(*args, **kwargs):  # pragma: no cover - should not be invoked
        api_calls.append((args, kwargs))
        raise AssertionError("vLLM API should not be invoked when API key is missing")

    monkeypatch.setattr(quality_assurance.requests, "post", fail_if_called)

    with pytest.raises(quality_assurance.AirflowException):
        quality_assurance.call_vllm_api("Ensure key is required")

    translated_md = tmp_path / "document.md"
    translated_md.write_text("Content that needs correction", encoding="utf-8")

    qa_context = {
        'load_translated_document': {
            'translated_content': translated_md.read_text(encoding="utf-8"),
            'translated_file': str(translated_md),
            'auto_correction': True,
            'session_id': 'missing-key',
        },
        'perform_enhanced_content_validation': {
            'issues_found': ['Formatting issue detected'],
        },
    }

    caplog.set_level(logging.ERROR, logger=quality_assurance.logger.name)

    result = quality_assurance.perform_auto_correction(
        task_instance=DummyTaskInstance(qa_context)
    )

    assert result['status'] == 'failed'
    assert result['validation_score'] == 0.0
    assert result['timed_out'] is False
    assert api_calls == []
    assert any('VLLM_API_KEY secret is not configured' in issue for issue in result['issues_found'])
    assert any(
        'Auto-correction aborted: VLLM_API_KEY secret is not configured' in record.message
        for record in caplog.records
        if record.name == quality_assurance.logger.name
    )


def test_auto_correction_fails_fast_on_auth_error(monkeypatch, tmp_path, caplog):
    ConfigUtils._SECRET_CACHE.clear()
    monkeypatch.setenv("VLLM_API_KEY", "invalid-key")
    monkeypatch.setenv("AIRFLOW_TEMP_DIR", str(tmp_path / "airflow_temp"))

    quality_assurance.VLLM_CONFIG['max_retries'] = 2
    quality_assurance.VLLM_CONFIG['endpoint'] = "http://test/v1/chat/completions"
    quality_assurance.VLLM_CONFIG['timeout'] = 5

    class UnauthorizedResponse:
        status_code = 401
        text = "Unauthorized"

        @staticmethod
        def json():
            return {"error": {"message": "invalid token"}}

    def fake_post(url, json=None, timeout=None, headers=None):
        return UnauthorizedResponse()

    monkeypatch.setattr(quality_assurance.requests, "post", fake_post)

    translated_md = tmp_path / "document.md"
    translated_text = "Original content that requires correction. " * 3
    translated_md.write_text(translated_text, encoding="utf-8")

    qa_context = {
        'load_translated_document': {
            'translated_content': translated_text,
            'translated_file': str(translated_md),
            'auto_correction': True,
            'session_id': 'session-401',
        },
        'perform_enhanced_content_validation': {
            'issues_found': ['Issue with formatting']
        },
    }

    caplog.set_level(logging.INFO, logger=quality_assurance.logger.name)
    result = quality_assurance.perform_auto_correction(
        task_instance=DummyTaskInstance(qa_context)
    )

    assert result['status'] == 'failed'
    assert result['validation_score'] == 0.0
    assert result['timed_out'] is False
    assert result['requires_manual_followup'] is True
    assert any('VLLM API authentication failed' in issue for issue in result['issues_found'])
    assert any(
        'authentication failure' in record.message
        for record in caplog.records
        if record.name == quality_assurance.logger.name
    )


def test_auto_correction_success_allows_translation(monkeypatch, tmp_path, caplog):
    ConfigUtils._SECRET_CACHE.clear()
    monkeypatch.setenv("VLLM_API_KEY", "test-key")
    monkeypatch.setenv("AIRFLOW_TEMP_DIR", str(tmp_path / "airflow_temp"))

    quality_assurance.VLLM_CONFIG['max_retries'] = 1
    quality_assurance.VLLM_CONFIG['endpoint'] = "http://test/v1/chat/completions"
    quality_assurance.VLLM_CONFIG['timeout'] = 5

    captured_request = {}

    class FakeResponse:
        status_code = 200
        text = "{}"

        @staticmethod
        def json():
            return {
                "choices": [
                    {
                        "message": {
                            "content": "Corrected content with additional context to satisfy QA thresholds."
                        }
                    }
                ]
            }

    def fake_post(url, json=None, timeout=None, headers=None):
        captured_request['url'] = url
        captured_request['json'] = json
        captured_request['headers'] = headers
        return FakeResponse()

    monkeypatch.setattr(quality_assurance.requests, "post", fake_post)

    translated_md = tmp_path / "document.md"
    translated_text = "Original content that requires correction. " * 5
    translated_md.write_text(translated_text, encoding="utf-8")

    qa_context = {
        'load_translated_document': {
            'translated_content': translated_text,
            'translated_file': str(translated_md),
            'auto_correction': True,
            'session_id': 'session-1',
        },
        'perform_enhanced_content_validation': {
            'issues_found': ['Issue with formatting']
        },
    }

    caplog.set_level(logging.INFO, logger=quality_assurance.logger.name)
    result = quality_assurance.perform_auto_correction(
        task_instance=DummyTaskInstance(qa_context)
    )

    assert result['status'] == 'completed'
    assert captured_request['url'] == quality_assurance.VLLM_CONFIG['endpoint']
    assert captured_request['headers']['Authorization'] == 'Bearer test-key'
    assert captured_request['headers']['Content-Type'] == 'application/json'
    assert any(
        "vLLM API request attempt" in record.message
        for record in caplog.records
        if record.name == quality_assurance.logger.name
    )
    assert any(
        "Auto-correction status: completed" in record.message
        for record in caplog.records
        if record.name == quality_assurance.logger.name
    )

    qa_payload = {
        'quality_passed': True,
        'qa_completed': True,
        'pipeline_ready': True,
        'qa_report': str(tmp_path / 'qa_report.json'),
        'issues_count': 0,
        'auto_correction_status': result['status'],
        'auto_correction_requires_manual_followup': result['requires_manual_followup'],
        'final_content': result['corrected_content'],
    }

    master_config = {
        'translation_required': True,
        'qa_fail_action': 'halt',
        'filename': 'document.pdf',
        'timestamp': '20240101T000000',
        'target_language': 'ru',
        'preserve_technical_terms': True,
    }

    stage2_result = {'markdown_file': str(translated_md)}
    qa_trigger_result = {'return_value': qa_payload}

    orchestrator_context = DummyTaskInstance({
        'validate_orchestrator_input': master_config,
        'check_stage2_completion': stage2_result,
        'trigger_stage4_quality_assurance': qa_trigger_result,
    })

    caplog.clear()
    caplog.set_level(logging.INFO, logger=orchestrator.logger.name)
    stage3_config = orchestrator.prepare_stage3_config(
        task_instance=orchestrator_context
    )

    assert stage3_config['qa_gate_passed'] is True
    assert stage3_config['qa_summary']['qa_gate_passed'] is True
    assert "Перевод заблокирован" not in caplog.text


def test_finalize_qa_process_marks_pipeline_ready(tmp_path):
    qa_report_path = tmp_path / "qa_report.json"
    qa_report_path.write_text("{}", encoding="utf-8")

    translated_file = tmp_path / "translated.md"
    translated_text = "Corrected content"
    translated_file.write_text(translated_text, encoding="utf-8")

    qa_session = {
        'translated_content': translated_text,
        'translated_file': str(translated_file),
    }

    comprehensive_report = {
        'overall_score': 98.5,
        'quality_passed': True,
        'report_file': str(qa_report_path),
        'all_issues': [],
        'corrections_applied': 2,
        'corrected_content': "Final, production-ready content.",
        'level_scores': {
            'level1_integrity': 1.0,
            'level2_formatting': 0.95,
            'level3_semantics': 0.98,
            'level4_visual': 1.0,
            'level5_auto_correction': 1.0,
        },
        'level_results': {
            'level5_auto_correction': {
                'status': 'completed',
                'requires_manual_followup': False,
                'partial_success': False,
            }
        },
    }

    context = {'load_translated_document': qa_session,
               'generate_comprehensive_qa_report': comprehensive_report}

    result = quality_assurance.finalize_qa_process(
        task_instance=DummyTaskInstance(context)
    )

    assert result['qa_completed'] is True
    assert result['pipeline_ready'] is True
    assert result['auto_correction_status'] == 'completed'
    assert result['auto_correction_requires_manual_followup'] is False


def test_full_qa_pass_sets_pipeline_ready_flag(tmp_path):
    qa_report_path = tmp_path / "qa_full_report.json"
    qa_report_path.write_text("{}", encoding="utf-8")

    translated_file = tmp_path / "translated_full.md"
    translated_text = "Translated content ready for production."
    translated_file.write_text(translated_text, encoding="utf-8")

    qa_session = {
        'translated_content': translated_text,
        'translated_file': str(translated_file),
    }

    comprehensive_report = {
        'overall_score': 99.2,
        'quality_passed': True,
        'report_file': str(qa_report_path),
        'all_issues': [],
        'corrections_applied': 3,
        'corrected_content': "Production-ready content.",
        'level_scores': {
            'level1_integrity': 1.0,
            'level2_formatting': 1.0,
            'level3_semantics': 1.0,
            'level4_visual': 1.0,
            'level5_auto_correction': 1.0,
        },
        'level_results': {
            'level5_auto_correction': {
                'status': 'completed',
                'requires_manual_followup': False,
                'partial_success': False,
            }
        },
    }

    context = {
        'load_translated_document': qa_session,
        'generate_comprehensive_qa_report': comprehensive_report,
    }

    result = quality_assurance.finalize_qa_process(
        task_instance=DummyTaskInstance(context)
    )

    assert result['qa_completed'] is True
    assert result['pipeline_ready'] is True
    assert result['auto_correction_performed'] is True
    assert result['corrections_applied'] == 3
    assert result['issues_count'] == 0
