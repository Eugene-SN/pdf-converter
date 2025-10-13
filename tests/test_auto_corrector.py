import asyncio
import importlib
import json
import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _ensure_stub_modules() -> None:
    if "httpx" not in sys.modules:
        sys.modules["httpx"] = types.ModuleType("httpx")

    if "aiohttp" not in sys.modules:
        aiohttp_stub = types.ModuleType("aiohttp")

        class ClientTimeout:  # minimal stub used for configuration
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        class ClientSession:  # pragma: no cover - only used as type placeholder
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs
                self.closed = False

            async def close(self):
                self.closed = True

        aiohttp_stub.ClientTimeout = ClientTimeout
        aiohttp_stub.ClientSession = ClientSession
        sys.modules["aiohttp"] = aiohttp_stub

    if "structlog" not in sys.modules:
        def _noop(*args, **kwargs):
            return None

        class _Logger:
            def info(self, *args, **kwargs):
                return None

            def warning(self, *args, **kwargs):
                return None

            def error(self, *args, **kwargs):
                return None

            def debug(self, *args, **kwargs):
                return None

        structlog_stub = types.ModuleType("structlog")
        structlog_stub.get_logger = lambda *args, **kwargs: _Logger()
        structlog_stub.configure = _noop
        structlog_stub.stdlib = types.SimpleNamespace()
        structlog_stub.processors = types.SimpleNamespace()
        structlog_stub.dev = types.SimpleNamespace()
        sys.modules["structlog"] = structlog_stub

    if "prometheus_client" not in sys.modules:
        class _Metric:
            def __init__(self, *args, **kwargs):
                return None

            def labels(self, *args, **kwargs):
                return self

            def inc(self, *args, **kwargs):
                return None

            def observe(self, *args, **kwargs):
                return None

            def set(self, *args, **kwargs):
                return None

        prom_stub = types.ModuleType("prometheus_client")
        prom_stub.Counter = _Metric
        prom_stub.Histogram = _Metric
        prom_stub.Gauge = _Metric
        sys.modules["prometheus_client"] = prom_stub

    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")

        class Session:
            def __init__(self):
                self.headers = {}

            def post(self, *args, **kwargs):  # pragma: no cover - not used
                return types.SimpleNamespace(status_code=200, text="{}")

            def close(self):
                return None

        def _request(*args, **kwargs):  # pragma: no cover - not used
            return types.SimpleNamespace(status_code=200, text="{}")

        requests_stub.Session = Session
        requests_stub.get = _request
        requests_stub.post = _request
        sys.modules["requests"] = requests_stub


def _reload_modules():
    _ensure_stub_modules()
    qa_vllm_config = importlib.import_module("quality_assurance.vllm_config")
    auto_corrector_module = importlib.import_module("quality_assurance.auto_corrector")
    qa_vllm_config = importlib.reload(qa_vllm_config)
    auto_corrector_module = importlib.reload(auto_corrector_module)
    return qa_vllm_config, auto_corrector_module


def test_auto_corrector_config_uses_shared_vllm_config(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("VLLM_SERVER_URL", "http://mock-vllm:9000")
    monkeypatch.setenv("VLLM_QA_ENDPOINT", "http://mock-vllm:9000/v1/chat/completions")
    monkeypatch.setenv("VLLM_QA_MAX_RETRIES", "7")
    monkeypatch.setenv("VLLM_API_KEY", "shared-key")

    qa_vllm_config, auto_corrector_module = _reload_modules()

    config = auto_corrector_module.AutoCorrectorConfig()

    assert config.vllm_endpoint == qa_vllm_config.VLLM_CONFIG["endpoint"]
    assert config.vllm_max_retries == qa_vllm_config.VLLM_CONFIG["max_retries"]
    assert config.vllm_api_key == "shared-key"
    assert config.vllm_base_url == "http://mock-vllm:9000"


def test_apply_single_correction_uses_messages(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("VLLM_QA_ENDPOINT", "http://localhost:9999/v1/chat/completions")
    monkeypatch.delenv("VLLM_API_KEY", raising=False)
    monkeypatch.setenv("VLLM_QA_MAX_RETRIES", "0")

    _, auto_corrector_module = _reload_modules()

    config = auto_corrector_module.AutoCorrectorConfig()
    corrector = auto_corrector_module.AutoCorrector(config)

    captured = {}

    class DummyResponse:
        def __init__(self, payload):
            self.status = 200
            self._payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover - interface compliance
            return False

        async def json(self, content_type=None):
            return self._payload

        async def text(self):  # pragma: no cover - interface compliance
            return json.dumps(self._payload)

    class DummySession:
        def __init__(self):
            self.closed = False

        async def close(self):  # pragma: no cover - compatibility
            self.closed = True

        def post(self, url, *, headers=None, json=None, timeout=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            captured["timeout"] = timeout
            return DummyResponse({
                "choices": [
                    {"message": {"content": "Corrected excerpt"}}
                ]
            })

    dummy_session = DummySession()

    async def fake_ensure_http_client(self):
        return dummy_session

    monkeypatch.setattr(auto_corrector_module.AutoCorrector, "_ensure_http_client", fake_ensure_http_client)

    correction = auto_corrector_module.CorrectionAction(
        type="formatting",
        description="Fix markdown",
        original_content="Original text",
        corrected_content="",
        confidence=0.5,
    )

    async def _run():
        return await corrector._apply_single_correction("Original text", correction)

    result = asyncio.run(_run())

    assert result == "Corrected excerpt"
    assert correction.corrected_content == "Corrected excerpt"

    payload = captured["json"]
    assert captured["url"] == config.vllm_endpoint
    assert "prompt" not in payload
    assert payload["model"] == config.vllm_model
    assert payload["messages"][0]["role"] == "system"
    assert payload["messages"][1]["role"] == "user"
    assert payload["messages"][0]["content"][0]["type"] == "text"
    assert payload["messages"][1]["content"][0]["text"].startswith("Apply the requested correction")
