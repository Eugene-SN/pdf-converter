"""Smoke tests for the reusable vLLM HTTP client helpers."""

from __future__ import annotations

import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from translator.vllm_client import MissingVLLMApiKeyError, create_vllm_requests_session


class _ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def _handler_factory(expected_token: str):
    class _Handler(BaseHTTPRequestHandler):
        server_version = "MockVLLM/1.0"

        def do_POST(self):
            _ = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            auth_header = self.headers.get("Authorization")
            status = 200 if auth_header == f"Bearer {expected_token}" else 401
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, format, *args):  # pragma: no cover - quiet test logs
            return

    return _Handler


@pytest.fixture
def mock_vllm_server():
    handler = _handler_factory("test-key")
    server = _ThreadedHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _server_url(server: HTTPServer) -> str:
    host, port = server.server_address
    return f"http://{host}:{port}"


def test_vllm_session_attaches_bearer_token(monkeypatch: pytest.MonkeyPatch, mock_vllm_server):
    """Ensure the shared HTTP client sends the API key when configured."""

    base_url = _server_url(mock_vllm_server)
    monkeypatch.setenv("VLLM_SERVER_URL", base_url)

    # No key -> explicit failure
    monkeypatch.delenv("VLLM_API_KEY", raising=False)
    with pytest.raises(MissingVLLMApiKeyError):
        create_vllm_requests_session()

    # With key -> authorized
    monkeypatch.setenv("VLLM_API_KEY", "test-key")
    session = create_vllm_requests_session()
    try:
        response = session.post(f"{base_url}/v1/chat/completions", json={})
    finally:
        session.close()
    assert response.status_code == 200

