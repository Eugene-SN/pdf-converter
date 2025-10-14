"""Tests ensuring the Airflow content_transformation DAG loads in this environment."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Dict, Optional


class _FakeDAG:
    """Minimal stub for :mod:`airflow.DAG` used only for import validation."""

    def __init__(self, *args, **kwargs):  # pragma: no cover - trivial
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):  # pragma: no cover - context manager compatibility
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover
        return False


class _FakePythonOperator:
    """Stub implementation matching the API required during module import."""

    def __init__(self, *args, **kwargs):  # pragma: no cover - trivial
        self.args = args
        self.kwargs = kwargs

    def __rshift__(self, other):  # pragma: no cover - task chaining support
        return other

    def __rrshift__(self, other):  # pragma: no cover
        return self


class _FakeRequestsSession:
    """Simplified ``requests.Session`` replacement used for import-time wiring."""

    def __init__(self, *args, **kwargs):  # pragma: no cover - trivial
        self.headers: Dict[str, str] = {}

    def mount(self, *args, **kwargs):  # pragma: no cover - no-op
        return None

    def close(self):  # pragma: no cover - no-op
        return None


class _FakeHTTPAdapter:  # pragma: no cover - trivial stub
    def __init__(self, *args, **kwargs):
        return None


def _install_airflow_stubs() -> Dict[str, Optional[ModuleType]]:
    """Install lightweight Airflow stubs required for DAG import."""

    original_modules: Dict[str, Optional[ModuleType]] = {}
    for name in (
        "airflow",
        "airflow.operators",
        "airflow.operators.python",
        "airflow.dags",
        "requests",
        "requests.adapters",
        "shared_utils",
    ):
        original_modules[name] = sys.modules.get(name)

    repo_root = Path(__file__).resolve().parents[1]
    airflow_path = repo_root / "airflow"
    dags_path = airflow_path / "dags"

    airflow_module = ModuleType("airflow")
    airflow_module.DAG = _FakeDAG  # type: ignore[attr-defined]
    airflow_module.__path__ = [str(airflow_path)]  # type: ignore[attr-defined]

    operators_module = ModuleType("airflow.operators")
    python_module = ModuleType("airflow.operators.python")
    python_module.PythonOperator = _FakePythonOperator  # type: ignore[attr-defined]
    operators_module.__path__ = []  # type: ignore[attr-defined]
    python_module.__path__ = []  # type: ignore[attr-defined]

    dags_module = ModuleType("airflow.dags")
    dags_module.__path__ = [str(dags_path)]  # type: ignore[attr-defined]

    operators_module.python = python_module  # type: ignore[attr-defined]
    airflow_module.operators = operators_module  # type: ignore[attr-defined]

    requests_module = ModuleType("requests")
    requests_module.Session = _FakeRequestsSession  # type: ignore[attr-defined]
    adapters_module = ModuleType("requests.adapters")
    adapters_module.HTTPAdapter = _FakeHTTPAdapter  # type: ignore[attr-defined]
    requests_module.adapters = adapters_module  # type: ignore[attr-defined]

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.python"] = python_module
    sys.modules["airflow.dags"] = dags_module
    sys.modules["requests"] = requests_module
    sys.modules["requests.adapters"] = adapters_module

    shared_utils_spec = importlib.util.spec_from_file_location(
        "shared_utils", str(dags_path / "shared_utils.py")
    )
    if shared_utils_spec and shared_utils_spec.loader:
        shared_utils_module = importlib.util.module_from_spec(shared_utils_spec)
        shared_utils_spec.loader.exec_module(shared_utils_module)
        sys.modules["shared_utils"] = shared_utils_module

    return original_modules


def _restore_modules(original_modules: Dict[str, Optional[ModuleType]]) -> None:
    """Restore any previously loaded modules after import completes."""

    for name, module in original_modules.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


def test_content_transformation_module_imports_without_translator_error():
    repo_root = Path(__file__).resolve().parents[1]
    repo_str = str(repo_root)

    # Simulate an Airflow environment where the repository root is not already
    # present on ``sys.path``. The DAG should restore this as part of its
    # import-time setup.
    removed_path = False
    if repo_str in sys.path:
        sys.path.remove(repo_str)
        removed_path = True

    # Ensure any cached translator modules are cleared so importlib performs
    # a fresh resolution using the DAG's sys.path adjustments.
    sys.modules.pop("translator", None)
    sys.modules.pop("translator.vllm_client", None)

    original_modules = _install_airflow_stubs()

    try:
        dag_module = importlib.import_module("airflow.dags.content_transformation")
        assert hasattr(dag_module, "dag"), "DAG module should expose a dag instance"
        assert "translator.vllm_client" in sys.modules, "translator package must be importable"
    finally:
        _restore_modules(original_modules)
        sys.modules.pop("translator", None)
        sys.modules.pop("translator.vllm_client", None)
        if removed_path:
            sys.path.insert(0, repo_str)
