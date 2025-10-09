"""Utility script to inspect QA logs for common issues."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Tuple


LOG_ROOT = Path(__file__).resolve().parents[1] / "logs"


ERROR_PATTERNS: Dict[str, re.Pattern[str]] = {
    "traceback": re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE),
    "error": re.compile(r"\bERROR\b", re.IGNORECASE),
    "exception": re.compile(r"Exception", re.IGNORECASE),
    "timeout": re.compile(r"timeout", re.IGNORECASE),
}


def iter_log_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    for path in root.rglob("*.log"):
        if path.is_file():
            yield path


def analyze_log_file(path: Path) -> Tuple[Counter, int]:
    counters: Counter = Counter()
    total_lines = 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            total_lines += 1
            for name, pattern in ERROR_PATTERNS.items():
                if pattern.search(line):
                    counters[name] += 1
    return counters, total_lines


def main() -> None:
    summary = {}
    for log_path in iter_log_files(LOG_ROOT):
        counters, total_lines = analyze_log_file(log_path)
        summary[str(log_path)] = {
            "total_lines": total_lines,
            "matches": dict(counters),
        }

    if not summary:
        print(json.dumps({"status": "no_logs_found", "log_root": str(LOG_ROOT)}))
        return

    print(json.dumps({"status": "ok", "logs": summary}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
