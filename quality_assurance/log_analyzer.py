#!/usr/bin/env python3
"""Utility for inspecting QA pipeline logs for errors and anomalies."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

SEVERITY_FIELDS = ("level", "level_name", "levelname")
ERROR_KEYS = ("error", "exception", "trace")
WARNING_EVENTS = {
    "auto correction failed": "Auto correction failures detected",
    "vllm_request_timeout": "vLLM timeouts detected during corrections",
    "vllm_request_failed": "vLLM HTTP errors detected",
    "unexpected_vllm_error": "Unexpected exceptions during vLLM corrections",
}


@dataclass
class LogIssue:
    message: str
    occurrences: int
    sample: Optional[str] = None


@dataclass
class LogSummary:
    log_files_scanned: int
    total_lines: int
    errors: List[LogIssue]
    warnings: List[LogIssue]
    info: Dict[str, int]


def _load_lines(log_file: Path) -> Iterable[str]:
    try:
        with log_file.open("r", encoding="utf-8", errors="ignore") as handle:
            yield from handle
    except FileNotFoundError:
        return


def _detect_severity(record: Dict[str, object]) -> Optional[str]:
    for field in SEVERITY_FIELDS:
        value = record.get(field)
        if isinstance(value, str):
            return value.lower()
    return None


def _normalize_record(raw_line: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
    stripped = raw_line.strip()
    if not stripped:
        return None, None
    try:
        return json.loads(stripped), None
    except json.JSONDecodeError:
        return None, stripped


def analyze_logs(log_dir: Path) -> LogSummary:
    if not log_dir.exists() or not any(log_dir.iterdir()):
        return LogSummary(0, 0, [], [], {})

    errors_counter: Counter[str] = Counter()
    warnings_counter: Counter[str] = Counter()
    info_counter: Counter[str] = Counter()
    error_samples: Dict[str, str] = {}
    warning_samples: Dict[str, str] = {}
    total_lines = 0
    log_files = 0

    for log_file in sorted(log_dir.rglob("*.log")):
        log_files += 1
        for line in _load_lines(log_file):
            total_lines += 1
            record, fallback = _normalize_record(line)
            if record is None:
                if fallback:
                    info_counter["unstructured"] += 1
                    lowered = fallback.lower()

                    if "error" in lowered:
                        message = "Unstructured error"
                        if "vllm" in lowered and "timeout" in lowered:
                            message = "vLLM API timeouts while awaiting corrections"

                        errors_counter[message] += 1
                        error_samples.setdefault(message, fallback.strip())
                        continue

                    if "timeout" in lowered and "vllm" in lowered:
                        message = "vLLM responses arrived after timeout"
                        warnings_counter[message] += 1
                        warning_samples.setdefault(message, fallback.strip())
                        continue

                    if "ответ сгенерирован" in lowered or "response generated" in lowered:
                        message = "vLLM generated response after pipeline completion"
                        warnings_counter[message] += 1
                        warning_samples.setdefault(message, fallback.strip())
                        continue

                    if "timeout" in lowered:
                        warnings_counter["Timeout events detected"] += 1
                        warning_samples.setdefault("Timeout events detected", fallback.strip())
                continue

            severity = _detect_severity(record)
            event = str(record.get("event", "")).lower()

            if severity in {"error", "exception", "critical"}:
                key = event or "generic_error"
                errors_counter[key] += 1
                error_samples.setdefault(key, line.strip())
                continue

            if severity == "warning":
                key = event or "generic_warning"
                warnings_counter[key] += 1
                warning_samples.setdefault(key, line.strip())
                continue

            if event:
                info_counter[event] += 1

            for keyword, description in WARNING_EVENTS.items():
                if keyword in event:
                    warnings_counter[description] += 1
                    warning_samples.setdefault(description, line.strip())

            for key in ERROR_KEYS:
                if key in record:
                    errors_counter[key] += 1
                    error_samples.setdefault(key, line.strip())

    def _issues(counter: Counter[str], samples: Dict[str, str]) -> List[LogIssue]:
        return [
            LogIssue(message=message, occurrences=count, sample=samples.get(message))
            for message, count in counter.most_common()
        ]

    return LogSummary(
        log_files_scanned=log_files,
        total_lines=total_lines,
        errors=_issues(errors_counter, error_samples),
        warnings=_issues(warnings_counter, warning_samples),
        info=dict(info_counter.most_common()),
    )


def format_summary(summary: LogSummary) -> str:
    if summary.log_files_scanned == 0:
        return "No log files found to analyze."

    lines: List[str] = [
        f"Analyzed {summary.log_files_scanned} log file(s) with {summary.total_lines} lines.",
    ]

    if summary.errors:
        lines.append("\nErrors:")
        for issue in summary.errors:
            lines.append(f"  - {issue.message} (x{issue.occurrences})")
            if issue.sample:
                lines.append(f"      sample: {issue.sample}")

    if summary.warnings:
        lines.append("\nWarnings:")
        for issue in summary.warnings:
            lines.append(f"  - {issue.message} (x{issue.occurrences})")
            if issue.sample:
                lines.append(f"      sample: {issue.sample}")

    if summary.info:
        lines.append("\nNotable events:")
        for event, count in summary.info.items():
            lines.append(f"  - {event}: {count}")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze QA pipeline log files")
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("logs"),
        help="Directory containing QA log files (default: ./logs)",
    )
    args = parser.parse_args()

    summary = analyze_logs(args.log_dir)
    print(format_summary(summary))


if __name__ == "__main__":
    main()
