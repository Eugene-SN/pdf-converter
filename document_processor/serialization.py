from __future__ import annotations

import math
import numbers
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional

from .table_extractor import ExtractedTable


def _coerce_cell_to_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, numbers.Number):
        if isinstance(value, float) and not math.isfinite(value):
            return ""
        return str(value)
    return str(value)


def _coerce_headers(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [_coerce_cell_to_string(item) for item in raw]
    if isinstance(raw, str):
        return [_coerce_cell_to_string(raw)]
    return []


def _coerce_matrix(raw: Any) -> List[List[str]]:
    if raw is None:
        return []
    matrix: List[List[str]] = []
    if isinstance(raw, (list, tuple)):
        for row in raw:
            if isinstance(row, (list, tuple)):
                matrix.append([_coerce_cell_to_string(cell) for cell in row])
            elif isinstance(row, dict):
                ordered = [row[key] for key in sorted(row.keys())]
                matrix.append([_coerce_cell_to_string(cell) for cell in ordered])
            else:
                matrix.append([_coerce_cell_to_string(row)])
    return matrix


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return int(value)
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, numbers.Real):
        value = float(value)
        if math.isfinite(value):
            return value
        return None
    try:
        numeric = float(str(value))
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _sanitize_bbox(bbox: Any) -> Optional[List[float]]:
    if bbox is None:
        return None
    if not isinstance(bbox, (list, tuple)):
        return None
    sanitized: List[float] = []
    for coord in bbox:
        value = _coerce_float(coord)
        if value is None:
            continue
        sanitized.append(value)
    return sanitized or None


def sanitize_for_json(obj: Any) -> Any:
    if isinstance(obj, ExtractedTable):
        return normalize_extracted_table(obj)

    if is_dataclass(obj):
        obj = asdict(obj)

    if isinstance(obj, dict):
        sanitized: Dict[str, Any] = {}
        for key, value in obj.items():
            if key == "content" and isinstance(value, (dict, list, tuple, set)):
                continue
            sanitized[key] = sanitize_for_json(value)
        return sanitized

    if isinstance(obj, (list, tuple, set)):
        return [sanitize_for_json(item) for item in obj]

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return ""
        return obj

    if isinstance(obj, numbers.Integral):
        return int(obj)

    if isinstance(obj, (numbers.Real,)):  # e.g. numpy floats
        value = float(obj)
        if math.isnan(value) or math.isinf(value):
            return ""
        return value

    if isinstance(obj, (str, bool)) or obj is None:
        return obj

    return str(obj)


def normalize_extracted_table(table: ExtractedTable) -> Dict[str, Any]:
    headers = _coerce_headers(table.headers)
    rows = _coerce_matrix(table.data)

    metadata: Dict[str, Any] = {}
    if table.metadata:
        metadata.update(table.metadata)
    if table.file_path:
        metadata.setdefault("file_path", table.file_path)

    sanitized_metadata = sanitize_for_json(metadata)
    if isinstance(sanitized_metadata, dict):
        sanitized_metadata.pop("content", None)

    normalized: Dict[str, Any] = {
        "id": _coerce_int(table.id) or 0,
        "page": _coerce_int(table.page),
        "engine": str(table.engine),
        "headers": headers,
        "rows": rows,
    }

    confidence = _coerce_float(table.confidence)
    if confidence is not None:
        normalized["confidence"] = confidence

    quality = _coerce_float(table.quality_score)
    if quality is not None:
        normalized["quality"] = quality

    bbox = _sanitize_bbox(table.bbox)
    if bbox is not None:
        normalized["bbox"] = bbox

    if sanitized_metadata:
        normalized["metadata"] = sanitized_metadata

    return normalized


def normalize_table_like(table_like: Any, fallback_id: int) -> Dict[str, Any]:
    if isinstance(table_like, ExtractedTable):
        normalized = normalize_extracted_table(table_like)
        if not normalized.get("id"):
            normalized["id"] = fallback_id
        return normalized

    if is_dataclass(table_like):
        table_dict = asdict(table_like)
    elif hasattr(table_like, "dict") and callable(table_like.dict):
        try:
            table_dict = table_like.dict()
        except Exception:
            table_dict = dict(table_like)
    elif isinstance(table_like, dict):
        table_dict = dict(table_like)
    else:
        return {
            "id": fallback_id,
            "page": _coerce_int(getattr(table_like, "page", None)),
            "engine": str(getattr(table_like, "engine", "unknown")),
            "headers": [],
            "rows": [],
            "metadata": {"raw": sanitize_for_json(table_like)},
        }

    content = table_dict.pop("content", None)
    table_id = _coerce_int(table_dict.pop("id", fallback_id)) or fallback_id
    page = _coerce_int(table_dict.pop("page", table_dict.pop("page_number", None)))
    engine = table_dict.pop("engine", table_dict.pop("source", "docling"))

    confidence = _coerce_float(table_dict.pop("confidence", table_dict.pop("accuracy", None)))
    quality = _coerce_float(table_dict.pop("quality", table_dict.pop("quality_score", None)))

    bbox = _sanitize_bbox(table_dict.pop("bbox", None))
    file_path = table_dict.get("file_path")

    raw_rows_field = table_dict.pop("rows", None)
    raw_data_field = table_dict.pop("data", None)
    raw_headers_field = table_dict.pop("headers", None)

    rows = _coerce_matrix(raw_rows_field)
    if not rows:
        rows = _coerce_matrix(raw_data_field)

    if not rows and isinstance(content, dict):
        rows = _coerce_matrix(content.get("data")) or _coerce_matrix(content.get("rows"))

    headers = _coerce_headers(raw_headers_field)
    if not headers and isinstance(content, dict):
        headers = _coerce_headers(content.get("headers"))
        if not headers:
            headers = _coerce_headers(content.get("columns"))

    metadata: Dict[str, Any] = {}
    if isinstance(content, dict):
        for key, value in content.items():
            if key in {"data", "rows", "headers", "columns"}:
                continue
            metadata[key] = value

    if isinstance(raw_rows_field, numbers.Number) and not isinstance(raw_rows_field, bool):
        metadata["row_count"] = int(raw_rows_field)

    if isinstance(content, dict):
        columns_value = content.get("columns")
        if isinstance(columns_value, numbers.Number) and not isinstance(columns_value, bool):
            metadata["column_count"] = int(columns_value)

    for key, value in table_dict.items():
        metadata[key] = value

    if file_path:
        metadata.setdefault("file_path", file_path)

    sanitized_metadata = sanitize_for_json(metadata)
    if isinstance(sanitized_metadata, dict):
        sanitized_metadata.pop("content", None)

    normalized: Dict[str, Any] = {
        "id": table_id,
        "page": page,
        "engine": str(engine) if engine is not None else "docling",
        "headers": headers,
        "rows": rows,
    }

    if confidence is not None:
        normalized["confidence"] = confidence

    if quality is not None:
        normalized["quality"] = quality

    if bbox is not None:
        normalized["bbox"] = bbox

    if sanitized_metadata:
        normalized["metadata"] = sanitized_metadata

    return normalized


# Backwards compatibility helper
safe_serialize_tabledata = sanitize_for_json
