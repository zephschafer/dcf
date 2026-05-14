from __future__ import annotations

from typing import Any

import pandas as pd

from ..config.models import Schema, Column
from .transforms import apply_transform
from ..engine.fetcher import _get_nested


_CAST = {
    "string":    str,
    "integer":   lambda v: int(float(v)) if v is not None else None,
    "float":     lambda v: float(v) if v is not None else None,
    "boolean":   lambda v: bool(v) if v is not None else None,
    "date":      lambda v: pd.to_datetime(v, errors="coerce"),
    "timestamp": lambda v: pd.to_datetime(v, errors="coerce"),
}


def _cast(value: Any, col_type: str | None) -> Any:
    if col_type is None or value is None:
        return value
    try:
        return _CAST[col_type](value)
    except (ValueError, TypeError):
        return None


def _extract(record: dict, col: Column) -> Any:
    if col.transform is not None:
        return apply_transform(col.transform, record)
    return _get_nested(record, col.path)


def project(records: list[dict], schema: Schema) -> pd.DataFrame:
    """
    Apply transforms, extract declared columns only, cast types.
    Columns not listed in the schema are dropped.
    """
    if not records:
        return pd.DataFrame(columns=[c.name for c in schema.columns])

    rows = []
    for record in records:
        row = {}
        for col in schema.columns:
            row[col.name] = _cast(_extract(record, col), col.type)
        rows.append(row)

    return pd.DataFrame(rows)
