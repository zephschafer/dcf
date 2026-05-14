from __future__ import annotations

from typing import Any

from ..config.models import ArrayJoinTransform, CrsReprojectTransform, Transform
from .fetcher import _get_nested


def apply_transform(transform: Transform, record: dict) -> Any:
    if isinstance(transform, CrsReprojectTransform):
        return _crs_reproject(transform, record)
    if isinstance(transform, ArrayJoinTransform):
        return _array_join(transform, record)
    raise ValueError(f"Unknown transform type: {type(transform)}")


def _array_join(t: ArrayJoinTransform, record: dict) -> str | None:
    value = _get_nested(record, t.path)
    if value is None:
        return None
    if not isinstance(value, list):
        return str(value)
    return t.separator.join(str(item) for item in value)


def _crs_reproject(t: CrsReprojectTransform, record: dict) -> float | None:
    from pyproj import Transformer as ProjTransformer

    try:
        raw_x = record.get(t.from_columns[0])
        raw_y = record.get(t.from_columns[1])
        if raw_x is None or raw_y is None:
            return None
        x = float(raw_x)
        y = float(raw_y)
    except (TypeError, ValueError):
        return None

    proj = ProjTransformer.from_crs(t.from_crs, t.to_crs, always_xy=True)
    lon, lat = proj.transform(x, y)
    return lon if t.component == "x" else lat
