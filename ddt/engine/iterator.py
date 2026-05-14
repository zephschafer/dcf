from __future__ import annotations

import itertools
from datetime import date, timedelta
from typing import Any

from ..config.models import DateRangeIterate, CategoricalIterate, IterateSpec, Param


def _parse_duration(s: str) -> timedelta:
    """Parse simple duration strings like '1 day', '7 days', '2 weeks'."""
    parts = s.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Cannot parse duration: '{s}'. Expected '<n> <unit>'")
    n = int(parts[0])
    unit = parts[1].rstrip("s")  # normalize "days" → "day"
    if unit == "day":
        return timedelta(days=n)
    if unit == "week":
        return timedelta(weeks=n)
    if unit == "month":
        return timedelta(days=30 * n)
    raise ValueError(f"Unknown duration unit: '{unit}'")


def _resolve_date(value: str) -> date:
    if value == "today":
        return date.today()
    return date.fromisoformat(value)


def _format_date(d: date, fmt: str | None, param_defs: dict[str, Param]) -> str:
    """Apply the param's declared format, falling back to ISO."""
    if fmt:
        return d.strftime(fmt)
    return d.isoformat()


def _date_range_steps(spec: DateRangeIterate, param_defs: dict[str, Param]) -> list[dict[str, Any]]:
    """
    Yield one dict per step. Each dict maps param names to their formatted values.
    When spec.params has two entries, the first receives the window start
    and the second receives the window end.
    """
    step = _parse_duration(spec.step)
    window = _parse_duration(spec.window) if spec.window else step

    start = _resolve_date(spec.start)
    end = _resolve_date(spec.end)

    steps = []
    window_start = start
    while window_start <= end:
        window_end = min(window_start + window - timedelta(days=1), end)

        def fmt(d: date, param_name: str) -> str:
            p = param_defs.get(param_name)
            return _format_date(d, p.format if p else None, param_defs)

        if len(spec.params) == 1:
            steps.append({spec.params[0]: fmt(window_start, spec.params[0])})
        else:
            steps.append({
                spec.params[0]: fmt(window_start, spec.params[0]),
                spec.params[1]: fmt(window_end, spec.params[1]),
            })
        window_start += step
    return steps


def _categorical_steps(spec: CategoricalIterate) -> list[dict[str, Any]]:
    return [{spec.param: v} for v in spec.values]


def build_request_sequence(
    iterate: list[IterateSpec],
    param_defs: dict[str, Param],
) -> list[dict[str, Any]]:
    """
    Return the cartesian product of all iteration axes.
    Each element is a dict of {param_name: value} for one request.
    """
    if not iterate:
        return [{}]

    axes: list[list[dict[str, Any]]] = []
    for spec in iterate:
        if isinstance(spec, DateRangeIterate):
            axes.append(_date_range_steps(spec, param_defs))
        elif isinstance(spec, CategoricalIterate):
            axes.append(_categorical_steps(spec))

    return [
        {k: v for d in combo for k, v in d.items()}
        for combo in itertools.product(*axes)
    ]
