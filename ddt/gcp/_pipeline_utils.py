"""Shared schema projection utilities used by both beam_runner (GCP/Dataflow)
and local_stream_runner (local Kafka)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import yaml

logger = logging.getLogger(__name__)

TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "integer": pa.int64(),
    "float": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}


def load_columns(pipeline_name: str) -> list[dict]:
    path = Path("pipelines") / f"{pipeline_name}.yml"
    data = yaml.safe_load(path.read_text())
    return data["schema"]["columns"]


def to_pyarrow_schema(columns: list[dict]) -> pa.Schema:
    fields = [
        pa.field(col["name"], TYPE_MAP.get(col.get("type", "string"), pa.string()))
        for col in columns
    ]
    return pa.schema(fields)


def cast_value(value, col_type: str | None):
    if value is None:
        return None
    if col_type == "integer":
        return int(value)
    if col_type == "float":
        return float(value)
    if col_type == "boolean":
        return bool(value)
    if col_type == "timestamp":
        if isinstance(value, str):
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z"):
                try:
                    dt = datetime.strptime(value.rstrip("Z") + "+00:00", fmt.replace("Z", "%z"))
                    return dt.astimezone(timezone.utc)
                except ValueError:
                    continue
        return value
    if col_type == "date":
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return value
        return value
    return str(value) if value is not None else None


def project_message(msg_bytes: bytes, columns: list[dict]) -> dict | None:
    try:
        record = json.loads(msg_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Skipping unparseable message")
        return None

    row: dict = {}
    for col in columns:
        path = col.get("path") or col["name"]
        parts = path.split(".")
        val = record
        for part in parts:
            if isinstance(val, dict):
                val = val.get(part)
            else:
                val = None
                break
        row[col["name"]] = cast_value(val, col.get("type"))
    return row
