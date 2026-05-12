"""Dataflow Flex Template entrypoint: reads from Pub/Sub, projects through
the pipeline schema, and writes windowed Parquet files to GCS."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import yaml

logger = logging.getLogger(__name__)

_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "integer": pa.int64(),
    "float": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}


def _load_columns(pipeline_name: str) -> list[dict]:
    path = Path("pipelines") / f"{pipeline_name}.yml"
    data = yaml.safe_load(path.read_text())
    return data["schema"]["columns"]


def _to_pyarrow_schema(columns: list[dict]) -> pa.Schema:
    fields = [
        pa.field(col["name"], _TYPE_MAP.get(col.get("type", "string"), pa.string()))
        for col in columns
    ]
    return pa.schema(fields)


def _cast_value(value, col_type: str | None):
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


def _project_message(msg_bytes: bytes, columns: list[dict]) -> dict | None:
    try:
        record = json.loads(msg_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Skipping unparseable Pub/Sub message")
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
        row[col["name"]] = _cast_value(val, col.get("type"))
    return row


def run() -> None:
    import apache_beam as beam
    from apache_beam.io.gcp.pubsub import ReadFromPubSub
    from apache_beam.io.parquetio import WriteToParquet
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
    from apache_beam.transforms.window import FixedWindows

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_name", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--warehouse_bucket", required=True)
    parser.add_argument("--window_seconds", type=int, default=60)
    known_args, pipeline_args = parser.parse_known_args()

    columns = _load_columns(known_args.pipeline_name)
    schema = _to_pyarrow_schema(columns)
    output_prefix = (
        f"gs://{known_args.warehouse_bucket}"
        f"/{known_args.pipeline_name}/{known_args.pipeline_name}/data/"
    )

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> ReadFromPubSub(subscription=known_args.subscription)
            | "ParseAndProject" >> beam.Map(
                _project_message, columns=columns
            )
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            | "Window" >> beam.WindowInto(FixedWindows(known_args.window_seconds))
            | "WriteParquet" >> WriteToParquet(
                file_path_prefix=output_prefix,
                schema=schema,
                file_name_suffix=".parquet",
                num_shards=1,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
