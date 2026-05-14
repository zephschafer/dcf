"""Local streaming runner: consumes from a Kafka topic, applies schema projection,
and writes windowed Parquet files to a local warehouse directory.

Runs inside a Docker container started by `ddt deploy` (local mode). No Beam,
no GCP dependencies — only kafka-python and pyarrow.
"""

from __future__ import annotations

import argparse
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ddt.gcp._pipeline_utils import load_columns, to_pyarrow_schema, project_message

logger = logging.getLogger(__name__)


def _flush(buffer: list, lock: threading.Lock, schema: pa.Schema,
           output_path: Path, pipeline_name: str,
           window_seconds: int, timer_holder: list) -> None:
    with lock:
        rows = buffer[:]
        buffer.clear()

    if rows:
        table = pa.Table.from_pylist(rows, schema=schema)
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dest = output_path / f"window-{ts}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, dest)
        logger.info("Wrote %d rows to %s", len(rows), dest)
    else:
        logger.debug("Window closed with no messages — no file written")

    # reschedule
    t = threading.Timer(window_seconds, _flush,
                        args=(buffer, lock, schema, output_path, pipeline_name,
                              window_seconds, timer_holder))
    t.daemon = True
    t.start()
    timer_holder[0] = t


def run() -> None:
    from kafka import KafkaConsumer

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_name", required=True)
    parser.add_argument("--bootstrap_servers", default="localhost:9092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--window_seconds", type=int, default=60)
    args = parser.parse_args()

    columns = load_columns(args.pipeline_name)
    schema = to_pyarrow_schema(columns)
    output_path = Path(args.output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    buffer: list[dict] = []
    lock = threading.Lock()
    timer_holder: list = [None]

    # Start the first window timer
    t = threading.Timer(args.window_seconds, _flush,
                        args=(buffer, lock, schema, output_path, args.pipeline_name,
                              args.window_seconds, timer_holder))
    t.daemon = True
    t.start()
    timer_holder[0] = t

    logger.info(
        "Local stream runner started — pipeline=%s topic=%s bootstrap=%s window=%ds output=%s",
        args.pipeline_name, args.topic, args.bootstrap_servers,
        args.window_seconds, args.output_path,
    )

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        value_deserializer=lambda m: m,
        auto_offset_reset="earliest",
        group_id=f"ddt-{args.pipeline_name}",
        consumer_timeout_ms=-1,  # block indefinitely
    )

    for message in consumer:
        row = project_message(message.value, columns)
        if row is not None:
            with lock:
                buffer.append(row)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
