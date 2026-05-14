"""Dataflow Flex Template entrypoint: reads from Pub/Sub, projects through
the pipeline schema, and writes windowed Parquet files to GCS (or a local path
when --output_path is provided for DirectRunner testing)."""

from __future__ import annotations

import argparse
import logging

from ddt.gcp._pipeline_utils import load_columns, to_pyarrow_schema, project_message

logger = logging.getLogger(__name__)


def run() -> None:
    import apache_beam as beam
    from apache_beam.io.gcp.pubsub import ReadFromPubSub
    from apache_beam.io.parquetio import WriteToParquet
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
    from apache_beam.transforms.window import FixedWindows

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_name", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--warehouse_bucket", default=None)
    parser.add_argument("--output_path", default=None,
                        help="Local output path prefix; overrides --warehouse_bucket")
    parser.add_argument("--window_seconds", type=int, default=60)
    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output_path:
        output_prefix = known_args.output_path
    elif known_args.warehouse_bucket:
        output_prefix = (
            f"gs://{known_args.warehouse_bucket}"
            f"/{known_args.pipeline_name}/{known_args.pipeline_name}/data/"
        )
    else:
        raise ValueError("Either --output_path or --warehouse_bucket must be provided")

    columns = load_columns(known_args.pipeline_name)
    schema = to_pyarrow_schema(columns)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> ReadFromPubSub(subscription=known_args.subscription)
            | "ParseAndProject" >> beam.Map(project_message, columns=columns)
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
