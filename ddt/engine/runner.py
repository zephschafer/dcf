from __future__ import annotations

import textwrap
import traceback

from ..config.models import Pipeline, PythonSource
from .iterator import build_request_sequence
from .fetcher import fetch_records
from .projector import project
from .. import writer as iceberg_writer


def run_pipeline(
    pipeline: Pipeline,
    catalog: str = "local",
    limit: int | None = None,
    param_overrides: dict | None = None,
) -> None:
    # GCS write path bypasses Spark entirely — skip JVM startup
    if catalog == "gcp":
        spark = None
    else:
        from ddt.spark_session import get_spark
        spark = get_spark("ddt")

    param_defs = {p.name: p for p in pipeline.source.params}
    request_sequence = build_request_sequence(pipeline.source.iterate, param_defs)

    if limit is not None:
        request_sequence = request_sequence[:limit]

    # Static params declared in the YAML (value is set) flow through to Python sources
    static_params = {p.name: p.value for p in pipeline.source.params if p.value is not None}

    print(f"\n[ddt] Running '{pipeline.name}' — {len(request_sequence)} requests\n")

    failed = 0

    for i, dynamic_params in enumerate(request_sequence, 1):
        label = " ".join(f"{k}={v}" for k, v in dynamic_params.items())
        print(f"  [{i}/{len(request_sequence)}] {label}")

        # Build full params: static defaults → iterate values → CLI overrides
        full_params = {**static_params, **dynamic_params, **(param_overrides or {})}

        # For http sources, iterate-driven params are already handled in the fetcher;
        # pass full_params only to python sources which need everything in one dict
        source_params = full_params if isinstance(pipeline.source, PythonSource) else dynamic_params

        try:
            records = fetch_records(pipeline.source, source_params)
        except Exception as e:
            failed += 1
            print(f"    fetch error ({type(e).__name__}): {e}")
            print(textwrap.indent(traceback.format_exc(), "      "))
            continue

        if not records:
            print(f"    0 records — skipping")
            continue

        df = project(records, pipeline.schema_)
        print(f"    {len(df)} rows → writing")

        iceberg_writer.write(spark, pipeline, df, catalog=catalog, dynamic_params=dynamic_params)

    namespace = pipeline.namespace or pipeline.name
    if catalog == "gcp":
        from .. import writer as _w
        bucket = _w.iceberg._gcs_warehouse_bucket()
        dest = f"gs://{bucket}/{namespace}/{pipeline.name}/data"
    else:
        from ..project import find_project_root
        dest = str(find_project_root() / "warehouse" / namespace / pipeline.name / "data")

    total = len(request_sequence)
    if failed == total:
        print(f"\n[ddt] '{pipeline.name}' FAILED — all {total} iteration(s) errored → {dest}\n")
    elif failed:
        print(f"\n[ddt] '{pipeline.name}' complete with errors — {failed}/{total} iteration(s) failed → {dest}\n")
    else:
        print(f"\n[ddt] '{pipeline.name}' complete → {dest}\n")

    if spark is not None:
        spark.stop()
