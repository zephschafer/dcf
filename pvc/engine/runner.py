from __future__ import annotations

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
    from pvc.spark_session import get_spark
    spark = get_spark("pvc")

    param_defs = {p.name: p for p in pipeline.source.params}
    request_sequence = build_request_sequence(pipeline.source.iterate, param_defs)

    if limit is not None:
        request_sequence = request_sequence[:limit]

    # Static params declared in the YAML (value is set) flow through to Python sources
    static_params = {p.name: p.value for p in pipeline.source.params if p.value is not None}

    print(f"\n[pvc] Running '{pipeline.name}' — {len(request_sequence)} requests\n")

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
            print(f"    fetch error: {e}")
            continue

        if not records:
            print(f"    0 records — skipping")
            continue

        df = project(records, pipeline.schema_)
        print(f"    {len(df)} rows → writing")

        iceberg_writer.write(spark, pipeline, df, catalog=catalog, dynamic_params=dynamic_params)

    from ..project import find_project_root
    namespace = pipeline.namespace or pipeline.name
    warehouse_path = find_project_root() / "warehouse" / namespace / pipeline.name / "data"
    print(f"\n[pvc] '{pipeline.name}' complete → {warehouse_path}\n")
    spark.stop()
