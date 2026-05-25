from __future__ import annotations

import textwrap
import traceback

from ..config.models import (
    Collector, HttpSource, PythonSource, SqlSource,
    DateRangeIterate, CategoricalIterate,
)
from .iterator import build_request_sequence
from .fetcher import fetch_records
from .projector import project
from .. import writer as iceberg_writer


def _log_preamble(collector: Collector, n_requests: int) -> None:
    print(f"\n[dcf] {collector.name}")
    src = collector.source
    iterate = collector.cadence.iterate

    iterated_params: set[str] = set()
    if iterate:
        for spec in iterate:
            if isinstance(spec, DateRangeIterate):
                iterated_params.update(spec.params)
            elif isinstance(spec, CategoricalIterate):
                iterated_params.add(spec.param)

    if isinstance(src, HttpSource):
        static = {p.name: p.value for p in src.params if p.value is not None}
        query_parts = [*[f"{k}={v}" for k, v in static.items()],
                       *[f"{name}={{{name}}}" for name in iterated_params]]
        url = f"{src.url}?{'&'.join(query_parts)}" if query_parts else src.url
        print(f"  url:     {url}")
    elif isinstance(src, PythonSource):
        print(f"  source:  {src.module}.{src.function}()")
    elif isinstance(src, SqlSource):
        conn = src.connection
        loc = (f"{conn.instance}/{conn.database}" if conn.type == "cloud_sql"
               else f"{conn.host}:{conn.port}/{conn.database}")
        print(f"  source:  {conn.type} {loc}")
        print(f"  tables:  {', '.join(t.table for t in src.tables)}")

    if not isinstance(src, SqlSource):
        if not iterate:
            print(f"  {n_requests} request, no iteration")
        else:
            parts = []
            for spec in iterate:
                if isinstance(spec, DateRangeIterate):
                    parts.append(f"date_range {spec.start} → {spec.end} in {spec.step} steps")
                elif isinstance(spec, CategoricalIterate):
                    parts.append(f"categorical {spec.param} ({len(spec.values)} values)")
            print(f"  iterate: {' × '.join(parts)} · {n_requests} requests")
    print()


def _run_sql_collector(spark, collector: Collector, catalog: str) -> None:
    from .fetcher import fetch_sql_table
    src = collector.source
    failed = 0

    for sql_table in src.tables:
        print(f"  [{sql_table.table}] fetching...", flush=True)
        try:
            records = fetch_sql_table(src, sql_table)
        except Exception as e:
            failed += 1
            print(f"    fetch error ({type(e).__name__}): {e}")
            print(textwrap.indent(traceback.format_exc(), "      "))
            continue

        if not records:
            print(f"    0 rows — skipping")
            continue

        df = project(records, None)
        print(f"    {len(df)} rows → writing")
        iceberg_writer.write(
            spark,
            collector,
            df,
            catalog=catalog,
            table_name_override=sql_table.table,
            primary_key_override=sql_table.primary_key,
        )

    total = len(src.tables)
    label = f"'{collector.name}'"
    if failed == total:
        print(f"\n[dcf] {label} FAILED — all {total} table(s) errored\n")
    elif failed:
        print(f"\n[dcf] {label} complete with errors — {failed}/{total} tables failed\n")
    else:
        print(f"\n[dcf] {label} complete\n")


def run_collector(
    collector: Collector,
    catalog: str = "local",
    limit: int | None = None,
    param_overrides: dict | None = None,
) -> None:
    request_sequence = build_request_sequence(collector.cadence.iterate)

    if limit is not None:
        request_sequence = request_sequence[:limit]

    _log_preamble(collector, len(request_sequence))

    # Spark is only needed for staging+merge collectors; PyIceberg handles everything else
    if collector.cadence.staging is not None:
        from dcf.spark_session import get_spark
        spark = get_spark("dcf")
    else:
        spark = None

    if isinstance(collector.source, SqlSource):
        _run_sql_collector(spark, collector, catalog)
        if spark is not None:
            spark.stop()
        return

    # Static params declared in the YAML (value is set) flow through to Python sources
    static_params = {p.name: p.value for p in collector.source.params if p.value is not None}

    failed = 0

    for i, dynamic_params in enumerate(request_sequence, 1):
        label = " ".join(f"{k}={v}" for k, v in dynamic_params.items())
        print(f"  [{i}/{len(request_sequence)}] {label}")

        # Build full params: static defaults → iterate values → CLI overrides
        full_params = {**static_params, **dynamic_params, **(param_overrides or {})}

        # For http sources, iterate-driven params are already handled in the fetcher;
        # pass full_params only to python sources which need everything in one dict
        source_params = full_params if isinstance(collector.source, PythonSource) else dynamic_params

        try:
            records = fetch_records(collector.source, source_params)
        except Exception as e:
            failed += 1
            print(f"    fetch error ({type(e).__name__}): {e}")
            print(textwrap.indent(traceback.format_exc(), "      "))
            continue

        if not records:
            print(f"    0 records — skipping")
            continue

        df = project(records, collector.source.schema_)
        print(f"    {len(df)} rows → writing")

        iceberg_writer.write(spark, collector, df, catalog=catalog, dynamic_params=dynamic_params)

    ns = collector.namespace or collector.name
    if catalog == "gcp":
        from .. import writer as _w
        bucket = _w.iceberg._gcs_warehouse_bucket()
        dest = f"gs://{bucket}/{ns}/{collector.name}/"
    else:
        from ..project import find_project_root
        dest = str(find_project_root() / "warehouse" / ns / collector.name)

    total = len(request_sequence)
    if failed == total:
        print(f"\n[dcf] '{collector.name}' FAILED — all {total} iteration(s) errored → {dest}\n")
    elif failed:
        print(f"\n[dcf] '{collector.name}' complete with errors — {failed}/{total} iteration(s) failed → {dest}\n")
    else:
        print(f"\n[dcf] '{collector.name}' complete → {dest}\n")

    if spark is not None:
        spark.stop()
