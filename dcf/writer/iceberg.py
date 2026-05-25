from __future__ import annotations

import datetime
import uuid
from pathlib import Path

import pandas as pd
import pytz

from ..config.models import Collector, StagingConfig, MergeConfig


def _gcs_warehouse_bucket() -> str:
    from ..state import load_state
    bucket = load_state().get("gcp", {}).get("warehouse_bucket")
    if not bucket:
        raise RuntimeError(
            "GCP warehouse bucket not configured. Run: dcf deploy"
        )
    return bucket


def _pst_now() -> str:
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    return utc_now.astimezone(pytz.timezone("America/Los_Angeles")).isoformat()


# ------------------------------------------------------------------ #
# PyIceberg helpers (all non-staging strategies)                       #
# ------------------------------------------------------------------ #

def _ensure_iceberg_namespace(catalog, namespace: str) -> None:
    if not catalog.namespace_exists((namespace,)):
        catalog.create_namespace((namespace,))


def _arrow_schema_from_df(df: pd.DataFrame):
    import pyarrow as pa
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    return _pyarrow_to_schema_without_ids(pa.Table.from_pandas(df, preserve_index=False).schema)


def _upsert_iceberg(catalog, df: pd.DataFrame, identifier: tuple, pk: str | None) -> None:
    import pyarrow as pa
    from pyiceberg.exceptions import NoSuchTableError

    arrow = pa.Table.from_pandas(df, preserve_index=False)
    _ensure_iceberg_namespace(catalog, identifier[0])

    try:
        table = catalog.load_table(identifier)
        if pk is not None:
            existing = table.scan().to_arrow().to_pandas()
            existing = existing[~existing[pk].isin(df[pk].values)]
            merged = pd.concat([existing, df], ignore_index=True)
            arrow = pa.Table.from_pandas(merged, preserve_index=False)
            table.overwrite(arrow)
        else:
            table.append(arrow)
    except NoSuchTableError:
        table = catalog.create_table(identifier, schema=_arrow_schema_from_df(df))
        table.append(arrow)


def _append_iceberg(catalog, df: pd.DataFrame, identifier: tuple) -> None:
    import pyarrow as pa
    from pyiceberg.exceptions import NoSuchTableError

    arrow = pa.Table.from_pandas(df, preserve_index=False)
    _ensure_iceberg_namespace(catalog, identifier[0])

    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        table = catalog.create_table(identifier, schema=_arrow_schema_from_df(df))

    table.append(arrow)


def _overwrite_iceberg(catalog, df: pd.DataFrame, identifier: tuple) -> None:
    import pyarrow as pa
    from pyiceberg.exceptions import NoSuchTableError

    arrow = pa.Table.from_pandas(df, preserve_index=False)
    _ensure_iceberg_namespace(catalog, identifier[0])

    try:
        table = catalog.load_table(identifier)
        table.overwrite(arrow)
    except NoSuchTableError:
        table = catalog.create_table(identifier, schema=_arrow_schema_from_df(df))
        table.append(arrow)


# ------------------------------------------------------------------ #
# Spark helpers (staging+merge only)                                   #
# ------------------------------------------------------------------ #

def _spark_df(spark, df: pd.DataFrame):
    from pyspark.sql.types import StructType, StructField, StringType
    df = df.astype(str)
    schema = StructType([StructField(col, StringType(), True) for col in df.columns])
    return spark.createDataFrame(df, schema=schema)


def _ensure_namespace_spark(spark, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def _append_spark(spark, df: pd.DataFrame, table_id: str) -> None:
    sdf = _spark_df(spark, df)
    if spark.catalog.tableExists(table_id):
        sdf.writeTo(table_id).append()
    else:
        sdf.writeTo(table_id).using("iceberg").tableProperty("format-version", "2").create()


def _write_staged(
    spark,
    collector: Collector,
    df: pd.DataFrame,
    catalog: str,
    namespace: str,
    staging: StagingConfig,
    merge_cfg: MergeConfig | None,
    dynamic_params: dict,
) -> None:
    param_value = dynamic_params.get(staging.partition_param, "default")
    table_name = staging.table_pattern.format(**{staging.partition_param: param_value})
    table_id = f"{catalog}.{namespace}.{table_name}"

    _append_spark(spark, df, table_id)

    if merge_cfg:
        _rebuild_merged(spark, catalog, namespace, staging, merge_cfg, collector.cadence.primary_key)


def _rebuild_merged(
    spark,
    catalog: str,
    namespace: str,
    staging: StagingConfig,
    merge_cfg: MergeConfig,
    primary_key: str | None,
) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    tables = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
    prefix = staging.table_pattern.split("{")[0]
    staging_ids = [
        f"{catalog}.{namespace}.{t['tableName']}"
        for t in tables
        if t["tableName"].startswith(prefix) and t["tableName"].endswith("_loader_staging")
    ]

    if not staging_ids:
        return

    combined = spark.table(staging_ids[0])
    for tid in staging_ids[1:]:
        combined = combined.union(spark.table(tid))

    if merge_cfg.dedup and merge_cfg.dedup.type == "latest_non_null" and primary_key:
        from functools import reduce
        import operator

        dedup_cols = merge_cfg.dedup.columns

        def safe_unix_ts(col_name):
            return F.when(
                F.upper(F.col(col_name)) != "NAN",
                F.col(col_name).cast("timestamp").cast("long"),
            ).otherwise(F.lit(None).cast("long"))

        def non_nan_flag(col_name):
            return F.when(F.upper(F.col(col_name)) != "NAN", F.lit(1)).otherwise(F.lit(0))

        flag_sum = reduce(operator.add, [non_nan_flag(c) for c in dedup_cols])

        w = Window.partitionBy(primary_key).orderBy(
            F.greatest(*[safe_unix_ts(c) for c in dedup_cols]).desc_nulls_last(),
            flag_sum.desc(),
        )
        combined = (
            combined
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    merged_id = f"{catalog}.{namespace}.{merge_cfg.table}"
    combined.writeTo(merged_id).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    print(f"  Rebuilt merged table → {merged_id} ({combined.count()} rows)")


# ------------------------------------------------------------------ #
# Public entry point                                                    #
# ------------------------------------------------------------------ #

def write(
    spark,
    collector: Collector,
    df: pd.DataFrame,
    catalog: str = "local",
    dynamic_params: dict | None = None,
    table_name_override: str | None = None,
    primary_key_override: str | None = None,
) -> None:
    if df.empty:
        return

    df = df.copy()
    df["dcf_updated_at"] = _pst_now()

    table_name = table_name_override or collector.name
    pk = primary_key_override if primary_key_override is not None else collector.cadence.primary_key
    namespace = collector.namespace or table_name
    identifier = (namespace, table_name)
    build = collector.cadence

    # Staging+merge: Spark Hadoop Iceberg catalog
    if build.staging:
        _ensure_namespace_spark(spark, catalog, namespace)
        _write_staged(spark, collector, df, catalog, namespace, build.staging, build.merge, dynamic_params or {})
        return

    # All other strategies: PyIceberg
    from ..catalog import get_catalog as _get_pyiceberg_catalog
    pyiceberg_catalog = _get_pyiceberg_catalog(catalog)

    if build.strategy == "incremental":
        _upsert_iceberg(pyiceberg_catalog, df, identifier, pk)
    elif build.strategy == "append":
        _append_iceberg(pyiceberg_catalog, df, identifier)
    elif build.strategy == "full_refresh":
        _overwrite_iceberg(pyiceberg_catalog, df, identifier)
