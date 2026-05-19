from __future__ import annotations

import datetime
import io
import uuid
from pathlib import Path

import pandas as pd
import pytz

from ..config.models import Collector, StagingConfig, MergeConfig


def _gcs_warehouse_bucket() -> str:
    from ..profiles import load_profile
    try:
        profile = load_profile("default")
    except (FileNotFoundError, KeyError):
        profile = {}
    bucket = profile.get("gcp", {}).get("warehouse_bucket")
    if not bucket:
        raise RuntimeError(
            "GCP warehouse bucket not configured. Run: dcf gcp setup --project-id ... --region ..."
        )
    return bucket


def _pst_now() -> str:
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    return utc_now.astimezone(pytz.timezone("America/Los_Angeles")).isoformat()


def _spark_df(spark, df: pd.DataFrame):
    from pyspark.sql.types import StructType, StructField, StringType
    df = df.astype(str)
    schema = StructType([StructField(col, StringType(), True) for col in df.columns])
    return spark.createDataFrame(df, schema=schema)


def _ensure_namespace(spark, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def write(
    spark,
    collector: Collector,
    df: pd.DataFrame,
    catalog: str = "local",
    dynamic_params: dict | None = None,
) -> None:
    """
    Write a projected DataFrame to the Iceberg warehouse according to
    the collector's build strategy.
    """
    if df.empty:
        return

    df = df.copy()
    df["dcf_updated_at"] = _pst_now()

    catalog_namespace = collector.namespace or collector.name  # Spark catalog always needs a namespace
    build = collector.cadence

    # GCS: use google-cloud-storage + PyArrow directly for all strategies — no Spark catalog needed
    if catalog == "gcp":
        warehouse_bucket = _gcs_warehouse_bucket()
        if build.strategy == "incremental":
            _upsert_gcs(df, warehouse_bucket, collector.namespace, collector.name, build.primary_key)
        elif build.strategy == "append":
            _append_gcs(df, warehouse_bucket, collector.namespace, collector.name)
        elif build.strategy == "full_refresh":
            _overwrite_gcs(df, warehouse_bucket, collector.namespace, collector.name)
        return

    _ensure_namespace(spark, catalog, catalog_namespace)

    if build.staging:
        _write_staged(spark, collector, df, catalog, catalog_namespace, build.staging, build.merge, dynamic_params or {})
    elif build.strategy == "incremental":
        warehouse_root = Path(spark.conf.get(f"spark.sql.catalog.{catalog}.warehouse"))
        _upsert(df, warehouse_root, collector.namespace, collector.name, build.primary_key)
    elif build.strategy == "append":
        _append(spark, df, f"{catalog}.{catalog_namespace}.{collector.name}")
    elif build.strategy == "full_refresh":
        _overwrite(spark, df, f"{catalog}.{catalog_namespace}.{collector.name}")


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

    warehouse_root = Path(spark.conf.get(f"spark.sql.catalog.{catalog}.warehouse"))
    _upsert(df, warehouse_root, namespace, table_name, collector.cadence.primary_key)

    if merge_cfg:
        _rebuild_merged(spark, catalog, namespace, staging, merge_cfg, collector.cadence.primary_key)


def _upsert(df: pd.DataFrame, warehouse_root: Path, namespace: str | None, table_name: str, primary_key: str | None) -> None:
    """Upsert df into warehouse_root/namespace/table_name using pyarrow directly.

    Manages parquet files without Iceberg so the data directory always contains
    exactly the current data. This lets DuckDB glob reads (warehouse_reader.py)
    see correct results without needing to parse Iceberg snapshot metadata.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = df.copy()
    if primary_key:
        df = df.drop_duplicates(subset=[primary_key])

    table_root = warehouse_root / namespace / table_name if namespace else warehouse_root / table_name
    data_dir = table_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    existing_files = sorted(data_dir.glob("*.parquet"))
    if existing_files:
        existing = pd.concat(
            [pq.read_table(f).to_pandas() for f in existing_files],
            ignore_index=True,
        )
        if primary_key:
            existing = existing[~existing[primary_key].isin(df[primary_key].values)]
        merged = pd.concat([existing, df], ignore_index=True)
    else:
        merged = df

    new_file = data_dir / f"{uuid.uuid4()}.parquet"
    pq.write_table(pa.Table.from_pandas(merged, preserve_index=False), new_file)

    for f in existing_files:
        f.unlink()


def _upsert_gcs(
    df: pd.DataFrame,
    bucket_name: str,
    namespace: str | None,
    table_name: str,
    primary_key: str | None,
) -> None:
    """Upsert df into GCS bucket using google-cloud-storage + PyArrow (no Spark needed)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"{namespace}/{table_name}/data" if namespace else f"{table_name}/data"

    parquet_blobs = [
        b for b in bucket.list_blobs(prefix=f"{prefix}/")
        if b.name.endswith(".parquet")
    ]

    df = df.copy()
    if primary_key:
        df = df.drop_duplicates(subset=[primary_key])

    if parquet_blobs:
        existing = pd.concat(
            [pq.read_table(io.BytesIO(b.download_as_bytes())).to_pandas() for b in parquet_blobs],
            ignore_index=True,
        )
        if primary_key:
            existing = existing[~existing[primary_key].isin(df[primary_key].values)]
        merged = pd.concat([existing, df], ignore_index=True)
    else:
        merged = df

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(merged, preserve_index=False), buf)
    buf.seek(0)

    new_blob = bucket.blob(f"{prefix}/{uuid.uuid4()}.parquet")
    new_blob.upload_from_file(buf, content_type="application/octet-stream")

    for blob in parquet_blobs:
        blob.delete()


def _append_gcs(
    df: pd.DataFrame,
    bucket_name: str,
    namespace: str | None,
    table_name: str,
) -> None:
    """Append df to GCS by writing a new Parquet file alongside existing ones."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"{namespace}/{table_name}/data" if namespace else f"{table_name}/data"

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df.copy(), preserve_index=False), buf)
    buf.seek(0)

    new_blob = bucket.blob(f"{prefix}/{uuid.uuid4()}.parquet")
    new_blob.upload_from_file(buf, content_type="application/octet-stream")


def _overwrite_gcs(
    df: pd.DataFrame,
    bucket_name: str,
    namespace: str | None,
    table_name: str,
) -> None:
    """Full-refresh: delete all existing Parquet blobs, write a fresh single file."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"{namespace}/{table_name}/data" if namespace else f"{table_name}/data"

    for blob in bucket.list_blobs(prefix=f"{prefix}/"):
        if blob.name.endswith(".parquet"):
            blob.delete()

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df.copy(), preserve_index=False), buf)
    buf.seek(0)

    new_blob = bucket.blob(f"{prefix}/{uuid.uuid4()}.parquet")
    new_blob.upload_from_file(buf, content_type="application/octet-stream")


def _append(spark, df: pd.DataFrame, table_id: str) -> None:
    sdf = _spark_df(spark, df)
    if spark.catalog.tableExists(table_id):
        sdf.writeTo(table_id).append()
    else:
        sdf.writeTo(table_id).using("iceberg").tableProperty("format-version", "2").create()


def _overwrite(spark, df: pd.DataFrame, table_id: str) -> None:
    sdf = _spark_df(spark, df)
    sdf.writeTo(table_id).using("iceberg").tableProperty("format-version", "2").createOrReplace()


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

    # Collect all staging tables that match the pattern by listing the namespace
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
    prefix = staging.table_pattern.split("{")[0]  # e.g. "permits_"
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
            # Cast to timestamp without a strict format so that both
            # 'M/d/yyyy' and 'yyyy-MM-dd HH:mm:ss' values are handled.
            # ANSI mode is disabled in the session so invalid strings
            # return null rather than throwing.
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
