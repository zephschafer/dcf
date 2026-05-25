"""
Tests for dcf.warehouse_reader covering F-018, F-019, F-020, F-021.

All tests use a temporary local warehouse with real PyIceberg tables — no GCS,
no mocking of DuckDB. Catalog instances are created directly in tmp_path.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pyarrow as pa
import pytest

import dcf.warehouse_reader as wr


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _make_iceberg_table(warehouse: Path, dcf_dir: Path, namespace: str, table: str, rows: list[dict]) -> None:
    """Write a real local PyIceberg table to warehouse/namespace/table."""
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids

    cat = SqlCatalog(
        "dcf",
        **{
            "uri": f"sqlite:///{dcf_dir / 'catalog.db'}",
            "warehouse": str(warehouse),
        },
    )
    if not cat.namespace_exists((namespace,)):
        cat.create_namespace((namespace,))
    identifier = (namespace, table)
    arrow = pa.table({k: [r[k] for r in rows] for k in rows[0]})
    try:
        tbl = cat.load_table(identifier)
        tbl.overwrite(arrow)
    except Exception:
        tbl = cat.create_table(identifier, schema=_pyarrow_to_schema_without_ids(arrow.schema))
        tbl.append(arrow)


def _project_and_warehouse(tmp_path: Path) -> tuple[Path, Path]:
    """Return (project_root, warehouse) paths, creating .dcf/ dir."""
    dcf_dir = tmp_path / ".dcf"
    dcf_dir.mkdir(exist_ok=True)
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(exist_ok=True)
    return tmp_path, warehouse


# ------------------------------------------------------------------ #
# F-018: list_tables returns tables with location field               #
# ------------------------------------------------------------------ #

class TestListTables:
    def test_local_table_appears_with_location_local(self, tmp_path):
        """list_tables() returns local Iceberg tables with location='local'."""
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "myns", "mytable",
                            [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}])

        with patch("dcf.catalog.find_project_root", return_value=proj):
            tables = wr.list_tables()

        assert len(tables) == 1
        t = tables[0]
        assert t["full_name"] == "myns.mytable"
        assert t["location"] == "local"
        assert t["row_count"] == 2
        assert any(c["name"] == "id" for c in t["columns"])

    def test_multiple_namespaces_and_tables(self, tmp_path):
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "ns_a", "t1", [{"x": 1}])
        _make_iceberg_table(wh, proj / ".dcf", "ns_b", "t2", [{"y": 2}])

        with patch("dcf.catalog.find_project_root", return_value=proj):
            tables = wr.list_tables()

        names = {t["full_name"] for t in tables}
        assert "ns_a.t1" in names
        assert "ns_b.t2" in names

    def test_empty_warehouse_returns_no_tables(self, tmp_path):
        proj, _ = _project_and_warehouse(tmp_path)
        with patch("dcf.catalog.find_project_root", return_value=proj):
            tables = wr.list_tables()
        assert tables == []


# ------------------------------------------------------------------ #
# F-019: query() does not wrap write/DDL statements in SELECT … LIMIT #
# ------------------------------------------------------------------ #

class TestIsWriteStatement:
    @pytest.mark.parametrize("sql", [
        "COPY (SELECT 1) TO '/tmp/x.parquet' (FORMAT PARQUET)",
        "copy (SELECT 1 LIMIT 10) to '/tmp/x.parquet'",
        "CREATE TABLE foo AS SELECT 1",
        "create or replace table foo as select 1",
        "INSERT INTO foo SELECT 1",
        "DROP TABLE foo",
        "DELETE FROM foo WHERE id = 1",
        "UPDATE foo SET x = 1",
        "ALTER TABLE foo ADD COLUMN y INT",
    ])
    def test_write_statements_detected(self, sql):
        assert wr._is_write_statement(sql) is True

    @pytest.mark.parametrize("sql", [
        "SELECT * FROM foo",
        "select count(*) from foo",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "DESCRIBE foo",
    ])
    def test_read_statements_not_detected(self, sql):
        assert wr._is_write_statement(sql) is False


class TestQueryDoesNotWrapDDL:
    def test_copy_without_limit_is_not_wrapped(self, tmp_path):
        """COPY TO must execute without being wrapped in SELECT … LIMIT."""
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "ns", "tbl", [{"x": 1}, {"x": 2}])
        out = str(tmp_path / "out.parquet")

        sql = f"COPY (SELECT x FROM ns.tbl) TO '{out}' (FORMAT PARQUET)"
        with patch("dcf.catalog.find_project_root", return_value=proj):
            result = wr.query(sql)

        assert isinstance(result, list)
        assert "error" not in str(result)
        assert Path(out).exists()

    def test_select_without_limit_is_wrapped(self, tmp_path):
        """SELECT without LIMIT returns rows (capped at _MAX_ROWS)."""
        proj, wh = _project_and_warehouse(tmp_path)
        rows = [{"x": i} for i in range(10)]
        _make_iceberg_table(wh, proj / ".dcf", "ns", "tbl", rows)

        with patch("dcf.catalog.find_project_root", return_value=proj):
            result = wr.query("SELECT x FROM ns.tbl")

        assert len(result) == 10


# ------------------------------------------------------------------ #
# F-020: materialize_model writes a new Iceberg table                 #
# ------------------------------------------------------------------ #

class TestMaterializeModel:
    def test_local_catalog_writes_iceberg_table(self, tmp_path):
        """materialize_model() creates an Iceberg table in the target namespace."""
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "src", "src_tbl",
                            [{"v": 10}, {"v": 20}])

        with patch("dcf.catalog.find_project_root", return_value=proj):
            result = wr.materialize_model(
                "SELECT v * 2 AS doubled FROM src.src_tbl", "out", "doubled"
            )

        assert result["ok"] is True
        assert result["row_count"] == 2
        assert result["namespace"] == "out"
        assert result["table"] == "doubled"
        # Iceberg metadata exists
        assert (wh / "out" / "doubled" / "metadata").exists()

    def test_result_is_queryable_after_materialization(self, tmp_path):
        """After materialize_model(), the new table is queryable via query()."""
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "src", "src_tbl",
                            [{"n": 1}, {"n": 2}, {"n": 3}])

        with patch("dcf.catalog.find_project_root", return_value=proj):
            wr.materialize_model("SELECT SUM(n) AS total FROM src.src_tbl", "agg", "totals")
            rows = wr.query("SELECT total FROM agg.totals")

        assert rows == [{"total": 6}]

    def test_materialize_overwrites_existing_table(self, tmp_path):
        """Calling materialize_model twice replaces the existing table."""
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "src", "t", [{"v": 1}])

        with patch("dcf.catalog.find_project_root", return_value=proj):
            wr.materialize_model("SELECT v FROM src.t", "out", "m")
            wr.materialize_model("SELECT v * 10 AS v FROM src.t", "out", "m")
            rows = wr.query("SELECT v FROM out.m")

        assert rows == [{"v": 10}]


# ------------------------------------------------------------------ #
# F-021: query() resolves tables via PyIceberg catalog                #
# ------------------------------------------------------------------ #

class TestQueryTableResolution:
    def test_namespace_table_ref_resolves(self, tmp_path):
        """namespace.table references are resolved from PyIceberg catalog."""
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "myns", "mytable", [{"x": 42}])

        with patch("dcf.catalog.find_project_root", return_value=proj):
            result = wr.query("SELECT x FROM myns.mytable")

        assert result == [{"x": 42}]

    def test_unknown_table_raises(self, tmp_path):
        """query() for a non-existent table raises a DuckDB error."""
        proj, _ = _project_and_warehouse(tmp_path)
        with patch("dcf.catalog.find_project_root", return_value=proj):
            with pytest.raises(Exception):
                wr.query("SELECT x FROM totally_unknown.table")

    def test_multiple_tables_in_one_query(self, tmp_path):
        """query() can join two tables from different namespaces."""
        proj, wh = _project_and_warehouse(tmp_path)
        _make_iceberg_table(wh, proj / ".dcf", "ns_a", "t1", [{"id": 1, "val": "a"}])
        _make_iceberg_table(wh, proj / ".dcf", "ns_b", "t2", [{"id": 1, "extra": "b"}])

        with patch("dcf.catalog.find_project_root", return_value=proj):
            result = wr.query(
                "SELECT t1.val, t2.extra FROM ns_a.t1 JOIN ns_b.t2 ON t1.id = t2.id"
            )

        assert len(result) == 1
        assert result[0] == {"val": "a", "extra": "b"}
