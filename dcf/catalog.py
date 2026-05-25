from __future__ import annotations

from pathlib import Path

from .project import find_project_root
from .state import load_state

_CATALOG_NAME = "dcf"


def get_catalog(catalog_type: str | None = None):
    """
    Return a PyIceberg SqlCatalog for this project.

    catalog_type: "local" | "gcp" | None (auto-detect from state)
      local → warehouse at <project_root>/warehouse/
      gcp   → warehouse at gs://<bucket>/

    Catalog metadata stored in .dcf/catalog.db (SQLite).
    PyArrowFileIO handles both local and GCS I/O via ADC.
    """
    from pyiceberg.catalog.sql import SqlCatalog

    if catalog_type is None:
        from .state import get_catalog as _get_type
        try:
            catalog_type = _get_type()
        except RuntimeError:
            catalog_type = "local"

    project_root = find_project_root()
    dcf_dir = project_root / ".dcf"
    dcf_dir.mkdir(exist_ok=True)
    catalog_db_uri = f"sqlite:///{dcf_dir / 'catalog.db'}"

    if catalog_type == "gcp":
        bucket = load_state().get("gcp", {}).get("warehouse_bucket")
        if not bucket:
            raise RuntimeError(
                "GCP warehouse bucket not configured. Run: dcf deploy"
            )
        warehouse = f"gs://{bucket}/"
    else:
        warehouse = str(project_root / "warehouse")

    return SqlCatalog(
        _CATALOG_NAME,
        **{
            "uri": catalog_db_uri,
            "warehouse": warehouse,
        },
    )
