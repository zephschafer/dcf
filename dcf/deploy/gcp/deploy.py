from __future__ import annotations


def ensure_warehouse_bucket(project_id: str, region: str) -> str:
    """Idempotently create gs://dcf-warehouse-{project_id} using ADC."""
    from google.cloud import storage
    from google.api_core.exceptions import Conflict

    bucket_name = f"dcf-warehouse-{project_id}"
    client = storage.Client(project=project_id)
    try:
        client.create_bucket(bucket_name, location=region)
    except Conflict:
        pass
    return bucket_name


def deploy(profile: dict) -> dict:
    """Create or connect to the GCS warehouse. Returns the gcp state dict."""
    project_id = profile.get("project_id")
    if not project_id:
        if profile.get("project_name"):
            raise RuntimeError(
                f"profiles.yml uses 'project_name: {profile['project_name']}' which is no longer supported.\n"
                "Replace it with 'project_id: <your-existing-gcp-project-id>'."
            )
        raise RuntimeError(
            "profiles.yml must contain 'project_id'. Example:\n"
            "  default:\n    type: gcp\n    project_id: my-project\n    region: us-central1"
        )

    region = profile.get("region", "us-central1")
    bucket = ensure_warehouse_bucket(project_id, region)
    return {
        "project_id": project_id,
        "region": region,
        "warehouse_bucket": bucket,
        "setup_status": "complete",
    }
