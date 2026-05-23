import json
import logging
import re
import subprocess
import time as _time

from google.api_core.exceptions import Conflict, Forbidden, NotFound
from google.auth.credentials import Credentials
from google.cloud import secretmanager, storage
from googleapiclient import discovery
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


def create_project(project_name: str, credentials: Credentials) -> str:
    """
    Create a new GCP project suffixed with the current epoch timestamp.
    Returns the generated project_id.
    """
    slug = re.sub(r"[^a-z0-9-]", "-", project_name.lower()).strip("-")[:19].rstrip("-")
    project_id = f"{slug}-{int(_time.time())}"

    service = discovery.build(
        "cloudresourcemanager", "v1", credentials=credentials, cache_discovery=False
    )
    operation = service.projects().create(
        body={"projectId": project_id, "name": project_name}
    ).execute()

    for attempt in range(30):
        op = service.operations().get(name=operation["name"]).execute()
        if op.get("done"):
            if "error" in op:
                raise RuntimeError(
                    f"GCP project creation failed: {op['error'].get('message', op['error'])}"
                )
            break
        print(f"  Waiting for project creation... ({(attempt + 1) * 2}s)", flush=True)
        _time.sleep(2)
    else:
        raise RuntimeError(
            f"GCP project creation timed out. "
            f"Check status at https://console.cloud.google.com/cloud-resource-manager"
        )

    logger.info("Created GCP project %s", project_id)
    return project_id


_REQUIRED_APIS = [
    "storage.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "run.googleapis.com",
    "sqladmin.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
]


def enable_required_apis(project_id: str, credentials: Credentials) -> None:
    """Enable all APIs required for dcf provisioning."""
    result = subprocess.run(
        ["gcloud", "services", "enable", "--project", project_id] + _REQUIRED_APIS,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to enable required GCP APIs")
    logger.info("Enabled required APIs on project %s", project_id)


def link_billing_account(project_id: str, credentials: Credentials) -> None:
    """Link the first active billing account to the project using gcloud CLI."""
    from .gcloud import get_active_billing_account
    billing_account = get_active_billing_account()
    if not billing_account:
        raise RuntimeError(
            "No active billing accounts found. Create one at: https://console.cloud.google.com/billing"
        )
    result = subprocess.run(
        ["gcloud", "billing", "projects", "link", project_id,
         f"--billing-account={billing_account}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to link billing account: {result.stderr}")
    logger.info("Linked billing account %s to project %s", billing_account, project_id)


def create_state_bucket(project_id: str, region: str, credentials: Credentials) -> str:
    """Create the GCS bucket used for Terraform state. Returns bucket name."""
    bucket_name = f"dcf-tf-state-{project_id}"
    client = storage.Client(project=project_id, credentials=credentials)
    last_err = None
    for attempt in range(10):
        try:
            bucket = client.create_bucket(bucket_name, location=region)
            bucket.versioning_enabled = True
            bucket.patch()
            logger.info("Created Terraform state bucket %s", bucket_name)
            return bucket_name
        except Conflict:
            logger.info("Terraform state bucket %s already exists", bucket_name)
            return bucket_name
        except Exception as e:
            if isinstance(e, Forbidden) or "billing" in str(e).lower():
                last_err = e
                print(f"  Waiting for billing to activate... (attempt {attempt + 1}/10)", flush=True)
                _time.sleep(5)
                continue
            raise
    raise RuntimeError(
        f"GCP billing did not activate for project '{project_id}' after 50s: {last_err}\n"
        f"Check billing at: https://console.cloud.google.com/billing/linkedaccount?project={project_id}"
    ) from last_err


def delete_secret(secret_name: str, credentials: Credentials) -> None:
    """Delete a Secret Manager secret and all its versions."""
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    try:
        client.delete_secret(request={"name": secret_name})
        logger.info("Deleted secret %s", secret_name)
    except NotFound:
        logger.info("Secret %s not found, skipping", secret_name)


def delete_gcs_bucket(bucket_name: str, credentials: Credentials) -> None:
    """Delete a GCS bucket and all its contents."""
    client = storage.Client(credentials=credentials)
    try:
        bucket = client.get_bucket(bucket_name)
        bucket.delete(force=True)
        logger.info("Deleted GCS bucket %s", bucket_name)
    except NotFound:
        logger.info("GCS bucket %s not found, skipping", bucket_name)


def delete_service_account(project_id: str, sa_email: str, credentials: Credentials) -> None:
    """Delete the dcf-lake service account."""
    service = discovery.build("iam", "v1", credentials=credentials, cache_discovery=False)
    try:
        service.projects().serviceAccounts().delete(
            name=f"projects/{project_id}/serviceAccounts/{sa_email}",
        ).execute()
        logger.info("Deleted service account %s", sa_email)
    except HttpError as e:
        if e.resp.status == 404:
            logger.info("Service account %s not found, skipping", sa_email)
        else:
            raise


def fetch_service_account_key(project_id: str, secret_name: str) -> dict:
    """Fetch the latest SA key from Secret Manager using ADC credentials."""
    from .gcloud import get_credentials
    credentials = get_credentials()
    client      = secretmanager.SecretManagerServiceClient(credentials=credentials)
    response    = client.access_secret_version(request={"name": f"{secret_name}/versions/latest"})
    return json.loads(response.payload.data.decode())
