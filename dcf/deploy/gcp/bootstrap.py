import base64
import json
import logging
import re
import time as _time

from google.api_core.exceptions import Conflict, NotFound
from google.auth.credentials import Credentials
from google.cloud import secretmanager, storage
from googleapiclient import discovery
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

_SA_ACCOUNT_ID = "dcf-lake"
_SECRET_ID     = "dcf-lake-sa-key"


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

    for _ in range(30):
        op = service.operations().get(name=operation["name"]).execute()
        if op.get("done"):
            if "error" in op:
                raise RuntimeError(
                    f"GCP project creation failed: {op['error'].get('message', op['error'])}"
                )
            break
        _time.sleep(2)
    else:
        raise RuntimeError(
            f"GCP project creation timed out. "
            f"Check status at https://console.cloud.google.com/cloud-resource-manager"
        )

    logger.info("Created GCP project %s", project_id)
    return project_id


def create_state_bucket(project_id: str, region: str, credentials: Credentials) -> str:
    """Create the GCS bucket used for Terraform state. Returns bucket name."""
    bucket_name = f"dcf-tf-state-{project_id}"
    client = storage.Client(project=project_id, credentials=credentials)
    try:
        bucket = client.create_bucket(bucket_name, location=region)
        bucket.versioning_enabled = True
        bucket.patch()
        logger.info("Created Terraform state bucket %s", bucket_name)
    except Conflict:
        logger.info("Terraform state bucket %s already exists", bucket_name)
    except Exception as e:
        from google.api_core.exceptions import Forbidden
        if isinstance(e, Forbidden) or "billing" in str(e).lower():
            raise RuntimeError(
                f"Billing is not enabled for project '{project_id}'. "
                f"Enable it at: https://console.cloud.google.com/billing"
            ) from e
        raise
    return bucket_name


def create_service_account(project_id: str, credentials: Credentials) -> str:
    """Create the dcf-lake service account. Returns SA email."""
    sa_email = f"{_SA_ACCOUNT_ID}@{project_id}.iam.gserviceaccount.com"
    service  = discovery.build("iam", "v1", credentials=credentials, cache_discovery=False)
    try:
        service.projects().serviceAccounts().create(
            name=f"projects/{project_id}",
            body={
                "accountId": _SA_ACCOUNT_ID,
                "serviceAccount": {"displayName": "dcf Lake Service Account"},
            },
        ).execute()
        logger.info("Created service account %s", sa_email)
    except HttpError as e:
        if e.resp.status == 409:
            logger.info("Service account %s already exists", sa_email)
        else:
            raise
    return sa_email


def create_service_account_key(project_id: str, sa_email: str, credentials: Credentials) -> dict:
    """Create a new JSON key for the SA. Returns decoded key dict."""
    service = discovery.build("iam", "v1", credentials=credentials, cache_discovery=False)
    result  = service.projects().serviceAccounts().keys().create(
        name=f"projects/{project_id}/serviceAccounts/{sa_email}",
        body={"privateKeyType": "TYPE_GOOGLE_CREDENTIALS_FILE"},
    ).execute()
    key_data = json.loads(base64.b64decode(result["privateKeyData"]).decode())
    logger.info("Created SA key for %s", sa_email)
    return key_data


def store_key_in_secret_manager(project_id: str, key_data: dict, credentials: Credentials) -> str:
    """
    Store the SA key in Secret Manager as 'dcf-lake-sa-key'.
    Creates the secret if it doesn't exist, then adds a new version.
    Returns the full secret resource name.
    """
    client      = secretmanager.SecretManagerServiceClient(credentials=credentials)
    parent      = f"projects/{project_id}"
    secret_name = f"{parent}/secrets/{_SECRET_ID}"

    try:
        client.create_secret(request={
            "parent":    parent,
            "secret_id": _SECRET_ID,
            "secret":    {"replication": {"automatic": {}}},
        })
        logger.info("Created Secret Manager secret %s", _SECRET_ID)
    except Conflict:
        logger.info("Secret %s already exists, adding new version", _SECRET_ID)

    client.add_secret_version(request={
        "parent":  secret_name,
        "payload": {"data": json.dumps(key_data).encode()},
    })
    logger.info("Stored SA key in Secret Manager")
    return secret_name


def delete_secret(secret_name: str, credentials: Credentials) -> None:
    """Delete a Secret Manager secret and all its versions."""
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    try:
        client.delete_secret(request={"name": secret_name})
        logger.info("Deleted secret %s", secret_name)
    except NotFound:
        logger.info("Secret %s not found, skipping", secret_name)


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
