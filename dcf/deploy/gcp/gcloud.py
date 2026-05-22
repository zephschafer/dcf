import shutil
import subprocess
import logging
import warnings

import google.auth
from google.auth.exceptions import DefaultCredentialsError

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
_INSTALL_URL = "https://cloud.google.com/sdk/docs/install"

# Suppress the "no quota project" warning — dcf strips quota_project_id intentionally
warnings.filterwarnings(
    "ignore",
    message="Your application has authenticated using end user credentials",
    category=UserWarning,
)


def get_credentials():
    """
    Return ADC credentials scoped to cloud-platform, with quota_project_id stripped.

    Resolution order:
      1. Existing ADC (GOOGLE_APPLICATION_CREDENTIALS env var or gcloud ADC file)
      2. If not configured but gcloud is installed, run `gcloud auth application-default login`
         (opens a browser on the local machine) then retry.
      3. If gcloud is not installed, raise RuntimeError with install instructions.
    """
    try:
        creds, _ = google.auth.default(scopes=_SCOPES)
        return _strip_quota_project(creds)
    except DefaultCredentialsError:
        pass

    gcloud = shutil.which("gcloud")
    if not gcloud:
        raise RuntimeError(
            "No Google credentials found and gcloud CLI is not installed.\n"
            f"Install it at: {_INSTALL_URL}\n"
            "Then run: gcloud auth application-default login"
        )

    logger.info("ADC not configured — running gcloud auth application-default login")
    subprocess.run(["gcloud", "auth", "application-default", "login"], check=True)

    creds, _ = google.auth.default(scopes=_SCOPES)
    return _strip_quota_project(creds)


def _strip_quota_project(creds):
    """Remove quota_project_id so stale ADC config doesn't route API calls to the wrong project."""
    try:
        return creds.with_quota_project(None)
    except AttributeError:
        return creds


def is_gcloud_installed() -> bool:
    return shutil.which("gcloud") is not None


def get_authenticated_account() -> str | None:
    """Return the active gcloud account, or None if not logged in."""
    result = subprocess.run(
        ["gcloud", "auth", "list", "--filter=status=ACTIVE", "--format=value(account)"],
        capture_output=True, text=True,
    )
    account = result.stdout.strip()
    return account if account else None


def is_adc_configured() -> bool:
    """Return True if application-default credentials are usable."""
    result = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
    )
    return result.returncode == 0


def get_active_billing_account() -> str | None:
    """Return the first open billing account name (billingAccounts/XXXXX), or None."""
    result = subprocess.run(
        ["gcloud", "billing", "accounts", "list",
         "--filter=open=true", "--format=value(name)", "--limit=1"],
        capture_output=True, text=True,
    )
    account = result.stdout.strip()
    return account if account else None
