import shutil
import subprocess
import logging

import google.auth
from google.auth.exceptions import DefaultCredentialsError

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
_INSTALL_URL = "https://cloud.google.com/sdk/docs/install"


def get_credentials():
    """
    Return ADC credentials scoped to cloud-platform.

    Resolution order:
      1. Existing ADC (GOOGLE_APPLICATION_CREDENTIALS env var or gcloud ADC file)
      2. If not configured but gcloud is installed, run `gcloud auth application-default login`
         (opens a browser on the local machine) then retry.
      3. If gcloud is not installed, raise RuntimeError with install instructions.
    """
    try:
        creds, _ = google.auth.default(scopes=_SCOPES)
        return creds
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
    return creds
