from __future__ import annotations

import re
import subprocess
import sys

import requests

HUB_OWNER = "zephschafer"
HUB_REPO = "dcf-hub"
HUB_BRANCH = "main"
_HUB_BASE = f"https://raw.githubusercontent.com/{HUB_OWNER}/{HUB_REPO}/{HUB_BRANCH}/collectors"

_ENV_RE = re.compile(r"\{\{\s*env\.(\w+)\s*\}\}")


def _fetch_url(url: str) -> str | None:
    resp = requests.get(url, timeout=15)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.text


def _install_and_get_yaml(pkg: str) -> str | None:
    """pip install dcf-<pkg>, then extract its bundled collector YAML."""
    subprocess.check_call([sys.executable, "-m", "pip", "install", f"dcf-{pkg}"])

    import importlib
    importlib.invalidate_caches()

    # Strategy 1: entry point group "dcf.collectors", key = pkg
    try:
        from importlib.metadata import entry_points
        ep = next((e for e in entry_points(group="dcf.collectors") if e.name == pkg), None)
        if ep:
            return ep.load()()
    except Exception:
        pass

    # Strategy 2: convention — dcf_<pkg>/collectors/<pkg>.yml
    module_name = f"dcf_{pkg.replace('-', '_')}"
    try:
        import importlib.resources
        return (importlib.resources.files(module_name) / "collectors" / f"{pkg}.yml").read_text()
    except Exception:
        pass

    return None


def resolve(source: str) -> tuple[str, str]:
    """
    Returns (yaml_content, suggested_name).

    Resolution order:
      pypi:<name>  → pip install dcf-<name>, extract bundled YAML
      user/repo    → fetch collector.yml from the GitHub repo root
      <name>       → try dcf-hub, fall back to pip install dcf-<name>
    """
    if source.startswith("pypi:"):
        pkg = source[5:]
        content = _install_and_get_yaml(pkg)
        if content is None:
            raise RuntimeError(
                f"Installed dcf-{pkg} but no collector YAML was found. "
                "The package should register a 'dcf.collectors' entry point "
                "or ship collectors/<name>.yml."
            )
        return content, pkg

    if "/" in source:
        user, repo = source.split("/", 1)
        name = repo.removeprefix("dcf-")
        url = f"https://raw.githubusercontent.com/{user}/{repo}/main/collector.yml"
        content = _fetch_url(url)
        if content is None:
            raise RuntimeError(
                f"No collector.yml found at github.com/{user}/{repo} (branch: main). "
                "Ensure a collector.yml exists at the repo root."
            )
        return content, name

    # Bare name: hub first
    content = _fetch_url(f"{_HUB_BASE}/{source}.yml")
    if content is not None:
        return content, source

    # Hub miss: try PyPI
    try:
        content = _install_and_get_yaml(source)
    except subprocess.CalledProcessError:
        content = None

    if content is not None:
        return content, source

    raise RuntimeError(
        f"'{source}' was not found in dcf-hub and 'dcf-{source}' is not available on PyPI.\n"
        "  For a GitHub source:        dcf import user/repo\n"
        "  For an explicit PyPI pkg:   dcf import pypi:<name>"
    )


def required_env_vars(yaml_content: str) -> list[str]:
    """Return ordered unique env var names referenced as {{ env.VAR }} in the YAML."""
    return list(dict.fromkeys(_ENV_RE.findall(yaml_content)))
