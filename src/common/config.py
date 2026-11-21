"""Load and resolve config/config.yaml.

Ingestion is parameterised by a config file, not hardcoded paths. One
`sources` block per source file. Each source's local or Azure path is resolved
from the single `environment: local | azure` switch. Secret values such as the
PII hash salt are loaded from `config/secrets.json` (never
`config/secrets.example.json`); no secret literal is ever hardcoded in code.
If the secrets file is missing, a random salt is generated and written to it on
first run, so a clean checkout needs no setup.
"""

from __future__ import annotations

import json
import os
import secrets as secrets_lib
from pathlib import Path

import yaml

VALID_ENVIRONMENTS = ("local", "azure")
PATH_KEY_BY_ENVIRONMENT = {"local": "local_path", "azure": "azure_path"}
ROOT_KEY_BY_ENVIRONMENT = {"local": "local_root", "azure": "azure_root"}
EXAMPLE_SECRETS_NAME = "secrets.example.json"
PLACEHOLDER_PREFIX = "REPLACE_"


class ConfigError(ValueError):
    """Raised when the config file is missing, malformed or inconsistent."""


def load_config(config_path: str) -> dict:
    """Read and validate the pipeline config.

    Checks that `environment` is `local` or `azure`, that `sources` is a
    non-empty mapping, and that every source declares `format` and the path
    for the active environment (`local_path` or `azure_path`). The other
    environment's path is optional, so the committed local config carries no
    cloud paths. Returns the parsed dict unchanged otherwise.
    """
    path = Path(config_path)
    if not path.is_file():
        raise ConfigError(
            f"Config file not found: {config_path}. "
            "Copy config/config.example.yaml to config/config.yaml."
        )

    with path.open(encoding="utf-8") as fh:
        config = yaml.safe_load(fh)

    if not isinstance(config, dict):
        raise ConfigError(f"Config file {config_path} must contain a YAML mapping.")

    environment = config.get("environment")
    if environment not in VALID_ENVIRONMENTS:
        raise ConfigError(f"environment must be one of {VALID_ENVIRONMENTS}, got {environment!r}.")

    sources = config.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise ConfigError("Config must define a non-empty `sources` mapping.")

    for name, source in sources.items():
        if not isinstance(source, dict):
            raise ConfigError(f"Source {name!r} must be a mapping.")
        required = ("format", PATH_KEY_BY_ENVIRONMENT[environment])
        missing = [key for key in required if not source.get(key)]
        if missing:
            raise ConfigError(f"Source {name!r} is missing required keys: {missing}.")

    return config


def resolve_source_path(config: dict, source_name: str) -> str:
    """Return the local or Azure path for a source, per `environment`."""
    sources = config.get("sources", {})
    if source_name not in sources:
        raise ConfigError(f"Unknown source {source_name!r}. Known sources: {sorted(sources)}.")
    return sources[source_name][PATH_KEY_BY_ENVIRONMENT[config["environment"]]]


def resolve_lakehouse_root(config: dict) -> str:
    """Return the lakehouse root for the current environment."""
    lakehouse = config.get("lakehouse", {})
    key = ROOT_KEY_BY_ENVIRONMENT[config["environment"]]
    root = lakehouse.get(key)
    if not root:
        raise ConfigError(f"Config is missing lakehouse.{key}.")
    return root


def _secrets_path(config: dict) -> Path:
    secrets_file = config.get("secrets_file")
    if not secrets_file:
        raise ConfigError("Config is missing `secrets_file`.")
    path = Path(secrets_file)
    if path.name == EXAMPLE_SECRETS_NAME:
        raise ConfigError(
            f"secrets_file points at {EXAMPLE_SECRETS_NAME}, which holds placeholders only. "
            "Point it at config/secrets.json (gitignored)."
        )
    return path


def ensure_secrets_file(config: dict) -> bool:
    """Create the secrets file with a random PII hash salt if it does not exist.

    The file is written owner-read/write only and is gitignored. An existing
    file is never touched, because changing the salt changes every hashed key.
    Returns True if a new file was created.
    """
    path = _secrets_path(config)
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = {
        "_comment": "Generated on first run. Local only, never commit. "
        "Changing pii_hash_salt changes every hashed key in gold.",
        "pii_hash_salt": secrets_lib.token_hex(32),
    }
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(content, fh, indent=2)
        fh.write("\n")
    return True


def load_secrets(config: dict) -> dict:
    """Load the secrets file named by `secrets_file`.

    Refuses the committed example file and a `pii_hash_salt` still set to a
    `REPLACE_...` placeholder, so a run can never hash PII with a public salt.
    Call `ensure_secrets_file` first to create the file on a clean checkout.
    """
    path = _secrets_path(config)
    secrets_file = str(path)
    if not path.is_file():
        raise ConfigError(
            f"Secrets file not found: {secrets_file}. "
            "Run the pipeline once to generate it, or create it with a pii_hash_salt."
        )

    with path.open(encoding="utf-8") as fh:
        secrets = json.load(fh)

    placeholders = sorted(
        key
        for key, value in secrets.items()
        if isinstance(value, str) and value.startswith(PLACEHOLDER_PREFIX)
    )
    if "pii_hash_salt" in placeholders:
        raise ConfigError(
            f"pii_hash_salt in {secrets_file} is still a placeholder; set a local salt."
        )

    return {key: value for key, value in secrets.items() if not key.startswith("_")}
