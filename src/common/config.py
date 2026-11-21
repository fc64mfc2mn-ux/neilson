"""Load and resolve config/config.yaml.

Ingestion is parameterised by a config file, not hardcoded paths. One
`sources` block per source file. Must resolve each source's local or Azure
path from the single `environment: local | azure` switch, and must load
`config/secrets.json` (never `config/secrets.example.json`) for secret values
such as the PII hash salt, without ever hardcoding a secret literal in code.
"""

from __future__ import annotations


def load_config(config_path: str) -> dict:
    raise NotImplementedError


def resolve_source_path(config: dict, source_name: str) -> str:
    raise NotImplementedError


def load_secrets(config: dict) -> dict:
    raise NotImplementedError
