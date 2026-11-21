"""Config loading, source-path resolution and secrets handling.

Plain-Python tests; only the timezone test needs the `spark` fixture.
"""

from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
import yaml

from src.common.config import (
    ConfigError,
    ensure_secrets_file,
    load_config,
    load_secrets,
    resolve_lakehouse_root,
    resolve_source_path,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG = REPO_ROOT / "config" / "config.example.yaml"
RUN_CONFIG = REPO_ROOT / "config" / "config.yaml"


def _write_config(tmp_path: Path, **overrides) -> Path:
    config = {
        "environment": "local",
        "secrets_file": str(tmp_path / "secrets.json"),
        "sources": {
            "leads": {
                "local_path": "data/marketing/leads.csv",
                "azure_path": "abfss://raw@acct.dfs.core.windows.net/marketing/leads.csv",
                "format": "csv",
                "options": {"header": True},
            }
        },
        "lakehouse": {"local_root": "lakehouse", "azure_root": "abfss://lakehouse@acct"},
    }
    config.update(overrides)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return path


def test_example_config_loads_with_all_twelve_sources():
    config = load_config(str(EXAMPLE_CONFIG))
    assert config["environment"] == "local"
    assert len(config["sources"]) == 12


@pytest.mark.parametrize("config_path", [EXAMPLE_CONFIG, RUN_CONFIG], ids=["example", "run"])
def test_calls_ca_us_delimiter_is_comma(config_path):
    # Defect 1: the starter config declared ";" for a comma-delimited file.
    config = load_config(str(config_path))
    assert config["sources"]["calls_ca_us"]["options"]["sep"] == ","


def test_run_config_is_local_only_with_every_path_present():
    config = load_config(str(RUN_CONFIG))
    assert config["environment"] == "local"
    assert len(config["sources"]) == 12
    assert "abfss://" not in RUN_CONFIG.read_text(encoding="utf-8")
    for name in config["sources"]:
        assert (REPO_ROOT / resolve_source_path(config, name)).is_file(), name


def test_run_config_sources_match_example():
    run = load_config(str(RUN_CONFIG))["sources"]
    example = load_config(str(EXAMPLE_CONFIG))["sources"]
    assert run.keys() == example.keys()
    for name in run:
        assert run[name]["local_path"] == example[name]["local_path"], name
        assert run[name]["format"] == example[name]["format"], name
        assert run[name]["options"] == example[name]["options"], name


def test_resolve_source_path_follows_environment(tmp_path):
    local = load_config(str(_write_config(tmp_path)))
    assert resolve_source_path(local, "leads") == "data/marketing/leads.csv"
    assert resolve_lakehouse_root(local) == "lakehouse"

    azure = load_config(str(_write_config(tmp_path, environment="azure")))
    assert resolve_source_path(azure, "leads").startswith("abfss://")
    assert resolve_lakehouse_root(azure).startswith("abfss://")


def test_resolve_source_path_rejects_unknown_source(tmp_path):
    config = load_config(str(_write_config(tmp_path)))
    with pytest.raises(ConfigError, match="Unknown source"):
        resolve_source_path(config, "not_a_source")


def test_load_config_rejects_bad_environment(tmp_path):
    with pytest.raises(ConfigError, match="environment"):
        load_config(str(_write_config(tmp_path, environment="prod")))


def test_load_config_needs_only_the_active_environment_path(tmp_path):
    local_only = {"leads": {"local_path": "data/marketing/leads.csv", "format": "csv"}}
    load_config(str(_write_config(tmp_path, sources=local_only)))  # local: fine
    with pytest.raises(ConfigError, match="azure_path"):
        load_config(str(_write_config(tmp_path, environment="azure", sources=local_only)))


def test_load_config_rejects_source_without_format(tmp_path):
    no_format = {"leads": {"local_path": "data/marketing/leads.csv"}}
    with pytest.raises(ConfigError, match="format"):
        load_config(str(_write_config(tmp_path, sources=no_format)))


def test_load_config_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(str(tmp_path / "nope.yaml"))


def test_load_secrets_reads_local_file(tmp_path):
    config = load_config(str(_write_config(tmp_path)))
    Path(config["secrets_file"]).write_text(
        json.dumps({"_comment": "x", "pii_hash_salt": "test-salt"}), encoding="utf-8"
    )
    assert load_secrets(config) == {"pii_hash_salt": "test-salt"}


def test_load_secrets_refuses_example_file(tmp_path):
    example = tmp_path / "secrets.example.json"
    example.write_text(json.dumps({"pii_hash_salt": "x"}), encoding="utf-8")
    config = load_config(str(_write_config(tmp_path, secrets_file=str(example))))
    with pytest.raises(ConfigError, match="placeholders only"):
        load_secrets(config)


def test_load_secrets_refuses_placeholder_salt(tmp_path):
    config = load_config(str(_write_config(tmp_path)))
    Path(config["secrets_file"]).write_text(
        json.dumps({"pii_hash_salt": "REPLACE_WITH_A_LOCAL_ONLY_SALT_VALUE"}), encoding="utf-8"
    )
    with pytest.raises(ConfigError, match="placeholder"):
        load_secrets(config)


def test_ensure_secrets_file_generates_private_random_salt(tmp_path):
    config = load_config(str(_write_config(tmp_path)))
    assert ensure_secrets_file(config) is True
    path = Path(config["secrets_file"])
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    salt = load_secrets(config)["pii_hash_salt"]
    assert len(salt) == 64
    int(salt, 16)  # hex

    other = tmp_path / "other"
    other.mkdir()
    other_config = load_config(str(_write_config(other)))
    ensure_secrets_file(other_config)
    assert load_secrets(other_config)["pii_hash_salt"] != salt


def test_ensure_secrets_file_never_overwrites(tmp_path):
    config = load_config(str(_write_config(tmp_path)))
    Path(config["secrets_file"]).write_text(
        json.dumps({"pii_hash_salt": "keep-me"}), encoding="utf-8"
    )
    assert ensure_secrets_file(config) is False
    assert load_secrets(config)["pii_hash_salt"] == "keep-me"


def test_spark_session_timezone_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
