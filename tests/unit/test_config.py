"""Configuration is the seam that makes one codebase run in two places.

If ``cfg.table()`` returns the wrong shape of address, every stage writes to the wrong
place — and on a cluster that failure appears minutes into a run, as a permissions
error with no obvious cause. These tests are cheap insurance against that.
"""

from __future__ import annotations

import pytest

from sentinel.config import ConfigError, load_config


def test_local_is_the_default(monkeypatch):
    """Nothing should reach for a workspace unless explicitly told to."""
    monkeypatch.delenv("SENTINEL_ENV", raising=False)
    assert load_config().env == "local"


def test_unknown_environment_names_the_available_ones():
    with pytest.raises(ConfigError, match="databricks"):
        load_config("azure")


def test_local_addresses_tables_by_path():
    cfg = load_config("local")
    assert cfg.uses_catalog is False
    assert cfg.table("silver", "upi_transactions") == "./data/silver/upi_transactions"


def test_databricks_addresses_tables_by_catalog_name():
    cfg = load_config("databricks")
    assert cfg.uses_catalog is True
    assert cfg.table("silver", "upi_transactions") == "sentinel.silver.upi_transactions"


def test_interpolation_expands_config_references():
    """``${catalog}`` inside a Volume path must resolve, not survive literally."""
    cfg = load_config("databricks")
    assert cfg.path("raw") == "/Volumes/sentinel/raw/upi_drop"
    assert "${" not in cfg.path("checkpoints")


def test_environment_overrides_are_merged_not_replaced():
    """base.yaml supplies the rules; the env file supplies only the locations."""
    cfg = load_config("databricks")
    assert cfg.gold["fraud_rules"]["high_amount"]["weight"] == 30
    assert cfg.generator["transactions"] > 0


def test_checkpoints_are_per_stage():
    """Two streams sharing a checkpoint corrupt each other's offsets."""
    cfg = load_config("local")
    assert cfg.checkpoint("bronze") != cfg.checkpoint("silver")


def test_unknown_zone_is_an_error_that_names_the_known_ones():
    cfg = load_config("local")
    with pytest.raises(ConfigError, match="platinum"):
        cfg.path("platinum")


def test_bank_names_survive_yaml_type_coercion():
    """YAML 1.1 reads a bare YES as the boolean true. Yes Bank is not a boolean."""
    cfg = load_config("local")
    assert "YES" in cfg.generator["banks"]
    assert all(isinstance(bank, str) for bank in cfg.generator["banks"])


def test_spark_conf_values_are_strings():
    """Spark rejects non-string config values several frames deep in the builder."""
    cfg = load_config("local")
    assert all(isinstance(v, str) for v in cfg.spark_conf.values())
