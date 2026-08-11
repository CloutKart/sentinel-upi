"""Shared Spark fixtures.

One session for the whole test module tree: starting a SparkSession costs several
seconds, and per-test sessions would make the suite unusable.
"""

from __future__ import annotations

import pytest

from sentinel.config import load_config
from sentinel.spark import get_spark, stop_spark


@pytest.fixture(scope="session")
def cfg():
    return load_config("local")


@pytest.fixture(scope="session")
def spark(cfg):
    session = get_spark(cfg)
    yield session
    stop_spark()


@pytest.fixture
def isolated_cfg(cfg, tmp_path, monkeypatch):
    """A config whose every zone points inside a fresh temporary directory.

    Tests must never write into ./data — a developer's generated dataset and a test
    run sharing checkpoints would leave both in an unexplainable state.
    """
    for zone in (
        "raw",
        "landing",
        "bronze",
        "silver",
        "gold",
        "quarantine",
        "checkpoints",
        "truth",
    ):
        monkeypatch.setitem(cfg.paths, zone, str(tmp_path / zone))
    return cfg
