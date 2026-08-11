"""Environment-aware configuration.

The portability of this project rests on this module. Transformation code never knows
whether it is running against a local directory or Unity Catalog — it asks for a table
and gets back either ``./data/silver/upi_transactions`` or
``sentinel.silver.upi_transactions``.

Selection is by the ``SENTINEL_ENV`` environment variable (``local`` or
``databricks``), defaulting to ``local`` so nothing accidentally reaches for a
workspace.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_ENV = "local"

# ${...} references inside the YAML, resolved against already-known config values and
# then the process environment.
_VAR = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


class ConfigError(RuntimeError):
    """Raised when configuration is missing or internally inconsistent."""


def _resolve_conf_dir() -> Path:
    """Locate the ``conf`` directory, whichever way the package was installed.

    In priority order:

    1. ``SENTINEL_CONF_DIR`` — an explicit override, for pointing a cluster at
       configuration deployed separately from the wheel.
    2. ``sentinel/conf`` inside the installed package. ``pyproject.toml``
       force-includes the repository's ``conf`` tree there, so a wheel is
       self-contained.
    3. ``<repo>/conf``, two levels above this file — the editable-install layout.

    Case 2 is why this is a function and not a one-line expression: resolving only
    against ``parents[2]`` works in the repository and silently points at
    ``<site-packages>/../../conf`` once installed as a wheel, which is where a cluster
    would look and not find it.
    """
    override = os.environ.get("SENTINEL_CONF_DIR")
    if override:
        candidate = Path(override).expanduser().resolve()
        if not (candidate / "base.yaml").exists():
            raise ConfigError(f"SENTINEL_CONF_DIR={override} does not contain base.yaml")
        return candidate

    here = Path(__file__).resolve()
    for candidate in (here.parent / "conf", here.parents[2] / "conf"):
        if (candidate / "base.yaml").exists():
            return candidate

    raise ConfigError(
        "Cannot locate the conf directory. Looked for a packaged copy at "
        f"{here.parent / 'conf'} and a source checkout at {here.parents[2] / 'conf'}."
    )


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge ``override`` onto ``base``, recursing into nested dicts."""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _interpolate(raw: dict[str, Any]) -> dict[str, Any]:
    """Expand ``${name}`` references in string values.

    Names resolve against the config's own top-level keys first (``root``,
    ``catalog``), then the process environment. An unresolved reference is a hard
    error — an empty string silently pointed at ``/`` would be far worse.
    """

    def lookup(name: str) -> str:
        if name in raw and isinstance(raw[name], (str, int, float)):
            return str(raw[name])
        if name in os.environ:
            return os.environ[name]
        raise ConfigError(
            f"Cannot resolve ${{{name}}} — not a config key or an environment variable"
        )

    def walk(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        if isinstance(node, str):
            return _VAR.sub(lambda m: lookup(m.group(1)), node)
        return node

    return walk(raw)


@dataclass(frozen=True)
class Config:
    """Resolved settings for one environment."""

    env: str
    catalog: str | None
    paths: dict[str, str]
    schemas: dict[str, str]
    generator: dict[str, Any]
    silver: dict[str, Any]
    gold: dict[str, Any]
    landing: dict[str, Any]
    app_name: str
    spark_master: str | None
    spark_conf: dict[str, str] = field(default_factory=dict)

    # ------------------------------------------------------------------ locations

    @property
    def uses_catalog(self) -> bool:
        """True when tables are addressed by name rather than by path."""
        return self.catalog is not None

    def path(self, zone: str) -> str:
        """Return the filesystem location of a zone (``raw``, ``checkpoints``, ...)."""
        try:
            return self.paths[zone]
        except KeyError:
            raise ConfigError(
                f"No path configured for zone '{zone}' in env '{self.env}'. "
                f"Known zones: {sorted(self.paths)}"
            ) from None

    def table(self, zone: str, name: str) -> str:
        """Return the address of a table: a catalog identifier, or a directory path.

        This is the single seam between environments. Everything upstream refers to
        ``cfg.table("silver", "upi_transactions")`` and never learns which it got.
        """
        if self.uses_catalog:
            schema = self.schemas.get(zone, zone)
            return f"{self.catalog}.{schema}.{name}"
        return f"{self.path(zone)}/{name}"

    def checkpoint(self, stage: str) -> str:
        """Return the streaming checkpoint location for a pipeline stage.

        Checkpoints are per-stage and never shared: two streams pointed at one
        checkpoint corrupt each other's offsets.
        """
        return f"{self.path('checkpoints')}/{stage}"


def load_config(env: str | None = None) -> Config:
    """Load and merge ``base.yaml`` with the environment's overrides."""
    env = env or os.environ.get("SENTINEL_ENV", DEFAULT_ENV)
    conf_dir = _resolve_conf_dir()

    env_file = conf_dir / f"{env}.yaml"
    if not env_file.exists():
        available = sorted(p.stem for p in conf_dir.glob("*.yaml") if p.stem != "base")
        raise ConfigError(f"Unknown environment '{env}'. Available: {available}")

    base = yaml.safe_load((conf_dir / "base.yaml").read_text()) or {}
    overrides = yaml.safe_load(env_file.read_text()) or {}
    raw = _interpolate(_deep_merge(base, overrides))

    spark_section = raw.get("spark", {})

    cfg = Config(
        env=raw["env"],
        catalog=raw.get("catalog"),
        paths=raw.get("paths", {}),
        schemas=raw.get("schemas", {}),
        generator=raw.get("generator", {}),
        silver=raw.get("silver", {}),
        gold=raw.get("gold", {}),
        landing=raw.get("landing", {}),
        app_name=spark_section.get("app_name", "sentinel"),
        spark_master=spark_section.get("master"),
        # Spark rejects non-string config values with an unhelpful error several
        # frames deep, so coerce here where the cause is obvious.
        spark_conf={k: str(v) for k, v in (spark_section.get("conf") or {}).items()},
    )

    if not cfg.uses_catalog:
        # A path-addressed environment with no `raw` zone would fail at the first
        # write, minutes in. Fail now instead.
        for required in ("raw", "landing", "bronze", "silver", "gold", "quarantine", "checkpoints"):
            cfg.path(required)

    return cfg
