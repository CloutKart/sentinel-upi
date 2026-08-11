"""The deployment template and the script that renders it, kept in step.

None of this can be tested against a real workspace from here — there are no
credentials. What *can* be tested is everything that would otherwise fail at deploy
time for a stupid reason: a placeholder nobody substitutes, a volume the script never
creates, a notebook the job references but the repo does not have.

Those are exactly the failures that are most expensive to find on a cluster, because
each one costs a full deploy cycle to discover.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
TEMPLATE = REPO / "databricks" / "job_sentinel_pipeline.json"
SCRIPT = REPO / "databricks" / "deploy.sh"
DATABRICKS_CONF = REPO / "conf" / "databricks.yaml"

PLACEHOLDER = re.compile(r"\$\{([A-Z_]+)\}")

# Placeholders deploy.sh substitutes as JSON strings. NUM_WORKERS is handled apart
# from these because it renders as a JSON number.
SUBSTITUTED_NAMES = (
    "JOB_NAME",
    "SPARK_VERSION",
    "NODE_TYPE",
    "NOTEBOOK_DIR",
    "WHEEL_PATH",
    "SCALE",
)


@pytest.fixture(scope="module")
def template_text() -> str:
    return TEMPLATE.read_text()


@pytest.fixture(scope="module")
def script_text() -> str:
    return SCRIPT.read_text()


def test_every_template_placeholder_is_substituted_by_the_script(template_text, script_text):
    """A placeholder the renderer does not know about ships literally to the API.

    Databricks then rejects `${NODE_TYPE}` as a node type id, or worse accepts it as a
    job name, and the cause is a string in a file nobody was looking at.
    """
    in_template = set(PLACEHOLDER.findall(template_text))
    # The renderer's substitution list, as it appears in deploy.sh.
    names = "JOB_NAME|SPARK_VERSION|NODE_TYPE|NOTEBOOK_DIR|WHEEL_PATH|SCALE"
    substituted = {m.strip('"') for m in re.findall(rf'"(?:{names})"', script_text)}
    substituted.add("NUM_WORKERS")  # substituted separately, as a JSON number

    missing = in_template - substituted
    assert not missing, f"template placeholders no one substitutes: {sorted(missing)}"


def test_template_renders_to_valid_json(template_text):
    """The template is not valid JSON until rendered; it must be valid afterwards."""
    rendered = template_text
    for name in SUBSTITUTED_NAMES:
        rendered = rendered.replace(f"${{{name}}}", "x")
    rendered = rendered.replace("${NUM_WORKERS}", "2")

    job = json.loads(rendered)
    assert job["tasks"], "no tasks in the job definition"


def test_the_job_runs_the_pipeline_in_order(template_text):
    """generate -> landing -> bronze -> silver -> gold, each depending on the last.

    Silver reading a Bronze table that this run has not written yet is not an error —
    it just silently processes nothing, and the run goes green with no new rows.
    """
    rendered = PLACEHOLDER.sub("x", template_text).replace('"num_workers": x', '"num_workers": 2')
    tasks = json.loads(rendered)["tasks"]

    order = [t["task_key"] for t in tasks]
    assert order == ["generate", "landing", "bronze", "silver", "gold"]

    # strict=False on purpose: the lists differ in length by one, since each task is
    # paired with the task *before* it.
    for previous, task in zip(order, tasks[1:], strict=False):
        depends = [d["task_key"] for d in task["depends_on"]]
        assert depends == [previous], f"{task['task_key']} should depend on {previous}"


def test_every_task_references_a_notebook_that_exists(template_text):
    rendered = PLACEHOLDER.sub("x", template_text).replace('"num_workers": x', '"num_workers": 2')
    for task in json.loads(rendered)["tasks"]:
        # The path is rendered to "x/<name>", so only the basename is meaningful here.
        name = task["notebook_task"]["notebook_path"].rsplit("/", 1)[-1]
        assert (REPO / "notebooks" / f"{name}.py").exists(), f"missing notebook {name}.py"


def test_every_task_installs_the_wheel(template_text):
    """A task without the library runs a notebook that cannot import sentinel."""
    rendered = PLACEHOLDER.sub("x", template_text).replace('"num_workers": x', '"num_workers": 2')
    for task in json.loads(rendered)["tasks"]:
        assert task["libraries"], f"{task['task_key']} installs no library"


def test_the_script_creates_every_volume_the_config_uses(script_text):
    """conf/databricks.yaml names volumes; deploy.sh creates them. They must agree.

    A missing volume does not fail at deploy — it fails on the first write to it,
    partway through a run, as a path error.

    Loaded through ``load_config`` rather than ``yaml.safe_load``: the raw file holds
    ``/Volumes/${catalog}/...``, so matching on a resolved prefix against the raw text
    finds nothing and the test passes without checking anything. It did exactly that
    until a deliberately broken volume name failed to fail it.
    """
    from sentinel.config import load_config

    cfg = load_config("databricks")

    # Every /Volumes/<catalog>/<schema>/<volume> prefix the config refers to.
    referenced = set()
    for value in list(cfg.paths.values()) + list(cfg.landing.get("cloud_files", {}).values()):
        if isinstance(value, str) and value.startswith(f"/Volumes/{cfg.catalog}/"):
            parts = value.split("/")
            if len(parts) >= 5:
                referenced.add((parts[3], parts[4]))

    assert referenced, "no volume paths found in the config — this test is not checking anything"

    declared = re.search(r"^VOLUMES=\(([^)]*)\)", script_text, re.MULTILINE)
    assert declared, "deploy.sh no longer declares a VOLUMES list"
    created = {("raw", name) for name in declared.group(1).split()}

    # The wheel volume is referenced by the job template, not by the config.
    created_names = {name for _, name in created}
    assert "libs" in created_names, "deploy.sh must create the volume the wheel is uploaded to"

    missing = referenced - created
    assert not missing, f"config uses volumes deploy.sh never creates: {sorted(missing)}"


def test_the_script_creates_every_schema_the_config_uses(script_text):
    conf = yaml.safe_load(DATABRICKS_CONF.read_text())
    referenced = set(conf["schemas"].values())

    declared = re.search(r"^SCHEMAS=\(([^)]*)\)", script_text, re.MULTILINE)
    assert declared, "deploy.sh no longer declares a SCHEMAS list"
    created = set(declared.group(1).split())

    missing = referenced - created
    assert not missing, f"config uses schemas deploy.sh never creates: {sorted(missing)}"


def test_the_script_is_executable():
    """Committed without the bit set, the documented `./databricks/deploy.sh` fails."""
    assert SCRIPT.stat().st_mode & 0o111, "deploy.sh is not executable"


def test_the_script_declares_its_unverified_status(script_text):
    """The header must keep saying this until someone has actually run it on a cluster.

    Removing the warning is a claim that it works, and that claim should be made
    deliberately rather than by tidying a comment.
    """
    assert "NOT YET RUN AGAINST A LIVE WORKSPACE" in script_text
