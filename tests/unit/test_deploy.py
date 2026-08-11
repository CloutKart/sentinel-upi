"""The shell scripts, and the templates they render, kept in step.

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
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
TEMPLATE = REPO / "databricks" / "job_sentinel_pipeline.json"
SCRIPT = REPO / "databricks" / "deploy.sh"
RUN_SCRIPT = REPO / "run.sh"
RUN_PS1 = REPO / "run.ps1"
DEPLOY_PS1 = REPO / "databricks" / "deploy.ps1"
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


@pytest.mark.parametrize("script", [SCRIPT, RUN_SCRIPT], ids=["deploy.sh", "run.sh"])
def test_the_scripts_are_executable(script):
    """Committed without the bit set, the documented `./run.sh` fails with Permission denied.

    git does track the bit, and it is easy to lose by copying a file into place.
    """
    assert script.stat().st_mode & 0o111, f"{script.name} is not executable"


@pytest.mark.parametrize("script", [SCRIPT, RUN_SCRIPT], ids=["deploy.sh", "run.sh"])
def test_the_scripts_parse(script):
    """`bash -n` on both, so a syntax error cannot reach a user who just cloned this."""
    result = subprocess.run(
        ["bash", "-n", str(script)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("script", [SCRIPT, RUN_SCRIPT], ids=["deploy.sh", "run.sh"])
def test_help_needs_no_environment(script):
    """--help must work before anything is installed.

    It is the first thing anyone runs, and it is the wrong moment to discover that the
    script dies on a missing venv while trying to explain itself.
    """
    result = subprocess.run(
        [str(script), "--help"], capture_output=True, text=True, check=False, cwd=REPO
    )
    assert result.returncode == 0, result.stderr
    assert "Options:" in result.stdout
    # The header is printed by walking the leading comment block; a stray line means
    # that walk has run past the end of it.
    assert "set -euo pipefail" not in result.stdout


@pytest.mark.parametrize("script", [SCRIPT, RUN_SCRIPT], ids=["deploy.sh", "run.sh"])
def test_unknown_options_fail_loudly(script):
    """Silently ignoring a typo'd flag means running something other than was asked."""
    result = subprocess.run(
        [str(script), "--not-a-real-flag"], capture_output=True, text=True, check=False, cwd=REPO
    )
    assert result.returncode != 0
    assert "Unknown option" in result.stderr


def test_powershell_deploy_declares_the_same_schemas_and_volumes():
    """The two deploy scripts must create the same Unity Catalog objects.

    They are separate implementations, so nothing but a test keeps them in step — and
    a volume created on Linux but not on Windows fails only on the first write to it,
    partway through a run.
    """
    ps = DEPLOY_PS1.read_text()
    sh = SCRIPT.read_text()

    for name in ("SCHEMAS", "VOLUMES"):
        bash_list = set(re.search(rf"^{name}=\(([^)]*)\)", sh, re.MULTILINE).group(1).split())
        pwsh_list = set(
            re.findall(
                r"'([a-z_]+)'",
                re.search(rf"\${name.capitalize()} = @\(([^)]*)\)", ps).group(1),
            )
        )
        assert bash_list == pwsh_list, f"{name} differs: bash {bash_list} vs powershell {pwsh_list}"


def test_powershell_run_refuses_to_run_on_the_wrong_platform():
    """It installs a *Windows* JDK into JAVA_HOME.

    On Linux that replaces a working JDK with binaries the platform cannot execute —
    which happened during development, to the machine this was written on.
    """
    assert "-not $IsWindows" in RUN_PS1.read_text()


def test_powershell_scripts_use_forward_slashes_in_paths():
    """Windows accepts forward slashes, and they keep the scripts runnable under
    PowerShell on Linux — which is the only way the job rendering gets tested at all."""
    for script in (RUN_PS1, DEPLOY_PS1):
        for line in script.read_text().splitlines():
            if line.lstrip().startswith("#") or ".EXAMPLE" in line:
                continue
            # Backslashes are legitimate inside PowerShell escapes and regexes; only
            # quoted repo-relative paths are being checked here.
            for match in re.findall(r"'([A-Za-z0-9_./\\*-]+)'", line):
                if "\\" in match and "/" not in match and not match.startswith("\\"):
                    raise AssertionError(
                        f"{script.name}: backslash path {match!r} in: {line.strip()}"
                    )


def test_run_script_checks_every_tool_it_shells_out_to():
    """A tool used but not checked produces the wrong error message.

    `make setup failed — run it directly to see why` is actively misleading when the
    real cause is that `make` is not installed: running it directly fails identically,
    and the cause is never named.
    """
    text = RUN_SCRIPT.read_text()
    for tool in ("uv", "npm", "make", "curl", "tar"):
        assert f"have {tool}" in text, f"run.sh uses {tool} without checking for it"


def test_run_script_checks_prerequisites_before_installing_anything():
    """Order matters: a machine without node should not spend minutes building a
    Python environment and a JDK before being told the dashboard cannot start."""
    text = RUN_SCRIPT.read_text()
    checks_end = text.index("Install the above, then re-run.")
    first_install = min(
        text.index("make setup"),
        text.index("make jdk"),
        text.index("npm install"),
    )
    assert checks_end < first_install, "run.sh installs something before checking for tools"


def test_run_script_documents_every_option_it_accepts():
    """A flag in the case statement but not in --help is a flag nobody will find."""
    text = RUN_SCRIPT.read_text()
    case_block = text.split("while [[ $# -gt 0 ]]", 1)[1].split("done", 1)[0]
    accepted = set(re.findall(r"^\s+(--[a-z-]+)\)", case_block, re.MULTILINE))
    documented = set(re.findall(r"^\s+(--[a-z-]+)\s", text, re.MULTILINE))

    undocumented = accepted - documented
    assert not undocumented, f"run.sh accepts undocumented flags: {sorted(undocumented)}"


def test_the_script_declares_its_unverified_status(script_text):
    """The header must keep saying this until someone has actually run it on a cluster.

    Removing the warning is a claim that it works, and that claim should be made
    deliberately rather than by tidying a comment.
    """
    assert "NOT YET RUN AGAINST A LIVE WORKSPACE" in script_text
