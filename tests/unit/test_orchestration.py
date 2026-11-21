"""Orchestration definitions, checked as files: Data Factory pipelines, the Databricks
job (Asset Bundle) and the job steps. No Azure, Databricks or Spark needed."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from dubai_property import jobs

ROOT = Path(__file__).parents[2]
PIPELINE_DIR = ROOT / "adf" / "pipeline"
PIPELINES = {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in PIPELINE_DIR.glob("*.json")}
TERRAFORM = (ROOT / "infra" / "orchestration" / "main.tf").read_text(encoding="utf-8")
BUNDLE = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
JOB = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text(encoding="utf-8"))["resources"][
    "jobs"
]["daily_pipeline"]


def activities(container: list[dict]):
    """Every activity, including those nested in ForEach, Until and IfCondition."""
    for activity in container:
        yield activity
        props = activity.get("typeProperties", {})
        for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            yield from activities(props.get(key, []))


def expressions(node) -> list[str]:
    if isinstance(node, dict):
        return [e for v in node.values() for e in expressions(v)]
    if isinstance(node, list):
        return [e for v in node for e in expressions(v)]
    return [node] if isinstance(node, str) and "@" in node else []


# --- Data Factory -------------------------------------------------------------------------


def test_the_planned_pipelines_exist():
    assert set(PIPELINES) == {
        "pl_ingest_dataset",
        "pl_land_day",
        "pl_run_job",
        "pl_alert",
        "pl_daily",
        "pl_backfill",
    }


@pytest.mark.parametrize("name", sorted(PIPELINES))
def test_file_name_matches_pipeline_name(name):
    assert PIPELINES[name]["name"] == name


@pytest.mark.parametrize("name", sorted(PIPELINES))
def test_parameters_are_strings_with_defaults(name):
    """Terraform's pipeline resource only deploys string parameters."""
    for parameter in PIPELINES[name]["properties"]["parameters"].values():
        assert parameter["type"] == "String"
        assert "defaultValue" in parameter


@pytest.mark.parametrize("name", sorted(PIPELINES))
def test_references_resolve(name):
    props = PIPELINES[name]["properties"]
    every = list(activities(props["activities"]))
    names = {a["name"] for a in every}
    params = set(props["parameters"])
    variables = set(props.get("variables", {}))

    assert len(names) == len(every), "activity names must be unique"
    for activity in every:
        for dependency in activity.get("dependsOn", []):
            assert dependency["activity"] in names
        if activity["type"] == "SetVariable":
            assert activity["typeProperties"]["variableName"] in variables

    for text in expressions(props["activities"]):
        for used in re.findall(r"pipeline\(\)\.parameters\.(\w+)", text):
            assert used in params, f"{name}: unknown parameter {used}"
        for used in re.findall(r"variables\('(\w+)'\)", text):
            assert used in variables, f"{name}: unknown variable {used}"
        for used in re.findall(r"activity\('([^']+)'\)", text):
            assert used in names, f"{name}: unknown activity {used}"


@pytest.mark.parametrize("name", sorted(PIPELINES))
def test_called_pipelines_exist_and_get_known_parameters(name):
    for activity in activities(PIPELINES[name]["properties"]["activities"]):
        if activity["type"] != "ExecutePipeline":
            continue
        callee = activity["typeProperties"]["pipeline"]["referenceName"]
        assert callee in PIPELINES
        passed = set(activity["typeProperties"]["parameters"])
        assert passed <= set(PIPELINES[callee]["properties"]["parameters"])


def test_datasets_used_are_deployed():
    used = {
        ref["referenceName"]
        for p in PIPELINES.values()
        for a in activities(p["properties"]["activities"])
        for ref in a.get("inputs", []) + a.get("outputs", [])
    }
    assert used == {"ds_source_file", "ds_landing_file"}
    for dataset in used:
        assert f'name            = "{dataset}"' in TERRAFORM


def test_terraform_creates_every_pipeline_after_the_ones_it_calls():
    levels = re.search(r"pipeline_levels = \{(.*?)\n  \}", TERRAFORM, re.S).group(1)
    level_of = {
        p: i
        for i, line in enumerate(re.findall(r"\[(.*?)\]", levels))
        for p in re.findall(r'"(\w+)"', line)
    }
    assert set(level_of) == set(PIPELINES)
    for name, doc in PIPELINES.items():
        for activity in activities(doc["properties"]["activities"]):
            if activity["type"] == "ExecutePipeline":
                callee = activity["typeProperties"]["pipeline"]["referenceName"]
                assert level_of[callee] < level_of[name], f"{name} calls {callee}"


def test_databricks_is_called_with_the_managed_identity():
    calls = [
        a
        for a in activities(PIPELINES["pl_run_job"]["properties"]["activities"])
        if a["type"] == "WebActivity"
    ]
    assert calls
    for call in calls:
        auth = call["typeProperties"]["authentication"]
        assert auth == {"type": "MSI", "resource": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"}


def test_every_failure_path_alerts_and_fails():
    """Landing failures and Databricks failures both go through pl_alert, which ends in Fail."""
    for name in ("pl_daily", "pl_backfill", "pl_run_job"):
        callees = [
            a["typeProperties"]["pipeline"]["referenceName"]
            for a in activities(PIPELINES[name]["properties"]["activities"])
            if a["type"] == "ExecutePipeline"
        ]
        assert "pl_alert" in callees, name
    alert = PIPELINES["pl_alert"]["properties"]["activities"]
    assert alert[-1]["type"] == "Fail"
    assert alert[-1]["dependsOn"][0]["dependencyConditions"] == ["Completed"]


def test_daily_trigger_is_off_until_enabled():
    variables = (ROOT / "infra" / "orchestration" / "variables.tf").read_text(encoding="utf-8")
    assert re.search(r'variable "enable_schedule" \{.*?default\s+= false', variables, re.S)
    assert 'time_zone       = "Arabian Standard Time"' in TERRAFORM


# --- Databricks job (Asset Bundle) ----------------------------------------------------------


def test_job_tasks_run_the_steps_in_order():
    tasks = JOB["tasks"]
    assert [t["task_key"] for t in tasks] == list(jobs.STEPS)
    for previous, task in zip(tasks, tasks[1:], strict=False):
        assert task["depends_on"] == [{"task_key": previous["task_key"]}]
    assert "depends_on" not in tasks[0]


def test_each_task_runs_its_step_in_an_existing_notebook():
    for task in JOB["tasks"]:
        notebook = task["notebook_task"]
        assert notebook["base_parameters"]["step"] == task["task_key"]
        assert (ROOT / "resources" / notebook["notebook_path"]).resolve().exists()


def test_job_parameters_alerts_and_limits():
    params = {p["name"]: p["default"] for p in JOB["parameters"]}
    assert params == {
        "catalog": "${var.catalog}",
        "datasets": "transactions,rents",
        "load_id": "{{job.run_id}}",
    }
    assert JOB["email_notifications"]["on_failure"] == ["${var.alert_email}"]
    assert JOB["max_concurrent_runs"] == 1  # two runs at once would race on the same tables


def test_azure_target_puts_every_task_on_the_job_cluster():
    azure = BUNDLE["targets"]["azure"]["resources"]["jobs"]["daily_pipeline"]
    keys = {t["task_key"]: t["job_cluster_key"] for t in azure["tasks"]}
    assert keys == dict.fromkeys(jobs.STEPS, "pipeline")
    assert azure["job_clusters"][0]["new_cluster"]["num_workers"] == 0  # single node


# --- job steps and their log ----------------------------------------------------------------


def test_quality_stage_of_a_step():
    assert jobs.quality_stage("quality_silver") == "silver"
    assert jobs.quality_stage("silver") is None


def test_step_record_row():
    started = datetime(2026, 11, 21, 2, 0, 0)
    record = jobs.StepRecord(
        "silver", jobs.FAILED, started, datetime(2026, 11, 21, 2, 1, 30), "adf-1", None, "x" * 5000
    )
    row = dict(zip(jobs.COLUMNS, record.as_row(), strict=True))
    assert row["duration_seconds"] == 90.0
    assert len(row["error_message"]) == 4000  # long tracebacks are cut


def test_step_record_rejects_unknown_steps_and_statuses():
    now = datetime(2026, 11, 21)
    with pytest.raises(ValueError, match="step"):
        jobs.StepRecord("platinum", jobs.SUCCESS, now, now)
    with pytest.raises(ValueError, match="status"):
        jobs.StepRecord("gold", "MAYBE", now, now)
