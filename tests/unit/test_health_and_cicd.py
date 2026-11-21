"""Pipeline health rules, and the CI/CD workflows checked as files."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

import yaml

from dubai_property import health, jobs

ROOT = Path(__file__).parents[2]
NOW = datetime(2026, 11, 21, 9, 0)


def complete_run(finished: datetime) -> dict[str, health.StepState]:
    return {step: health.StepState(jobs.SUCCESS, finished) for step in jobs.STEPS}


# --- health -------------------------------------------------------------------------------


def test_a_recent_complete_run_is_ok():
    result = health.assess(complete_run(NOW - timedelta(hours=7)), ["PASS"] * 20, NOW)
    assert result.status == health.OK
    assert result.healthy


def test_warnings_from_the_latest_checks():
    result = health.assess(complete_run(NOW - timedelta(hours=7)), ["PASS", "WARN", "WARN"], NOW)
    assert result.status == health.WARN
    assert result.healthy  # a warning does not page anyone
    assert "2 data quality check(s) warned" in result.reasons[0]


def test_a_failed_step_is_failed_with_its_error():
    states = complete_run(NOW - timedelta(hours=31))
    states["silver"] = health.StepState(
        jobs.FAILED, NOW - timedelta(hours=7), "MergeError: conflict\ntraceback..."
    )
    result = health.assess(states, [], NOW)
    assert result.status == health.FAILED
    assert not result.healthy
    assert any("silver failed" in r and "MergeError: conflict" in r for r in result.reasons)
    assert not any("traceback" in r for r in result.reasons)


def test_no_run_for_more_than_a_day_is_stale():
    """What failure alerts cannot catch: nothing ran at all."""
    result = health.assess(complete_run(NOW - timedelta(hours=30)), ["PASS"], NOW)
    assert result.status == health.STALE
    assert "30 hours ago" in result.reasons[0]


def test_never_run_is_stale():
    result = health.assess({}, [], NOW)
    assert result.status == health.STALE
    assert result.reasons == ["no complete run recorded yet"]


def test_a_failed_check_in_the_latest_results_is_failed():
    result = health.assess(complete_run(NOW - timedelta(hours=7)), ["PASS", "FAIL"], NOW)
    assert result.status == health.FAILED


def test_a_failure_fixed_by_a_later_run_is_forgotten():
    """Only each step's latest run counts: a re-run that succeeded clears the failure."""
    result = health.assess(complete_run(NOW - timedelta(hours=2)), [], NOW)
    assert result.status == health.OK


def test_a_failed_final_check_is_not_reported_as_never_run():
    states = complete_run(NOW - timedelta(hours=7))
    states["quality_gold"] = health.StepState(jobs.FAILED, NOW - timedelta(hours=7), "x")
    result = health.assess(states, [], NOW)
    assert result.status == health.FAILED
    assert not any("no complete run" in r for r in result.reasons)


# --- workflows ------------------------------------------------------------------------------


def workflow(name: str) -> dict:
    text = (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    data["on"] = data.pop(True, data.get("on"))  # YAML reads the bare key `on` as True
    return data


def test_no_workflow_uses_secrets():
    """Identity comes from OIDC; everything else is a plain repository variable."""
    for path in (ROOT / ".github" / "workflows").glob("*.yml"):
        assert "secrets." not in path.read_text(encoding="utf-8"), path.name


def test_deploy_runs_only_from_main_through_the_production_environment():
    deploy = workflow("deploy.yml")
    assert deploy["on"]["push"]["branches"] == ["main"]
    assert "pull_request" not in deploy["on"]
    job = deploy["jobs"]["bundle"]
    assert job["environment"] == "production"
    assert deploy["permissions"]["id-token"] == "write"
    assert deploy["concurrency"]["cancel-in-progress"] is False  # never stop a half-done deploy
    steps = [s.get("run", "") for s in job["steps"]]
    assert steps.index("databricks bundle validate -t azure") < steps.index(
        "databricks bundle deploy -t azure"
    )


def test_cloud_jobs_stay_off_until_azure_is_configured():
    assert workflow("deploy.yml")["jobs"]["bundle"]["if"].startswith("vars.AZURE_CLIENT_ID != ''")
    validate = workflow("ci.yml")["jobs"]["bundle-validate"]
    assert validate["if"].startswith("vars.AZURE_CLIENT_ID != ''")
    assert "github.event_name == 'pull_request'" in validate["if"]


def test_federated_credentials_match_the_workflows():
    """Entra ID trusts exactly the two token subjects the workflows present."""
    terraform = (ROOT / "infra" / "cicd" / "main.tf").read_text(encoding="utf-8")
    subjects = re.findall(r'subject\s+= "repo:\$\{var.github_repository\}:([^"]+)"', terraform)
    assert sorted(subjects) == ["environment:production", "pull_request"]
    assert "azuread_application_password" not in terraform  # no client secret, ever


def test_health_check_job_alerts_and_starts_paused():
    jobs_yml = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text(encoding="utf-8"))
    check = jobs_yml["resources"]["jobs"]["health_check"]
    assert check["schedule"]["pause_status"] == "PAUSED"
    assert check["email_notifications"]["on_failure"] == ["${var.alert_email}"]
    [task] = check["tasks"]
    assert task["notebook_task"]["base_parameters"]["fail_on_unhealthy"] == "true"
    assert (ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve().exists()
