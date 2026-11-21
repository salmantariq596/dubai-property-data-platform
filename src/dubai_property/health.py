"""Is the pipeline healthy? One status from the pipeline's own logs. No Spark.

The scheduled job and Data Factory alert when something *fails*. They cannot
alert when nothing runs at all: a disabled trigger, an expired credential, a
paused job. ``assess`` also reports STALE when the last successful end-to-end
run is too old, which is what the daily health check (``09_monitoring``) is for.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from dubai_property import jobs

OK, WARN, FAILED, STALE = "OK", "WARN", "FAILED", "STALE"
_ORDER = {OK: 0, WARN: 1, STALE: 2, FAILED: 3}

# A daily run plus a couple of hours' slack: anything older means a run was missed.
MAX_AGE = timedelta(hours=26)
LAST_STEP = jobs.STEPS[-1]  # the run is complete when its final check has passed


@dataclass(frozen=True)
class StepState:
    status: str
    finished_at: datetime
    error_message: str | None = None


@dataclass(frozen=True)
class Health:
    status: str
    reasons: list[str] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return self.status in (OK, WARN)

    def __str__(self) -> str:
        return "\n".join([f"[{self.status}] pipeline health"] + [f"  - {r}" for r in self.reasons])


def assess(
    latest_by_step: dict[str, StepState],
    dq_statuses: list[str],
    now: datetime,
    max_age: timedelta = MAX_AGE,
) -> Health:
    """Combine the latest state of each job step and the latest data quality results.

    ``latest_by_step`` holds the most recent ``quality.pipeline_runs`` row per step;
    ``dq_statuses`` the statuses of the most recent data quality run of each stage.
    """
    status, reasons = OK, []

    def worse(new_status: str, reason: str) -> None:
        nonlocal status
        if _ORDER[new_status] > _ORDER[status]:
            status = new_status
        reasons.append(reason)

    for step in jobs.STEPS:
        state = latest_by_step.get(step)
        if state and state.status == jobs.FAILED:
            error = (state.error_message or "").splitlines()[0][:200] if state.error_message else ""
            worse(
                FAILED, f"{step} failed at {state.finished_at:%Y-%m-%d %H:%M}: {error}".rstrip(": ")
            )

    last = latest_by_step.get(LAST_STEP)
    if last is None:
        worse(STALE, "no complete run recorded yet")
    elif last.status == jobs.SUCCESS and now - last.finished_at > max_age:
        hours = (now - last.finished_at).total_seconds() / 3600
        worse(
            STALE,
            f"last complete run finished {hours:.0f} hours ago ({last.finished_at:%Y-%m-%d %H:%M})",
        )

    warnings = sum(s == "WARN" for s in dq_statuses)
    if warnings:
        worse(WARN, f"{warnings} data quality check(s) warned in the latest runs")
    if "FAIL" in dq_statuses and status != FAILED:
        worse(FAILED, "a data quality check failed in the latest runs")

    return Health(status, reasons or ["latest run complete, no warnings"])
