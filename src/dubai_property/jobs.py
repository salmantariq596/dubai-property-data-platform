"""The scheduled job as data: its steps, in order, and the log every step writes.

No Spark here, so the step order can be checked against the Databricks job
definition (``resources/jobs.yml``) without a cluster.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

# One Databricks job task per step, run in this order. A quality step that
# FAILs stops the job, so the next layer never sees bad data.
STEPS = (
    "bronze",
    "quality_bronze",
    "silver",
    "quality_silver",
    "gold",
    "quality_gold",
)

SUCCESS = "SUCCESS"
FAILED = "FAILED"

PIPELINE_RUNS_DDL = """
    load_id STRING,
    step STRING NOT NULL,
    status STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    duration_seconds DOUBLE,
    summary STRING,
    error_message STRING
"""
COLUMNS = [line.split()[0] for line in PIPELINE_RUNS_DDL.strip().splitlines()]

_MAX_TEXT = 4000


def quality_stage(step: str) -> str | None:
    """``"quality_silver"`` -> ``"silver"``; None for a layer step."""
    return step.removeprefix("quality_") if step.startswith("quality_") else None


@dataclass(frozen=True)
class StepRecord:
    """One row of ``quality.pipeline_runs``: what a step did, or how it failed."""

    step: str
    status: str
    started_at: datetime
    finished_at: datetime
    load_id: str | None = None
    summary: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.step not in STEPS:
            raise ValueError(f"Unknown step {self.step!r}. Steps: {STEPS}")
        if self.status not in (SUCCESS, FAILED):
            raise ValueError(f"Unknown status {self.status!r}")

    @property
    def duration_seconds(self) -> float:
        return round((self.finished_at - self.started_at).total_seconds(), 3)

    def as_row(self) -> tuple:
        values = {
            "load_id": self.load_id,
            "step": self.step,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "summary": self.summary[:_MAX_TEXT] if self.summary else None,
            "error_message": self.error_message[:_MAX_TEXT] if self.error_message else None,
        }
        return tuple(values[c] for c in COLUMNS)
