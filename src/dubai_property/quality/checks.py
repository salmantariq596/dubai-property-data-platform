"""Data quality checks as plain data. No Spark.

Every check is one SQL query returning two numbers, ``rows_checked`` and
``rows_failed``, plus two thresholds on the failed share:

* above ``warn_above`` % -> WARN: recorded, the pipeline carries on;
* above ``fail_above`` % -> FAIL: recorded, and the run stops before the next
  layer, so bad data never reaches the dashboard.

``None`` disables a threshold. A threshold of ``0`` means "any failed row".
Row-level problems were already handled in silver, where a bad row is
quarantined. These checks look at whole tables and whole loads.

Table names in the SQL are placeholders, ``{silver_transactions}``, filled
from :meth:`dubai_property.config.QualitySettings.tables`.
"""

from __future__ import annotations

import string
from dataclasses import dataclass

STAGES = ("bronze", "silver", "gold")

PASS, WARN, FAIL, SKIPPED = "PASS", "WARN", "FAIL", "SKIPPED"
STATUSES = (PASS, WARN, FAIL, SKIPPED)
_SEVERITY = {SKIPPED: 0, PASS: 0, WARN: 1, FAIL: 2}

# Plan, Phase 4: a load whose row count moves more than this against the previous one
MAX_ROW_COUNT_CHANGE = 0.30


@dataclass(frozen=True)
class Check:
    name: str
    stage: str
    table: str  # placeholder of the table the check is about, e.g. "silver_transactions"
    sql: str  # returns one row: rows_checked, rows_failed
    description: str
    warn_above: float | None = None
    fail_above: float | None = None

    def __post_init__(self) -> None:
        if self.stage not in STAGES:
            raise ValueError(f"{self.name}: unknown stage {self.stage!r}")
        if self.warn_above is None and self.fail_above is None:
            raise ValueError(f"{self.name}: a check needs a warn or a fail threshold")

    @property
    def placeholders(self) -> set[str]:
        """Every table the check reads. A check is skipped if one does not exist."""
        return {f for _, f, _, _ in string.Formatter().parse(self.sql) if f}

    def render(self, tables: dict[str, str]) -> str:
        return self.sql.format(**tables)


def failed_pct(rows_checked: int, rows_failed: int) -> float:
    return round(100.0 * rows_failed / rows_checked, 4) if rows_checked else 0.0


def classify(check: Check, rows_checked: int, rows_failed: int) -> str:
    pct = failed_pct(rows_checked, rows_failed)
    if rows_failed and check.fail_above is not None and pct > check.fail_above:
        return FAIL
    if rows_failed and check.warn_above is not None and pct > check.warn_above:
        return WARN
    return PASS


def worst(statuses: list[str]) -> str:
    return max(statuses, key=_SEVERITY.__getitem__, default=PASS)


# --- check builders -------------------------------------------------------------------


def row_rule(
    name: str,
    stage: str,
    table: str,
    condition: str,
    description: str,
    where: str | None = None,
    **thresholds: float | None,
) -> Check:
    """Every row (matching ``where``) must satisfy ``condition``; null counts as failed."""
    filter_ = f" WHERE {where}" if where else ""
    sql = (
        "SELECT count(*) AS rows_checked, "
        f"count_if(NOT coalesce({condition}, false)) AS rows_failed FROM {{{table}}}{filter_}"
    )
    return Check(name, stage, table, sql, description, **thresholds)


def unique(name: str, stage: str, table: str, columns: list[str], description: str) -> Check:
    """Rows beyond the first per key count as failed (null keys group together)."""
    keys = ", ".join(columns)
    sql = (
        "SELECT coalesce(sum(n), 0) AS rows_checked, coalesce(sum(n - 1), 0) AS rows_failed "
        f"FROM (SELECT count(*) AS n FROM {{{table}}} GROUP BY {keys})"
    )
    return Check(name, stage, table, sql, description, fail_above=0)


def _sales(table: str) -> str:
    return f"SELECT count(*) AS n FROM {{{table}}} WHERE lower(trim(trans_group_en)) = 'sales'"


# --- the catalogue ----------------------------------------------------------------------------


def _bronze_checks(dataset: str) -> list[Check]:
    loads = (
        "SELECT load_id, sum(rows_read) AS row_total, max(finished_at) AS loaded_at "
        "FROM {bronze_load_log} "
        f"WHERE dataset = '{dataset}' AND status = 'SUCCESS' GROUP BY load_id"
    )
    ranked = f"SELECT row_total, row_number() OVER (ORDER BY loaded_at DESC) AS n FROM ({loads})"
    change = (
        f"abs(latest.row_total - previous.row_total) > {MAX_ROW_COUNT_CHANGE} * previous.row_total"
    )
    count_change = (
        f"WITH ranked AS ({ranked}) "
        "SELECT count(*) AS rows_checked, "
        f"count_if({change}) AS rows_failed "
        "FROM (SELECT row_total FROM ranked WHERE n = 1) latest "
        "CROSS JOIN (SELECT row_total FROM ranked WHERE n = 2) previous"
    )
    last_load = (
        "SELECT count(*) AS rows_checked, "
        "count_if(status IN ('FAILED', 'MISMATCH')) AS rows_failed FROM {bronze_load_log} "
        f"WHERE dataset = '{dataset}' AND load_id = (SELECT max_by(load_id, finished_at) "
        f"FROM {{bronze_load_log}} WHERE dataset = '{dataset}')"
    )
    return [
        Check(
            f"bronze_{dataset}_row_count_change",
            "bronze",
            "bronze_load_log",
            count_change,
            f"Latest {dataset} load is within 30% of the previous load's row count",
            warn_above=0,
        ),
        Check(
            f"bronze_{dataset}_last_load_ok",
            "bronze",
            "bronze_load_log",
            last_load,
            f"Latest {dataset} load finished without FAILED or MISMATCH files",
            fail_above=0,
        ),
    ]


def _silver_common(dataset: str, key: str) -> list[Check]:
    table = f"silver_{dataset}"
    latest_run = (
        "SELECT coalesce(sum(rows_read), 0) AS rows_checked, "
        "coalesce(sum(rows_quarantined), 0) AS rows_failed FROM {silver_processed_loads} "
        f"WHERE dataset = '{dataset}' AND processed_at = (SELECT max(processed_at) "
        f"FROM {{silver_processed_loads}} WHERE dataset = '{dataset}')"
    )
    return [
        row_rule(
            f"{table}_key_not_null",
            "silver",
            table,
            f"{key} IS NOT NULL",
            f"Every {dataset} row has a {key}",
            fail_above=0,
        ),
        unique(f"{table}_key_unique", "silver", table, [key], f"{key} is unique in silver"),
        row_rule(
            f"{table}_area_mapped",
            "silver",
            table,
            "is_area_mapped",
            f"{dataset.capitalize()} rows are mapped to an area",
            warn_above=1,
            fail_above=20,
        ),
        Check(
            f"{table}_quarantine_rate",
            "silver",
            "silver_processed_loads",
            latest_run,
            f"Share of rows quarantined in the latest {dataset} silver run",
            warn_above=1,
            fail_above=20,
        ),
    ]


def _silver_checks() -> list[Check]:
    return [
        *_silver_common("transactions", "transaction_id"),
        row_rule(
            "silver_transactions_date_in_calendar",
            "silver",
            "silver_transactions",
            "instance_date BETWEEN DATE'2000-01-01' AND DATE'2030-12-31'",
            "Transaction dates fall inside dim_date (2000-2030)",
            warn_above=0,
            fail_above=5,
        ),
        row_rule(
            "silver_transactions_date_not_future",
            "silver",
            "silver_transactions",
            "instance_date <= date_add(current_date(), 31)",
            "No transaction dated more than a month ahead",
            warn_above=0,
        ),
        row_rule(
            "silver_transactions_positive_size",
            "silver",
            "silver_transactions",
            "size_sqft > 0",
            "Sizes are positive (zero sizes are flagged outliers, kept in silver)",
            warn_above=0,
            fail_above=20,
        ),
        row_rule(
            "silver_transactions_positive_value",
            "silver",
            "silver_transactions",
            "actual_worth > 0",
            "Values are positive (zero values are flagged outliers, kept in silver)",
            warn_above=0,
            fail_above=20,
        ),
        *_silver_common("rents", "contract_id"),
        row_rule(
            "silver_rents_positive_rent",
            "silver",
            "silver_rents",
            "annual_rent_aed > 0",
            "Every contract has a positive annual rent",
            warn_above=0,
            fail_above=20,
        ),
    ]


def _fact_orphans(fact: str) -> str:
    return (
        "SELECT count(*) AS rows_checked, "
        "count_if(a.area_sk IS NULL OR t.property_type_sk IS NULL OR p.project_sk IS NULL) "
        f"AS rows_failed FROM {{gold_{fact}}} f "
        "LEFT JOIN (SELECT DISTINCT area_sk FROM {gold_dim_area}) a ON f.area_sk = a.area_sk "
        "LEFT JOIN {gold_dim_property_type} t ON f.property_type_sk = t.property_type_sk "
        "LEFT JOIN {gold_dim_project} p ON f.project_sk = p.project_sk"
    )


def _gold_checks() -> list[Check]:
    reconcile_sales = (
        "SELECT s.n AS rows_checked, abs(s.n - f.n) AS rows_failed "
        f"FROM ({_sales('silver_transactions')}) s "
        "CROSS JOIN (SELECT count(*) AS n FROM {gold_fact_sales}) f"
    )
    reconcile_rents = (
        "SELECT s.n AS rows_checked, abs(s.n - f.n) AS rows_failed "
        "FROM (SELECT count(*) AS n FROM {silver_rents}) s "
        "CROSS JOIN (SELECT count(*) AS n FROM {gold_fact_rents}) f"
    )
    one_current = (
        "SELECT count(*) AS rows_checked, count_if(current_versions <> 1) AS rows_failed "
        "FROM (SELECT area_key, count_if(is_current) AS current_versions "
        "FROM {gold_dim_area} WHERE area_key IS NOT NULL GROUP BY area_key)"
    )
    return [
        Check(
            "gold_fact_sales_reconciles",
            "gold",
            "gold_fact_sales",
            reconcile_sales,
            "fact_sales has exactly one row per silver sale",
            fail_above=0,
        ),
        Check(
            "gold_fact_rents_reconciles",
            "gold",
            "gold_fact_rents",
            reconcile_rents,
            "fact_rents has exactly one row per silver contract",
            fail_above=0,
        ),
        unique(
            "gold_fact_sales_key_unique",
            "gold",
            "gold_fact_sales",
            ["transaction_id"],
            "transaction_id is unique in fact_sales",
        ),
        Check(
            "gold_fact_sales_keys_resolve",
            "gold",
            "gold_fact_sales",
            _fact_orphans("fact_sales"),
            "Every fact_sales key finds its dimension row",
            fail_above=0,
        ),
        Check(
            "gold_fact_rents_keys_resolve",
            "gold",
            "gold_fact_rents",
            _fact_orphans("fact_rents"),
            "Every fact_rents key finds its dimension row",
            fail_above=0,
        ),
        Check(
            "gold_dim_area_one_current_version",
            "gold",
            "gold_dim_area",
            one_current,
            "Every area has exactly one current version",
            fail_above=0,
        ),
        row_rule(
            "gold_fact_sales_known_area",
            "gold",
            "gold_fact_sales",
            "area_sk <> -1",
            "Sales point at a real area, not the Unknown row",
            warn_above=5,
        ),
        row_rule(
            "gold_agg_yield_plausible",
            "gold",
            "gold_agg_area_monthly",
            "gross_yield_pct BETWEEN 1 AND 20",
            "Published gross yields lie between 1% and 20%",
            where="gross_yield_pct IS NOT NULL",
            warn_above=0,
        ),
    ]


CHECKS: tuple[Check, ...] = (
    *_bronze_checks("transactions"),
    *_bronze_checks("rents"),
    *_silver_checks(),
    *_gold_checks(),
)


def checks_for(stage: str) -> list[Check]:
    if stage not in STAGES:
        raise ValueError(f"Unknown stage {stage!r}. Known: {STAGES}")
    return [c for c in CHECKS if c.stage == stage]
