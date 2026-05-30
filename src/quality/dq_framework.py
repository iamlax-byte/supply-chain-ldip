"""
YAML-driven Data Quality Framework.

Loads rules from config/dq_rules.yaml, executes each check against staging
tables, and writes results to staging.dq_results.

Supported check_type values:
  not_null        — column has no NULL values
  unique          — column values are distinct
  range           — numeric value is between params.min and params.max
  accepted_values — value is in params.values list
  row_count       — table row count >= params.min_rows
  referential     — value exists in params.ref_table.params.ref_column
  custom          — raw SQL expression (params.expression evaluates to TRUE)

Severity:
  critical — failure blocks downstream (Airflow branch skips warehouse/marts)
  warning  — failure logged but pipeline continues

Usage::

    python -m src.quality.dq_framework <batch_id> <run_id>
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import text

from src.utils.db import get_engine

log = logging.getLogger(__name__)

_RULES_PATH = Path(__file__).parents[2] / "config" / "dq_rules.yaml"


@dataclass
class DQResult:
    rule_name:      str
    table_name:     str | None
    column_name:    str | None
    check_type:     str
    severity:       str
    rows_checked:   int
    rows_failed:    int
    pass_rate:      float
    passed:         bool
    failure_detail: str | None = None


@dataclass
class DQRunSummary:
    run_id:           str
    batch_id:         str
    results:          list[DQResult] = field(default_factory=list)
    critical_failures: int = 0
    warning_failures:  int = 0

    @property
    def has_critical_failures(self) -> bool:
        return self.critical_failures > 0

    @property
    def overall_pass_rate(self) -> float:
        if not self.results:
            return 1.0
        return sum(r.passed for r in self.results) / len(self.results)


class DQFramework:
    """Executes DQ rules from a YAML config file against staging tables."""

    def __init__(
        self,
        rules_path: Path = _RULES_PATH,
        schema: str = "staging",
    ) -> None:
        self.schema = schema
        self.engine = get_engine(schema)
        self.rules  = self._load_rules(rules_path)

    def _load_rules(self, path: Path) -> list[dict]:
        with path.open() as f:
            config = yaml.safe_load(f)
        rules = config.get("rules", [])
        log.info("Loaded %d DQ rules from %s", len(rules), path.name)
        return rules

    def run_all(self, batch_id: str, run_id: str | None = None) -> DQRunSummary:
        """Execute every rule and write results to staging.dq_results.

        Args:
            batch_id: The load batch this check run covers.
            run_id:   Optional caller-supplied run ID. Auto-generated if None.

        Returns:
            DQRunSummary — check .has_critical_failures to decide branching.
        """
        run_id = run_id or f"dq-{uuid.uuid4().hex[:12]}"
        summary = DQRunSummary(run_id=run_id, batch_id=batch_id)

        for rule in self.rules:
            result = self._execute_rule(rule)
            summary.results.append(result)
            self._write_result(result, run_id, batch_id)

            level = logging.WARNING if not result.passed else logging.INFO
            log.log(
                level,
                "DQ [%s] %s | passed=%s | failed_rows=%d | rate=%.4f",
                result.severity.upper(), result.rule_name,
                result.passed, result.rows_failed, result.pass_rate,
            )

            if not result.passed:
                if result.severity == "critical":
                    summary.critical_failures += 1
                else:
                    summary.warning_failures += 1

        log.info(
            "DQ run complete | run_id=%s | rules=%d | critical_failures=%d | "
            "warning_failures=%d | overall_pass_rate=%.4f",
            run_id, len(self.rules),
            summary.critical_failures, summary.warning_failures,
            summary.overall_pass_rate,
        )
        return summary

    def _execute_rule(self, rule: dict[str, Any]) -> DQResult:
        """Dispatch to the appropriate check implementation."""
        name       = rule["name"]
        table      = rule.get("table")
        column     = rule.get("column")
        check_type = rule["check_type"]
        severity   = rule.get("severity", "warning")
        params     = rule.get("params", {})

        try:
            rows_checked, rows_failed, detail = self._run_check(
                check_type, table, column, params
            )
        except Exception as exc:
            log.error("DQ rule %s raised an error: %s", name, exc)
            rows_checked, rows_failed, detail = 0, 1, str(exc)

        pass_rate = 1.0 - (rows_failed / rows_checked) if rows_checked > 0 else 0.0
        passed    = rows_failed == 0

        return DQResult(
            rule_name=name,
            table_name=table,
            column_name=column,
            check_type=check_type,
            severity=severity,
            rows_checked=rows_checked,
            rows_failed=rows_failed,
            pass_rate=round(pass_rate, 6),
            passed=passed,
            failure_detail=detail if not passed else None,
        )

    def _run_check(
        self,
        check_type: str,
        table: str | None,
        column: str | None,
        params: dict,
    ) -> tuple[int, int, str | None]:
        """Return (rows_checked, rows_failed, detail_message)."""
        qualified = f"{self.schema}.{table}" if table else None

        with self.engine.connect() as conn:

            if check_type == "not_null":
                total  = conn.execute(text(f"select count(*) from {qualified}")).scalar()
                failed = conn.execute(
                    text(f"select count(*) from {qualified} where `{column}` is null")
                ).scalar()
                return total, failed, f"{failed} NULL values in {column}"

            elif check_type == "unique":
                total  = conn.execute(text(f"select count(*) from {qualified}")).scalar()
                dups   = conn.execute(
                    text(f"select count(*) - count(distinct `{column}`) from {qualified}")
                ).scalar()
                return total, dups, f"{dups} duplicate values in {column}"

            elif check_type == "range":
                min_v  = params["min"]
                max_v  = params["max"]
                total  = conn.execute(text(f"select count(*) from {qualified}")).scalar()
                failed = conn.execute(
                    text(
                        f"select count(*) from {qualified} "
                        f"where `{column}` < :min or `{column}` > :max"
                    ),
                    {"min": min_v, "max": max_v},
                ).scalar()
                return total, failed, f"{failed} values outside [{min_v}, {max_v}]"

            elif check_type == "accepted_values":
                values = params["values"]
                total  = conn.execute(text(f"select count(*) from {qualified}")).scalar()
                placeholders = ", ".join(f":v{i}" for i in range(len(values)))
                bind = {f"v{i}": v for i, v in enumerate(values)}
                failed = conn.execute(
                    text(
                        f"select count(*) from {qualified} "
                        f"where `{column}` not in ({placeholders})"
                    ),
                    bind,
                ).scalar()
                return total, failed, f"{failed} values not in accepted list"

            elif check_type == "row_count":
                min_rows = params["min_rows"]
                total    = conn.execute(text(f"select count(*) from {qualified}")).scalar()
                failed   = 0 if total >= min_rows else 1
                detail   = f"table has {total} rows, minimum is {min_rows}"
                return total, failed, detail

            elif check_type == "referential":
                ref_table  = params["ref_table"]
                ref_column = params["ref_column"]
                ref_qual   = f"{self.schema}.{ref_table}"
                total      = conn.execute(text(f"select count(*) from {qualified}")).scalar()
                failed     = conn.execute(
                    text(
                        f"select count(*) from {qualified} t "
                        f"where not exists ("
                        f"  select 1 from {ref_qual} r "
                        f"  where r.`{ref_column}` = t.`{column}`"
                        f")"
                    )
                ).scalar()
                return total, failed, f"{failed} orphan values in {column}"

            elif check_type == "custom":
                expr   = params["expression"]
                total  = conn.execute(text(f"select count(*) from {qualified}")).scalar()
                failed = conn.execute(
                    text(f"select count(*) from {qualified} where not ({expr})")
                ).scalar()
                return total, failed, f"{failed} rows failed expression: {expr}"

            else:
                raise ValueError(f"Unknown check_type: {check_type}")

    def _write_result(self, result: DQResult, run_id: str, batch_id: str) -> None:
        """Append one row to staging.dq_results."""
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    insert into staging.dq_results
                        (run_id, batch_id, rule_name, table_name, column_name,
                         check_type, severity, rows_checked, rows_failed,
                         pass_rate, passed, failure_detail, executed_at)
                    values
                        (:run_id, :batch_id, :rule_name, :table_name, :column_name,
                         :check_type, :severity, :rows_checked, :rows_failed,
                         :pass_rate, :passed, :failure_detail, :executed_at)
                """),
                {
                    "run_id":         run_id,
                    "batch_id":       batch_id,
                    "rule_name":      result.rule_name,
                    "table_name":     result.table_name,
                    "column_name":    result.column_name,
                    "check_type":     result.check_type,
                    "severity":       result.severity,
                    "rows_checked":   result.rows_checked,
                    "rows_failed":    result.rows_failed,
                    "pass_rate":      result.pass_rate,
                    "passed":         int(result.passed),
                    "failure_detail": result.failure_detail,
                    "executed_at":    datetime.now(timezone.utc),
                },
            )
            conn.commit()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python -m src.quality.dq_framework <batch_id> [run_id]")
        sys.exit(1)
    bid = sys.argv[1]
    rid = sys.argv[2] if len(sys.argv) > 2 else None
    fw  = DQFramework()
    summary = fw.run_all(batch_id=bid, run_id=rid)
    print(f"Critical failures: {summary.critical_failures}")
    print(f"Warning failures:  {summary.warning_failures}")
    print(f"Overall pass rate: {summary.overall_pass_rate:.2%}")
    sys.exit(1 if summary.has_critical_failures else 0)
