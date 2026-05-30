"""Unit tests for src/quality/dq_framework.py — pure logic (no DB, no YAML)."""
import pytest

from src.quality.dq_framework import DQResult, DQRunSummary


class TestDQResult:
    def test_passed_when_no_failures(self):
        r = DQResult(
            rule_name="test", table_name="t", column_name="c",
            check_type="not_null", severity="critical",
            rows_checked=100, rows_failed=0, pass_rate=1.0, passed=True,
        )
        assert r.passed is True

    def test_failed_when_rows_failed(self):
        r = DQResult(
            rule_name="test", table_name="t", column_name="c",
            check_type="not_null", severity="critical",
            rows_checked=100, rows_failed=5, pass_rate=0.95, passed=False,
            failure_detail="5 NULL values",
        )
        assert r.passed is False
        assert r.failure_detail == "5 NULL values"


class TestDQRunSummary:
    def _make_result(self, passed: bool, severity: str = "critical") -> DQResult:
        return DQResult(
            rule_name="r", table_name="t", column_name="c",
            check_type="not_null", severity=severity,
            rows_checked=10, rows_failed=0 if passed else 1,
            pass_rate=1.0 if passed else 0.9, passed=passed,
        )

    def test_no_critical_failures_when_all_pass(self):
        summary = DQRunSummary(run_id="r1", batch_id="b1")
        summary.results = [self._make_result(True), self._make_result(True)]
        summary.critical_failures = 0
        assert not summary.has_critical_failures

    def test_has_critical_failures(self):
        summary = DQRunSummary(run_id="r1", batch_id="b1")
        summary.results = [self._make_result(False, "critical")]
        summary.critical_failures = 1
        assert summary.has_critical_failures

    def test_overall_pass_rate_all_pass(self):
        summary = DQRunSummary(run_id="r1", batch_id="b1")
        summary.results = [self._make_result(True)] * 4
        assert summary.overall_pass_rate == 1.0

    def test_overall_pass_rate_half_fail(self):
        summary = DQRunSummary(run_id="r1", batch_id="b1")
        summary.results = [self._make_result(True), self._make_result(False)]
        assert summary.overall_pass_rate == 0.5

    def test_empty_results_returns_1(self):
        summary = DQRunSummary(run_id="r1", batch_id="b1")
        assert summary.overall_pass_rate == 1.0

    def test_warning_failure_does_not_set_critical(self):
        summary = DQRunSummary(run_id="r1", batch_id="b1")
        summary.results = [self._make_result(False, "warning")]
        summary.warning_failures = 1
        summary.critical_failures = 0
        assert not summary.has_critical_failures
        assert summary.warning_failures == 1

    def test_pass_rate_rounds_correctly(self):
        summary = DQRunSummary(run_id="r1", batch_id="b1")
        # 3 pass out of 4 = 0.75
        summary.results = [
            self._make_result(True), self._make_result(True),
            self._make_result(True), self._make_result(False),
        ]
        assert summary.overall_pass_rate == 0.75
