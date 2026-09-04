"""
============================================================
PROJECT: Configurable Data Quality Validation Framework
Author : Anil Kumar Nukala
Domain : Data Engineering / Data Governance
Description:
    A rule-based data quality framework that validates
    DataFrames (or database tables) against configurable
    checks — nullability, uniqueness, range bounds,
    referential integrity, regex patterns, and freshness.
    Generates a structured quality report and can block
    downstream pipeline steps on critical failures.
============================================================
"""

import re
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger(__name__)


# ----------------------------------------------------------------
# Enumerations
# ----------------------------------------------------------------
class Severity(str, Enum):
    CRITICAL = "CRITICAL"   # blocks downstream — pipeline stops
    HIGH     = "HIGH"       # alerts sent, pipeline continues
    MEDIUM   = "MEDIUM"     # logged, no alert
    LOW      = "LOW"        # informational only


class CheckStatus(str, Enum):
    PASS    = "PASS"
    FAIL    = "FAIL"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"


# ----------------------------------------------------------------
# Data classes
# ----------------------------------------------------------------
@dataclass
class QualityRule:
    """Defines a single data quality check."""
    rule_id:      str
    description:  str
    severity:     Severity
    check_fn:     Callable[[pd.DataFrame], "CheckResult"]
    tags:         list[str] = field(default_factory=list)


@dataclass
class CheckResult:
    rule_id:        str
    description:    str
    severity:       Severity
    status:         CheckStatus
    rows_checked:   int
    rows_failed:    int
    failure_pct:    float
    sample_failures: list[Any] = field(default_factory=list)
    message:        str = ""

    def to_dict(self) -> dict:
        return {
            "rule_id":        self.rule_id,
            "description":    self.description,
            "severity":       self.severity.value,
            "status":         self.status.value,
            "rows_checked":   self.rows_checked,
            "rows_failed":    self.rows_failed,
            "failure_pct":    round(self.failure_pct, 2),
            "message":        self.message,
            "sample_failures": str(self.sample_failures[:5]),
        }


# ----------------------------------------------------------------
# Built-in rule factory functions
# ----------------------------------------------------------------
def not_null(column: str, severity: Severity = Severity.CRITICAL) -> QualityRule:
    """Fails if any value in `column` is null."""
    def _check(df: pd.DataFrame) -> CheckResult:
        nulls       = df[column].isna()
        rows_failed = int(nulls.sum())
        total       = len(df)
        status      = CheckStatus.FAIL if rows_failed > 0 else CheckStatus.PASS
        return CheckResult(
            rule_id       = f"not_null_{column}",
            description   = f"Column '{column}' must not contain NULLs",
            severity      = severity,
            status        = status,
            rows_checked  = total,
            rows_failed   = rows_failed,
            failure_pct   = rows_failed / total * 100 if total else 0,
            sample_failures = df.loc[nulls].index.tolist()[:5],
            message       = f"{rows_failed} NULL(s) found" if rows_failed else "All values present",
        )
    return QualityRule(
        rule_id=f"not_null_{column}", description=f"Column '{column}' must not contain NULLs",
        severity=severity, check_fn=_check, tags=["completeness"],
    )


def is_unique(column: str, severity: Severity = Severity.CRITICAL) -> QualityRule:
    """Fails if any value in `column` appears more than once."""
    def _check(df: pd.DataFrame) -> CheckResult:
        dupes       = df[df.duplicated(subset=[column], keep=False)]
        rows_failed = len(dupes)
        total       = len(df)
        return CheckResult(
            rule_id       = f"unique_{column}",
            description   = f"Column '{column}' must be unique",
            severity      = severity,
            status        = CheckStatus.FAIL if rows_failed > 0 else CheckStatus.PASS,
            rows_checked  = total,
            rows_failed   = rows_failed,
            failure_pct   = rows_failed / total * 100 if total else 0,
            sample_failures = dupes[column].unique().tolist()[:5],
            message       = f"{rows_failed} duplicate row(s) on '{column}'" if rows_failed else "All values unique",
        )
    return QualityRule(
        rule_id=f"unique_{column}", description=f"Column '{column}' must be unique",
        severity=severity, check_fn=_check, tags=["uniqueness"],
    )


def in_range(
    column: str,
    min_val: float,
    max_val: float,
    severity: Severity = Severity.HIGH,
) -> QualityRule:
    """Fails if values in `column` fall outside [min_val, max_val]."""
    def _check(df: pd.DataFrame) -> CheckResult:
        numeric     = pd.to_numeric(df[column], errors="coerce")
        out_of_range = numeric[(numeric < min_val) | (numeric > max_val)]
        rows_failed  = len(out_of_range)
        total        = len(df)
        return CheckResult(
            rule_id       = f"range_{column}",
            description   = f"Column '{column}' must be between {min_val} and {max_val}",
            severity      = severity,
            status        = CheckStatus.FAIL if rows_failed > 0 else CheckStatus.PASS,
            rows_checked  = total,
            rows_failed   = rows_failed,
            failure_pct   = rows_failed / total * 100 if total else 0,
            sample_failures = out_of_range.tolist()[:5],
            message       = f"{rows_failed} value(s) outside [{min_val}, {max_val}]" if rows_failed else "All in range",
        )
    return QualityRule(
        rule_id=f"range_{column}", description=f"Column '{column}' must be in [{min_val}, {max_val}]",
        severity=severity, check_fn=_check, tags=["validity"],
    )


def matches_regex(
    column: str,
    pattern: str,
    severity: Severity = Severity.MEDIUM,
) -> QualityRule:
    """Fails if any non-null value in `column` does not match `pattern`."""
    compiled = re.compile(pattern)
    def _check(df: pd.DataFrame) -> CheckResult:
        non_null    = df[column].dropna()
        failures    = non_null[~non_null.astype(str).str.match(compiled)]
        rows_failed = len(failures)
        total       = len(df)
        return CheckResult(
            rule_id       = f"regex_{column}",
            description   = f"Column '{column}' must match pattern '{pattern}'",
            severity      = severity,
            status        = CheckStatus.FAIL if rows_failed > 0 else CheckStatus.PASS,
            rows_checked  = total,
            rows_failed   = rows_failed,
            failure_pct   = rows_failed / total * 100 if total else 0,
            sample_failures = failures.tolist()[:5],
            message       = f"{rows_failed} value(s) fail pattern match" if rows_failed else "All match pattern",
        )
    return QualityRule(
        rule_id=f"regex_{column}", description=f"'{column}' matches '{pattern}'",
        severity=severity, check_fn=_check, tags=["validity", "format"],
    )


def referential_integrity(
    column: str,
    valid_values: set,
    severity: Severity = Severity.HIGH,
) -> QualityRule:
    """Fails if any value in `column` is not in `valid_values`."""
    def _check(df: pd.DataFrame) -> CheckResult:
        bad         = df[~df[column].isin(valid_values) & df[column].notna()]
        rows_failed = len(bad)
        total       = len(df)
        return CheckResult(
            rule_id       = f"ref_int_{column}",
            description   = f"Column '{column}' values must be in allowed set",
            severity      = severity,
            status        = CheckStatus.FAIL if rows_failed > 0 else CheckStatus.PASS,
            rows_checked  = total,
            rows_failed   = rows_failed,
            failure_pct   = rows_failed / total * 100 if total else 0,
            sample_failures = bad[column].unique().tolist()[:5],
            message       = f"{rows_failed} unexpected value(s)" if rows_failed else "All values valid",
        )
    return QualityRule(
        rule_id=f"ref_int_{column}", description=f"'{column}' referential integrity",
        severity=severity, check_fn=_check, tags=["consistency"],
    )


def row_count_threshold(
    min_rows: int,
    max_rows: Optional[int] = None,
    severity: Severity = Severity.CRITICAL,
) -> QualityRule:
    """Fails if the DataFrame row count is outside the expected range."""
    def _check(df: pd.DataFrame) -> CheckResult:
        actual      = len(df)
        failed      = actual < min_rows or (max_rows is not None and actual > max_rows)
        desc_range  = f">= {min_rows}" if max_rows is None else f"{min_rows}–{max_rows}"
        return CheckResult(
            rule_id       = "row_count",
            description   = f"Row count must be {desc_range}",
            severity      = severity,
            status        = CheckStatus.FAIL if failed else CheckStatus.PASS,
            rows_checked  = actual,
            rows_failed   = 1 if failed else 0,
            failure_pct   = 100.0 if failed else 0.0,
            message       = f"Got {actual:,} rows (expected {desc_range})",
        )
    return QualityRule(
        rule_id="row_count", description=f"Row count must be >= {min_rows}",
        severity=severity, check_fn=_check, tags=["completeness"],
    )


def freshness(
    date_column: str,
    max_age_hours: int = 24,
    severity: Severity = Severity.HIGH,
) -> QualityRule:
    """Fails if the most recent date in `date_column` is older than `max_age_hours`."""
    def _check(df: pd.DataFrame) -> CheckResult:
        dates       = pd.to_datetime(df[date_column], errors="coerce").dropna()
        if dates.empty:
            return CheckResult(
                rule_id="freshness", description=f"'{date_column}' freshness",
                severity=severity, status=CheckStatus.WARNING,
                rows_checked=len(df), rows_failed=0, failure_pct=0,
                message="No parseable dates found",
            )
        latest      = dates.max()
        age_hours   = (datetime.utcnow() - latest.to_pydatetime().replace(tzinfo=None)).total_seconds() / 3600
        failed      = age_hours > max_age_hours
        return CheckResult(
            rule_id       = f"freshness_{date_column}",
            description   = f"'{date_column}' must be refreshed within {max_age_hours}h",
            severity      = severity,
            status        = CheckStatus.FAIL if failed else CheckStatus.PASS,
            rows_checked  = len(df),
            rows_failed   = 1 if failed else 0,
            failure_pct   = 100.0 if failed else 0.0,
            message       = f"Latest record: {latest:%Y-%m-%d %H:%M} ({age_hours:.1f}h ago)",
        )
    return QualityRule(
        rule_id=f"freshness_{date_column}", description=f"'{date_column}' freshness check",
        severity=severity, check_fn=_check, tags=["timeliness"],
    )


# ----------------------------------------------------------------
# Validation engine
# ----------------------------------------------------------------
class DataQualityValidator:
    """
    Runs a suite of quality rules against a DataFrame
    and generates a structured report.
    """

    def __init__(self, dataset_name: str, rules: list[QualityRule]):
        self.dataset_name = dataset_name
        self.rules        = rules
        self.results: list[CheckResult] = []

    def run(self, df: pd.DataFrame) -> "DataQualityReport":
        log.info(f"Running {len(self.rules)} quality checks on '{self.dataset_name}' ({len(df):,} rows)")
        self.results = []

        for rule in self.rules:
            try:
                result = rule.check_fn(df)
                self.results.append(result)
                icon = "✅" if result.status == CheckStatus.PASS else "❌"
                log.info(f"  {icon} [{result.severity.value:8}] {result.rule_id} — {result.message}")
            except Exception as exc:
                log.exception(f"  Rule '{rule.rule_id}' raised an error: {exc}")
                self.results.append(CheckResult(
                    rule_id=rule.rule_id, description=rule.description,
                    severity=rule.severity, status=CheckStatus.SKIPPED,
                    rows_checked=len(df), rows_failed=0, failure_pct=0,
                    message=f"Error during check: {exc}",
                ))

        return DataQualityReport(self.dataset_name, df, self.results)


# ----------------------------------------------------------------
# Quality report
# ----------------------------------------------------------------
class DataQualityReport:
    def __init__(
        self,
        dataset_name: str,
        df: pd.DataFrame,
        results: list[CheckResult],
    ):
        self.dataset_name    = dataset_name
        self.row_count       = len(df)
        self.results         = results
        self.run_timestamp   = datetime.utcnow()
        self.passed          = [r for r in results if r.status == CheckStatus.PASS]
        self.failed          = [r for r in results if r.status == CheckStatus.FAIL]
        self.critical_fails  = [r for r in self.failed if r.severity == Severity.CRITICAL]
        self.overall_pass    = len(self.critical_fails) == 0

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([r.to_dict() for r in self.results])

    def print_summary(self) -> None:
        total   = len(self.results)
        passed  = len(self.passed)
        failed  = len(self.failed)
        score   = round(passed / total * 100, 1) if total else 0

        print(f"\n{'='*60}")
        print(f" DATA QUALITY REPORT — {self.dataset_name}")
        print(f" Run at: {self.run_timestamp:%Y-%m-%d %H:%M UTC}")
        print(f" Rows scanned: {self.row_count:,}")
        print(f"{'='*60}")
        print(f" Checks run    : {total}")
        print(f" Passed        : {passed}  ({score}%)")
        print(f" Failed        : {failed}")
        print(f" Critical fails: {len(self.critical_fails)}")
        print(f" Overall status: {'✅ PASS' if self.overall_pass else '❌ FAIL'}")
        print(f"{'='*60}\n")

        if self.failed:
            print("FAILURES:")
            for r in sorted(self.failed, key=lambda x: x.severity.value):
                print(f"  [{r.severity.value:8}] {r.rule_id}: {r.message}")
        print()

    def raise_on_critical(self) -> None:
        """Raise an exception if any CRITICAL check failed — use to block pipelines."""
        if self.critical_fails:
            fail_ids = [r.rule_id for r in self.critical_fails]
            raise ValueError(
                f"Data quality CRITICAL failures in '{self.dataset_name}': {fail_ids}"
            )


# ----------------------------------------------------------------
# Example usage — Inventory transactions validation
# ----------------------------------------------------------------
if __name__ == "__main__":
    # Simulate loading data (replace with your actual DataFrame)
    sample_data = pd.DataFrame({
        "transaction_id"  : ["T001", "T002", "T003", "T004", "T004"],  # T004 duplicate
        "sku_id"          : ["SKU-A", "SKU-B", None,   "SKU-D", "SKU-D"],  # None is null
        "warehouse_id"    : ["WH-01", "WH-02", "WH-01", "WH-99", "WH-01"],  # WH-99 invalid
        "quantity"        : [100, 50, 200, -5, 80],  # -5 is out of range
        "transaction_type": ["RECEIPT", "SHIPMENT", "RECEIPT", "UNKNOWN", "RECEIPT"],
        "transaction_date": [
            datetime.utcnow() - timedelta(hours=2),
            datetime.utcnow() - timedelta(hours=5),
            datetime.utcnow() - timedelta(hours=1),
            datetime.utcnow() - timedelta(hours=3),
            datetime.utcnow() - timedelta(hours=4),
        ],
        "unit_cost": [25.00, 18.50, 42.00, 10.00, 30.00],
    })

    VALID_WAREHOUSES = {"WH-01", "WH-02", "WH-03", "WH-04"}
    VALID_TXN_TYPES  = {"RECEIPT", "SHIPMENT", "TRANSFER", "ADJUSTMENT_POSITIVE", "ADJUSTMENT_NEGATIVE", "RETURN"}

    rules = [
        row_count_threshold(min_rows=1, severity=Severity.CRITICAL),
        not_null("transaction_id", severity=Severity.CRITICAL),
        is_unique("transaction_id", severity=Severity.CRITICAL),
        not_null("sku_id",          severity=Severity.CRITICAL),
        not_null("warehouse_id",    severity=Severity.HIGH),
        in_range("quantity", min_val=0, max_val=100_000, severity=Severity.HIGH),
        in_range("unit_cost", min_val=0.01, max_val=50_000, severity=Severity.MEDIUM),
        referential_integrity("warehouse_id",    VALID_WAREHOUSES, severity=Severity.HIGH),
        referential_integrity("transaction_type", VALID_TXN_TYPES, severity=Severity.HIGH),
        freshness("transaction_date", max_age_hours=12, severity=Severity.HIGH),
    ]

    validator = DataQualityValidator("WMS Inventory Transactions", rules)
    report    = validator.run(sample_data)
    report.print_summary()

    # Export results to CSV
    report.to_dataframe().to_csv("data_quality_report.csv", index=False)
    log.info("Quality report saved to data_quality_report.csv")

    # Uncomment to block pipeline on critical failures:
    # report.raise_on_critical()
