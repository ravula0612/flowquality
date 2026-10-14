"""No Python UDFs, data sampling, implicit writes, or cloud dependencies."""
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import math
from typing import Any


@dataclass(frozen=True)
class Check:
    """A declarative rule. Columns are literal, top-level names (not paths)."""

    name: str
    kind: str
    columns: tuple[str, ...] = ()
    severity: str = "error"
    max_failure_rate: float = 0.0
    ignore_nulls: bool = False
    values: tuple[Any, ...] = ()
    minimum: float | None = None
    maximum: float | None = None
    expected_type: str | None = None

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("Check name must be a nonempty string")
        if self.name == "__empty_dataset__":
            raise ValueError("__empty_dataset__ is reserved for the dataset policy")
        kinds = {"not_null", "unique", "allowed_values", "range", "type", "row_count"}
        if self.kind not in kinds:
            raise ValueError(f"Unknown check kind: {self.kind}")
        if self.severity not in {"error", "warning"}:
            raise ValueError("severity must be error or warning")
        if not isinstance(self.ignore_nulls, bool):
            raise ValueError("ignore_nulls must be boolean")
        rate = self.max_failure_rate
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or not 0 <= rate <= 1:
            raise ValueError("max_failure_rate must be finite and between 0 and 1")
        if isinstance(self.columns, str):
            raise ValueError("columns must be a sequence, not a string")
        if isinstance(self.values, (str, bytes)):
            raise ValueError("values must be a sequence, not a string")
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "values", tuple(self.values))
        if any(not isinstance(c, str) or not c for c in self.columns) or len(set(self.columns)) != len(self.columns):
            raise ValueError("columns must contain distinct nonempty names")
        if self.kind == "row_count":
            if self.columns:
                raise ValueError("row_count does not accept columns")
        elif self.kind == "unique":
            if not self.columns:
                raise ValueError("unique requires at least one column")
        elif len(self.columns) != 1:
            raise ValueError(f"{self.kind} requires exactly one column")
        if self.kind in {"range", "row_count"}:
            if self.minimum is None and self.maximum is None:
                raise ValueError("Provide minimum or maximum")
            for bound in (self.minimum, self.maximum):
                if bound is not None and (isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound)):
                    raise ValueError("Bounds must be finite numbers")
                if self.kind == "row_count" and bound is not None and (not isinstance(bound, int) or bound < 0):
                    raise ValueError("row_count bounds must be nonnegative integers")
            if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
                raise ValueError("minimum cannot exceed maximum")
        if self.kind == "allowed_values":
            if not self.values or any(v is None or not isinstance(v, (str, int, float, bool)) for v in self.values):
                raise ValueError("allowed_values requires non-null scalar values")
            if any(isinstance(v, float) and not math.isfinite(v) for v in self.values):
                raise ValueError("Allowed numeric values must be finite")
        if self.kind == "type" and (not isinstance(self.expected_type, str) or not self.expected_type):
            raise ValueError("type requires a Spark simpleString, e.g. bigint")
        if self.kind != "type" and self.expected_type is not None:
            raise ValueError("expected_type only applies to type")
        if self.kind not in {"range", "row_count"} and (self.minimum is not None or self.maximum is not None):
            raise ValueError("Bounds only apply to range or row_count")
        if self.kind != "allowed_values" and self.values:
            raise ValueError("values only applies to allowed_values")
        if self.kind in {"type", "row_count"} and self.max_failure_rate:
            raise ValueError("Failure-rate tolerance only applies to row-level rules")
        if self.kind in {"not_null", "type", "row_count"} and self.ignore_nulls:
            raise ValueError("ignore_nulls does not apply to this rule")

    @classmethod
    def from_dict(cls, config):
        """Unknown keys and invalid options raise; never silently skip a rule."""
        return cls(**config)


@dataclass(frozen=True)
class Result:
    name: str
    kind: str
    severity: str
    status: str
    evaluated_rows: int = 0
    failed_rows: int = 0
    failure_rate: float = 0.0
    message: str = ""


class QualityError(RuntimeError):
    def __init__(self, report):
        self.report = report
        names = [r.name for r in report.results if r.severity == "error" and r.status in {"fail", "error"}]
        super().__init__("Data quality gate failed: " + ", ".join(names))


@dataclass(frozen=True)
class Report:
    dataset: str
    row_count: int
    results: tuple[Result, ...]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def passed(self):
        return not any(r.severity == "error" and r.status in {"fail", "error"} for r in self.results)

    def raise_for_errors(self):
        if not self.passed:
            raise QualityError(self)
        return self

    def to_dict(self):
        return {**asdict(self), "passed": self.passed, "report_version": 1}

    def to_json(self, *, indent=2):
        return json.dumps(self.to_dict(), indent=indent, allow_nan=False)


def validate(df, checks, *, dataset="dataset", empty_policy="fail"):
    """Evaluate batch data; Spark execution errors propagate rather than pass.

    Row predicates share one aggregate action. Each uniqueness check requires
    another groupBy/aggregate action. Only aggregate rows reach the driver.
    Caller owns snapshot consistency/caching and all persistence.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, NumericType, StringType, FloatType, DoubleType

    if df.isStreaming:
        raise ValueError("Streaming DataFrames are unsupported; use validate inside foreachBatch")
    if empty_policy not in {"fail", "allow"}:
        raise ValueError("empty_policy must be fail or allow")
    checks = tuple(checks)
    if not checks or not all(isinstance(c, Check) for c in checks):
        raise ValueError("Provide at least one Check")
    if len({c.name for c in checks}) != len(checks):
        raise ValueError("Check names must be unique")
    # Spark may resolve names case-insensitively; reject ambiguity early.
    names = df.columns
    if len(set(n.casefold() for n in names)) != len(names):
        raise ValueError("Duplicate or case-ambiguous DataFrame column names")
    schema = {f.name: f.dataType for f in df.schema.fields}

    def col(name):
        return F.col("`" + name.replace("`", "``") + "`")

    def missing(name):
        x = col(name)
        return x.isNull() | F.isnan(x) if isinstance(schema[name], (FloatType, DoubleType)) else x.isNull()

    results = {}
    metrics = [F.count(F.lit(1)).alias("total")]
    row_rules = []
    unique_rules = []
    for i, c in enumerate(checks):
        absent = [name for name in c.columns if name not in schema]
        if absent:
            results[i] = Result(c.name, c.kind, c.severity, "error", message=f"Missing columns: {absent}")
            continue
        if c.kind == "type":
            actual = schema[c.columns[0]].simpleString()
            results[i] = Result(c.name, c.kind, c.severity, "pass" if actual == c.expected_type else "fail", message=f"Expected {c.expected_type}; actual {actual}")
            continue
        if c.kind == "row_count":
            continue
        if c.kind == "unique":
            unique_rules.append((i, c))
            continue
        name = c.columns[0]
        dtype = schema[name]
        x = col(name)
        null = missing(name)
        eligible = ~null if c.ignore_nulls else F.lit(True)
        if c.kind == "not_null":
            valid = ~null
        elif c.kind == "range":
            if not isinstance(dtype, NumericType):
                results[i] = Result(c.name, c.kind, c.severity, "error", message="range requires a numeric column; no implicit cast")
                continue
            valid = ~null
            if c.minimum is not None:
                valid = valid & (x >= F.lit(c.minimum))
            if c.maximum is not None:
                valid = valid & (x <= F.lit(c.maximum))
        else:
            compatible = (
                isinstance(dtype, StringType) and all(isinstance(v, str) for v in c.values)
                or isinstance(dtype, BooleanType) and all(isinstance(v, bool) for v in c.values)
                or isinstance(dtype, NumericType) and all(type(v) in (int, float) for v in c.values)
            )
            if not compatible:
                results[i] = Result(c.name, c.kind, c.severity, "error", message="Allowed values must match a string, boolean, or numeric column")
                continue
            valid = ~null & x.isin(list(c.values))
        invalid = eligible & ~F.coalesce(valid, F.lit(False))
        metrics.extend([
            F.count(F.when(eligible, 1)).alias(f"n{i}"),
            F.count(F.when(invalid, 1)).alias(f"f{i}"),
        ])
        row_rules.append((i, c))

    aggregate = df.agg(*metrics).first().asDict()
    total = aggregate["total"]

    def measured(c, n, failed):
        rate = failed / n if n else 0.0
        status = "skip" if n == 0 else ("pass" if rate <= c.max_failure_rate else "fail")
        return Result(c.name, c.kind, c.severity, status, n, failed, rate,
                      "No eligible rows" if n == 0 else f"Allowed failure rate: {c.max_failure_rate}")

    for i, c in row_rules:
        results[i] = measured(c, aggregate[f"n{i}"], aggregate[f"f{i}"])
    for i, c in unique_rules:
        null = missing(c.columns[0])
        for name in c.columns[1:]:
            null = null | missing(name)
        # Default: any null/NaN key fails; every row in a duplicate group fails.
        non_null = df.filter(~null)
        keys = [col(name).alias(f"k{j}") for j, name in enumerate(c.columns)]
        groups = non_null.select(*keys).groupBy(*[f"k{j}" for j in range(len(keys))]).count()
        counts = groups.agg(
            F.coalesce(F.sum("count"), F.lit(0)).alias("n"),
            F.coalesce(F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)), F.lit(0)).alias("f"),
        ).first()
        n = counts["n"] if c.ignore_nulls else total
        failed = counts["f"] + (0 if c.ignore_nulls else total - counts["n"])
        results[i] = measured(c, n, failed)
    for i, c in enumerate(checks):
        if c.kind == "row_count":
            ok = (c.minimum is None or total >= c.minimum) and (c.maximum is None or total <= c.maximum)
            results[i] = Result(c.name, c.kind, c.severity, "pass" if ok else "fail", message=f"Row count {total}; inclusive bounds [{c.minimum}, {c.maximum}]")
    ordered = tuple(results[i] for i in range(len(checks)))
    if total == 0 and empty_policy == "fail":
        ordered += (Result("__empty_dataset__", "dataset", "error", "fail", message="Empty dataset rejected by policy"),)
    return Report(dataset, total, ordered)
