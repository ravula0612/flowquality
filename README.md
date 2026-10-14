# FlowQuality

A small Python library for explicit data quality gates on **batch PySpark DataFrames**.
Built for data engineers working with Spark, Azure Databricks, and medallion pipelines.
Version 0.1.0 is a starter release, not a production certification or a published PyPI package.
The working name has not been checked for package-name or trademark availability.

## Install

Local development (Python 3.10+, Java 17):

```bash
python -m pip install -e '.[spark,dev]'
python examples/cargo_quality.py
pytest -q
```

On Databricks, install the included wheel as a library, or use `%pip install /path/to/flowquality-0.1.0-py3-none-any.whl`.
Do not install the `spark` extra on a managed runtime: use its provided PySpark.
Local verification targets Spark 3.5.3; Databricks, Spark Connect, and Spark 4 have not been validated.

## Use before writing the silver layer

```python
from flowquality import Check, validate

report = validate(bronze_df, [
    Check('awb_required', 'not_null', ('awb',)),
    Check('awb_unique', 'unique', ('awb',)),
    Check('weight_nonnegative', 'range', ('weight_kg',), minimum=0),
    Check('valid_status', 'allowed_values', ('status',),
          values=('ACCEPTED', 'DEPARTED', 'DELIVERED'), severity='warning'),
    Check('weight_type', 'type', ('weight_kg',), expected_type='double'),
    Check('volume', 'row_count', minimum=1),
], dataset='cargo_bronze')

print(report.to_json())
report.raise_for_errors()  # QualityError includes .report
# Only after this gate, perform your existing silver write.
```

Rules can live in JSON, independent of notebook code:

```python
import json
with open('examples/rules.json') as handle:
    checks = [Check.from_dict(item) for item in json.load(handle)]
report = validate(df, checks)
```

## Rule semantics

| Rule | Meaning |
| --- | --- |
| `not_null` | SQL null and float/double NaN fail; empty strings do not |
| `unique` | All rows in duplicate key groups fail, not just extra copies; null/NaN keys also fail by default |
| `allowed_values` | Non-null values must belong to the set; compatible scalar types required |
| `range` | Numeric inclusive bounds; null/NaN fails by default; no string-to-number casting |
| `type` | Exact schema `simpleString()` match, e.g. `bigint`, `decimal(10,2)`; no row casting |
| `row_count` | Inclusive nonnegative integer bounds on dataset size |

- `max_failure_rate=0.01` permits at most 1% failed eligible rows, including the boundary. Applies only to row-level rules.
- `ignore_nulls=True` excludes null/NaN from both numerator and denominator for range, allowed-values, and unique rules. For composite keys, any missing key component excludes that row.
- Zero eligible rows yields `skip`, not a false assertion that data passed. Add a `not_null` rule when completeness matters.
- Empty datasets fail by default. Set `empty_policy='allow'` explicitly to permit them; row-count and schema rules still apply.
- `warning` results appear in reports but do not fail the gate. Missing columns and incompatible types are `error` results; warning severity still makes them nonblocking.
- Invalid rule configurations raise before evaluation. Spark execution failures propagate; they are never converted into a passing result.
- Rule names must be unique. Column names are literal top-level names, including names with dots/backticks. Nested paths are not supported. Case-ambiguous column names are rejected.
- Type and volume results report the observation in `message`; their row-failure counters remain zero because they are not row-level checks.

## Execution and operational boundaries

Simple row checks and dataset count share one aggregate action. Each uniqueness rule needs an additional groupBy and aggregation, including a shuffle. This is **not** a single scan for every rule and has not been benchmarked at scale. Only aggregate counts are returned to the driver; no source records or values are sampled into the report.

Validation does not cache, mutate, quarantine, or write source data. Because multiple actions are involved, use a stable snapshot (for example, a fixed Delta table version) for consistent metrics and downstream writes. The caller owns caching/unpersisting if needed.

For Structured Streaming, call `validate(batch_df, ...)` inside `foreachBatch`. Uniqueness then applies only within that microbatch, not across the stream. The library provides no checkpointing, idempotency, or exactly-once sink guarantees.

Reports include dataset and rule names, counts, rates, statuses, and a UTC timestamp. Store reports in your approved audit sink; names and metrics may still be sensitive. No telemetry or network calls are made by the library.

## Scope of the first release

Included: six rule types, composite keys, warning/error gates, JSON configuration and output, synthetic cargo example, tests, wheel packaging, and a CI workflow.

Next: row-level quarantine with reason codes; dataset reconciliation and referential integrity; Delta audit sink; performance benchmarks; broader Spark/Databricks compatibility. These are not implemented in 0.1.0.

No license has been chosen yet. Select one before distributing this as an open-source package.

## Spark references

- https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/dataframe.html
- https://spark.apache.org/docs/3.5.7/sql-ref-null-semantics.html
