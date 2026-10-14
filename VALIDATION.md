# Validation of 0.1.0

Verified on 2026-10-14 in a fresh virtual environment with Python 3.12.14, Apache Spark 3.5.3, and OpenJDK 17.

- Source suite: **12 tests passed** in 9.89 seconds.
- Built wheel installed without source dependencies; import verified from site-packages.
- Installed-wheel suite: **12 tests passed** in 10.19 seconds, with pytest source-path injection disabled.
- Synthetic cargo example: completed successfully. Verified 4 rows, a failing gate, and exact failure counts of 1 missing AWB, 3 invalid/duplicate AWB rows, 1 negative weight, and 1 invalid status.
- Build: source distribution and Python wheel produced successfully.

Tests cover clean data passing all six rules, failing quality gates, aggregate failure counts, composite duplicates, literal dotted/backtick column names, boolean allowed values, null and NaN semantics, tolerance boundaries, empty datasets, skipped checks, warning gates, schema mismatches, invalid configurations, and direct-streaming rejection.

Review fixes: reject a bare string for allowed values rather than splitting it into characters; reserve the internal empty-dataset rule name; include examples and validation notes in source distributions.

Commands:

```bash
SPARK_LOCAL_IP=127.0.0.1 python -m pytest -q --tb=short
python -m build
python -m pip install --no-deps dist/flowquality-0.1.0-py3-none-any.whl
# From outside the source tree, after wheel installation:
SPARK_LOCAL_IP=127.0.0.1 python -m pytest flowquality/tests -q --override-ini pythonpath=
```

The loopback override was necessary for local Spark networking. Runtime logged a non-fatal native Hadoop fallback warning. No production datasets, Databricks runtime, Spark Connect, Spark 4, or large-scale performance tests were used. CI is provided separately from these local results.
