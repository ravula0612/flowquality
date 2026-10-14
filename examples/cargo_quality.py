"""Synthetic data only. Run: python examples/cargo_quality.py"""
from pyspark.sql import SparkSession
from flowquality import Check, validate

spark = SparkSession.builder.master("local[2]").appName("cargo-quality").getOrCreate()
df = spark.createDataFrame([
    ("AWB001", 120.5, "ACCEPTED"),
    ("AWB002", -5.0, "UNKNOWN"),
    ("AWB002", 42.0, "DEPARTED"),
    (None, 10.0, "DELIVERED"),
], "awb string, weight_kg double, status string")
report = validate(df, [
    Check("awb_required", "not_null", ("awb",)),
    Check("awb_unique", "unique", ("awb",)),
    Check("weight_nonnegative", "range", ("weight_kg",), minimum=0),
    Check("known_status", "allowed_values", ("status",),
          values=("ACCEPTED", "DEPARTED", "DELIVERED"), severity="warning"),
    Check("nonempty", "row_count", minimum=1),
], dataset="synthetic_cargo")
print(report.to_json())
spark.stop()
# In a pipeline, call report.raise_for_errors() before publishing data.
