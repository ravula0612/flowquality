import json
import unittest

from pyspark.sql import SparkSession
from flowquality import Check, QualityError, validate


class QualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("flowquality-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_cargo_gate_and_report(self):
        df = self.spark.createDataFrame([(1, 10.0, "DEL"), (1, -2.0, "XXX"), (None, None, None)],
                                        "id long, weight double, status string")
        report = validate(df, [Check("id", "not_null", ("id",)),
                               Check("duplicate", "unique", ("id",)),
                               Check("weight", "range", ("weight",), minimum=0),
                               Check("status", "allowed_values", ("status",), values=("DEL",), severity="warning"),
                               Check("schema", "type", ("id",), expected_type="bigint"),
                               Check("volume", "row_count", minimum=3)])
        self.assertEqual([r.failed_rows for r in report.results[:4]], [1, 3, 2, 2])
        self.assertEqual(report.results[2].failure_rate, 2 / 3)
        self.assertFalse(json.loads(report.to_json())["passed"])
        with self.assertRaises(QualityError) as error:
            report.raise_for_errors()
        self.assertIs(error.exception.report, report)

    def test_null_exclusion_nan_and_threshold(self):
        df = self.spark.createDataFrame([(1.0,), (float("nan"),), (None,), (-1.0,)], "x double")
        report = validate(df, [Check("null", "not_null", ("x",), max_failure_rate=0.5),
                               Check("range", "range", ("x",), minimum=0, ignore_nulls=True, max_failure_rate=0.5)])
        self.assertTrue(report.passed)
        self.assertEqual([(r.evaluated_rows, r.failed_rows) for r in report.results], [(4, 2), (2, 1)])

    def test_unique_composite_and_literal_names(self):
        df = self.spark.createDataFrame([(1, "a"), (1, "a"), (1, "b"), (None, "a")], ["a.b", "count"])
        report = validate(df, [Check("key", "unique", ("a.b", "count"), ignore_nulls=True)])
        self.assertEqual(report.results[0].evaluated_rows, 3)
        self.assertEqual(report.results[0].failed_rows, 2)

    def test_empty_is_not_silent_success(self):
        df = self.spark.createDataFrame([], "id long")
        checks = [Check("id", "not_null", ("id",)), Check("key", "unique", ("id",))]
        self.assertFalse(validate(df, checks).passed)
        report = validate(df, checks, empty_policy="allow")
        self.assertTrue(report.passed)
        self.assertEqual([r.status for r in report.results], ["skip", "skip"])

    def test_warnings_missing_and_schema(self):
        df = self.spark.createDataFrame([("bad",)], "id string")
        checks = [Check("absent", "not_null", ("absent",), severity="warning"),
                  Check("schema", "type", ("id",), expected_type="bigint", severity="warning")]
        report = validate(df, checks)
        self.assertTrue(report.passed)
        self.assertEqual([r.status for r in report.results], ["error", "fail"])

    def test_invalid_type_never_cast(self):
        df = self.spark.createDataFrame([("12",)], "id string")
        report = validate(df, [Check("range", "range", ("id",), minimum=0),
                               Check("set", "allowed_values", ("id",), values=(12,))])
        self.assertEqual([r.status for r in report.results], ["error", "error"])
        self.assertFalse(report.passed)

    def test_all_null_excluded_is_skip(self):
        df = self.spark.createDataFrame([(None,)], "id long")
        report = validate(df, [Check("key", "unique", ("id",), ignore_nulls=True)])
        self.assertEqual(report.results[0].status, "skip")

    def test_invalid_configuration(self):
        with self.assertRaises(ValueError):
            Check("status", "allowed_values", ("status",), values="ACCEPTED")
        with self.assertRaises(ValueError):
            Check("__empty_dataset__", "row_count", minimum=0)
        for kwargs in ({"max_failure_rate": float("nan")}, {"max_failure_rate": 2}, {"ignore_nulls": True}):
            with self.assertRaises(ValueError):
                Check("id", "not_null", ("id",), **kwargs)
        with self.assertRaises(ValueError):
            Check("r", "range", ("id",), minimum=10, maximum=0)
        with self.assertRaises(ValueError):
            Check("v", "row_count", minimum=1.5)
        with self.assertRaises(TypeError):
            Check.from_dict({"name": "x", "kind": "not_null", "typo": 3})

    def test_duplicate_check_names_and_streaming(self):
        df = self.spark.createDataFrame([(1,)], "id long")
        check = Check("id", "not_null", ("id",))
        with self.assertRaises(ValueError):
            validate(df, [check, check])
        stream = self.spark.readStream.format("rate").load()
        with self.assertRaisesRegex(ValueError, "foreachBatch"):
            validate(stream, [check])

    def test_clean_data_passes_every_rule_and_gate(self):
        df = self.spark.createDataFrame([(1, 0.0, "A"), (2, 100.0, "B")],
                                        "id long, amount double, status string")
        report = validate(df, [Check("id", "not_null", ("id",)),
                               Check("key", "unique", ("id",)),
                               Check("bounds", "range", ("amount",), minimum=0, maximum=100),
                               Check("set", "allowed_values", ("status",), values=("A", "B")),
                               Check("type", "type", ("id",), expected_type="bigint"),
                               Check("size", "row_count", minimum=2, maximum=2)])
        self.assertTrue(report.passed)
        self.assertTrue(all(r.status == "pass" for r in report.results))
        self.assertIs(report.raise_for_errors(), report)

    def test_volume_failure_blocks_gate(self):
        df = self.spark.createDataFrame([(1,)], "id long")
        with self.assertRaises(QualityError):
            validate(df, [Check("size", "row_count", minimum=2)]).raise_for_errors()

    def test_boolean_set_and_backtick_name(self):
        df = self.spark.createDataFrame([(True,), (False,), (None,)], ["a`b"])
        result = validate(df, [Check("set", "allowed_values", ("a`b",), values=(True,))]).results[0]
        self.assertEqual((result.evaluated_rows, result.failed_rows), (3, 2))


if __name__ == "__main__":
    unittest.main()
