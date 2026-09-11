import unittest
import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline import (
    ParseAndValidateTelemetryFn,
    TelemetryCombineFn,
    FilterAndFormatAlertsFn,
)


class ParseAndValidateTelemetryFnTest(unittest.TestCase):
    def test_valid_payload_parses_to_main_output(self):
        valid = json.dumps({
            "machine_id": "MCH-01",
            "temperature": 75.0,
            "vibration": 2.0,
            "timestamp": "2026-01-01T00:00:00Z",
        }).encode("utf-8")

        with TestPipeline() as p:
            results = (
                p
                | beam.Create([valid])
                | beam.ParDo(ParseAndValidateTelemetryFn()).with_outputs(
                    ParseAndValidateTelemetryFn.TAG_DLQ, main="valid"
                )
            )
            assert_that(
                results["valid"],
                equal_to([{
                    "machine_id": "MCH-01",
                    "temperature": 75.0,
                    "vibration": 2.0,
                    "timestamp": "2026-01-01T00:00:00Z",
                }]),
                label="valid_check",
            )
            assert_that(results[ParseAndValidateTelemetryFn.TAG_DLQ], equal_to([]), label="dlq_check")

    def test_malformed_json_routes_to_dlq(self):
        bad = b"{not valid json"
        with TestPipeline() as p:
            results = (
                p
                | beam.Create([bad])
                | beam.ParDo(ParseAndValidateTelemetryFn()).with_outputs(
                    ParseAndValidateTelemetryFn.TAG_DLQ, main="valid"
                )
            )
            assert_that(results["valid"], equal_to([]), label="valid_check")
            assert_that(results[ParseAndValidateTelemetryFn.TAG_DLQ], equal_to([bad]), label="dlq_check")

    def test_missing_required_field_routes_to_dlq(self):
        # vibration deliberately omitted
        missing_field = json.dumps({
            "machine_id": "MCH-01",
            "temperature": 75.0,
            "timestamp": "2026-01-01T00:00:00Z",
        }).encode("utf-8")
        with TestPipeline() as p:
            results = (
                p
                | beam.Create([missing_field])
                | beam.ParDo(ParseAndValidateTelemetryFn()).with_outputs(
                    ParseAndValidateTelemetryFn.TAG_DLQ, main="valid"
                )
            )
            assert_that(results["valid"], equal_to([]), label="valid_check")


class TelemetryCombineFnTest(unittest.TestCase):
    def test_averages_computed_correctly(self):
        combine_fn = TelemetryCombineFn()
        acc = combine_fn.create_accumulator()
        acc = combine_fn.add_input(acc, {"temperature": 70.0, "vibration": 2.0})
        acc = combine_fn.add_input(acc, {"temperature": 80.0, "vibration": 4.0})
        result = combine_fn.extract_output(acc)
        self.assertAlmostEqual(result["avg_temperature"], 75.0)
        self.assertAlmostEqual(result["avg_vibration"], 3.0)

    def test_merge_accumulators_combines_correctly(self):
        combine_fn = TelemetryCombineFn()
        acc1 = (70.0, 1, 2.0, 1)
        acc2 = (80.0, 1, 4.0, 1)
        merged = combine_fn.merge_accumulators([acc1, acc2])
        result = combine_fn.extract_output(merged)
        self.assertAlmostEqual(result["avg_temperature"], 75.0)
        self.assertAlmostEqual(result["avg_vibration"], 3.0)

    def test_empty_accumulator_returns_zero(self):
        combine_fn = TelemetryCombineFn()
        result = combine_fn.extract_output(combine_fn.create_accumulator())
        self.assertEqual(result["avg_temperature"], 0.0)
        self.assertEqual(result["avg_vibration"], 0.0)


class FilterAndFormatAlertsFnTest(unittest.TestCase):
    """Covers the OR-threshold logic directly — this is the rule that
    controls whether Gemini ever gets called, worth pinning explicitly."""

    def _breach_element(self, avg_temperature, avg_vibration):
        return {
            "machine_id": "MCH-01",
            "avg_temperature": avg_temperature,
            "avg_vibration": avg_vibration,
            "window_end": "2026-01-01T00:01:00Z",
        }

    def test_temperature_alone_over_threshold_triggers_alert(self):
        fn = FilterAndFormatAlertsFn()
        outputs = list(fn.process(self._breach_element(85.0, 2.0)))
        self.assertEqual(len(outputs), 1)

    def test_vibration_alone_over_threshold_triggers_alert(self):
        fn = FilterAndFormatAlertsFn()
        outputs = list(fn.process(self._breach_element(70.0, 6.0)))
        self.assertEqual(len(outputs), 1)

    def test_both_over_threshold_still_triggers_exactly_one_alert(self):
        fn = FilterAndFormatAlertsFn()
        outputs = list(fn.process(self._breach_element(90.0, 8.0)))
        self.assertEqual(len(outputs), 1)

    def test_neither_over_threshold_no_alert(self):
        fn = FilterAndFormatAlertsFn()
        outputs = list(fn.process(self._breach_element(70.0, 2.0)))
        self.assertEqual(len(outputs), 0)

    def test_exactly_at_threshold_is_not_a_breach(self):
        # Strict > comparison, not >= — pin the boundary explicitly
        fn = FilterAndFormatAlertsFn()
        outputs = list(fn.process(self._breach_element(80.0, 5.0)))
        self.assertEqual(len(outputs), 0)


if __name__ == "__main__":
    unittest.main()
