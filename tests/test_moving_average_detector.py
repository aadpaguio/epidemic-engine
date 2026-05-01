"""Unit tests for ml/detectors.py moving-average outbreak logic."""

import unittest

import pandas as pd

from ml.detectors import moving_average_detector


class MovingAverageDetectorTest(unittest.TestCase):
    def test_spike_triggers_high_risk_after_stable_baseline(self) -> None:
        rows = []
        base_ts = pd.Timestamp("2026-04-28T12:00:00Z")
        for i in range(6):
            rows.append(
                {
                    "region": "Boston",
                    "bucket_start": base_ts + pd.Timedelta(hours=i),
                    "total_case_events": 10.0,
                    "avg_severity": 2.0,
                }
            )
        rows.append(
            {
                "region": "Boston",
                "bucket_start": base_ts + pd.Timedelta(hours=6),
                "total_case_events": 80.0,
                "avg_severity": 7.0,
            }
        )
        df = pd.DataFrame(rows)
        out = moving_average_detector(
            df, k=6, min_periods=2, z_threshold=2.0, severity_threshold=2.3
        )
        last = out.sort_values("bucket_start").iloc[-1]
        self.assertEqual(last["ma_predicted_label"], "high_risk")
        self.assertGreater(last["ma_count_z"], 2.0)

    def test_flat_series_stays_normal(self) -> None:
        base_ts = pd.Timestamp("2026-04-28T12:00:00Z")
        rows = [
            {
                "region": "Cambridge",
                "bucket_start": base_ts + pd.Timedelta(hours=i),
                "total_case_events": 10.0,
                "avg_severity": 2.0,
            }
            for i in range(8)
        ]
        df = pd.DataFrame(rows)
        out = moving_average_detector(df, k=6, min_periods=2)
        labels = set(out["ma_predicted_label"].unique())
        self.assertEqual(labels, {"normal"})

    def test_missing_columns_raises(self) -> None:
        df = pd.DataFrame({"region": ["A"], "bucket_start": [pd.Timestamp.now(tz="UTC")]})
        with self.assertRaises(ValueError) as ctx:
            moving_average_detector(df)
        self.assertIn("missing columns", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
