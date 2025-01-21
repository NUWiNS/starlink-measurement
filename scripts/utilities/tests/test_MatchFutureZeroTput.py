
import unittest
import pandas as pd

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.utilities.trace_sync import MatchFutureNearestZeroTput

class UnittestMatchFutureNearestZeroTput(unittest.TestCase):
    def setUp(self):
        self.matcher = MatchFutureNearestZeroTput(
            base_dir="test_dir",
            field_id="id",
            field_data="tput",
            field_time="time"
        )

    def test_empty_dataframes(self):
        """Test with empty dataframes"""
        ref_df = pd.DataFrame(columns=["id", "time", "tput"])
        target_df = pd.DataFrame(columns=["id", "time", "tput"])
        res = self.matcher.match_future_nearest_zero_tput(ref_df, target_df)
        idx_pairs, time_pairs = res
        self.assertEqual(len(idx_pairs), 0)
        self.assertEqual(len(time_pairs), 0)

    def test_no_zero_throughput(self):
        """Test when there are no zero throughput points"""
        ref_df = pd.DataFrame({
            "id": [1, 2],
            "time": ["2024-01-01 10:00:00", "2024-01-01 10:01:00"],
            "tput": [1.0, 2.0]
        })
        target_df = pd.DataFrame({
            "id": [3, 4],
            "time": ["2024-01-01 10:00:00", "2024-01-01 10:01:00"],
            "tput": [1.0, 2.0]
        })
        idx_pairs, time_pairs = self.matcher.match_future_nearest_zero_tput(ref_df, target_df)
        self.assertEqual(len(idx_pairs), 0)
        self.assertEqual(len(time_pairs), 0)

    def test_basic_matching(self):
        """Test basic matching with future zero throughput points"""
        ref_df = pd.DataFrame({
            "id": [1, 2, 3],
            "time": ["2024-01-01 10:00:00", "2024-01-01 10:01:00", "2024-01-01 10:02:00"],
            "tput": [0.0, 1.0, 0.0]
        })
        target_df = pd.DataFrame({
            "id": [4, 5, 6],
            "time": ["2024-01-01 10:00:30", "2024-01-01 10:01:30", "2024-01-01 10:02:30"],
            "tput": [0.0, 0.0, 1.0]
        })
        idx_pairs, time_pairs = self.matcher.match_future_nearest_zero_tput(ref_df, target_df)
        
        # First zero point in ref (id=1) should match with first zero point in target (id=4)
        # Second zero point in ref (id=3) should match with second zero point in target (id=5)
        expected_idx_pairs = [(1, 4), (3, None)]
        self.assertEqual(idx_pairs, expected_idx_pairs)
        self.assertEqual(len(time_pairs), 2)
        expected_time_pairs = [
            (pd.Timestamp('2024-01-01 10:00:00'), pd.Timestamp('2024-01-01 10:00:30')),
            (pd.Timestamp('2024-01-01 10:02:00'), pd.Timestamp('2024-01-01 10:02:30')), # matched with max time
        ]
        self.assertEqual(time_pairs, expected_time_pairs)

    def test_no_future_match(self):
        """Test when there are no future zero throughput points"""
        ref_df = pd.DataFrame({
            "id": [1, 2],
            "time": ["2024-01-01 10:02:00", "2024-01-01 10:03:00"],
            "tput": [0.0, 0.0]
        })
        target_df = pd.DataFrame({
            "id": [3, 4],
            "time": ["2024-01-01 10:00:00", "2024-01-01 10:01:00"],
            "tput": [0.0, 0.0]
        })
        idx_pairs, time_pairs = self.matcher.match_future_nearest_zero_tput(ref_df, target_df)
        
        # Both ref points should have None as target since no future matches exist
        expected_idx_pairs = [(1, None), (2, None)]
        self.assertEqual(idx_pairs, expected_idx_pairs)
        self.assertEqual(len(time_pairs), 2)
        expected_time_pairs = [
            (pd.Timestamp('2024-01-01 10:02:00'), pd.Timestamp('2024-01-01 10:03:00')),
            (pd.Timestamp('2024-01-01 10:03:00'), pd.Timestamp('2024-01-01 10:03:00')),
        ]
        self.assertEqual(time_pairs, expected_time_pairs)

if __name__ == '__main__':
    unittest.main(verbosity=2)
