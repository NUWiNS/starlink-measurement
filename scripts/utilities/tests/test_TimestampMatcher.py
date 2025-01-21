import unittest

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.utilities.trace_sync import TimestampMatcher


class TestTimestampMatcher(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.matcher = TimestampMatcher(threshold_seconds=600)
        
        # Test case data
        self.list_a = [
            "20250118_000000",  # should be unmatched (not closest to any)
            "20250118_000900",  # should match with 000800
            "20250118_002800",  # should match with 002900
            "20250118_004000",  # should be unmatched (beyond threshold)
            "20250118_005100"   # should match with 005200
        ]
        
        self.list_b = [
            "20250118_000800",  # should match with 000900
            "20250118_002900",  # should match with 002800
            "20250118_005200",   # should match with 005100
            "20250118_005300"   # should be unmatched (not closest to any)
        ]

    def test_convert_to_seconds(self):
        """Test timestamp conversion to seconds"""
        # Test regular case
        res = self.matcher.convert_to_timestamp("20250118_010000")
        self.assertEqual(res, 1737180000)

    def test_invalid_timestamp_format(self):
        """Test handling of invalid timestamp formats"""
        with self.assertRaises(ValueError):
            self.matcher.convert_to_seconds("invalid_format")

    def test_matching_basic(self):
        """Test basic matching functionality"""
        matched_pairs, leftover_a, leftover_b = self.matcher.match_datetimes(self.list_a, self.list_b)
        
        # Test number of matches
        self.assertEqual(len(matched_pairs), 3)
        
        # Test correct pairs are matched
        expected_pairs = [
            ("20250118_000900", "20250118_000800"),
            ("20250118_002800", "20250118_002900"),
            ("20250118_005100", "20250118_005200")
        ]
        self.assertEqual(sorted(matched_pairs), sorted(expected_pairs))

    def test_matching_cross_dates(self):
        """Test matching timestamps across different dates"""
        list_a = ["20250118_000000", "20250118_000900"]
        list_b = ["20250119_000800", "20250119_002900"]
        matched_pairs, leftover_a, leftover_b = self.matcher.match_datetimes(list_a, list_b)
        self.assertEqual(len(matched_pairs), 0)
        self.assertEqual(len(leftover_a), 2)
        self.assertEqual(len(leftover_b), 2)

    def test_leftover_timestamps(self):
        """Test unmatched timestamps"""
        matched_pairs, leftover_a, leftover_b = self.matcher.match_datetimes(self.list_a, self.list_b)
        
        # Test leftover timestamps from list A
        expected_leftover_a = [
            "20250118_000000",
            "20250118_004000",
        ]
        self.assertEqual(sorted(leftover_a), sorted(expected_leftover_a))
        
        # Test leftover timestamps from list B
        expected_leftover_b = ["20250118_005300"]
        self.assertEqual(sorted(leftover_b), sorted(expected_leftover_b))

    def test_threshold_boundary(self):
        """Test matching behavior at threshold boundaries"""
        # Create timestamps exactly 10 minutes apart
        list_a = ["20250118_000000"]
        list_b = ["20250118_001000"]  # 10 minutes later
        
        # Test with 600-second threshold (should match)
        matcher_600 = TimestampMatcher(threshold_seconds=600)
        matched_pairs, _, _ = matcher_600.match_datetimes(list_a, list_b)
        self.assertEqual(len(matched_pairs), 1)
        
        # Test with 9-minute threshold (should not match)
        matcher_540 = TimestampMatcher(threshold_seconds=540)
        matched_pairs, leftover_a, leftover_b = matcher_540.match_datetimes(list_a, list_b)
        self.assertEqual(len(matched_pairs), 0)
        self.assertEqual(len(leftover_a), 1)
        self.assertEqual(len(leftover_b), 1)

    def test_empty_lists(self):
        """Test behavior with empty lists"""
        # Both empty
        matched_pairs, leftover_a, leftover_b = self.matcher.match_datetimes([], [])
        self.assertEqual(len(matched_pairs), 0)
        self.assertEqual(len(leftover_a), 0)
        self.assertEqual(len(leftover_b), 0)
        
        # One empty
        matched_pairs, leftover_a, leftover_b = self.matcher.match_datetimes(self.list_a, [])
        self.assertEqual(len(matched_pairs), 0)
        self.assertEqual(len(leftover_a), len(self.list_a))
        self.assertEqual(len(leftover_b), 0)

    def test_one_to_one_matching(self):
        """Test that each timestamp is matched at most once"""
        # Create list with duplicate timestamps
        list_a = ["20250118_000000", "20250118_000000"]
        list_b = ["20250118_000000"]
        
        matched_pairs, leftover_a, leftover_b = self.matcher.match_datetimes(list_a, list_b)
        
        # Should only match one pair
        self.assertEqual(len(matched_pairs), 1)
        self.assertEqual(len(leftover_a), 1)
        self.assertEqual(len(leftover_b), 0)

    def test_closest_match_priority(self):
        """Test that closest timestamps are matched first"""
        list_a = ["20250118_000500"]  # 5 minutes
        list_b = ["20250118_000000", "20250118_000900"]  # 0 and 9 minutes
        
        matched_pairs, _, _ = self.matcher.match_datetimes(list_a, list_b)
        
        # Should match with 000900 as it's closer (4 minutes difference vs 5 minutes)
        self.assertEqual(matched_pairs[0], ("20250118_000500", "20250118_000900"))

if __name__ == '__main__':
    unittest.main(verbosity=2)
