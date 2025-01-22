import os
import sys
import unittest
import pandas as pd
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.cell_leo_in_remote_us.inter_operator_coherent_time.main import InterOperatorCoherentTime

class TestInterOperatorCoherentTime(unittest.TestCase):
    def setUp(self):
        self.coherent_time = InterOperatorCoherentTime(
            base_dir="dummy_dir",
            data_field="throughput",
            time_field="timestamp",
        )

    def test_get_coherent_time_positive_threshold(self):
        # Create test data where A has higher throughput than B in specific regions
        data = {
            'A_throughput': [10, 10, 10, 2, 2, 8, 8, 8, 2],
            'B_throughput': [2, 2, 2, 2, 2, 2, 2, 2, 2],
            'A_timestamp': ['2023-01-01T00:00:00', '2023-01-01T00:00:01', '2023-01-01T00:00:02', 
                         '2023-01-01T00:00:03', '2023-01-01T00:00:04', '2023-01-01T00:00:05',
                         '2023-01-01T00:00:06', '2023-01-01T00:00:07', '2023-01-01T00:00:08']
        }
        df = pd.DataFrame(data)
        df['diff_throughput'] = df['A_throughput'] - df['B_throughput']
        
        # Test with threshold of 5
        threshold = 5
        coherent_groups = self.coherent_time.get_coherent_time_above_threshold_of_two_operators(df, threshold)
        
        # Should find two groups:
        # First group: indices 0-2 where diff is 8
        # Second group: indices 5-7 where diff is 6
        self.assertEqual(len(coherent_groups), 2)
        self.assertEqual(len(coherent_groups[0]), 3)  # First group has 3 points
        self.assertEqual(len(coherent_groups[1]), 3)  # Second group has 3 points
        
        # Verify the values in the groups
        self.assertTrue(all(coherent_groups[0]['diff_throughput'] == 8))
        self.assertTrue(all(coherent_groups[1]['diff_throughput'] == 6))

    def test_get_coherent_time_negative_threshold(self):
        # Create test data where B has higher throughput than A in specific regions
        data = {
            'A_throughput': [2, 2, 2, 5, 5, 2, 2, 2],
            'B_throughput': [10, 10, 10, 5, 5, 8, 8, 8],
            'A_timestamp': ['2023-01-01T00:00:00', '2023-01-01T00:00:01', '2023-01-01T00:00:02', 
                         '2023-01-01T00:00:03', '2023-01-01T00:00:04', '2023-01-01T00:00:05',
                         '2023-01-01T00:00:06', '2023-01-01T00:00:07']
        }
        df = pd.DataFrame(data)
        df['diff_throughput'] = df['A_throughput'] - df['B_throughput']
        
        # Test with threshold of 5
        threshold = 5
        coherent_groups = self.coherent_time.get_coherent_time_above_threshold_of_two_operators(df, threshold)
        
        # Should find two groups:
        # First group: indices 0-2 where diff is -8
        # Second group: indices 5-7 where diff is -6
        self.assertEqual(len(coherent_groups), 2)
        self.assertEqual(len(coherent_groups[0]), 3)  # First group has 3 points
        self.assertEqual(len(coherent_groups[1]), 3)  # Second group has 3 points
        
        # Verify the values in the groups
        self.assertTrue(all(coherent_groups[0]['diff_throughput'] == -8))
        self.assertTrue(all(coherent_groups[1]['diff_throughput'] == -6))

    def test_get_coherent_time_no_threshold_exceeded(self):
        # Create test data where differences never exceed threshold
        data = {
            'A_throughput': [3, 4, 3, 4, 3],
            'B_throughput': [2, 2, 2, 2, 2],
            'A_timestamp': ['2023-01-01T00:00:00', '2023-01-01T00:00:01', '2023-01-01T00:00:02', 
                         '2023-01-01T00:00:03', '2023-01-01T00:00:04']
        }
        df = pd.DataFrame(data)
        df['diff_throughput'] = df['A_throughput'] - df['B_throughput']
        
        # Test with threshold of 5
        threshold = 5
        coherent_groups = self.coherent_time.get_coherent_time_above_threshold_of_two_operators(df, threshold)
        
        # Should find no groups as no differences exceed threshold
        self.assertEqual(len(coherent_groups), 0)

    def test_get_coherent_time_durations(self):
        # Create test data with specific timestamps
        data = {
            'A_throughput': [10, 10, 10, 2, 2, 8, 8, 8],
            'B_throughput': [2, 2, 2, 2, 2, 2, 2, 2],
            'A_timestamp': ['2023-01-01T00:00:00', '2023-01-01T00:00:01', '2023-01-01T00:00:02', 
                         '2023-01-01T00:00:03', '2023-01-01T00:00:04', '2023-01-01T00:00:10',
                         '2023-01-01T00:00:11', '2023-01-01T00:00:12']  # Non-continuous timestamps
        }
        df = pd.DataFrame(data)
        df['diff_throughput'] = df['A_throughput'] - df['B_throughput']
        
        threshold = 5
        coherent_groups = self.coherent_time.get_coherent_time_above_threshold_of_two_operators(df, threshold)
        durations = self.coherent_time.get_coherent_time_durations(coherent_groups)
        
        # Should find two durations:
        # First group: timestamps 0-2 (duration = 2)
        # Second group: timestamps 10-12 (duration = 2)
        self.assertEqual(len(durations), 2)
        self.assertEqual(durations[0], 2)  # First group spans 2 seconds (0 to 2)
        self.assertEqual(durations[1], 2)  # Second group spans 2 seconds (10 to 12)

if __name__ == '__main__':
    unittest.main() 