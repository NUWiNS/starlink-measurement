import unittest
from unittest.mock import patch, mock_open, MagicMock
import pandas as pd
from scripts.validations.validate_data_points import validate_data_points

class TestValidateDataPoints(unittest.TestCase):

    def setUp(self):
        self.sample_log = """
2024-10-18 15:43:00 - metrics - INFO - [start processing] /path/to/file1.out
2024-10-18 15:43:00 - metrics - INFO - -- [estimating data points] 280 (start_time: 2024-06-21 15:54:10.362000-08:00, end_time: 2024-06-21 15:56:30.402000-08:00)
2024-10-18 15:43:00 - metrics - INFO - -- [check validity] It is a Status.TIMEOUT result
2024-10-18 15:43:00 - metrics - INFO - -- [extracted data points] 279, diff from estiamte: -1, diff from expected: 39
2024-10-18 15:43:00 - metrics - INFO - -- [start postprocessing]
2024-10-18 15:43:00 - metrics - INFO - -- [start auto_complete_data_points] before: 279
2024-10-18 15:43:00 - metrics - INFO - -- [end auto_complete_data_points] after: 279
2024-10-18 15:43:00 - metrics - INFO - -- [end postprocessing]
2024-10-18 15:43:00 - metrics - INFO - [end processing] /path/to/file1.out


2024-10-18 15:43:00 - metrics - INFO - [start processing] /path/to/file2.out
2024-10-18 15:43:00 - metrics - INFO - -- [estimating data points] 240 (start_time: 2024-06-21 15:54:10.362000-08:00, end_time: 2024-06-21 15:56:30.402000-08:00)
2024-10-18 15:43:00 - metrics - INFO - -- [check validity] It is a Status.TIMEOUT result
2024-10-18 15:43:00 - metrics - INFO - -- [extracted data points] 210, diff from estiamte: -30, diff from expected: 39
2024-10-18 15:43:00 - metrics - INFO - -- [start postprocessing]
2024-10-18 15:43:00 - metrics - INFO - -- [start auto_complete_data_points] before: 210
2024-10-18 15:43:00 - metrics - INFO - -- [end auto_complete_data_points] after: 240
2024-10-18 15:43:00 - metrics - INFO - -- [end postprocessing]
2024-10-18 15:43:00 - metrics - INFO - [end processing] /path/to/file2.out
"""

    @patch('builtins.open', new_callable=mock_open)
    def test_validate_data_points(self, mock_file):
        mock_file.return_value.__enter__.return_value.read.return_value = self.sample_log
        mock_logger = MagicMock()

        result = validate_data_points('dummy_path.log', logger=mock_logger)
        # Check if the result is a DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Check if the DataFrame has the correct number of rows
        self.assertEqual(len(result), 2)

        # Check if the DataFrame has the correct columns
        expected_columns = ['file_path', 'estimated', 'extracted', 'final']
        self.assertListEqual(list(result.columns), expected_columns)

        # Check the values for the first row
        self.assertEqual(result.iloc[0]['file_path'], '/path/to/file1.out')
        self.assertEqual(result.iloc[0]['estimated'], 280)
        self.assertEqual(result.iloc[0]['extracted'], 279)
        self.assertEqual(result.iloc[0]['final'], 279)

        # Check the values for the second row
        self.assertEqual(result.iloc[1]['file_path'], '/path/to/file2.out')
        self.assertEqual(result.iloc[1]['estimated'], 240)
        self.assertEqual(result.iloc[1]['extracted'], 210)
        self.assertEqual(result.iloc[1]['final'], 240)

        # Check the sums
        self.assertEqual(result['estimated'].sum(), 520)
        self.assertEqual(result['extracted'].sum(), 489)
        self.assertEqual(result['final'].sum(), 519)

        # Check if logger was called with correct information
        mock_logger.info.assert_any_call("Files processed: 2")
        mock_logger.info.assert_any_call("Total estimated data points: 520")
        mock_logger.info.assert_any_call("Total extracted data points: 489")
        mock_logger.info.assert_any_call("Total final data points after auto-completion: 519")


if __name__ == '__main__':
    unittest.main()
