import glob
from os import path
import re
import unittest
from unittest.mock import patch, mock_open


def extract_period_from_file(file: str) -> tuple[str, str]:
    with open(file, 'r') as f:
        lines = f.readlines()
        start_time_row = lines[0].strip()
        end_time_row = lines[-1].strip() 
        # use regex to extract the timestamp
        start_match = re.search(r'Start time: (\d+)', start_time_row)
        end_match = re.search(r'End time: (\d+)', end_time_row)
        if start_match and end_match:
            return (start_match.group(1), end_match.group(1))
        else:
            raise ValueError(f'Failed to extract start and end time from {file}')


def collect_periods_of_tput_measurements(base_dir: str, protocol: str, direction: str) -> list[tuple[str, str]]:
    # find all the files starting with {protocol}_{direction}*.out
    pattern = f'{protocol}_{direction}*.out'
    files = glob.glob(path.join(base_dir, pattern))

    periods = []
    # read each file and extract the first and last line as the start and end timestamp
    for file in files:
        try:
            period = extract_period_from_file(file)
            periods.append(period)
        except ValueError as e:
            print(f"Warning: {str(e)}")

    return periods


class TestCollectPeriodsOfTputMeasurements(unittest.TestCase):
    @patch('glob.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_collect_periods_of_tput_measurements(self, mock_file, mock_glob):
        # Mock the glob.glob to return a list of files
        mock_glob.return_value = ['file1.out', 'file2.out']
        
        # Mock the file content
        mock_file.return_value.__enter__.return_value.readlines.side_effect = [
            ['Start time: 1000\n', 'Some content\n', 'End time: 2000\n'],
            ['Start time: 3000\n', 'Some content\n', 'End time: 4000\n']
        ]
        
        # Call the function
        result = collect_periods_of_tput_measurements('/base/dir', 'tcp', 'downlink')
        
        # Assert the result
        self.assertEqual(result, [('1000', '2000'), ('3000', '4000')])
        
        # Assert that glob was called with the correct pattern
        mock_glob.assert_called_once_with('/base/dir/tcp_downlink*.out')
        
        # Assert that open was called for each file
        mock_file.assert_any_call('file1.out', 'r')
        mock_file.assert_any_call('file2.out', 'r')

    @patch('glob.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_collect_periods_of_tput_measurements_value_error(self, mock_file, mock_glob):
        # Mock the glob.glob to return a list of files
        mock_glob.return_value = ['file1.out', 'file2.out']
        
        # Mock the file content with one valid and one invalid file
        mock_file.return_value.__enter__.return_value.readlines.side_effect = [
            ['Invalid start time\n', 'Some content\n', 'Invalid end time\n'],
            ['Start time: 3000\n', 'Some content\n', 'End time: 4000\n']
        ]
        
        # Capture print output
        with patch('builtins.print') as mock_print:
            result = collect_periods_of_tput_measurements('/base/dir', 'tcp', 'downlink')
        
        # Assert that we got the valid result and a warning was printed
        self.assertEqual(result, [('3000', '4000')])
        mock_print.assert_called_once_with("Warning: Failed to extract start and end time from file1.out")

if __name__ == '__main__':
    unittest.main()
