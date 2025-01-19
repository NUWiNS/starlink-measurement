from datetime import datetime
import glob
from os import path
import re
import unittest
from unittest.mock import patch, mock_open

import pandas as pd

from scripts.celllular_analysis.TechBreakdown import TechBreakdown
from scripts.constants import XcalField

def extract_period_from_file(file: str) -> tuple[datetime, datetime, str]:
    with open(file, 'r') as f:
        # file name is like tcp_downlink_142919618.out
        protocol, direction, _ = path.basename(file).split('_')

        lines = f.readlines()
        start_match, end_match = extract_start_end_timestamps(lines)
        if start_match and end_match:
            start_time = pd.to_datetime(pd.to_numeric(start_match.group(1)), unit='ms', utc=True)
            end_time = pd.to_datetime(pd.to_numeric(end_match.group(1)), unit='ms', utc=True)
            return (start_time, end_time, f'{protocol}_{direction}')
        else:
            raise ValueError(f'Failed to extract start and end time from {file}')
        
def extract_period_from_ping_file(file: str) -> tuple[datetime, datetime, str]:
    with open(file, 'r') as f:
        # file name is like ping_142919618.out
        protocol = 'icmp'
        direction = 'uplink'
        lines = f.readlines()
        start_match, end_match = extract_start_end_timestamps(lines)
        if start_match and end_match:
            start_time = pd.to_datetime(pd.to_numeric(start_match.group(1)), unit='ms', utc=True)
            end_time = pd.to_datetime(pd.to_numeric(end_match.group(1)), unit='ms', utc=True)
            return (start_time, end_time, f'{protocol}_{direction}')
        else:
            raise ValueError(f'Failed to extract start and end time from {file}')

def extract_start_end_timestamps(lines):
    start_time_row = lines[0].strip()
    end_time_row = lines[-1].strip() 
        # use regex to extract the timestamp
    start_match = re.search(r'Start time: (\d+)', start_time_row)
    end_match = re.search(r'End time: (\d+)', end_time_row)
    return start_match,end_match


def collect_periods_of_tput_measurements(base_dir: str, protocol: str, direction: str) -> list[tuple[datetime, datetime, str]]:
    # find all the files starting with {protocol}_{direction}*.out
    pattern = path.join(base_dir, f'**/{protocol}_{direction}*.out')

    files = glob.glob(pattern, recursive=True)

    periods = []
    # read each file and extract the first and last line as the start and end timestamp
    for file in files:
        try:
            period = extract_period_from_file(file)
            periods.append(period)
        except ValueError as e:
            print(f"Warning: {str(e)}")

    return periods


def collect_periods_of_ping_measurements(base_dir: str) -> list[tuple[datetime, datetime, str]]:
    pattern = path.join(base_dir, f'**/ping_*.out')
    files = glob.glob(pattern, recursive=True)
    periods = []
    # read each file and extract the first and last line as the start and end timestamp
    for file in files:
        try:
            period = extract_period_from_ping_file(file)
            periods.append(period)
        except ValueError as e:
            print(f"Warning: {str(e)}")

    return periods


def read_daily_xcal_data(base_dir: str, date: str, operator: str, location: str) -> pd.DataFrame:
    """
    name of xcal log looks like 20240527_ATT_MAINE_100MS.xlsx
    :date: str, YYYYMMDD
    :operator: str, att, verizon, tmobile
    :location: str, alaska, hawaii, maine
    """

    # find all files with dates that contain {Upper(operator)}_{Uppser(location)}
    filename = f'{date}_{operator.upper()}_{location.upper()}_100MS.xlsx'
    filepath = path.join(base_dir, filename)
    if not path.exists(filepath):
        raise ValueError(f'No xcal log files found for {operator} {location}')
    
    df_xcal_log = pd.read_excel(filepath)
    return df_xcal_log

def filter_xcal_logs(
        df_xcal_logs: pd.DataFrame, 
        periods: list[tuple[datetime, datetime, str]],
        xcal_timezone: str = 'US/Eastern',
        label: str = None,
    ) -> pd.DataFrame:
    """
    Filter the xcal logs to only include the specified periods.

    - Add a column 'app_layer_label' to the filtered rows to indicate the protocol and direction

    :df_xcal_logs: pd.DataFrame, the xcal logs, with a column 'TIME_STAMP' as the timestamp in Eastern time
    :periods: list of tuples, each tuple contains a start and end timestamp (UTC aware datetime objects)
    """
    # Create a temporary datetime column (from Eastern time) for filtering, and convert to UTC

    if XcalField.CUSTOM_UTC_TIME in df_xcal_logs.columns:
        df_xcal_logs[XcalField.CUSTOM_UTC_TIME] = pd.to_datetime(
            df_xcal_logs[XcalField.CUSTOM_UTC_TIME],
            errors='coerce',
            utc=True)
    else:
        df_xcal_logs[XcalField.CUSTOM_UTC_TIME] = pd.to_datetime(
            df_xcal_logs[XcalField.TIMESTAMP],
            errors='coerce').dt.tz_localize(
                xcal_timezone, 
                ambiguous='raise', 
                nonexistent='raise'
            ).dt.tz_convert('UTC')

    # drop rows with empty utc_dt
    df_xcal_logs = df_xcal_logs.dropna(subset=[XcalField.CUSTOM_UTC_TIME])

    # Initialize an empty list to store filtered rows
    filtered_rows = []

    for period in periods:
        utc_start_dt, utc_end_dt, protocol_direction = period

        # Skip periods less than 1 seconds
        if (utc_end_dt - utc_start_dt).total_seconds() < 3:
            continue

        # Ensure start and end datetimes are timezone-aware
        utc_start_dt = pd.to_datetime(utc_start_dt, utc=True)
        utc_end_dt = pd.to_datetime(utc_end_dt, utc=True)
        # Filter rows within the current period
        period_rows_df = df_xcal_logs[
            (df_xcal_logs[XcalField.CUSTOM_UTC_TIME] >= utc_start_dt) & 
            (df_xcal_logs[XcalField.CUSTOM_UTC_TIME] <= utc_end_dt)
        ].copy()
        protocol, direction = protocol_direction.split('_')
        period_rows_df[XcalField.APP_TPUT_PROTOCOL] = protocol
        period_rows_df[XcalField.APP_TPUT_DIRECTION] = direction

        # detect if 5G-NR column exists in df; if not, use LTE
        event_field = XcalField.EVENT_5G_LTE if XcalField.EVENT_5G_LTE in df_xcal_logs.columns else XcalField.EVENT_LTE

        # Breakdown technology
        tech_breakdown = TechBreakdown(
            period_rows_df, 
            app_tput_protocol=protocol,
            app_tput_direction=direction, 
            event_field=event_field,
            label=label
        )
        try:
            segments = tech_breakdown.process()
        except Exception as e:
            print(f"Error in tech breakdown: {str(e)}")
            raise e
        try:
            reassembled_period_rows_df = tech_breakdown.reassemble_segments(segments)
        except Exception as e:
            print(f"Error in reassemble segments: {str(e)}")
            raise e

        if reassembled_period_rows_df is not None:
            reassembled_period_rows_df[XcalField.RUN_ID] = utc_start_dt.timestamp()
        # Add filtered rows to the list
        filtered_rows.append(reassembled_period_rows_df)

    # Combine all filtered rows
    if filtered_rows:
        filtered_df = pd.concat(filtered_rows, ignore_index=True)
    else:
        filtered_df = pd.DataFrame()

    return filtered_df

def tag_xcal_logs_with_essential_info(df: pd.DataFrame, periods: list[tuple[str, str]], timezone: str) -> pd.DataFrame:
    # Convert the TIME_STAMP from Eastern to UTC
    df['utc_dt'] = pd.to_datetime(df['TIME_STAMP'], errors='coerce').dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
    df = df.dropna(subset=['utc_dt'])
    # Convert the UTC time to the target timezone
    df['local_dt'] = df['utc_dt'].dt.tz_convert(timezone)

    for period in periods:
        start = period[0]
        end = period[1]
        protocol, direction = period[2].split('_')
        
        df.loc[(df['utc_dt'] >= start) & (df['utc_dt'] <= end), 'app_tput_protocol'] = protocol
        df.loc[(df['utc_dt'] >= start) & (df['utc_dt'] <= end), 'app_tput_direction'] = direction

    return df

class TestCollectPeriodsOfTputMeasurements(unittest.TestCase):
    @patch('glob.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_collect_periods_of_tput_measurements(self, mock_file, mock_glob):
        # Mock the glob.glob to return a list of files
        mock_glob.return_value = ['tcp_downlink_142919618.out', 'tcp_uplink_142919618.out']
        
        # Mock the file content
        mock_file.return_value.__enter__.return_value.readlines.side_effect = [
            [f'Start time: {int(pd.Timestamp("2024-06-26 06:00:00", tz="UTC").timestamp() * 1000)}\n', 
             'Some content\n', 
             f'End time: {int(pd.Timestamp("2024-06-26 07:00:00", tz="UTC").timestamp() * 1000)}\n'],
            [f'Start time: {int(pd.Timestamp("2024-06-26 08:00:00", tz="UTC").timestamp() * 1000)}\n', 
             'Some content\n', 
             f'End time: {int(pd.Timestamp("2024-06-26 09:00:00", tz="UTC").timestamp() * 1000)}\n'] 
        ]
        
        # Call the function
        result = collect_periods_of_tput_measurements('/base/dir', 'tcp', 'downlink')
        
        # Assert the result
        expected_result = [
            (pd.Timestamp("2024-06-26 06:00:00", tz="UTC").to_pydatetime(),
             pd.Timestamp("2024-06-26 07:00:00", tz="UTC").to_pydatetime(),
             'tcp_downlink'),
            (pd.Timestamp("2024-06-26 08:00:00", tz="UTC").to_pydatetime(),
             pd.Timestamp("2024-06-26 09:00:00", tz="UTC").to_pydatetime(),
             'tcp_uplink')
        ]
        self.assertEqual(result, expected_result)
        
        # Assert that open was called for each file
        mock_file.assert_any_call('tcp_downlink_142919618.out', 'r')
        mock_file.assert_any_call('tcp_uplink_142919618.out', 'r')

    @patch('glob.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_collect_periods_of_tput_measurements_value_error(self, mock_file, mock_glob):
        # Mock the glob.glob to return a list of files
        mock_glob.return_value = ['tcp_downlink_142919618.out', 'tcp_uplink_142919618.out']

        # Mock the file content with one valid and one invalid file
        mock_file.return_value.__enter__.return_value.readlines.side_effect = [
            ['Invalid start time\n', 'Some content\n', 'Invalid end time\n'],
            [f'Start time: {int(pd.Timestamp("2024-06-26 08:00:00", tz="UTC").timestamp() * 1000)}\n', 
             'Some content\n', 
             f'End time: {int(pd.Timestamp("2024-06-26 09:00:00", tz="UTC").timestamp() * 1000)}\n']
        ]
        
        # Capture print output
        with patch('builtins.print') as mock_print:
            result = collect_periods_of_tput_measurements('/base/dir', 'tcp', 'downlink')
        
        # Assert that we got the valid result and a warning was printed
        expected_result = [
            (pd.Timestamp("2024-06-26 08:00:00", tz="UTC").to_pydatetime(),
             pd.Timestamp("2024-06-26 09:00:00", tz="UTC").to_pydatetime(),
             'tcp_uplink')
        ]
        self.assertEqual(result, expected_result)
        mock_print.assert_called_once_with("Warning: Failed to extract start and end time from tcp_downlink_142919618.out")


class TestFilterXcalLogs(unittest.TestCase):
    def test_filter_xcal_logs(self):
        # Create a sample DataFrame
        # US/Eastern is UTC-4, so 00:00:00 Eastern is 04:00:00 UTC
        datetime_str = '2024-06-26T00:00:00.000'
        periods = [(
            pd.Timestamp('2024-06-26T03:59:00.000', tz='UTC'), 
            pd.Timestamp('2024-06-26T04:01:00.000', tz='UTC'), 
            'tcp_downlink'
        )]

        df = pd.DataFrame({
            'TIME_STAMP': [datetime_str],
        })
        
        # Call the function
        result = filter_xcal_logs(df, periods)
        
        # Assert the result
        expected = pd.DataFrame({
            'TIME_STAMP': [datetime_str],
        }).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)

    def test_filter_xcal_logs_no_match(self):
        # Create a sample DataFrame
        datetime_str = '2024-06-26T00:00:00.000'
        df = pd.DataFrame({
            'TIME_STAMP': [datetime_str],
        })
        
        # Define periods that don't match any timestamps
        periods = [(
            pd.Timestamp('2024-06-26T05:00:00.000', tz='UTC'),
            pd.Timestamp('2024-06-26T06:00:00.000', tz='UTC'),
            'tcp_downlink'
        )]
        
        # Call the function
        result = filter_xcal_logs(df, periods)
        
        # Assert the result is an empty DataFrame
        expected = pd.DataFrame(columns=['TIME_STAMP'])
        pd.testing.assert_frame_equal(result, expected)

    def test_filter_xcal_logs_overlapping_periods(self):
        # Create a sample DataFrame with easily recognizable timestamps
        df = pd.DataFrame({
            'TIME_STAMP': ['2024-06-26 06:00:00', '2024-06-26 07:00:00', 
                           '2024-06-26 08:00:00', '2024-06-26 09:00:00', 
                           '2024-06-26 10:00:00'],
        })
        
        # Define overlapping periods (10:30 to 12:30 and 11:30 to 13:30) in UTC
        periods = [
            (pd.Timestamp('2024-06-26 10:30:00', tz='UTC'),
             pd.Timestamp('2024-06-26 12:30:00', tz='UTC'),
             'tcp_downlink'),
            (pd.Timestamp('2024-06-26 11:30:00', tz='UTC'),
             pd.Timestamp('2024-06-26 13:30:00', tz='UTC'),
             'tcp_uplink')
        ]
        
        # Call the function
        result = filter_xcal_logs(df, periods)
        
        # Assert the result (should include 11:00, 12:00, and 13:00, removing duplicates)
        expected = pd.DataFrame({
            'TIME_STAMP': ['2024-06-26 07:00:00', '2024-06-26 08:00:00', '2024-06-26 09:00:00'],
        }).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)


class TestTagXcalLogsWithEssentialInfo(unittest.TestCase):
    def test_tag_xcal_logs_with_essential_info(self):
        datetime_str = '2024-06-26T00:00:00.000'
        source_timezone = 'US/Eastern'
        target_timezone = 'US/Alaska'
        target_local_dt = pd.to_datetime(datetime_str).tz_localize(source_timezone).tz_convert(target_timezone)

        periods = [(
            pd.Timestamp('2024-06-26T03:59:00.000', tz='UTC'), 
            pd.Timestamp('2024-06-26T04:01:00.000', tz='UTC'), 
            'tcp_downlink'
        )]

        df = pd.DataFrame({
            'TIME_STAMP': [datetime_str],
        })
        
        # Call the function
        result = tag_xcal_logs_with_essential_info(df, periods, timezone=target_timezone)
        
        # Assert the result
        expected = pd.DataFrame({
            'TIME_STAMP': [datetime_str],
            'utc_dt': [pd.to_datetime(datetime_str).tz_localize(source_timezone).tz_convert('UTC')],
            'local_dt': [target_local_dt],
            'app_tput_protocol': ['tcp'],
            'app_tput_direction': ['downlink'],
        }).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)


if __name__ == '__main__':
    unittest.main()