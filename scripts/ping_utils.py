import logging
import unittest
from datetime import datetime
from typing import Tuple, List
import pandas as pd
import pytz
import os
import re


from scripts.time_utils import StartEndLogTimeProcessor, ensure_timezone
from scripts.utils import find_files
from scripts.validations.utils import estimate_data_points


def find_ping_file(base_dir):
    return find_files(base_dir, prefix="ping", suffix=".out")


def format_datetime_as_iso_8601(dt: datetime):
    """
    Format the time in the EDT timezone
    :param dt:
    :return:
    """
    return dt.isoformat()


def append_timezone(dt: datetime, timezone_str: str, is_dst: bool = True):
    timezone = pytz.timezone(timezone_str)
    dt_aware = timezone.localize(dt, is_dst=is_dst)  # is_dst=True for daylight saving time
    return dt_aware


def append_edt_timezone(dt: datetime, is_dst: bool = True):
    return append_timezone(dt, "US/Eastern", is_dst)


def parse_timestamp_of_ping(timestamp):
    # Parse the timestamp in the format of "2024-05-27 15:00:00.000000"
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")


def format_timestamp(dt_str: str):
    dt = parse_timestamp_of_ping(dt_str)
    dt_edt = append_edt_timezone(dt)
    return format_datetime_as_iso_8601(dt_edt)


def match_ping_line(line: str):
    pattern = re.compile(
        r"\[(.*?)\].*?time=([\d.]+)\s+ms"
    )
    match = pattern.search(line)
    if not match:
        return None
    dt, rtt = match.groups()
    return {
        "time": dt,
        "rtt_ms": rtt
    }


def parse_ping_result(content: str, timezone: str = None):
    """
    :param content:
    :return: List[Tuple[time, rtt_ms]]
    """
    extracted_data = []

    for line in content.splitlines():
        match = match_ping_line(line)
        if match:
            match['time'] = pd.to_datetime(match['time'])
            if timezone:
                match['time'] = ensure_timezone(match['time'], timezone)
            extracted_data.append(match)

    return extracted_data

def extract_ping_data(
        file_path: str, 
        logger: logging.Logger | None = None,
        timezone: str = None,
    ):
    INTERVAL_SEC = 0.2
    DURATION_SEC = 30
    EXPECTED_NUM_OF_DATA_POINTS = int(DURATION_SEC / INTERVAL_SEC)

    logger.info(f'[start processing] {file_path}')
    with open(file_path, 'r') as f:
        content = f.read()
        total_lines = len(content.splitlines())
        extracted_data = parse_ping_result(content, timezone)
        logger.info(f'-- total lines: {total_lines}')
        logger.info(f'-- extracted lines: {len(extracted_data)}')

        start_end_time_list = StartEndLogTimeProcessor.get_start_end_time_from_log(content)
        if start_end_time_list:
            # should only be one pair of start and end time
            start_time, end_time = start_end_time_list[0]
            estimated_data_points = estimate_data_points(start_time, end_time, interval_sec=INTERVAL_SEC)
            logger.info(f'-- [estimating data points] {estimated_data_points} (start_time: {start_time}, end_time: {end_time})')

            num_data_points = len(extracted_data)
            logger.info(f'-- [extracted data points] {num_data_points}, diff from estiamte: {num_data_points - estimated_data_points}, diff from expected: {num_data_points - EXPECTED_NUM_OF_DATA_POINTS}')
    logger.info(f'[end processing] {file_path}\n\n')
    return extracted_data

def save_to_csv(row_list: List[Tuple[str, str, str]], output_file):
    header = ['time', 'rtt_ms', 'operator']
    with open(output_file, 'w') as f:
        f.write(','.join(header) + '\n')
        for row in row_list:
            line = ','.join(row)
            f.write(line + '\n')


def find_ping_file(base_dir):
    return find_files(base_dir, prefix="ping", suffix=".out")


def find_ping_files_by_dir_list(dir_list: List[str]):
    files = []
    for dir in dir_list:
        files.extend(find_ping_file(dir))
    return files


def count_subfolders(base_dir):
    return len(os.listdir(base_dir))


class Unittest(unittest.TestCase):
    def test_parse_ping_result(self):
        content = """
        [2024-05-27 10:59:19.628708] PING 35.245.244.238 (35.245.244.238) 38(66) bytes of data.
[2024-05-27 10:59:19.629152] 46 bytes from 35.245.244.238: icmp_seq=1 ttl=54 time=50.3 ms
[2024-05-27 10:59:19.817146] 46 bytes from 35.245.244.238: icmp_seq=2 ttl=54 time=37.7 ms
"""
        expected = [
            {
                "time": "2024-05-27T10:59:19.629152-04:00",
                "rtt_ms": "50.3"
            },
            {
                "time": "2024-05-27T10:59:19.817146-04:00",
                "rtt_ms": "37.7"
            }
        ]

        self.assertEqual(parse_ping_result(content), expected)

        content = """
        [2024-05-27 11:02:08.775511] PING 35.245.244.238 (35.245.244.238) 38(66) bytes of data.
[2024-05-27 11:02:08.776170] 
[2024-05-27 11:02:08.776243] --- 35.245.244.238 ping statistics ---
[2024-05-27 11:02:08.776287] 146 packets transmitted, 0 received, 100% packet loss, time 29904ms
[2024-05-27 11:02:08.776318] 
        """
        expected = []

        self.assertEqual(parse_ping_result(content), expected)
