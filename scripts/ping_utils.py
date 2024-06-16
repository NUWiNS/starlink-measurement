import unittest
from datetime import datetime
from typing import Tuple, List

import pandas as pd
import pytz

from scripts.utils import find_files

import os
import re

from scripts.common import extract_operator_from_filename


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
        "time": format_timestamp(dt),
        "rtt_ms": rtt
    }


def parse_ping_result(content: str):
    """
    :param content:
    :return: List[Tuple[time, rtt_ms]]
    """
    extracted_data = []

    for line in content.splitlines():
        match = match_ping_line(line)
        if match:
            extracted_data.append(match)

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