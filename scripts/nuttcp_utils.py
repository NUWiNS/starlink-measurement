import unittest
from datetime import datetime
from typing import List, Dict

import pandas as pd

from scripts.time_utils import append_timezone, format_datetime_as_iso_8601
from scripts.utils import find_files

import os
import re


def parse_nuttcp_timestamp(timestamp: str):
    # Parse the timestamp in the format of "2024-05-27 15:00:00.000000"
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")


def format_nuttcp_timestamp(dt_str: str, timezone_str=None):
    """
    Format the nuttcp timestamp to ISO 8601 format
    :param dt_str:
    :param timezone_str:
    :return:
    """
    dt = parse_nuttcp_timestamp(dt_str)
    if timezone_str:
        dt = append_timezone(dt, timezone_str)
    return format_datetime_as_iso_8601(dt)


def parse_nuttcp_tcp_line(line: str) -> Dict[str, str]:
    """
    Parse a single line of nuttcp TCP log
    :param line:
    :return:
    """
    pattern = re.compile(
        r"\[(.*?)\]\s+.*?=\s+([\d.]+)\s+Mbps\s+(\d+)\s+retrans\s+(\d+)\s+KB-cwnd"
    )
    match = pattern.search(line)
    if not match:
        return None
    dt, throughput, retrans, cwnd = match.groups()
    dt_isoformat = format_nuttcp_timestamp(dt)
    return {
        'time': dt_isoformat,
        'throughput_mbps': throughput,
        'retrans': retrans,
        'cwnd_kb': cwnd
    }


def parse_nuttcp_tcp_result(content: str) -> List[Dict[str, str]]:
    """
    Extract timestamp, throughput, retrans and cwnd from the TCP log of nuttcp
    :param content:
    :return: List[Dict[str, str]]
    """
    extracted_data = []

    for line in content.splitlines():
        match = parse_nuttcp_tcp_line(line)
        if match:
            extracted_data.append(match)
    return extracted_data


def parse_nuttcp_udp_result(content: str) -> List[Dict[str, str]]:
    """
    Extract timestamp, throughput, pkt_drop, pkt_total and loss from the UDP log of nuttcp
    :param content:
    :return:
    """
    pattern = re.compile(r'\[(.*?)\]\s+.*=\s+([\d.]+) Mbps\s+([-\d]+) /\s+(\d+) ~drop/pkt\s+([-\d.]+) ~%loss')

    extracted_data = []
    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            dt, throughput, pkt_drop, pkt_total, loss = match.groups()
            dt_isoformat = format_nuttcp_timestamp(dt)
            extracted_data.append({
                'time': dt_isoformat,
                'throughput_mbps': throughput,
                'pkt_drop': pkt_drop,
                'pkt_total': pkt_total,
                'loss': loss
            })
    return extracted_data


def save_extracted_data_to_csv(data: List[Dict[str, str]], output_filename: str):
    df = pd.DataFrame(data)
    df.to_csv(output_filename, index=False)


def find_tcp_downlink_files(base_dir: str):
    return find_files(base_dir, prefix="tcp_downlink", suffix=".out")


def find_tcp_uplink_files(base_dir: str):
    return find_files(base_dir, prefix="tcp_uplink", suffix=".out")


def find_udp_uplink_files(base_dir: str):
    return find_files(base_dir, prefix="udp_uplink", suffix=".out")


class UnitTest(unittest.TestCase):
    def test_tcp_downlink_result_parsing(self):
        line = '[2024-05-27 10:57:21.047467]     1.6875 MB /   0.50 sec =   28.3104 Mbps     1 retrans   1375 KB-cwnd'
        self.assertEqual({
            'time': '2024-05-27T10:57:21.047467',
            'throughput_mbps': '28.3104',
            'retrans': '1',
            'cwnd_kb': '1375'
        }, parse_nuttcp_tcp_line(line))
