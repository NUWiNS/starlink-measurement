import enum
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


class NuttcpContentProcessor:
    class Status(enum.Enum):
        EMPTY = 'EMPTY'
        NORMAL = 'NORMAL'
        TIMEOUT = 'TIMEOUT'
        INCOMPLETE = 'INCOMPLETE'

    def __init__(self, content: str, protocol: str, file_path: str = None):
        self.content = content
        self.protocol = protocol
        self.file_path = file_path
        self.data_points = None
        self.status = self.Status.EMPTY

    def process(self):
        result = self.parse_nuttcp_content(self.content, self.protocol)
        self.data_points = result['data_points']
        self.status = self.check_validity(result)

    @staticmethod
    def parse_nuttcp_content(content, protocol):
        """
        Parse nuttcp content
        :param content:
        :param protocol:
        :return:
        """

        if protocol == 'tcp':
            data_points = parse_nuttcp_tcp_result(content)
        elif protocol == 'udp':
            data_points = parse_nuttcp_udp_result(content)
        else:
            raise ValueError('Please specify protocol with eithers tcp or udp')

        summary = extract_nuttcp_receiver_summary(content)

        return {
            'data_points': data_points,
            'has_summary': summary['has_summary'],
            'avg_tput_mbps': summary['avg_tput_mbps'],
        }

    @staticmethod
    def check_validity(parsed_result: Dict):
        data_points = parsed_result['data_points']

        if parsed_result['has_summary']:
            return NuttcpContentProcessor.Status.NORMAL

        if data_points is None or len(data_points) == 0:
            return NuttcpContentProcessor.Status.EMPTY

        data_len = len(data_points)
        if data_len >= 240:
            return NuttcpContentProcessor.Status.TIMEOUT
        else:
            return NuttcpContentProcessor.Status.INCOMPLETE

    def get_result(self):
        return self.data_points

    def get_status(self):
        return self.status.value


class NuttcpDataAnalyst:
    def __init__(self):
        self.processors = {}
        self.current_id = 0
        self.status_summary = {}

    def add_processor(self, processor: NuttcpContentProcessor):
        self.processors[self.current_id] = processor
        self.current_id += 1

    @staticmethod
    def format_status_summary(status_summary):
        summary = {}
        for status in status_summary:
            summary[status] = len(status_summary[status])
        return summary

    def get_status_summary(self):
        self.status_summary = {}
        for id in self.processors.keys():
            processor = self.processors[id]
            status = processor.get_status()
            if status not in self.status_summary:
                self.status_summary[status] = set()
            self.status_summary[status].add(id)
        return self.status_summary

    def get_summary(self):
        summary = {}
        status_summary = self.get_status_summary()
        summary['total'] = len(self.processors)
        summary['status'] = status_summary
        return summary

    def describe(self):
        summary = self.get_summary()
        status_count_summary = self.format_status_summary(summary['status'])
        print('NuttcpDataAnalyst Summary:')
        print(f"- Total files: {summary['total']}")
        print(f"- Count by Status:")
        for status in status_count_summary:
            print(f"-- {status}: {status_count_summary[status]}")
        return summary


def has_nuttcp_receiver_summary(content: str) -> bool:
    pattern = r'nuttcp-r:.*?real seconds.*?bps'
    return re.search(pattern, content) is not None


def extract_nuttcp_receiver_summary(content: str) -> Dict[str, float]:
    if not has_nuttcp_receiver_summary(content):
        return {'has_summary': False, 'avg_tput_mbps': -1}

    # Define the regex pattern to match the receiver summary log
    pattern = r'nuttcp-r: .* = (\d+\.\d+) Mbps'

    # Search for the pattern in the log
    match = re.search(pattern, content)

    # Check if the pattern was found
    if match:
        # Extract the average throughput in Mbps
        avg_tput_mbps = float(match.group(1))
        return {'has_summary': True, 'avg_tput_mbps': avg_tput_mbps}
    else:
        return {'has_summary': True, 'avg_tput_mbps': -1}


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


def find_tcp_downlink_files_by_dir_list(dir_list: List[str]):
    files = []
    for dir in dir_list:
        files.extend(find_tcp_downlink_files(dir))
    return files


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

    def test_check_validity(self):
        # --- has summary cases ---
        input_data = {
            'data_points': [0] * 240,
            'has_summary': True,
            'avg_tput_mbps': 10
        }
        self.assertEqual(NuttcpContentProcessor.Status.NORMAL, NuttcpContentProcessor.check_validity(input_data))

        input_data = {
            'data_points': [0] * 250,
            'has_summary': True,
            'avg_tput_mbps': 10
        }
        self.assertEqual(NuttcpContentProcessor.Status.NORMAL, NuttcpContentProcessor.check_validity(input_data))

        # --- no summary cases ---
        input_data = {
            'data_points': [],
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpContentProcessor.Status.EMPTY, NuttcpContentProcessor.check_validity(input_data))

        input_data = {
            'data_points': None,
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpContentProcessor.Status.EMPTY, NuttcpContentProcessor.check_validity(input_data))

        input_data = {
            'data_points': [0] * 239,
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpContentProcessor.Status.INCOMPLETE, NuttcpContentProcessor.check_validity(input_data))

        input_data = {
            'data_points': [0] * 250,
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpContentProcessor.Status.TIMEOUT, NuttcpContentProcessor.check_validity(input_data))
