import enum
import unittest
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Any

import pandas as pd

from scripts.time_utils import append_timezone, format_datetime_as_iso_8601, StartEndLogTimeProcessor
from scripts.utils import find_files

import os
import re


@dataclass
class NuttcpTcpMetric:
    time: str
    throughput_mbps: str
    retrans: str
    cwnd_kb: str


@dataclass
class NuttcpUdpMetric:
    time: str
    throughput_mbps: str
    pkt_drop: str
    pkt_total: str
    loss: str


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
    return asdict(
        NuttcpTcpMetric(
            time=dt_isoformat,
            throughput_mbps=throughput,
            retrans=retrans,
            cwnd_kb=cwnd
        )
    )


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


class NuttcpBaseProcessor(ABC):
    # Assume data is collected for 2min with 500ms interval
    EXPECTED_NUM_OF_DATA_POINTS = 240
    INTERVAL_SEC = 0.5

    class Status(enum.Enum):
        EMPTY = 'EMPTY'
        NORMAL = 'NORMAL'
        TIMEOUT = 'TIMEOUT'
        INCOMPLETE = 'INCOMPLETE'

    def __init__(
            self,
            content: str,
            protocol: str,
            direction: str,
            file_path: str = None,
            timezone_str: str = None,
    ):
        self.content = content
        self.protocol = protocol
        self.direction = direction
        self.file_path = file_path
        self.data_points: List = []
        self.status = self.Status.EMPTY
        self.timezone_str = timezone_str
        self.start_time = None
        self.end_time = None

    def process(self):
        start_end_time_list = StartEndLogTimeProcessor.get_start_end_time_from_log(
            self.content,
            timezone_str=self.timezone_str
        )

        if start_end_time_list:
            # should only be one pair of start and end time
            self.start_time, self.end_time = start_end_time_list[0]

        extracted_result = self.parse_measurement_content(self.content)
        self.status = self.check_validity(extracted_result)

        self.data_points = extracted_result['data_points']
        self.postprocess_data_points()

    def parse_measurement_content(self, content: str):
        data_points = self.parse_data_points(content)
        summary = extract_nuttcp_receiver_summary(content)
        return {
            'data_points': data_points,
            'has_summary': summary['has_summary'],
            'avg_tput_mbps': summary['avg_tput_mbps'],
        }

    @abstractmethod
    def postprocess_data_points(self):
        pass

    @abstractmethod
    def parse_data_points(self, content: str):
        pass

    @staticmethod
    def check_validity(parsed_result: Dict):
        data_points = parsed_result['data_points']

        if parsed_result['has_summary']:
            return NuttcpBaseProcessor.Status.NORMAL

        if data_points is None or len(data_points) == 0:
            return NuttcpBaseProcessor.Status.EMPTY

        data_len = len(data_points)
        if data_len >= 240:
            return NuttcpBaseProcessor.Status.TIMEOUT
        else:
            return NuttcpBaseProcessor.Status.INCOMPLETE

    @staticmethod
    def pad_tput_data_points(
            raw_data: List[NuttcpTcpMetric],
            create_default_value: Callable[[str], Any],
            expected_len: int,
            start_time: datetime = None,
            interval_sec: float = 1,
    ):
        """
        Pad the throughput data points to the expected length
        """
        _raw_data = raw_data.copy()
        raw_data_len = len(_raw_data)
        if raw_data_len >= expected_len:
            return _raw_data
        if raw_data_len == 0 and start_time is None:
            raise ValueError('Please specify start time if the raw data is empty')

        new_time = start_time
        if start_time is None:
            # get the last time in the raw data + interval
            new_time = datetime.fromisoformat(_raw_data[-1].time) + timedelta(seconds=interval_sec)

        missing_count = expected_len - raw_data_len
        for i in range(0, missing_count):
            default_value = create_default_value(format_datetime_as_iso_8601(new_time))
            _raw_data.append(default_value)
            new_time += timedelta(seconds=interval_sec)
        return _raw_data

    def get_result(self) -> List[Dict[str, str]]:
        return list(map(lambda x: asdict(x), self.data_points))

    def get_status(self) -> str:
        return self.status.value


class NuttcpProcessorFactory:
    @staticmethod
    def create(content: str, protocol: str, direction: str, file_path: str,
               timezone_str: str) -> NuttcpBaseProcessor:
        if protocol == 'tcp':
            if direction == 'downlink':
                return NuttcpTcpDownlinkProcessor(content, protocol, direction, file_path, timezone_str)
            elif direction == 'uplink':
                return NuttcpTcpUplinkProcessor(content, protocol, direction, file_path, timezone_str)
        if protocol == 'udp':
            if direction == 'uplink':
                return NuttcpUdpUplinkProcessor(content, protocol, direction, file_path, timezone_str)
        raise ValueError('Unsupported protocol')


# --- TCP processors START ---
class NuttcpTcpBaseProcessor(NuttcpBaseProcessor):
    def parse_data_points(self, content: str):
        json_data = parse_nuttcp_tcp_result(content)
        return list(map(lambda x: NuttcpTcpMetric(**x), json_data))

    def postprocess_data_points(self):
        self.auto_complete_data_points()

    def auto_complete_data_points(self):
        """
        if the data points are less than 240, auto complete the data points with 0 throughput
        :return:
        """
        missing_count = self.EXPECTED_NUM_OF_DATA_POINTS - len(self.data_points)
        if missing_count <= 0:
            return

        def create_default_value(time):
            return NuttcpTcpMetric(
                time=time,
                throughput_mbps='0',
                retrans='-1',
                cwnd_kb='-1'
            )

        if self.status == NuttcpBaseProcessor.Status.INCOMPLETE:
            self.data_points = NuttcpBaseProcessor.pad_tput_data_points(
                raw_data=self.data_points,
                create_default_value=create_default_value,
                expected_len=self.EXPECTED_NUM_OF_DATA_POINTS,
                interval_sec=self.INTERVAL_SEC
            )
        elif self.status == NuttcpBaseProcessor.Status.EMPTY:
            self.data_points = NuttcpBaseProcessor.pad_tput_data_points(
                raw_data=self.data_points,
                create_default_value=create_default_value,
                expected_len=self.EXPECTED_NUM_OF_DATA_POINTS,
                start_time=self.start_time,  # need to specify start time if the raw data is empty
                interval_sec=self.INTERVAL_SEC
            )


class NuttcpTcpDownlinkProcessor(NuttcpTcpBaseProcessor):
    pass


class NuttcpTcpUplinkProcessor(NuttcpTcpBaseProcessor):
    def postprocess_data_points(self):
        # recalculate timestamp for data points
        self.recalculate_timestamps_of_receiver_for_data_points()
        self.auto_complete_data_points()

    def recalculate_timestamps_of_receiver_for_data_points(self):
        """
        Because we care about receiver's throughput and timestamp, we need to recalculate all timestamps using sender cues
        :return:
        """
        if len(self.data_points) == 0:
            return

        init_rtt_ms = extract_nuttcp_rtt_ms_from_tcp_log(self.content)
        # subtract half of the RTT from the first timestamp of data points on the sender side
        new_time = datetime.fromisoformat(self.data_points[0].time) - timedelta(milliseconds=init_rtt_ms * 0.5)

        for data_point in self.data_points:
            data_point.time = format_datetime_as_iso_8601(new_time)
            new_time += timedelta(seconds=self.INTERVAL_SEC)


# --- TCP processors END ---

# --- UDP processors START ---
class NuttcpUdpBaseProcessor(NuttcpBaseProcessor):
    def parse_data_points(self, content: str):
        json_data = parse_nuttcp_udp_result(content)
        return list(map(lambda x: NuttcpUdpMetric(**x), json_data))

    def postprocess_data_points(self):
        self.auto_complete_data_points()

    def auto_complete_data_points(self):
        """
        if the data points are less than 240, auto complete the data points with 0 throughput
        :return:
        """
        missing_count = self.EXPECTED_NUM_OF_DATA_POINTS - len(self.data_points)
        if missing_count <= 0:
            return

        def create_default_value(time):
            return NuttcpUdpMetric(
                time=time,
                throughput_mbps='0',
                pkt_total='-1',
                pkt_drop='-1',
                loss='-1'
            )

        if self.status == NuttcpBaseProcessor.Status.INCOMPLETE:
            self.data_points = NuttcpBaseProcessor.pad_tput_data_points(
                raw_data=self.data_points,
                create_default_value=create_default_value,
                expected_len=self.EXPECTED_NUM_OF_DATA_POINTS,
                interval_sec=self.INTERVAL_SEC
            )
        elif self.status == NuttcpBaseProcessor.Status.EMPTY:
            self.data_points = NuttcpBaseProcessor.pad_tput_data_points(
                raw_data=self.data_points,
                create_default_value=create_default_value,
                expected_len=self.EXPECTED_NUM_OF_DATA_POINTS,
                start_time=self.start_time,  # need to specify start time if the raw data is empty
                interval_sec=self.INTERVAL_SEC
            )


class NuttcpUdpUplinkProcessor(NuttcpUdpBaseProcessor):
    pass


# --- UDP processors END ---

class NuttcpDataAnalyst:
    def __init__(self):
        self.processors = {}
        self.current_id = 0
        self.status_summary = {}

    def add_processor(self, processor: NuttcpBaseProcessor):
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
        logs = []
        logs.append('NuttcpDataAnalyst Summary:')
        logs.append(f"- Total files: {summary['total']}")
        logs.append(f"- Count by Status:")
        for status in status_count_summary:
            logs.append(f"-- {status}: {status_count_summary[status]}")
        return logs


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


def extract_nuttcp_rtt_ms_from_tcp_log(content: str) -> float:
    pattern = re.compile(r".*?connect to.*?RTT=(\d+\.\d+) ms")
    match = pattern.search(content)
    if not match:
        return 0
    return float(match.group(1))


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
            extracted_data.append(asdict(
                NuttcpUdpMetric(
                    time=dt_isoformat,
                    throughput_mbps=throughput,
                    pkt_total=pkt_total,
                    pkt_drop=pkt_drop,
                    loss=loss
                )
            ))
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


def find_tcp_uplink_files_by_dir_list(dir_list: List[str]):
    files = []
    for dir in dir_list:
        files.extend(find_tcp_uplink_files(dir))
    return files


def find_udp_uplink_files_by_dir_list(dir_list: List[str]):
    files = []
    for dir in dir_list:
        files.extend(find_udp_uplink_files(dir))
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
        self.assertEqual(NuttcpBaseProcessor.Status.NORMAL,
                         NuttcpBaseProcessor.check_validity(input_data))

        input_data = {
            'data_points': [0] * 250,
            'has_summary': True,
            'avg_tput_mbps': 10
        }
        self.assertEqual(NuttcpBaseProcessor.Status.NORMAL,
                         NuttcpBaseProcessor.check_validity(input_data))

        # --- no summary cases ---
        input_data = {
            'data_points': [],
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpBaseProcessor.Status.EMPTY, NuttcpBaseProcessor.check_validity(input_data))

        input_data = {
            'data_points': None,
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpBaseProcessor.Status.EMPTY, NuttcpBaseProcessor.check_validity(input_data))

        input_data = {
            'data_points': [0] * 239,
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpBaseProcessor.Status.INCOMPLETE,
                         NuttcpBaseProcessor.check_validity(input_data))

        input_data = {
            'data_points': [0] * 250,
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(NuttcpBaseProcessor.Status.TIMEOUT,
                         NuttcpBaseProcessor.check_validity(input_data))

    def test_pad_tcp_tput_data_points(self):
        raw_data = [
            NuttcpTcpMetric(time='2024-05-27T10:57:21.047467', throughput_mbps='28.3104', retrans='1', cwnd_kb='1375'),
            NuttcpTcpMetric(time='2024-05-27T10:57:21.547467', throughput_mbps='28.3104', retrans='1', cwnd_kb='1375'),
        ]
        expected_len = 240
        interval = 0.5

        def create_default_value(time):
            return NuttcpTcpMetric(
                time=time,
                throughput_mbps='0',
                retrans='-1',
                cwnd_kb='-1')

        start_time = datetime.fromisoformat('2024-05-27T10:57:21.547467')
        data_after_padding = NuttcpTcpBaseProcessor.pad_tput_data_points(
            raw_data=raw_data,
            create_default_value=create_default_value,
            expected_len=expected_len,
            start_time=start_time,
            interval_sec=interval
        )

        self.assertEqual(expected_len, len(data_after_padding))
