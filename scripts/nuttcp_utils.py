from logging import Logger
import unittest
from abc import abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Any

import pandas as pd
from overrides import overrides

from scripts.common import TputBaseProcessor
from scripts.time_utils import append_timezone, format_datetime_as_iso_8601
from scripts.utils import find_files

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


def parse_nuttcp_tcp_line(line: str, timezone_str: str) -> Dict[str, str]:
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
    dt_isoformat = format_nuttcp_timestamp(dt, timezone_str)
    return asdict(
        NuttcpTcpMetric(
            time=dt_isoformat,
            throughput_mbps=throughput,
            retrans=retrans,
            cwnd_kb=cwnd
        )
    )


def parse_nuttcp_tcp_result(content: str, timezone_str: str) -> List[Dict[str, str]]:
    """
    Extract timestamp, throughput, retrans and cwnd from the TCP log of nuttcp
    :param content:
    :return: List[Dict[str, str]]
    """
    extracted_data = []

    for line in content.splitlines():
        match = parse_nuttcp_tcp_line(line, timezone_str)
        if match:
            extracted_data.append(match)
    return extracted_data


class NuttcpBaseProcessor(TputBaseProcessor):
    # Assume data is collected for 2min with 500ms interval
    EXPECTED_NUM_OF_DATA_POINTS = 240
    INTERVAL_SEC = 0.5

    @overrides
    def postprocess_data_points(self):
        self.logger.info(f'-- [start auto_complete_data_points] before: {len(self.data_points)}')
        self.auto_complete_data_points()
        self.logger.info(f'-- [end auto_complete_data_points] after: {len(self.data_points)}')

    @overrides
    def parse_measurement_summary(self, content: str):
        return extract_nuttcp_receiver_summary(content)

    @abstractmethod
    def create_default_value(self, time: str):
        pass

    def auto_complete_data_points(self):
        """
        if the data points are less than 240, auto complete the data points with 0 throughput
        :return:
        """
        missing_count = self.EXPECTED_NUM_OF_DATA_POINTS - len(self.data_points)
        if missing_count <= 0:
            return

        if self.status == self.Status.INCOMPLETE:
            self.data_points = self.pad_tput_data_points(
                raw_data=self.data_points,
                create_default_value=self.create_default_value,
                expected_len=self.EXPECTED_NUM_OF_DATA_POINTS,
                interval_sec=self.INTERVAL_SEC
            )
        elif self.status == self.Status.EMPTY:
            self.data_points = self.pad_tput_data_points(
                raw_data=self.data_points,
                create_default_value=self.create_default_value,
                expected_len=self.EXPECTED_NUM_OF_DATA_POINTS,
                start_time=self.start_time,  # need to specify start time if the raw data is empty
                interval_sec=self.INTERVAL_SEC
            )


# --- TCP processors START ---
class NuttcpTcpBaseProcessor(NuttcpBaseProcessor):
    @overrides
    def parse_data_points(self, content: str):
        json_data = parse_nuttcp_tcp_result(content, timezone_str=self.timezone_str)
        return list(map(lambda x: NuttcpTcpMetric(**x), json_data))

    @overrides
    def create_default_value(self, time):
        return NuttcpTcpMetric(
            time=time,
            throughput_mbps='0',
            retrans='-1',
            cwnd_kb='-1'
        )


class NuttcpTcpDownlinkProcessor(NuttcpTcpBaseProcessor):
    pass


class NuttcpTcpUplinkProcessor(NuttcpTcpBaseProcessor):
    @overrides
    def postprocess_data_points(self):
        # recalculate timestamp for data points
        self.logger.info(f'-- [start recalculating timestamps]')
        self.recalculate_timestamps_of_receiver_for_data_points()
        self.logger.info(f'-- [end recalculating timestamps]')

        self.logger.info(f'-- [start auto-completing data points] before: {len(self.data_points)}')
        self.auto_complete_data_points()
        self.logger.info(f'-- [end auto-completing data points] after: {len(self.data_points)}')

    def recalculate_timestamps_of_receiver_for_data_points(self):
        """
        Because we care about receiver's throughput and timestamp, we need to recalculate all timestamps using sender cues
        :return:
        """
        if len(self.data_points) == 0:
            return

        init_rtt_ms = extract_nuttcp_rtt_ms_from_tcp_log(self.content)
        # add half of the RTT to reflect the timestamp on the receiver side
        new_time = datetime.fromisoformat(self.data_points[0].time) + timedelta(milliseconds=init_rtt_ms * 0.5)

        for data_point in self.data_points:
            data_point.time = format_datetime_as_iso_8601(new_time)
            new_time += timedelta(seconds=self.INTERVAL_SEC)


# --- TCP processors END ---

# --- UDP processors START ---
class NuttcpUdpBaseProcessor(NuttcpBaseProcessor):
    @overrides
    def parse_data_points(self, content: str):
        json_data = parse_nuttcp_udp_result(content, timezone_str=self.timezone_str)
        return list(map(lambda x: NuttcpUdpMetric(**x), json_data))

    @overrides
    def create_default_value(self, time: str):
        return NuttcpUdpMetric(
            time=time,
            throughput_mbps='0',
            pkt_total='-1',
            pkt_drop='-1',
            loss='-1'
        )


class NuttcpUdpUplinkProcessor(NuttcpUdpBaseProcessor):
    pass


class NuttcpProcessorFactory:
    @staticmethod
    def create(
        content: str, 
        protocol: str, 
        direction: str, 
        file_path: str,
        timezone_str: str,
        logger: Logger = None
    ) -> NuttcpBaseProcessor:
        if protocol == 'tcp':
            if direction == 'downlink':
                return NuttcpTcpDownlinkProcessor(content, protocol, direction, file_path, timezone_str, logger)
            elif direction == 'uplink':
                return NuttcpTcpUplinkProcessor(content, protocol, direction, file_path, timezone_str, logger)
        if protocol == 'udp':
            if direction == 'uplink':
                return NuttcpUdpUplinkProcessor(content, protocol, direction, file_path, timezone_str, logger)
        raise ValueError('Unsupported protocol')


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
        logs.append(f'Summary:')
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


def parse_nuttcp_udp_result(content: str, timezone_str: str = None) -> List[Dict[str, str]]:
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
            dt_isoformat = format_nuttcp_timestamp(dt, timezone_str=timezone_str)
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
