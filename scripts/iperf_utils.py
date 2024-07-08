import re
import unittest
from abc import abstractmethod
from dataclasses import dataclass, asdict
from typing import List, Dict

from overrides import overrides

from scripts.common import TputBaseProcessor
from scripts.nuttcp_utils import parse_nuttcp_timestamp, append_timezone, NuttcpBaseProcessor, NuttcpDataAnalyst
from scripts.time_utils import format_datetime_as_iso_8601
from scripts.utils import find_files


@dataclass
class IperfUdpMetric:
    time: str
    throughput_mbps: str
    pkt_drop: str
    pkt_total: str
    loss: str


def format_iperf_timestamp(dt_str: str, timezone_str: str = None):
    dt = parse_nuttcp_timestamp(dt_str)
    if timezone_str:
        dt = append_timezone(dt, timezone_str)
    return format_datetime_as_iso_8601(dt)


def convert_to_mbps(tput: str):
    digit = float(tput[:-1].strip())
    if 'K' in tput:
        return "{:.2f}".format(digit / 1000)
    elif 'M' in tput:
        return tput[:-1].strip()
    else:
        # assume it's in bps
        return "{:.2f}".format(float(tput.strip()) / 1000000)


def extract_data_from_line(line: str, timezone_str: str = None):
    pattern = re.compile(
        r"\[(.*?)\]\s+\[\s*\d+\]\s+[\d.]+-[\d.]+\s+sec\s+[\d.]+\s+\w?Bytes\s+([\d.]+\s+\w?)bits/sec\s+[\d.]+\s+ms\s+([-\d]+)/(\d+)\s+\((.+)%\)\s*$"
    )
    match = pattern.search(line)
    if not match:
        return None

    timestamp, throughput, pkt_drop, pkt_total, loss = match.groups()
    dt_isoformat = format_iperf_timestamp(timestamp, timezone_str=timezone_str)
    return asdict(IperfUdpMetric(
        time=dt_isoformat,
        throughput_mbps=convert_to_mbps(throughput),
        pkt_drop=pkt_drop,
        pkt_total=pkt_total,
        loss=loss
    ))


def parse_iperf_udp_result(content: str, timezone_str: str = None):
    """
    Extract timestamp, throughput, pkt_drop, pkt_total and loss from the UDP log of iperf
    :param content:
    :return: List[Dict[str, str]]
    """
    # Regular expression to match the target line

    extracted_data = []

    for line in content.splitlines():
        match = extract_data_from_line(line, timezone_str=timezone_str)
        if match:
            extracted_data.append(match)
    return extracted_data


def find_udp_downlink_files(base_dir: str):
    return find_files(base_dir, prefix="udp_downlink", suffix=".out")


def find_udp_downlink_files_by_dir_list(dir_list: List[str]):
    udp_downlink_files = []
    for dir_name in dir_list:
        udp_downlink_files.extend(find_udp_downlink_files(dir_name))
    return udp_downlink_files


def has_iperf_receiver_summary(content: str) -> bool:
    pattern = r'.*?(\d+(\.\d+)?) (M|K|G)?bits/sec.*receiver'
    return re.search(pattern, content) is not None


def extract_iperf_receiver_summary(content: str) -> Dict[str, float]:
    if not has_iperf_receiver_summary(content):
        return {'has_summary': False, 'avg_tput_mbps': -1}

    # Define the regex pattern to match the receiver summary log
    # TODO: optimize the regex pattern to speed up
    pattern = r'.*?(\d+(\.\d+)?) (M|K|G)?bits/sec.*receiver'

    # Search for the pattern in the log
    match = re.search(pattern, content)

    # Check if the pattern was found
    if match:
        # Extract the average throughput in Mbps

        avg_tput_value = match.group(1)
        tput_unit = match.group(3)
        if tput_unit:
            tput_str = avg_tput_value + tput_unit
        else:
            tput_str = avg_tput_value + ' '
        avg_tput_mbps = convert_to_mbps(tput_str)

        return {'has_summary': True, 'avg_tput_mbps': float(avg_tput_mbps)}
    else:
        return {'has_summary': True, 'avg_tput_mbps': -1}


class IperfUdpBaseProcessor(TputBaseProcessor):
    @overrides
    def parse_data_points(self, content: str):
        json_data = parse_iperf_udp_result(content, timezone_str=self.timezone_str)
        return list(map(lambda x: IperfUdpMetric(**x), json_data))

    @overrides
    def postprocess_data_points(self):
        self.auto_complete_data_points()

    @overrides
    def parse_measurement_summary(self, content: str):
        return extract_iperf_receiver_summary(content)

    def create_default_value(self, time: str):
        return IperfUdpMetric(
            time=time,
            throughput_mbps='0',
            pkt_drop='-1',
            pkt_total='-1',
            loss='-1'
        )

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


class IperfUdpDownlinkProcessor(IperfUdpBaseProcessor):
    pass


class IperfDataAnalyst(NuttcpDataAnalyst):
    pass


class IperfProcessorFactory:
    @staticmethod
    def create(content: str, protocol: str, direction: str, file_path: str,
               timezone_str: str) -> IperfUdpBaseProcessor:
        if protocol == 'udp':
            if direction == 'downlink':
                return IperfUdpDownlinkProcessor(
                    content=content,
                    protocol=protocol,
                    direction=direction,
                    file_path=file_path,
                    timezone_str=timezone_str
                )
        raise NotImplementedError('No processor found for the given protocol and direction.')


class Unittest(unittest.TestCase):
    def test_match_line(self):
        line = '[2024-05-27 11:13:05.680006] [  5]   2.00-2.50   sec  0.00 Bytes  0.00 bits/sec  0.147 ms  0/0 (0%)'
        self.assertEqual({
            'time': '2024-05-27T11:13:05.680006',
            'throughput_mbps': '0.00',
            'pkt_drop': '0',
            'pkt_total': '0',
            'loss': '0'
        }, extract_data_from_line(line)
        )

        line = '[2024-05-27 11:13:05.679848] [  5]   0.00-0.50   sec  1.74 MBytes  29.2 Mbits/sec  0.183 ms  1431/2737 (52%)'
        self.assertEqual({
            'time': '2024-05-27T11:13:05.679848',
            'throughput_mbps': '29.2',
            'pkt_drop': '1431',
            'pkt_total': '2737',
            'loss': '52'
        }, extract_data_from_line(line)
        )

        line = '[2024-05-27 11:41:47.912273] [  5]  87.00-87.50  sec  10.9 KBytes   179 Kbits/sec  45.200 ms  1145/1153 (99%) '
        self.assertEqual({
            'time': '2024-05-27T11:41:47.912273',
            'throughput_mbps': '0.18',
            'pkt_drop': '1145',
            'pkt_total': '1153',
            'loss': '99'
        }, extract_data_from_line(line)
        )

        line = '[2024-05-27 11:13:05.680045] [  5]   2.50-3.00   sec   168 KBytes  2.76 Mbits/sec  0.235 ms  95562/95685 (1e+02%)'
        self.assertEqual({
            'time': '2024-05-27T11:13:05.680045',
            'throughput_mbps': '2.76',
            'pkt_drop': '95562',
            'pkt_total': '95685',
            'loss': '1e+02'
        }, extract_data_from_line(line)
        )

        line = '[2024-05-27 15:08:35.501865] [  5]  96.50-97.00  sec   561 KBytes  9.18 Mbits/sec  0.873 ms  -26/384 (-6.8%)'
        self.assertEqual({
            'time': '2024-05-27T15:08:35.501865',
            'throughput_mbps': '9.18',
            'pkt_drop': '-26',
            'pkt_total': '384',
            'loss': '-6.8'
        }, extract_data_from_line(line)
        )

    def test_extract_iperf_summary(self):
        content = """
        [2024-06-22 20:12:17.965928] [  5]   0.00-100.41 sec  2.18 GBytes   187 Mbits/sec  0.026 ms  4899217/6701718 (73%)  receiver        
        """
        self.assertEqual({
            'has_summary': True,
            'avg_tput_mbps': 187
        }, extract_iperf_receiver_summary(content))

        content = """
        [2024-06-22 20:12:17.965928] [  5]   0.00-100.41 sec  2.18 GBytes   187.11 Mbits/sec  0.026 ms  4899217/6701718 (73%)  receiver        
        """
        self.assertEqual({
            'has_summary': True,
            'avg_tput_mbps': 187.11
        }, extract_iperf_receiver_summary(content))

        content = """
                [2024-06-22 20:12:17.965928] [  5]   0.00-100.41 sec  2.18 GBytes   187 Kbits/sec  0.026 ms  4899217/6701718 (73%)  receiver        
                """
        self.assertEqual({
            'has_summary': True,
            'avg_tput_mbps': 0.19
        }, extract_iperf_receiver_summary(content))

        content = """
            [2024-06-22 20:12:17.965928] [  5]   0.00-100.41 sec  2.18 GBytes   0 bits/sec  0.026 ms  4899217/6701718 (73%)  receiver        
            """
        self.assertEqual({
            'has_summary': True,
            'avg_tput_mbps': 0
        }, extract_iperf_receiver_summary(content))

        content = """
        [2024-06-22 20:12:17.965928] [  5]   0.00-100.41 sec  2.18 GBytes   187 Mbits/sec  0.026 ms  4899217/6701718 (73%)  sender        
        """
        self.assertEqual({
            'has_summary': False,
            'avg_tput_mbps': -1
        }, extract_iperf_receiver_summary(content))

        content = """
        [2024-06-22 20:12:17.965928] [  5]   0.00-100.41 sec  2.18 GBytes   187 Mbits/sec  0.026 ms  4899217/6701718 (73%)  receiver        
        [2024-06-22 20:12:17.965928] [  5]   0.00-100.41 sec  2.18 GBytes   187 Mbits/sec  0.026 ms  4899217/6701718 (73%)  sender        
        """
        self.assertEqual({
            'has_summary': True,
            'avg_tput_mbps': 187
        }, extract_iperf_receiver_summary(content))
