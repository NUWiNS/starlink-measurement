import re
import unittest

from scripts.nuttcp_utils import parse_nuttcp_timestamp, append_timezone
from scripts.time_utils import format_datetime_as_iso_8601


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


def extract_data_from_line(line: str):
    pattern = re.compile(
        r"\[(.*?)\]\s+\[\s*\d+\]\s+[\d.]+-[\d.]+\s+sec\s+[\d.]+\s+\w?Bytes\s+([\d.]+\s+\w?)bits/sec\s+[\d.]+\s+ms\s+([-\d]+)/(\d+)\s+\((.+)%\)\s*$"
    )
    match = pattern.search(line)
    if not match:
        return None

    timestamp, throughput, pkt_drop, pkt_total, loss = match.groups()
    dt_isoformat = format_iperf_timestamp(timestamp)
    return {
        'time': dt_isoformat,
        'throughput_mbps': convert_to_mbps(throughput),
        'pkt_drop': pkt_drop,
        'pkt_total': pkt_total,
        'loss': loss
    }


def parse_iperf_udp_result(content: str):
    """
    Extract timestamp, throughput, pkt_drop, pkt_total and loss from the UDP log of iperf
    :param content:
    :return: List[Dict[str, str]]
    """
    # Regular expression to match the target line

    extracted_data = []

    for line in content.splitlines():
        match = extract_data_from_line(line)
        if match:
            extracted_data.append(match)
    return extracted_data


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