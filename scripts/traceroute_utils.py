import re
import unittest
from typing import Dict, List, Tuple

from scripts.utils import find_files


def parse_traceroute_log(content):
    # Regular expression pattern to match hop lines
    pattern = r'.?]\s+(\d+)\s+(.*)'

    # Find all matches in the traceroute log
    matches = re.findall(pattern, content)

    # Extract hop information
    hops = []
    for match in matches:
        hop_number = match[0]
        content = match[1]
        res = parse_traceroute_line(content)
        res = list(map(lambda x: {**x, 'hop_number': hop_number}, res))
        hops.append(res)
    return hops


def sanitize_probe_result(probe_result: str):
    res = probe_result.replace('!N', '')
    res = res.strip()
    return res


def separate_three_probes(line: str) -> List[str]:
    # remove all *
    first_sep_list = re.findall(r'\*|\S+(?:\s+(?!\*)\S+)*', line)
    res = []
    for i in range(len(first_sep_list)):
        ele = first_sep_list[i]
        if ele == '':
            res.append('')
        else:
            # split by ms
            separation_reg = re.compile(r'(.*?)\sms|\*')
            matches = separation_reg.findall(ele)
            res.extend(list(map(sanitize_probe_result, matches)))
    # get first 3 elements
    return res[:3]


def extract_host_info(probe_str: str) -> Dict:
    res = {
        'hostname': None,
        'ip': None,
        'rtt_ms': None
    }
    if not probe_str or probe_str == '*':
        return res
    if '(' in probe_str:
        pattern = r'([^\s]+) \(([^\s]+)\) \s*([\d\.]+)'
        match = re.match(pattern, probe_str)
        if match:
            res['hostname'] = match.group(1)
            res['ip'] = match.group(2)
            res['rtt_ms'] = match.group(3)
        return res
    # only rtt
    res['rtt_ms'] = probe_str
    return res


def parse_traceroute_line(line: str) -> List[Dict[str, str]]:
    three_probes = separate_three_probes(line)

    results = []
    last_valid_probe_info = None
    for probe in three_probes:
        probe_info = extract_host_info(probe)
        if probe_info['rtt_ms'] and probe_info['hostname'] is None:
            if last_valid_probe_info:
                probe_info['hostname'] = last_valid_probe_info['hostname']
                probe_info['ip'] = last_valid_probe_info['ip']
        results.append(probe_info)

        if probe_info['hostname']:
            last_valid_probe_info = probe_info
    return results


def find_traceroute_files(base_dir: str):
    return find_files(base_dir, prefix="traceroute", suffix=".out")


def find_traceroute_files_by_dir_list(dir_list: List[str]):
    traceroute_files = []
    for dir in dir_list:
        traceroute_files.extend(find_traceroute_files(dir))
    return traceroute_files


class Unittest(unittest.TestCase):
    def test_separate_three_probes(self):
        line = '192.168.1.1 (192.168.1.1)  2.875 ms  2.454 ms  2.127 ms'
        expected = ['192.168.1.1 (192.168.1.1)  2.875', '2.454', '2.127']
        self.assertEqual(expected, separate_three_probes(line))

        line = 'undefined.hostname.localhost (206.224.65.146)  91.417 ms  91.123 ms  103.222 ms'
        expected = ['undefined.hostname.localhost (206.224.65.146)  91.417', '91.123', '103.222']
        self.assertEqual(expected, separate_three_probes(line))

        line = '* * *'
        expected = ['', '', '']
        self.assertEqual(expected, separate_three_probes(line))

        line = '108.166.240.39 (108.166.240.39)  86.353 ms * *'
        expected = ['108.166.240.39 (108.166.240.39)  86.353', '', '']
        self.assertEqual(expected, separate_three_probes(line))

        line = '* ec2-50-112-93-113.us-west-2.compute.amazonaws.com (50.112.93.113)  95.990 ms *'
        expected = ['', 'ec2-50-112-93-113.us-west-2.compute.amazonaws.com (50.112.93.113)  95.990', '']
        self.assertEqual(expected, separate_three_probes(line))

        # line = '* 172.16.252.156 (172.16.252.156)  128.874 ms  128.845 ms'
        # expected = ['*', '', '']
        # self.assertEqual(expected, separate_three_probes(line))

    def test_traceroute_parsing(self):
        # Sample traceroute log
        traceroute_log = """
        Start time: 1719116806332
        [2024-06-22 20:26:46.481949] traceroute to 50.112.93.113 (50.112.93.113), 30 hops max, 60 byte packets
        [2024-06-22 20:26:46.595866]  1  192.168.1.1 (192.168.1.1)  2.875 ms  2.454 ms  2.127 ms
        [2024-06-22 20:26:46.717686]  2  100.64.0.1 (100.64.0.1)  72.716 ms  79.912 ms  79.730 ms
        [2024-06-22 20:26:46.846336]  3  172.16.252.156 (172.16.252.156)  92.178 ms  91.962 ms  91.700 ms
        [2024-06-22 20:26:47.006893]  4  undefined.hostname.localhost (206.224.65.146)  91.417 ms  91.123 ms  103.222 ms
        [2024-06-22 20:26:47.375600]  5  undefined.hostname.localhost (206.224.66.150)  90.098 ms undefined.hostname.localhost (206.224.66.149)  89.829 ms undefined.hostname.localhost (206.224.66.147)  89.304 ms
        [2024-06-22 20:26:47.623307]  6  99.83.118.220 (99.83.118.220)  89.410 ms 99.83.118.214 (99.83.118.214)  109.933 ms 99.83.118.220 (99.83.118.220)  109.469 ms
        [2024-06-22 20:26:48.077454]  7  150.222.214.149 (150.222.214.149)  108.817 ms 150.222.214.157 (150.222.214.157)  156.606 ms 150.222.214.151 (150.222.214.151)  156.091 ms
        [2024-06-22 20:26:48.527108]  8  52.95.53.11 (52.95.53.11)  155.485 ms 52.95.52.61 (52.95.52.61)  155.224 ms 52.95.53.159 (52.95.53.159)  154.725 ms
        [2024-06-22 20:26:48.527682]  9  * * *
        [2024-06-22 20:26:48.527834] 10  * * *
        [2024-06-22 20:26:49.184276] 11  * * *
        [2024-06-22 20:26:49.924931] 12  * * *
        [2024-06-22 20:26:50.188619] 13  * * *
        [2024-06-22 20:26:50.665719] 14  150.222.251.58 (150.222.251.58)  154.598 ms 15.230.233.204 (15.230.233.204)  119.725 ms 150.222.255.102 (150.222.255.102)  159.894 ms
        [2024-06-22 20:26:50.666251] 15  * * *
        [2024-06-22 20:26:50.666428] 16  * * *
        [2024-06-22 20:26:50.666555] 17  * * *
        [2024-06-22 20:26:50.795110] 18  * * *
        [2024-06-22 20:26:51.543112] 19  15.230.233.59 (15.230.233.59)  86.164 ms * 54.239.45.125 (54.239.45.125)  88.643 ms
        [2024-06-22 20:26:51.672933] 20  * * *
        [2024-06-22 20:26:51.754709] 21  * * *
        [2024-06-22 20:26:51.772981] 22  108.166.240.39 (108.166.240.39)  86.353 ms * *
        [2024-06-22 20:26:51.773253] 23  * * *
        [2024-06-22 20:26:51.864641] 24  * ec2-50-112-93-113.us-west-2.compute.amazonaws.com (50.112.93.113)  95.990 ms *
        End time: 1719116811892
        """

        hops = parse_traceroute_log(traceroute_log)
        self.assertEqual(len(hops), 24)

    def test_traceroute_line_parsing(self):
        # one host, three valid RTTs
        line = "192.168.1.1 (192.168.1.1)  2.875 ms  2.454 ms  2.127 ms"
        expected = [
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '2.875'
            },
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '2.454'
            },
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '2.127'
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

        line = "* 192.168.1.1 (192.168.1.1)  86.353 ms 87.353 ms "
        expected = [
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '86.353'
            },
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '87.353'
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

        line = "192.168.1.1 (192.168.1.1) 86.353 ms * 192.168.1.2 (192.168.1.2) 87.353 ms"
        expected = [
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '86.353'
            },
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
            {
                'hostname': '192.168.1.2',
                'ip': '192.168.1.2',
                'rtt_ms': '87.353'
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

        line = "192.168.1.1 (192.168.1.1) 86.353 ms 87.353 ms *"
        expected = [
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '86.353'
            },
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '87.353'
            },
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

        line = "* * *"
        expected = [
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))
        line = 'undefined.hostname.localhost (206.224.65.150)  170.340 ms undefined.hostname.localhost (206.224.65.146)  138.302 ms *'
        expected = [
            {
                'hostname': 'undefined.hostname.localhost',
                'ip': '206.224.65.150',
                'rtt_ms': '170.340'
            },
            {
                'hostname': 'undefined.hostname.localhost',
                'ip': '206.224.65.146',
                'rtt_ms': '138.302'
            },
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

        line = "108.166.240.39 (108.166.240.39)  86.353 ms * *"
        expected = [
            {
                'hostname': '108.166.240.39',
                'ip': '108.166.240.39',
                'rtt_ms': '86.353'
            },
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

        line = '* 172.16.252.156 (172.16.252.156)  128.874 ms  128.845 ms'
        expected = [
            {
                'hostname': None,
                'ip': None,
                'rtt_ms': None
            },
            {
                'hostname': '172.16.252.156',
                'ip': '172.16.252.156',
                'rtt_ms': '128.874',
            },
            {
                'hostname': '172.16.252.156',
                'ip': '172.16.252.156',
                'rtt_ms': '128.845',
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

        line = "undefined.hostname.localhost (206.224.66.150)  90.098 ms undefined.hostname.localhost (206.224.66.149)  89.829 ms undefined.hostname.localhost (206.224.66.147)  89.304 ms"
        expected = [
            {
                'hostname': 'undefined.hostname.localhost',
                'ip': '206.224.66.150',
                'rtt_ms': '90.098'
            },
            {
                'hostname': 'undefined.hostname.localhost',
                'ip': '206.224.66.149',
                'rtt_ms': '89.829'
            },
            {
                'hostname': 'undefined.hostname.localhost',
                'ip': '206.224.66.147',
                'rtt_ms': '89.304'
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))

    def test_line_parsing_with_exceptions(self):
        line = '192.168.1.1 (192.168.1.1)  22.141 ms !N  20.443 ms !N  19.786 ms !N'
        expected = [
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '22.141',
            },
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '20.443',
            },
            {
                'hostname': '192.168.1.1',
                'ip': '192.168.1.1',
                'rtt_ms': '19.786',
            },
        ]
        self.assertEqual(expected, parse_traceroute_line(line))
