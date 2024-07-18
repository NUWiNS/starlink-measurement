import re
import unittest
from typing import List

from scripts.utils import find_files


def parse_nslookup_result(result: str):
    dns_server_reg = r"Server:\s+(\d+\.\d+\.\d+\.\d+)\s+Address:\s+\d+\.\d+\.\d+\.\d+#(\d+)"
    match = re.search(dns_server_reg, result)
    if not match:
        # if no dns server info, meaning the result is invalid
        return None
    dns_server_addr = match.group(1)
    dns_server_port = match.group(2)

    non_authoritative_reg = r"Non-authoritative answer:"
    is_non_authoritative = bool(re.search(non_authoritative_reg, result))

    answer_reg = r"Name:\s+([^\s]+)\s+Address:\s+([^\s]+)"
    answer_match = re.findall(answer_reg, result) or []

    return {
        "dns_server": dns_server_addr,
        "dns_server_port": dns_server_port,
        "is_non_authoritative": is_non_authoritative,
        "answers": answer_match,
    }


def split_multiple_nslookup_results(results: str):
    res = results.split("\n\n")
    return [r.strip() for r in res if r.strip()]



def find_nslookup_files(base_dir: str):
    return find_files(base_dir, prefix="nslookup", suffix=".out")


def find_nslookup_files_by_dir_list(dir_list: List[str]):
    nslookup_files = []
    for dir in dir_list:
        nslookup_files.extend(find_nslookup_files(dir))
    return nslookup_files


class Unittest(unittest.TestCase):
    def test_parse_nslookup_result(self):
        result = """
        Server:		8.8.8.8
        Address:	8.8.8.8#53
        Non-authoritative answer:
        Name:	facebook.com
        Address: 157.240.3.35
        Name:	facebook.com
        Address: 2a03:2880:f101:83:face:b00c:0:25de
        """

        expected = {
            "dns_server": "8.8.8.8",
            "dns_server_port": "53",
            "is_non_authoritative": True,
            "answers": [
                ("facebook.com", "157.240.3.35"),
                ("facebook.com", "2a03:2880:f101:83:face:b00c:0:25de"),
            ],
        }
        self.assertEqual(parse_nslookup_result(result), expected)

        # More complex case
        result = """
        Server:		8.8.8.8
        Address:	8.8.8.8#53
        Non-authoritative answer:
        Name:	google.com
        Address: 172.253.122.102
        Name:	google.com
        Address: 172.253.122.113
        Name:	google.com
        Address: 172.253.122.139
        Name:	google.com
        Address: 172.253.122.100
        Name:	google.com
        Address: 172.253.122.101
        Name:	google.com
        Address: 172.253.122.138
        Name:	google.com
        Address: 2607:f8b0:4008:809::200e
        """
        expected = {
            "dns_server": "8.8.8.8",
            "dns_server_port": "53",
            "is_non_authoritative": True,
            "answers": [
                ("google.com", "172.253.122.102"),
                ("google.com", "172.253.122.113"),
                ("google.com", "172.253.122.139"),
                ("google.com", "172.253.122.100"),
                ("google.com", "172.253.122.101"),
                ("google.com", "172.253.122.138"),
                ("google.com", "2607:f8b0:4008:809::200e"),
            ],
        }
        self.assertEqual(parse_nslookup_result(result), expected)
