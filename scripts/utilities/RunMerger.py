import datetime
import os
import re
import unittest
from typing import List

from scripts.utils import get_datetime_from_path


class RunMerger:
    def fmt_list(self, lst):
        return "\n".join(lst)

    def fmt_tuple_list(self, lst):
        return "\n".join([f'{x[0]}, {x[1]}' for x in lst])

    def get_timestamp_as_key(self, folder_path: str):
        dt = get_datetime_from_path(folder_path)
        return dt.timestamp()

    def classify_folders_by_protocol(self, dir: str):
        """
        Classify folders by protocol, assuming dir order is tcp and then udp
        :param dir:
        :return:
        """
        # walk through the directory and classify folders by protocol
        tcp_folders = []
        udp_folders = []
        for root, dirs, files in os.walk(dir):
            if not dirs:
                # leaf node
                for file in files:
                    if 'tcp' in file:
                        tcp_folders.append(root)
                        break
                    elif 'udp' in file:
                        udp_folders.append(root)
                        break
        tcp_folders.sort(key=self.get_timestamp_as_key)
        udp_folders.sort(key=self.get_timestamp_as_key)
        return tcp_folders, udp_folders

    def save_merged_folders(self, operator: str, merged_folder_tuples: List[tuple]):
        for tcp_folder, udp_folder in merged_folder_tuples:
            # copy files in tcp_folder and udp_folder to output_dir
            new_folder = re.sub(r'^(.*?)/([^/]+)/(\d+/\d+)$', r'\1/' + operator + r'_merged/\3', tcp_folder)
            if not os.path.exists(new_folder):
                os.makedirs(new_folder)
            os.system(f'cp -r {tcp_folder}/* {new_folder}')
            os.system(f'cp -r {udp_folder}/* {new_folder}')

    def match_folders(self, tcp_folders: List[str], udp_folders: List[str]) -> (List[tuple], List[str], List[str]):
        idx1 = 0
        idx2 = 0
        len_1 = len(tcp_folders)
        len_2 = len(udp_folders)
        matched_folder_tuples = []
        leftover_tcp_folders = []
        leftover_udp_folders = []
        while idx1 < len_1 and idx2 < len_2:
            tcp_folder = tcp_folders[idx1]
            tcp_folder_time = get_datetime_from_path(tcp_folder)

            udp_folder = udp_folders[idx2]
            udp_folder_time = get_datetime_from_path(udp_folder)
            if datetime.timedelta(minutes=0) < udp_folder_time - tcp_folder_time < datetime.timedelta(minutes=9):
                matched_folder_tuples.append((tcp_folder, udp_folder))
                idx1 += 1
                idx2 += 1
            elif tcp_folder_time < udp_folder_time:
                leftover_tcp_folders.append(tcp_folder)
                idx1 += 1
            else:
                leftover_udp_folders.append(udp_folder)
                idx2 += 1
        leftover_tcp_folders = leftover_tcp_folders + tcp_folders[idx1:]
        leftover_udp_folders = leftover_udp_folders + udp_folders[idx2:]
        return matched_folder_tuples, leftover_tcp_folders, leftover_udp_folders


class Unittest(unittest.TestCase):
    # def test_classify_folders_by_protocol(self):
    #     tcp_folders, udp_folders = RunMerger().classify_folders_by_protocol(
    #         dir=os.path.join('path/to/starlink/20240621'))
    #     self.assertEqual(len(tcp_folders), 1)
    #     self.assertEqual(len(udp_folders), 1)

    def test_matched_folder_tuples(self):
        tcp_folders = ['20240618/000100000']
        udp_folders = ['20240618/000500000']
        matched_folder_tuples, leftover_tcp_folders, leftover_udp_folders = RunMerger().match_folders(tcp_folders,
                                                                                                      udp_folders)
        self.assertEqual([('20240618/000100000', '20240618/000500000')], matched_folder_tuples)
        self.assertEqual(len(leftover_tcp_folders), 0)
        self.assertEqual(len(leftover_udp_folders), 0)

        tcp_folders = ['20240618/000100000', '20240618/001500000']
        udp_folders = ['20240618/000500000']
        matched_folder_tuples, leftover_tcp_folders, leftover_udp_folders = RunMerger().match_folders(tcp_folders,
                                                                                                      udp_folders)
        self.assertEqual([('20240618/000100000', '20240618/000500000')], matched_folder_tuples)
        self.assertEqual(len(leftover_tcp_folders), 1)
        self.assertEqual(len(leftover_udp_folders), 0)
