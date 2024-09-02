import logging
import os
from typing import List

import pandas as pd

from scripts.common import TputBaseProcessor
from scripts.utils import find_files


class UdpBlockageHelper:
    def __init__(self, logger=logging.getLogger(__name__)):
        self.logger = logger

    def label_udp_dl_blockage_files(self, run_dir_list: List[str]):
        """
        Label the UDP DL blockage files by checking the corresponding TCP DL and UDP UL files
        :param run_dir_list:
        :return:
        """
        self.logger.info('-- Start checking for UDP DL blockage files')
        count = 0
        for run_dir in run_dir_list:
            try:
                tcp_dl_labeled_csv = find_files(run_dir, prefix='tcp_downlink', suffix='.csv')[0]
                udp_dl_labeled_csv = find_files(run_dir, prefix='udp_downlink', suffix='.csv')[0]
            except IndexError as e:
                self.logger.error(f'fail to find labeled csv: {e}')
                continue
            tcp_dl_status = self.get_validity_label_from_filename(tcp_dl_labeled_csv)
            udp_dl_status = self.get_validity_label_from_filename(udp_dl_labeled_csv)
            if udp_dl_status == TputBaseProcessor.Status.EMPTY.value and \
                    tcp_dl_status != TputBaseProcessor.Status.EMPTY.value:
                # UDP DL is blocked
                os.rename(udp_dl_labeled_csv, udp_dl_labeled_csv.replace(f'.{udp_dl_status}.csv',
                                                                         f'.{TputBaseProcessor.Status.EMPTY_BLOCKED.value}.csv'))
                self.logger.info(f'Label the blocked UDP DL file in this run: {run_dir}')
                count += 1
            elif udp_dl_status == TputBaseProcessor.Status.EMPTY_BLOCKED.value:
                self.logger.info(f'Label the blocked UDP DL file in this run: {run_dir}')
                count += 1
        self.logger.info(f'-- Finish, labeled all UDP DL blockage files, count: {count}')

    def get_validity_label_from_filename(self, filename: str):
        filename, label, ext = filename.split('.')
        labels = set([x.value for x in TputBaseProcessor.Status])
        if label in labels:
            return label
        raise ValueError(f'No valid label detected in the filename: {filename}')

    def merge_csv_files(self, dir_list: List[str], filename: str):
        udp_dl_valid_csv_files = []
        for run_dir in dir_list:
            udp_dl_file = find_files(run_dir, prefix='udp_downlink', suffix='.csv')[0]
            if self.get_validity_label_from_filename(udp_dl_file) != TputBaseProcessor.Status.EMPTY_BLOCKED.value:
                udp_dl_valid_csv_files.append(udp_dl_file)

        df = pd.concat([pd.read_csv(f) for f in udp_dl_valid_csv_files], ignore_index=True)
        df.to_csv(filename, index=False)
        self.logger.info(f'Saved all extracted data to the CSV file: {filename}')
