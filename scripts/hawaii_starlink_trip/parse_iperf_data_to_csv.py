import os
import sys
from typing import List

from scripts.common import TputBaseProcessor
from scripts.hawaii_starlink_trip.labels import DatasetLabel
from scripts.hawaii_starlink_trip.separate_dataset import read_dataset
from scripts.hawaii_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.logging_utils import create_logger
from scripts.utils import find_files

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.iperf_utils import find_udp_downlink_files_by_dir_list, \
    IperfDataAnalyst, IperfProcessorFactory

import pandas as pd

base_dir = os.path.join(ROOT_DIR, 'raw')
merged_csv_dir = os.path.join(ROOT_DIR, 'throughput')
merged_csv_dir_for_cubic = os.path.join(ROOT_DIR, 'throughput_cubic')
merged_csv_dir_for_bbr = os.path.join(ROOT_DIR, 'throughput_bbr')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')

logger = create_logger('iperf_parsing', filename=os.path.join(tmp_data_path, 'parse_iperf_data_to_csv.log'))


def process_iperf_files(files: List[str], protocol: str, direction: str):
    """
    Process iperf logs and save the extracted data to CSV files
    :param files:
    :param protocol: tcp | udp
    :return:
    """
    data_analyst = IperfDataAnalyst()
    main_data_frame = pd.DataFrame()
    for file in files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                processor = IperfProcessorFactory.create(
                    content=content,
                    protocol=protocol,
                    direction=direction,
                    file_path=file,
                    timezone_str=TIMEZONE
                )
                processor.process()
                data_points = processor.get_result()
                status = processor.get_status()
                data_analyst.add_processor(processor)

                df = pd.DataFrame(data_points)
                main_data_frame = pd.concat([main_data_frame, df], ignore_index=True)
                # Save to the same directory with the same filename but with .csv extension
                csv_file_path = file.replace('.out', f'.{status}.csv')
                df.to_csv(csv_file_path, index=False)
        except Exception as e:
            logger.error(f"Error reading {file}: {e}")

    logger.info('-----------------------')
    logs = data_analyst.describe()
    for log in logs:
        logger.info(log)
    logger.info('-----------------------')


def get_merged_csv_filename(operator: str, protocol: str, direction: str, base_dir=merged_csv_dir):
    return os.path.join(base_dir, f'{operator}_{protocol}_{direction}.csv')


def process_iperf_data_for_operator(
        operator: str,
        data_label: str = DatasetLabel.NORMAL.value,
        output_dir: str = merged_csv_dir
):
    """
    :param operator: att | verizon | starlink
    :param data_label: DatasetLabel
    :param output_dir:
    :return:
    """
    dir_list = read_dataset(operator, label=data_label)
    udp_downlink_files = find_udp_downlink_files_by_dir_list(dir_list)

    logger.info(f'Processing {operator.capitalize()} Phone\'s iperf throughput data...')
    logger.info(f'Found {len(udp_downlink_files)} UDP downlink files, processing...')
    process_iperf_files(
        udp_downlink_files,
        protocol='udp',
        direction='downlink'
    )

    # Label UDP DL blockage files
    label_udp_dl_blockage_files(dir_list)

    # Merge CSV files into one
    merge_csv_files(dir_list, filename=get_merged_csv_filename(operator, 'udp', 'downlink', base_dir=output_dir))


def get_validity_label_from_filename(filename: str):
    filename, label, ext = filename.split('.')
    labels = set([x.value for x in TputBaseProcessor.Status])
    if label in labels:
        return label
    raise ValueError(f'No valid label detected in the filename: {filename}')


def label_udp_dl_blockage_files(run_dir_list: List[str]):
    """
    Label the UDP DL blockage files by checking the corresponding TCP DL and UDP UL files
    :param run_dir_list:
    :return:
    """
    logger.info('-- Start checking for UDP DL blockage files')
    count = 0
    for run_dir in run_dir_list:
        tcp_dl_labeled_csv = find_files(run_dir, prefix='tcp_downlink', suffix='.csv')[0]
        udp_dl_labeled_csv = find_files(run_dir, prefix='udp_downlink', suffix='.csv')[0]
        udp_ul_labeled_csv = find_files(run_dir, prefix='udp_uplink', suffix='.csv')[0]
        tcp_dl_status = get_validity_label_from_filename(tcp_dl_labeled_csv)
        udp_dl_status = get_validity_label_from_filename(udp_dl_labeled_csv)
        udp_ul_status = get_validity_label_from_filename(udp_ul_labeled_csv)
        if udp_dl_status == TputBaseProcessor.Status.EMPTY.value and \
                udp_ul_status != TputBaseProcessor.Status.EMPTY.value and \
                tcp_dl_status != TputBaseProcessor.Status.EMPTY.value:
            # UDP DL is blocked
            os.rename(udp_dl_labeled_csv, udp_dl_labeled_csv.replace(f'.{udp_dl_status}.csv',
                                                                     f'.{TputBaseProcessor.Status.EMPTY_BLOCKED.value}.csv'))
            logger.info(f'Label the blocked UDP DL file in this run: {run_dir}')
            count += 1
    logger.info(f'-- Finish, labeled all UDP DL blockage files, count: {count}')


def merge_csv_files(dir_list: List[str], filename: str):
    udp_dl_valid_csv_files = []
    for run_dir in dir_list:
        udp_dl_file = find_files(run_dir, prefix='udp_downlink', suffix='.csv')[0]
        if get_validity_label_from_filename(udp_dl_file) != TputBaseProcessor.Status.EMPTY_BLOCKED.value:
            udp_dl_valid_csv_files.append(udp_dl_file)

    df = pd.concat([pd.read_csv(f) for f in udp_dl_valid_csv_files], ignore_index=True)
    df.to_csv(filename, index=False)
    logger.info(f'Saved all extracted data to the CSV file: {filename}')


def main():
    """
    Require NUTTCP data to be parsed first to detect UDP DL blockage files
    """
    for dir in [base_dir, merged_csv_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir)

    # Normal data
    process_iperf_data_for_operator('att')
    logger.info('----------------------------------')
    process_iperf_data_for_operator('verizon')
    logger.info('----------------------------------')
    process_iperf_data_for_operator('starlink')
    logger.info('----------------------------------')
    process_iperf_data_for_operator('tmobile')
    logger.info('----------------------------------')


if __name__ == '__main__':
    main()
