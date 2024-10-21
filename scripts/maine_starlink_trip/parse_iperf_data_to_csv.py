import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.logging_utils import create_logger
from scripts.maine_starlink_trip.configs import ROOT_DIR
from scripts.maine_starlink_trip.labels import DatasetLabel
from scripts.maine_starlink_trip.separate_dataset import read_dataset
from scripts.utilities.UdpBlockageHelper import UdpBlockageHelper

from scripts.iperf_utils import parse_iperf_udp_result, find_udp_downlink_files_by_dir_list, IperfProcessorFactory, \
    IperfDataAnalyst
from scripts.utils import find_files

import pandas as pd

base_dir = os.path.join(ROOT_DIR, 'raw')
merged_csv_dir = os.path.join(ROOT_DIR, 'throughput')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
validation_dir = os.path.join(ROOT_DIR, 'validation')

logger = create_logger('iperf_parsing', filename=os.path.join(tmp_data_path, 'parse_iperf_data_to_csv.log'))
validation_logger = create_logger('validation', filename=os.path.join(validation_dir, f'iperf_data_validation.log'), filemode='w')

def find_udp_downlink_files(base_dir: str):
    return find_files(base_dir, prefix="udp_downlink", suffix=".out")


def process_iperf_files(files: List[str], protocol: str, direction: str):
    """
    Process iperf logs and save the extracted data to CSV files
    :param files:
    :param protocol: tcp | udp
    :return:
    """
    data_analyst = IperfDataAnalyst()
    for file in files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                processor = IperfProcessorFactory.create(
                    content=content,
                    protocol=protocol,
                    direction=direction,
                    file_path=file,
                    timezone_str='US/Eastern',
                    logger=validation_logger,
                )
                processor.process()
                data_points = processor.get_result()
                status = processor.get_status()
                data_analyst.add_processor(processor)

                df = pd.DataFrame(data_points)
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


def process_iperf_data_for_operator(operator: str):
    """
    :param operator: att | verizon | starlink
    :return:
    """
    dir_list = read_dataset(operator, DatasetLabel.NORMAL.value)
    udp_downlink_files = find_udp_downlink_files_by_dir_list(dir_list)

    print(f'Processing {operator.capitalize()} Phone\'s iperf throughput data...')
    print(f'Found {len(udp_downlink_files)} UDP downlink files, processing...')
    process_iperf_files(
        udp_downlink_files,
        protocol='udp',
        direction='downlink',
    )

    udp_blockage_helper = UdpBlockageHelper(logger=logger)
    udp_blockage_helper.label_udp_dl_blockage_files(dir_list)
    udp_blockage_helper.merge_csv_files(dir_list,
                                        filename=get_merged_csv_filename(
                                            operator, 'udp', 'downlink',
                                            base_dir=merged_csv_dir
                                        ))


def main():
    if not os.path.exists(merged_csv_dir):
        os.mkdir(merged_csv_dir)

    process_iperf_data_for_operator('att')
    print('----------------------------------')
    process_iperf_data_for_operator('verizon')
    print('----------------------------------')
    process_iperf_data_for_operator('starlink')
    print('----------------------------------')


if __name__ == '__main__':
    main()
