import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.common import TputBaseProcessor
from scripts.hawaii_starlink_trip.labels import DatasetLabel
from scripts.hawaii_starlink_trip.separate_dataset import read_dataset
from scripts.hawaii_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.logging_utils import create_logger
from scripts.utilities.UdpBlockageHelper import UdpBlockageHelper
from scripts.utils import find_files

from scripts.iperf_utils import find_udp_downlink_files_by_dir_list, \
    IperfDataAnalyst, IperfProcessorFactory

import pandas as pd

base_dir = os.path.join(ROOT_DIR, 'raw')
merged_csv_dir = os.path.join(ROOT_DIR, 'throughput')
merged_csv_dir_for_cubic = os.path.join(ROOT_DIR, 'throughput_cubic')
merged_csv_dir_for_bbr = os.path.join(ROOT_DIR, 'throughput_bbr')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
validation_dir = os.path.join(ROOT_DIR, 'validation')

logger = create_logger('iperf_parsing', filename=os.path.join(tmp_data_path, 'parse_iperf_data_to_csv.log'))
validation_logger = create_logger('validation', filename=os.path.join(validation_dir, f'iperf_data_validation.log'), filemode='w')

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
                    timezone_str=TIMEZONE,
                    logger=validation_logger,
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

    udp_blockage_helper = UdpBlockageHelper(logger=logger)
    udp_blockage_helper.label_udp_dl_blockage_files(dir_list)
    udp_blockage_helper.merge_csv_files(dir_list,
                                        filename=get_merged_csv_filename(
                                            operator, 'udp', 'downlink',
                                            base_dir=output_dir
                                        ))


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
