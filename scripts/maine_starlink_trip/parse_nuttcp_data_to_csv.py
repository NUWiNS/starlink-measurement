import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.maine_starlink_trip.configs import ROOT_DIR
from scripts.logging_utils import create_logger
from scripts.maine_starlink_trip.labels import DatasetLabel
from scripts.maine_starlink_trip.separate_dataset import read_dataset
from scripts.time_utils import now

import pandas as pd

from scripts.constants import DATASET_DIR
from scripts.nuttcp_utils import find_tcp_downlink_files_by_dir_list, \
    find_tcp_uplink_files_by_dir_list, find_udp_uplink_files_by_dir_list, NuttcpProcessorFactory, NuttcpDataAnalyst

base_dir = os.path.join(ROOT_DIR, 'raw')
merged_csv_dir = os.path.join(ROOT_DIR, 'throughput')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
validation_dir = os.path.join(ROOT_DIR, 'validation')

logger = create_logger('nuttcp_parsing', filename=os.path.join(tmp_data_path, f'parse_nuttcp_data_to_csv.{now()}.log'))
validation_logger = create_logger('validation', filename=os.path.join(validation_dir, f'nuttcp_data_validation.log'), filemode='w')


def process_nuttcp_files(files: List[str], protocol: str, direction: str, output_csv_filename: str):
    """
    Process nuttcp TCP files and save the extracted data to CSV files
    :param files:
    :param protocol: tcp | udp
    :param direction: uplink | downlink
    :return:
    """
    data_analyst = NuttcpDataAnalyst()
    main_data_frame = pd.DataFrame()
    for file in files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                processor = NuttcpProcessorFactory.create(
                    content=content,
                    protocol=protocol,
                    direction=direction,
                    file_path=file,
                    timezone_str='US/Eastern',
                    logger=validation_logger
                )
                processor.process()
                data_points = processor.get_result()
                status = processor.get_status()
                data_analyst.add_processor(processor)

                df = pd.DataFrame(data_points)
                # Save to the same directory with the same filename but with .csv extension
                csv_file_path = file.replace('.out', f'.{status}.csv')
                df.to_csv(csv_file_path, index=False)
                main_data_frame = pd.concat([main_data_frame, df], ignore_index=True)
        except Exception as e:
            logger.error(f"Error reading {file}: {e}")

    logger.info('-----------------------')
    logs = data_analyst.describe()
    for log in logs:
        logger.info(log)
    logger.info('-----------------------')

    main_data_frame.to_csv(output_csv_filename, index=False)
    logger.info(f'Saved all extracted data to the CSV file: {output_csv_filename}')


def get_merged_csv_filename(operator: str, protocol: str, direction: str, base_dir=merged_csv_dir):
    return os.path.join(base_dir, f'{operator}_{protocol}_{direction}.csv')


def process_nuttcp_data_for_operator(operator: str):
    """
    :param operator: att | verizon | starlink
    :return:
    """
    dir_list = read_dataset(operator, DatasetLabel.NORMAL.value)

    tcp_downlink_files = find_tcp_downlink_files_by_dir_list(dir_list)
    print(f'Processing {operator.capitalize()} Phone\'s NUTTCP throughput data...')
    print(f'Found {len(tcp_downlink_files)} TCP downlink files, processing...')
    process_nuttcp_files(
        tcp_downlink_files,
        protocol='tcp',
        direction='downlink',
        output_csv_filename=get_merged_csv_filename(operator, 'tcp', 'downlink')
    )

    tcp_uplink_files = find_tcp_uplink_files_by_dir_list(dir_list)
    print(f'Found {len(tcp_uplink_files)} TCP uplink files, processing...')
    process_nuttcp_files(
        tcp_uplink_files,
        protocol='tcp',
        direction='uplink',
        output_csv_filename=get_merged_csv_filename(operator, 'tcp', 'uplink')
    )

    udp_uplink_files = find_udp_uplink_files_by_dir_list(dir_list)
    print(f'Found {len(udp_uplink_files)} UDP uplink files, processing...')
    process_nuttcp_files(
        udp_uplink_files,
        protocol='udp',
        direction='uplink',
        output_csv_filename=get_merged_csv_filename(operator, 'udp', 'uplink')
    )
    print(
        '--- NOTE: We skip the UDP uplink data on 20240527 because we used iperf3 and they are not the throughput from the receiver side')


def main():
    for dir in [base_dir, merged_csv_dir, tmp_data_path]:
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)

    process_nuttcp_data_for_operator('att')
    print('----------------------------------')
    process_nuttcp_data_for_operator('verizon')
    print('----------------------------------')
    process_nuttcp_data_for_operator('starlink')
    print('----------------------------------')


if __name__ == '__main__':
    main()
