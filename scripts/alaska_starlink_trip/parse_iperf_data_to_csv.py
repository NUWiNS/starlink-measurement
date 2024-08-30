import os
import sys
from typing import List

from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset
from scripts.logging_utils import create_logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.iperf_utils import parse_iperf_udp_result, find_udp_downlink_files, find_udp_downlink_files_by_dir_list, \
    IperfDataAnalyst, IperfProcessorFactory

import pandas as pd

from scripts.constants import DATASET_DIR

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/raw')
merged_csv_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput')
merged_csv_dir_for_cubic = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput_cubic')
merged_csv_dir_for_bbr = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput_bbr')
tmp_data_path = os.path.join(DATASET_DIR, 'alaska_starlink_trip/tmp')

logger = create_logger('iperf_parsing', filename=os.path.join(tmp_data_path, 'parse_iperf_data_to_csv.log'))


def process_iperf_files(files: List[str], protocol: str, direction: str, output_csv_filename: str):
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
                    timezone_str='US/Alaska'
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

    main_data_frame.to_csv(output_csv_filename, index=False)
    logger.info(f'Saved all extracted data to the CSV file: {output_csv_filename}')


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
        direction='downlink',
        output_csv_filename=get_merged_csv_filename(operator, 'udp', 'downlink', base_dir=output_dir)
    )

    # TODO: identify UDP DL blocked files
    # How to find the corresponding TCP DL for UDP DL files in the separated folders?

def main():
    for dir in [base_dir, merged_csv_dir, merged_csv_dir_for_cubic, merged_csv_dir_for_bbr]:
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

    # Labeled data
    process_iperf_data_for_operator('att', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                    output_dir=merged_csv_dir_for_cubic)
    logger.info('----------------------------------')
    process_iperf_data_for_operator('verizon', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                    output_dir=merged_csv_dir_for_cubic)
    logger.info('----------------------------------')
    process_iperf_data_for_operator('starlink', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                    output_dir=merged_csv_dir_for_cubic)
    logger.info('----------------------------------')
    process_iperf_data_for_operator('tmobile', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                    output_dir=merged_csv_dir_for_cubic)
    logger.info('----------------------------------')

    process_iperf_data_for_operator('att', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                    output_dir=merged_csv_dir_for_bbr)
    logger.info('----------------------------------')
    process_iperf_data_for_operator('verizon', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                    output_dir=merged_csv_dir_for_bbr)
    logger.info('----------------------------------')
    process_iperf_data_for_operator('starlink', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                    output_dir=merged_csv_dir_for_bbr)
    logger.info('----------------------------------')
    process_iperf_data_for_operator('tmobile', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                    output_dir=merged_csv_dir_for_bbr)
    logger.info('----------------------------------')


if __name__ == '__main__':
    main()
