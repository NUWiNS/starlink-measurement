import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset
from scripts.logging_utils import PrintLogger, create_logger
from scripts.time_utils import now


import pandas as pd

from scripts.constants import DATASET_DIR
from scripts.nuttcp_utils import parse_nuttcp_tcp_result, \
    parse_nuttcp_udp_result, find_tcp_downlink_files_by_dir_list, \
    NuttcpDataAnalyst, NuttcpProcessorFactory, find_tcp_uplink_files_by_dir_list, find_udp_uplink_files_by_dir_list

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/raw')
merged_csv_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput')
merged_csv_dir_for_cubic = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput_cubic')
merged_csv_dir_for_bbr = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput_bbr')
tmp_data_path = os.path.join(DATASET_DIR, 'alaska_starlink_trip/tmp')
accounting_dir = os.path.join('./accounting')

logger = create_logger('nuttcp_parsing', filename=os.path.join(tmp_data_path, f'parse_nuttcp_data_to_csv.{now()}.log'))
accounting_logger = create_logger('metrics', filename=os.path.join(accounting_dir, f'nuttcp_accounting.{now()}.log'))

def parse_nuttcp_content(content, protocol):
    """
    Parse nuttcp content and return the extracted data points
    :param content:
    :param protocol:
    :return:
    """
    if protocol == 'tcp':
        extracted_data = parse_nuttcp_tcp_result(content)
    elif protocol == 'udp':
        extracted_data = parse_nuttcp_udp_result(content)
    else:
        raise ValueError('Please specify protocol with eithers tcp or udp')

    if not extracted_data:
        return None
    return extracted_data


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
                    timezone_str='US/Alaska',
                    logger=accounting_logger
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


def get_merged_csv_filename(operator: str, protocol: str, direction: str, base_dir: str = merged_csv_dir):
    return os.path.join(base_dir, f'{operator}_{protocol}_{direction}.csv')


def process_nuttcp_data_for_operator(
        operator: str,
        data_label: str = DatasetLabel.NORMAL.value,
        output_dir: str = merged_csv_dir
):
    """
    :param operator: att | verizon | starlink
    :return:
    """
    dir_list = read_dataset(operator, data_label)
    
    logger.info(f'Processing {operator.capitalize()} Phone\'s NUTTCP throughput data...')

    tcp_downlink_files = find_tcp_downlink_files_by_dir_list(dir_list)
    logger.info(f'Found {len(tcp_downlink_files)} TCP downlink files, processing...')
    process_nuttcp_files(
        tcp_downlink_files,
        protocol='tcp',
        direction='downlink',
        output_csv_filename=get_merged_csv_filename(operator, 'tcp', 'downlink', base_dir=output_dir)
    )

    tcp_uplink_files = find_tcp_uplink_files_by_dir_list(dir_list)
    logger.info(f'Found {len(tcp_uplink_files)} TCP uplink files, processing...')
    process_nuttcp_files(
        tcp_uplink_files,
        protocol='tcp',
        direction='uplink',
        output_csv_filename=get_merged_csv_filename(operator, 'tcp', 'uplink', base_dir=output_dir)
    )

    udp_uplink_files = find_udp_uplink_files_by_dir_list(dir_list)
    logger.info(f'Found {len(udp_uplink_files)} UDP uplink files, processing...')
    process_nuttcp_files(
        udp_uplink_files,
        protocol='udp',
        direction='uplink',
        output_csv_filename=get_merged_csv_filename(operator, 'udp', 'uplink', base_dir=output_dir)
    )


def main():
    for dir_path in [merged_csv_dir, merged_csv_dir_for_cubic, merged_csv_dir_for_bbr]:
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

    # Normal dataset
    process_nuttcp_data_for_operator('starlink')
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('att')
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('verizon')
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('tmobile')
    logger.info('----------------------------------')

    # Labeled dataset
    process_nuttcp_data_for_operator('starlink', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                     output_dir=merged_csv_dir_for_cubic)
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('att', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                     output_dir=merged_csv_dir_for_cubic)
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('verizon', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                     output_dir=merged_csv_dir_for_cubic)
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('tmobile', data_label=DatasetLabel.SMALL_MEMORY_AND_CUBIC.value,
                                     output_dir=merged_csv_dir_for_cubic)

    process_nuttcp_data_for_operator('starlink', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                     output_dir=merged_csv_dir_for_bbr)
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('att', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                     output_dir=merged_csv_dir_for_bbr)
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('verizon', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                     output_dir=merged_csv_dir_for_bbr)
    logger.info('----------------------------------')
    process_nuttcp_data_for_operator('tmobile', data_label=DatasetLabel.BBR_TESTING_DATA.value,
                                     output_dir=merged_csv_dir_for_bbr)


if __name__ == '__main__':
    main()
