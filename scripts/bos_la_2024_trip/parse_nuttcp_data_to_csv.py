import os
import sys
from typing import List
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.bos_la_2024_trip.configs import ROOT_DIR, DatasetLabel
from scripts.bos_la_2024_trip.separate_dataset import read_dataset
from scripts.logging_utils import create_logger
from scripts.time_utils import now
from scripts.nuttcp_utils import find_5g_booster_files_by_dir_list, parse_nuttcp_tcp_result, \
    parse_nuttcp_udp_result, find_tcp_downlink_files_by_dir_list, \
    NuttcpDataAnalyst, NuttcpProcessorFactory, find_tcp_uplink_files_by_dir_list, find_udp_uplink_files_by_dir_list

base_dir = os.path.join(ROOT_DIR, 'raw')
merged_csv_dir = os.path.join(ROOT_DIR, 'throughput')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
validation_dir = os.path.join(ROOT_DIR, 'validation')

logger = create_logger('nuttcp_parsing', filename=os.path.join(tmp_data_path, f'parse_nuttcp_data_to_csv.{now()}.log'))
accounting_logger = create_logger('validation', filename=os.path.join(validation_dir, f'nuttcp_data_validation.log'), filemode='w')


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


def process_nuttcp_files(
        files: List[str], 
        protocol: str, 
        direction: str, 
        output_csv_filename: str,
        extend_existing_csv=False, 
        allow_zero_tput_padding = True,
        expected_data_points=240,
    ):
    """
    Process nuttcp TCP files and save the extracted data to CSV files
    :param files:
    :param protocol: tcp | udp
    :param direction: uplink | downlink
    :return:
    """
    data_analyst = NuttcpDataAnalyst()
    if extend_existing_csv:
        main_data_frame = pd.read_csv(output_csv_filename)
    else:        
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
                    logger=accounting_logger,
                    allow_zero_tput_padding=allow_zero_tput_padding,
                    expected_data_points=expected_data_points
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

    booster_5g_files = find_5g_booster_files_by_dir_list(dir_list)
    logger.info(f'Found {len(booster_5g_files)} 5g booster files, processing...')
    process_nuttcp_files(
        booster_5g_files,
        protocol='tcp',
        direction='downlink',
        output_csv_filename=get_merged_csv_filename(operator, 'tcp', 'downlink', base_dir=output_dir),
        extend_existing_csv=True,
        expected_data_points=11,
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
    for dir_path in [merged_csv_dir]:
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

if __name__ == '__main__':
    main()
