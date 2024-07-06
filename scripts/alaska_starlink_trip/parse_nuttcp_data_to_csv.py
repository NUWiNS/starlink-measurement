import os
import sys
from typing import List

from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd

from scripts.constants import DATASET_DIR
from scripts.nuttcp_utils import find_tcp_downlink_files, parse_nuttcp_tcp_result, save_extracted_data_to_csv, \
    find_tcp_uplink_files, find_udp_uplink_files, parse_nuttcp_udp_result, find_tcp_downlink_files_by_dir_list, \
    NuttcpDataAnalyst, NuttcpContentProcessor

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/raw')
merged_csv_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput')

if not os.path.exists(merged_csv_dir):
    os.mkdir(merged_csv_dir)


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


def process_nuttcp_files(files: List[str], protocol: str, output_csv_filename: str):
    """
    Process nuttcp TCP files and save the extracted data to CSV files
    :param files:
    :param protocol: tcp | udp
    :return:
    """
    data_analyst = NuttcpDataAnalyst()
    main_data_frame = pd.DataFrame()
    for file in files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                processor = NuttcpContentProcessor(
                    content,
                    protocol,
                    file_path=file
                )
                processor.process()
                data_points = processor.get_result()
                data_analyst.add_processor(processor)

                df = pd.DataFrame(data_points)
                # Save to the same directory with the same filename but with .csv extension
                csv_file_path = file.replace('.out', '.csv')
                df.to_csv(csv_file_path, index=False)
                main_data_frame = pd.concat([main_data_frame, df], ignore_index=True)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print('-----------------------')
    data_analyst.describe()
    print('-----------------------')

    main_data_frame.to_csv(output_csv_filename, index=False)
    print(f'Saved all extracted data to the CSV file: {output_csv_filename}')


def get_merged_csv_filename(operator: str, protocol: str, direction: str):
    return os.path.join(merged_csv_dir, f'{operator}_{protocol}_{direction}.csv')


def process_nuttcp_data_for_operator(operator: str):
    """
    :param operator: att | verizon | starlink
    :return:
    """
    operator_base_dir = os.path.join(base_dir, operator)
    dir_list = read_dataset(operator, DatasetLabel.NORMAL.value)

    # tcp_downlink_files = find_tcp_downlink_files(operator_base_dir)
    tcp_downlink_files = find_tcp_downlink_files_by_dir_list(dir_list)

    print(f'Processing {operator.capitalize()} Phone\'s NUTTCP throughput data...')
    print(f'Found {len(tcp_downlink_files)} TCP downlink files, processing...')
    process_nuttcp_files(
        tcp_downlink_files,
        protocol='tcp',
        output_csv_filename=get_merged_csv_filename(operator, 'tcp', 'downlink')
    )

    tcp_uplink_files = find_tcp_uplink_files(operator_base_dir)
    print(f'Found {len(tcp_uplink_files)} TCP uplink files, processing...')
    process_nuttcp_files(
        tcp_uplink_files,
        protocol='tcp',
        output_csv_filename=get_merged_csv_filename(operator, 'tcp', 'uplink')
    )

    udp_uplink_files = find_udp_uplink_files(operator_base_dir)
    print(f'Found {len(udp_uplink_files)} UDP uplink files, processing...')
    process_nuttcp_files(
        udp_uplink_files,
        protocol='udp',
        output_csv_filename=get_merged_csv_filename(operator, 'udp', 'uplink')
    )


def main():
    process_nuttcp_data_for_operator('starlink')
    print('----------------------------------')
    # process_nuttcp_data_for_operator('att')
    # print('----------------------------------')
    # process_nuttcp_data_for_operator('verizon')
    # print('----------------------------------')
    # process_nuttcp_data_for_operator('tmobile')
    # print('----------------------------------')


if __name__ == '__main__':
    main()
