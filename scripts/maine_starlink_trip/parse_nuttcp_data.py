import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd

from scripts.constants import DATASET_DIR
from scripts.nuttcp_utils import find_tcp_downlink_files, parse_nuttcp_tcp_result, save_extracted_data_to_csv, \
    find_tcp_uplink_files, find_udp_uplink_files, parse_nuttcp_udp_result

base_dir = os.path.join(DATASET_DIR, 'maine_starlink_trip/')


def process_nuttcp_files(files: List[str], protocol: str):
    """
    Process nuttcp TCP files and save the extracted data to CSV files
    :param files:
    :param protocol: tcp | udp
    :return:
    """
    empty_files = []
    for file in files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                if protocol == 'tcp':
                    extracted_data = parse_nuttcp_tcp_result(content)
                elif protocol == 'udp':
                    extracted_data = parse_nuttcp_udp_result(content)
                else:
                    raise ValueError('Please specify protocol with eithers tcp or udp')

                if not extracted_data:
                    empty_files.append(file)
                    continue
                df = pd.DataFrame(extracted_data)

                # Save to the same directory with the same filename but with .csv extension
                csv_file_path = file.replace('.out', '.csv')
                df.to_csv(csv_file_path, index=False)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    total_file_length = len(files)
    empty_files_length = len(empty_files)
    processed_files_length = total_file_length - empty_files_length
    print(
        f'Processing complete: {processed_files_length}/{total_file_length} files, {empty_files_length} files are empty.')


def process_nuttcp_data_for_operator(operator: str):
    """
    :param operator: att | verizon | starlink
    :return:
    """
    operator_base_dir = os.path.join(base_dir, operator)

    tcp_downlink_files = find_tcp_downlink_files(operator_base_dir)
    print(f'Processing {operator.capitalize()} Phone\'s NUTTCP throughput data...')
    print(f'Found {len(tcp_downlink_files)} TCP downlink files, processing...')
    process_nuttcp_files(tcp_downlink_files, protocol='tcp')

    tcp_uplink_files = find_tcp_uplink_files(operator_base_dir)
    print(f'Found {len(tcp_uplink_files)} TCP uplink files, processing...')
    process_nuttcp_files(tcp_uplink_files, protocol='tcp')

    udp_uplink_files = find_udp_uplink_files(operator_base_dir)
    print(f'Found {len(udp_uplink_files)} UDP uplink files, processing...')
    process_nuttcp_files(udp_uplink_files, protocol='udp')
    print(
        '--- NOTE: We skip the UDP uplink data on 20240527 because we used iperf3 and they are not the throughputs from the receiver side')


def main():
    process_nuttcp_data_for_operator('att')
    print('----------------------------------')
    process_nuttcp_data_for_operator('verizon')
    print('----------------------------------')
    process_nuttcp_data_for_operator('starlink')
    print('----------------------------------')


if __name__ == '__main__':
    main()
