import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.iperf_utils import parse_iperf_udp_result
from scripts.utils import find_files


import pandas as pd

from scripts.constants import DATASET_DIR

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/raw')
merged_csv_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput')


def find_udp_downlink_files(base_dir: str):
    return find_files(base_dir, prefix="udp_downlink", suffix=".out")


def process_iperf_files(files: List[str], protocol: str, output_csv_filename: str):
    """
    Process iperf logs and save the extracted data to CSV files
    :param files:
    :param protocol: tcp | udp
    :return:
    """
    empty_files = []
    main_data_frame = pd.DataFrame()
    for file in files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                if protocol == 'udp':
                    extracted_data = parse_iperf_udp_result(content)
                elif protocol == 'tcp':
                    raise ValueError('Parsing TCP from iperf data is not implemented yet')
                else:
                    raise ValueError('Please specify protocol with udp')

                if not extracted_data:
                    empty_files.append(file)
                    continue
                df = pd.DataFrame(extracted_data)
                main_data_frame = pd.concat([main_data_frame, df], ignore_index=True)
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

    main_data_frame.to_csv(output_csv_filename, index=False)
    print(f'Saved all extracted data to the CSV file: {output_csv_filename}')


def get_merged_csv_filename(operator: str, protocol: str, direction: str):
    return os.path.join(merged_csv_dir, f'{operator}_{protocol}_{direction}.csv')


def process_iperf_data_for_operator(operator: str):
    """
    :param operator: att | verizon | starlink
    :return:
    """
    operator_base_dir = os.path.join(base_dir, operator)

    udp_downlink_files = find_udp_downlink_files(operator_base_dir)
    print(f'Processing {operator.capitalize()} Phone\'s iperf throughput data...')
    print(f'Found {len(udp_downlink_files)} UDP downlink files, processing...')
    process_iperf_files(
        udp_downlink_files,
        protocol='udp',
        output_csv_filename=get_merged_csv_filename(operator, 'udp', 'downlink')
    )


def main():
    if not os.path.exists(merged_csv_dir):
        os.mkdir(merged_csv_dir)

    process_iperf_data_for_operator('att')
    print('----------------------------------')
    process_iperf_data_for_operator('verizon')
    print('----------------------------------')
    process_iperf_data_for_operator('starlink')
    print('----------------------------------')
    process_iperf_data_for_operator('tmobile')
    print('----------------------------------')


if __name__ == '__main__':
    main()
