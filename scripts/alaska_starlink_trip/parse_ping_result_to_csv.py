import os
import sys

from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.ping_utils import parse_ping_result, find_ping_files_by_dir_list

from scripts.constants import DATASET_DIR

from typing import Tuple, List

import pandas as pd

output_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/ping')


def save_to_csv(row_list: List[Tuple[str, str, str]], output_file):
    header = ['time', 'rtt_ms', 'operator']
    with open(output_file, 'w') as f:
        f.write(','.join(header) + '\n')
        for row in row_list:
            line = ','.join(row)
            f.write(line + '\n')


def count_subfolders(base_dir):
    return len(os.listdir(base_dir))


def save_data_frame_to_csv(data_frame, output_dir='.'):
    csv_filepath = os.path.join(output_dir, f'ping_all.csv')
    data_frame.to_csv(csv_filepath, index=False)
    print(f'save all the ping data to csv file: {csv_filepath}')


def parse_ping_for_operator(operator: str):
    print(f'Processing {operator} phone\'s ping data...')
    dir_list = read_dataset(operator, DatasetLabel.NORMAL.value)

    ping_files = find_ping_files_by_dir_list(dir_list)
    excluded_files = []
    total_df = pd.DataFrame()

    # Example to read and print the content of the found files
    for file in ping_files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                total_lines = len(content.splitlines())
                extracted_data = parse_ping_result(content)
                print('Processing:', file)
                print(f'-- total lines: {total_lines}')
                print(f'-- extracted lines: {len(extracted_data)}')

                if not extracted_data:
                    excluded_files.append(file)
                    continue

                df = pd.DataFrame(extracted_data, columns=['time', 'rtt_ms'])

                csv_file_path = file.replace('.out', '.csv')
                df.to_csv(csv_file_path, index=False)
                # print(f"Extracted data is saved to {csv_file_path}")

                total_df = pd.concat([total_df, df], ignore_index=True)
        except Exception as e:
            excluded_files.append(file)
            print(f"Error reading {file}: {e}")

    print('Total files:', len(ping_files))
    print('Excluded files:', len(excluded_files))

    # Save all the data to a single csv file
    total_df['operator'] = operator
    total_ping_csv = os.path.join(output_dir, f'{operator}_ping.csv')
    total_df.to_csv(total_ping_csv, index=False)
    print(f'Saved all the ping data to csv file: {total_ping_csv}')


def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    operators = ['att', 'verizon', 'starlink', 'tmobile']
    for operator in operators:
        parse_ping_for_operator(operator)
        print('-------')


if __name__ == '__main__':
    main()
