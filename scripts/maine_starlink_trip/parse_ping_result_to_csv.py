import os
import sys
from datetime import datetime
from typing import List

from scripts.ping_utils import find_ping_file, parse_ping_result

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR, OUTPUT_DIR

from typing import Tuple, List

import pandas as pd
import pytz

import re

from scripts.common import extract_operator_from_filename


def find_files(base_dir, prefix, suffix):
    target_files = []

    # Walk through the directory structure
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.startswith(prefix) and file.endswith(suffix):
                target_files.append(os.path.join(root, file))
    return target_files


def format_datetime_as_iso_8601(dt: datetime):
    """
    Format the time in the EDT timezone
    :param dt:
    :return:
    """
    return dt.isoformat()


def append_timezone(dt: datetime, timezone_str: str, is_dst: bool = True):
    timezone = pytz.timezone(timezone_str)
    dt_aware = timezone.localize(dt, is_dst=is_dst)  # is_dst=True for daylight saving time
    return dt_aware


def append_edt_timezone(dt: datetime, is_dst: bool = True):
    return append_timezone(dt, "US/Eastern", is_dst)


def parse_timestamp_of_ping(timestamp):
    # Parse the timestamp in the format of "2024-05-27 15:00:00.000000"
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")


def format_timestamp(dt_str: str):
    dt = parse_timestamp_of_ping(dt_str)
    dt_edt = append_edt_timezone(dt)
    return format_datetime_as_iso_8601(dt_edt)


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
    base_dir = os.path.join(DATASET_DIR, f"maine_starlink_trip/raw/{operator}")
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"Operator {operator} does not exist.")

    output_dir = os.path.join(DATASET_DIR, 'maine_starlink_trip/ping')

    ping_files = find_ping_file(base_dir)
    excluded_files = []
    total_df = pd.DataFrame()

    # Example to read and print the content of the found files
    for file in ping_files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                extracted_data = parse_ping_result(content)
                if not extracted_data:
                    excluded_files.append(file)
                    continue

                df = pd.DataFrame(extracted_data, columns=['time', 'rtt_ms'])

                csv_file_path = file.replace('.out', '.csv')
                df.to_csv(csv_file_path, index=False)
                # print(f"Extracted data is saved to {csv_file_path}")

                total_df = pd.concat([total_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print('Total files:', len(ping_files))
    print('Excluded files:', len(excluded_files))

    # Save all the data to a single csv file
    total_df['operator'] = operator
    total_ping_csv = os.path.join(output_dir, f'{operator}_ping.csv')
    total_df.to_csv(total_ping_csv, index=False)
    print(f'Saved all the ping data to csv file: {total_ping_csv}')


def main():
    operators = ['att', 'verizon', 'starlink']
    for operator in operators:
        parse_ping_for_operator(operator)
        print('-------')


if __name__ == '__main__':
    main()
