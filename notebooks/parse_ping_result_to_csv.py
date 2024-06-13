import unittest
from typing import Tuple, List

import pandas as pd
import pytz
from datetime import datetime

import os
import re

from notebooks.common import extract_operator_from_filename


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


def parse_ping_result(content: str):
    """
    :param content:
    :return: List[Tuple[time, rtt_ms]]
    """
    pattern = re.compile(
        r"\[(.*?)\].*?time=([\d.]+)\s+ms"
    )

    extracted_data = []

    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            dt, rtt = match.groups()
            extracted_data.append((format_timestamp(dt), rtt))

    return extracted_data


def save_to_csv(row_list: List[Tuple[str, str, str]], output_file):
    header = ['time', 'rtt_ms', 'operator']
    with open(output_file, 'w') as f:
        f.write(','.join(header) + '\n')
        for row in row_list:
            line = ','.join(row)
            f.write(line + '\n')


def find_ping_file(base_dir):
    return find_files(base_dir, prefix="ping", suffix=".out")


def count_subfolders(base_dir):
    return len(os.listdir(base_dir))


def save_data_frame_to_csv(data_frame, output_dir='.'):
    csv_filepath = os.path.join(output_dir, f'ping_all.csv')
    data_frame.to_csv(csv_filepath, index=False)
    print(f'save all the ping data to csv file: {csv_filepath}')


def main():
    base_dir = os.path.join(os.getcwd(), "../outputs/maine_starlink_trip/")
    output_dir = os.path.join(base_dir, 'datasets')

    ping_files = find_ping_file(base_dir)
    total_row_list = []
    excluded_files = []

    # Example to read and print the content of the found files
    for file in ping_files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                extracted_data = parse_ping_result(content)
                operator = extract_operator_from_filename(file)
                if not extracted_data:
                    excluded_files.append(file)
                    continue
                updated_row_list = [(time, rtt_ms, operator) for time, rtt_ms in extracted_data]
                csv_file_path = file.replace('.out', '.csv')
                save_to_csv(updated_row_list, csv_file_path)
                print(f"Extracted data is saved to {csv_file_path}")

                total_row_list.extend(updated_row_list)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print('Total files:', len(ping_files))
    print('Excluded files:', len(excluded_files))

    # Save all the data to a single csv file
    total_df = pd.DataFrame(total_row_list, columns=['time', 'rtt_ms', 'operator'])
    save_data_frame_to_csv(total_df, output_dir=output_dir)

# class Unittest(unittest.TestCase):
#     def test_parse_ping_result(self):
#         content = """
#         [2024-05-27 10:59:19.628708] PING 35.245.244.238 (35.245.244.238) 38(66) bytes of data.
# [2024-05-27 10:59:19.629152] 46 bytes from 35.245.244.238: icmp_seq=1 ttl=54 time=50.3 ms
# [2024-05-27 10:59:19.817146] 46 bytes from 35.245.244.238: icmp_seq=2 ttl=54 time=37.7 ms
# """
#         expected = [
#             ('2024-05-27T10:59:19.629152-04:00', '50.3'),
#             ('2024-05-27T10:59:19.817146-04:00', '37.7')
#         ]
#
#         self.assertEqual(parse_ping_result(content), expected)


if __name__ == '__main__':
    main()
