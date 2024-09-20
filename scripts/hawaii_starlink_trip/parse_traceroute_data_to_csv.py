import os
import sys

from pandas.core.common import flatten
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.hawaii_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.time_utils import StartEndLogTimeProcessor, format_datetime_as_iso_8601
from scripts.traceroute_utils import find_traceroute_files_by_dir_list, parse_traceroute_log

from typing import List, Dict
from scripts.hawaii_starlink_trip.labels import DatasetLabel
from scripts.hawaii_starlink_trip.separate_dataset import read_dataset
from scripts.logging_utils import create_logger

import pandas as pd

base_dir = os.path.join(ROOT_DIR, 'raw')
merged_csv_dir = os.path.join(ROOT_DIR, 'traceroute')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')

logger = create_logger('traceroute_parsing', filename=os.path.join(tmp_data_path, 'parse_traceroute_data_to_csv.log'))


def save_hop_info_to_csv(parsed_results: List[Dict], output_filename: str):
    """
    Save the resolve duration information to a csv file
    :param parsed_result:
    :return:
    """
    df = pd.DataFrame(parsed_results)
    df.to_csv(output_filename, index=False)
    return df


def main():
    if not os.path.exists(merged_csv_dir):
        os.mkdir(merged_csv_dir)

    file_list = read_dataset('starlink', DatasetLabel.NORMAL.value)
    traceroute_files = find_traceroute_files_by_dir_list(file_list)
    total_file_count = len(traceroute_files)
    logger.info(f'Found traceroute files: {total_file_count}')
    main_data_frame = pd.DataFrame()
    failed_files = []

    for file in traceroute_files:
        logger.info(f'Start to process {file} ...')
        try:
            with open(file, 'r') as f:
                lines = f.readlines()
                content = ''.join(lines)
                start_end_time = StartEndLogTimeProcessor.get_start_end_time_from_log(content,
                                                                                      timezone_str=TIMEZONE)[0]
                hops = parse_traceroute_log(content)
                data_points = []
                for hop in hops:
                    for probe in hop:
                        data_points.append(probe)

            output_filename = file.replace('.out', '.csv')
            df = save_hop_info_to_csv(data_points, output_filename=output_filename)
            df['start_time'] = format_datetime_as_iso_8601(start_end_time[0])
            df['end_time'] = format_datetime_as_iso_8601(start_end_time[1])
            main_data_frame = pd.concat([main_data_frame, df], ignore_index=True)
            logger.info(f'Saved traceroute data to {output_filename}')
        except Exception as e:
            logger.error(f'Failed to process {file}: {e}')
            failed_files.append(file)

    # Save the merged data frame to a CSV file
    merged_csv_filename = os.path.join(merged_csv_dir, 'starlink_traceroute.csv')
    main_data_frame.to_csv(merged_csv_filename, index=False)
    logger.info(f'Saved merged traceroute resolve data to {merged_csv_filename}')

    failed_file_count = len(failed_files)
    processed_file_count = total_file_count - failed_file_count
    logger.info(
        f'Process summary: total: {total_file_count}, processed: ({processed_file_count}), failed ({failed_file_count})')
    if failed_files:
        logger.error(f'Failed files: {failed_files}')


if __name__ == '__main__':
    main()