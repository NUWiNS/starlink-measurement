import os
import sys

from scripts.time_utils import StartEndLogTimeProcessor, format_datetime_as_iso_8601

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from typing import List, Dict
from scripts.nslook_utils import find_nslookup_files_by_dir_list, split_multiple_nslookup_results, parse_nslookup_result
from scripts.maine_starlink_trip.labels import DatasetLabel
from scripts.maine_starlink_trip.separate_dataset import read_dataset
from scripts.constants import DATASET_DIR
from scripts.logging_utils import create_logger

import pandas as pd

base_dir = os.path.join(DATASET_DIR, 'maine_starlink_trip/raw')
merged_csv_dir = os.path.join(DATASET_DIR, 'maine_starlink_trip/nslookup')
tmp_data_path = os.path.join(DATASET_DIR, 'maine_starlink_trip/tmp')
timezone_str = 'US/Eastern'

logger = create_logger('nslookup_parsing', filename=os.path.join(tmp_data_path, 'parse_nslookup_data_to_csv.log'))


def save_resolve_duration_info_to_csv(parsed_results: List[Dict], output_filename: str):
    """
    Save the resolve duration information to a csv file
    :param parsed_result:
    :return:
    """
    df = pd.DataFrame(parsed_results)
    df.drop(columns=['answers'], inplace=True)
    df.to_csv(output_filename, index=False)
    return df


def main():
    if not os.path.exists(merged_csv_dir):
        os.mkdir(merged_csv_dir)

    file_list = read_dataset('starlink', DatasetLabel.NORMAL.value)
    nslookup_files = find_nslookup_files_by_dir_list(file_list)
    total_file_count = len(nslookup_files)
    logger.info(f'Found NSLookup files: {total_file_count}')
    main_data_frame = pd.DataFrame()
    failed_files = []

    for file in nslookup_files:
        logger.info(f'Start to process {file} ...')
        try:
            with open(file, 'r') as f:
                lines = f.readlines()
                content = ''.join(lines)
                start_end_times = StartEndLogTimeProcessor.get_start_end_time_from_log(content,
                                                                                       timezone_str=timezone_str)
                parsed_results = [parse_nslookup_result(result) for result in
                                  (split_multiple_nslookup_results(content))]
                for start_end_time, result in zip(start_end_times, parsed_results):
                    result['start_ms'] = format_datetime_as_iso_8601(start_end_time[0])
                    result['end_ms'] = format_datetime_as_iso_8601(start_end_time[1])
                    result['duration_ms'] = (start_end_time[1] - start_end_time[0]).total_seconds() * 1000
            output_filename = file.replace('.out', '.csv')
            df = save_resolve_duration_info_to_csv(parsed_results, output_filename=output_filename)
            main_data_frame = pd.concat([main_data_frame, df], ignore_index=True)
            logger.info(f'Saved DNS resolve data to {output_filename}')
        except Exception as e:
            logger.error(f'Failed to process {file}: {e}')
            failed_files.append(file)

    # Save the merged data frame to a CSV file
    merged_csv_filename = os.path.join(merged_csv_dir, 'starlink_dns_resolve.csv')
    main_data_frame.to_csv(merged_csv_filename, index=False)
    logger.info(f'Saved merged DNS resolve data to {merged_csv_filename}')

    failed_file_count = len(failed_files)
    processed_file_count = total_file_count - failed_file_count
    logger.info(
        f'Process summary: total: {total_file_count}, processed: ({processed_file_count}), failed ({failed_file_count})')
    if failed_files:
        logger.error(f'Failed files: {failed_files}')


if __name__ == '__main__':
    main()
