from datetime import datetime
from os import path
import os
import sys
import pandas as pd

sys.path.append(path.join(path.dirname(__file__), '../..'))

from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.constants import DATASET_DIR
from scripts.utilities.xcal_processing_utils import collect_periods_of_tput_measurements, filter_xcal_logs, read_daily_xcal_data, tag_xcal_logs_with_essential_info


# XCAL XLSX FIELDS
XCAL_TIME_STAMP = 'TIME_STAMP'
XCAL_LOCAL_DATETIME = 'LOCAL_DATETIME'
XCAL_EVENT_TECHNOLOGY = 'Event Technology'
XCAL_EVENT_TECHNOLOGY_BAND = 'Event Technology(Band)'
XCAL_SMART_TPUT_DL = 'Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'
XCAL_SMART_TPUT_UL = 'Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'

def save_extracted_periods_as_csv(periods: list[tuple[datetime, datetime, str]], output_file_path: str):
    df = pd.DataFrame(periods, columns=['start_time', 'end_time', 'protocol_direction'])
    df.to_csv(output_file_path, index=False)


def main():
    # deal with one location and one operator per time
    operator = 'att'
    location = 'alaska'

    xcal_log_dir = path.join(DATASET_DIR, 'xcal')
    output_dir = path.join(ROOT_DIR, f'xcal')
    tmp_dir = path.join(ROOT_DIR, f'tmp')

    if not path.exists(output_dir):
        os.makedirs(output_dir)

    dir_list = read_dataset(operator, label=DatasetLabel.NORMAL.value)

    all_tput_periods = []
    all_dates = set()
    for dir in dir_list:
        # dir is like /path/to/20240621/153752852
        date = dir.split('/')[-2]
        all_dates.add(date)

        try:
          periods_of_tcp_dl = collect_periods_of_tput_measurements(base_dir=dir, protocol='tcp', direction='downlink')
          periods_of_tcp_ul = collect_periods_of_tput_measurements(base_dir=dir, protocol='tcp', direction='uplink')
          periods_of_udp_dl = collect_periods_of_tput_measurements(base_dir=dir, protocol='udp', direction='downlink')
          periods_of_udp_ul = collect_periods_of_tput_measurements(base_dir=dir, protocol='udp', direction='uplink')
          all_tput_periods.extend(periods_of_tcp_dl + periods_of_tcp_ul + periods_of_udp_dl + periods_of_udp_ul)
        except Exception as e:
            raise Exception(f"Failed to collect periods of tput measurements: {str(e)}")
        
    save_extracted_periods_as_csv(all_tput_periods, output_file_path=os.path.join(tmp_dir, f'{operator}_app_tput_periods.csv'))
    print("Successfully collected periods of tput measurements.")

    # df_xcal_all_logs = pd.DataFrame()
    # for date in all_dates:
    #     try:
    #         df_xcal_daily_data = read_daily_xcal_data(base_dir=xcal_log_dir, date=date, location=location, operator=operator)
    #         df_xcal_all_logs = pd.concat([df_xcal_all_logs, df_xcal_daily_data])
    #         print(f"Successfully read and concatenated xcal data for date: {date}")
    #     except Exception as e:
    #         print(f"Failed to read or concatenate xcal data for date {date}: {str(e)}")

    # try:
    #     df_xcal_tput_logs = filter_xcal_logs(df_xcal_all_logs, periods=all_tput_periods)
    #     print("Successfully filtered xcal logs.")

    #     # drop rows with empty dl and ul tput columns
    #     df_xcal_tput_logs = df_xcal_tput_logs[df_xcal_tput_logs[XCAL_SMART_TPUT_DL].notna() & df_xcal_tput_logs[XCAL_SMART_TPUT_UL].notna()]

    #     df_xcal_tput_logs = tag_xcal_logs_with_essential_info(df_xcal_tput_logs, periods=all_tput_periods, timezone=TIMEZONE)
        
    #     # sort by LOCAL_DATETIME
    #     df_xcal_tput_logs = df_xcal_tput_logs.sort_values(by='utc_dt')
    #     # fill empty columns with the nearest non-null value
    #     for col in [XCAL_EVENT_TECHNOLOGY, XCAL_EVENT_TECHNOLOGY_BAND]:
    #         df_xcal_tput_logs[col] = df_xcal_tput_logs[col].ffill()
        
    #     output_file = path.join(output_dir, f'{operator}_smart_tput.csv')
    #     df_xcal_tput_logs.to_csv(output_file, index=False)
    #     print(f"Successfully saved filtered xcal logs to {output_file}")
    # except Exception as e:
    #     print(f"Failed to filter xcal logs: {str(e)}")


if __name__ == "__main__":
    main()