from datetime import datetime
from os import path
import os
import sys
import pandas as pd

sys.path.append(path.join(path.dirname(__file__), '../..'))

from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.constants import DATASET_DIR, XcalField
from scripts.utilities.xcal_processing_utils import collect_periods_of_tput_measurements, filter_xcal_logs, read_daily_xcal_data, tag_xcal_logs_with_essential_info


# XCAL XLSX FIELDS
XCAL_TIME_STAMP = 'TIME_STAMP'
XCAL_EVENT_TECHNOLOGY = 'Event Technology'
XCAL_EVENT_TECHNOLOGY_BAND = 'Event Technology(Band)'
XCAL_EVENT_LTE = 'Event LTE Events'
XCAL_EVENT_5GNR = 'Event 5G-NR Events'
XCAL_EVENT_ALL = 'Event 5G-NR/LTE Events'
XCAL_SMART_TPUT_DL = 'Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'
XCAL_SMART_TPUT_UL = 'Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'

FIELD_LOCAL_DATETIME = 'local_dt'
FIELD_UTC_DATETIME = 'utc_dt'
FIELD_DL = 'dl'
FIELD_UL = 'ul'
FIELD_TECHNOLOGY = 'tech'
FIELD_BAND = 'band'
FIELD_EVENT_LTE = 'event_lte'
FIELD_EVENT_5GNR = 'event_5gnr'
FIELD_EVENT_ALL = 'event_all'
FIELD_APP_TPUT_PROTOCOL = 'app_tput_protocol'
FIELD_APP_TPUT_DIRECTION = 'app_tput_direction'

def save_extracted_periods_as_csv(periods: list[tuple[datetime, datetime, str]], output_file_path: str):
    df = pd.DataFrame(periods, columns=['start_time', 'end_time', 'protocol_direction'])
    df.to_csv(output_file_path, index=False)

def get_app_tput_periods(dir_list: list[str]) -> list[tuple[datetime, datetime, str]]:
    all_tput_periods = []
    for dir in dir_list:
        try:
          periods_of_tcp_dl = collect_periods_of_tput_measurements(base_dir=dir, protocol='tcp', direction='downlink')
          periods_of_tcp_ul = collect_periods_of_tput_measurements(base_dir=dir, protocol='tcp', direction='uplink')
          periods_of_udp_dl = collect_periods_of_tput_measurements(base_dir=dir, protocol='udp', direction='downlink')
          periods_of_udp_ul = collect_periods_of_tput_measurements(base_dir=dir, protocol='udp', direction='uplink')
          all_tput_periods.extend(periods_of_tcp_dl + periods_of_tcp_ul + periods_of_udp_dl + periods_of_udp_ul)
        except Exception as e:
            raise Exception(f"Failed to collect periods of tput measurements: {str(e)}")
    return all_tput_periods

def process_operator_xcal_tput(operator: str, location: str, output_dir: str):
    dir_list = read_dataset(operator, label=DatasetLabel.NORMAL.value)
    all_dates = set()
    for dir in dir_list:
        # dir is like /path/to/20240621/153752852
        date = dir.split('/')[-2]
        all_dates.add(date)
    
    print("--Stage 1: save app tput periods as csv")
    app_tput_periods_csv = path.join(output_dir, f'{operator}_app_tput_periods.csv')
    if not os.path.exists(app_tput_periods_csv):
        all_tput_periods = get_app_tput_periods(dir_list=dir_list)
        save_extracted_periods_as_csv(all_tput_periods, output_file_path=app_tput_periods_csv)
        print(f"Successfully collected periods of tput measurements and saved to {app_tput_periods_csv}")
    else:
        df_tput_periods = pd.read_csv(app_tput_periods_csv)
        # convert to list of tuples
        all_tput_periods = [(row['start_time'], row['end_time'], row['protocol_direction']) for _, row in df_tput_periods.iterrows()]
        print(f"Found existing app tput periods csv file: {app_tput_periods_csv}")
    
    print("-- Stage 2: read all xcal logs related to one location")
    xcal_log_dir = path.join(DATASET_DIR, 'xcal')
    df_xcal_all_logs = pd.DataFrame()
    all_dates = sorted(all_dates)
    
    # FIXME: only read one date for now!!!
    all_dates = [all_dates[0]]

    for date in all_dates:
        try:
            df_xcal_daily_data = read_daily_xcal_data(base_dir=xcal_log_dir, date=date, location=location, operator=operator)
            df_xcal_all_logs = pd.concat([df_xcal_all_logs, df_xcal_daily_data])
        except Exception as e:
            print(f"Failed to read or concatenate xcal data for date {date}: {str(e)}")
    print(f"Successfully load xcal data for all dates: {all_dates}")

    print("-- Stage 3: filter xcal logs by app tput periods")
    try:
        filtered_df = filter_xcal_logs(
            df_xcal_all_logs, 
            periods=all_tput_periods, 
            xcal_timezone='US/Eastern'
        )
        filtered_df.to_csv(path.join(output_dir, f'{operator}_xcal_raw_tput_logs.csv'), index=False)
        print("Successfully filtered xcal logs by app tput periods")
    except Exception as e:
        print(f"Failed to filter xcal logs: {str(e)}")
        raise e

    # print("-- Stage 4: rename and add useful columns")
    df_tput_cols = {
        XcalField.CUSTOM_UTC_TIME: filtered_df[XcalField.CUSTOM_UTC_TIME],
        XcalField.LOCAL_TIME: filtered_df[XcalField.CUSTOM_UTC_TIME].dt.tz_convert(TIMEZONE),
        XcalField.TPUT_DL: filtered_df[XCAL_SMART_TPUT_DL],
        XcalField.TPUT_UL: filtered_df[XCAL_SMART_TPUT_UL],
        XcalField.EVENT_LTE: filtered_df[XCAL_EVENT_LTE],
        XcalField.EVENT_5G_LTE: filtered_df[XCAL_EVENT_ALL],
        XcalField.TECH: filtered_df[XCAL_EVENT_TECHNOLOGY],
        XcalField.BAND: filtered_df[XCAL_EVENT_TECHNOLOGY_BAND],
        XcalField.APP_TPUT_PROTOCOL: filtered_df[FIELD_APP_TPUT_PROTOCOL],
        XcalField.APP_TPUT_DIRECTION: filtered_df[FIELD_APP_TPUT_DIRECTION],
    }
    if XcalField.EVENT_5G_LTE in filtered_df.columns:
        df_tput_cols[XcalField.EVENT_5G_LTE] = filtered_df[XcalField.EVENT_5G_LTE]

    df_tput = pd.DataFrame(df_tput_cols)
    xcal_tput_logs_csv = path.join(output_dir, f'{operator}_xcal_renamed_tput_logs.csv')
    df_tput.to_csv(xcal_tput_logs_csv, index=False)
    print(f"Successfully saved xcal tput logs to {xcal_tput_logs_csv}")

    if path.exists(xcal_tput_logs_csv):
        df_tput = pd.read_csv(xcal_tput_logs_csv)
    else:
        raise Exception(f"xcal tput logs csv file {xcal_tput_logs_csv} does not exist")
    
    print("-- Stage 5: process rows for dl and ul")
    df_tput = df_tput.dropna(subset=[XcalField.TPUT_DL, XcalField.TPUT_UL])
    # filter extreme values
    DL_THRESHOLD = 10 * 1000  # 10 Gbps
    UL_THRESHOLD = 10 * 1000  # 10 Gbps
    before_filter_count = len(df_tput)
    df_tput = df_tput[df_tput[XcalField.TPUT_DL] < DL_THRESHOLD]
    after_filter_count = len(df_tput)
    if before_filter_count - after_filter_count > 0:
        print(f"WARNING: Filtered out {before_filter_count - after_filter_count} rows for DOWNLINK due to extreme values")

    before_filter_count = len(df_tput)
    df_tput = df_tput[df_tput[XcalField.TPUT_UL] < UL_THRESHOLD]
    after_filter_count = len(df_tput)
    if before_filter_count - after_filter_count > 0:
        print(f"WARNING: Filtered out {before_filter_count - after_filter_count} rows for UPLINK due to extreme values")

    xcal_smart_tput_csv = path.join(output_dir, f'{operator}_xcal_smart_tput.csv')
    df_tput.to_csv(xcal_smart_tput_csv, index=False)
    print(f"Successfully saved xcal cleaned tput logs to {xcal_smart_tput_csv}")


def main():
    output_dir = path.join(ROOT_DIR, f'xcal')
    location = 'alaska'

    if not path.exists(output_dir):
        os.makedirs(output_dir)

    for operator in ['att', 'verizon', 'tmobile']:
        print(f"--- Processing {operator}...")
        process_operator_xcal_tput(operator, location, output_dir)
        print(f"--- Finished processing {operator}")


if __name__ == "__main__":
    main()