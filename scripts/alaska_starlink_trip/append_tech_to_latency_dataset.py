from datetime import datetime
from os import path
import os
import sys
import pandas as pd

sys.path.append(path.join(path.dirname(__file__), '../..'))

from scripts.alaska_starlink_trip.common import patch_actual_tech
from scripts.time_utils import now
from scripts.logging_utils import create_logger
from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.constants import DATASET_DIR, CommonField, XcalField
from scripts.utilities.xcal_processing_utils import collect_periods_of_ping_measurements, collect_periods_of_tput_measurements, filter_xcal_logs, read_daily_xcal_data


tmp_dir = os.path.join(ROOT_DIR, 'tmp')
# ping_dir = os.path.join(ROOT_DIR, 'ping')
ping_dir = os.path.join(ROOT_DIR, 'ping', 'sizhe_new_data')
logger = create_logger(__name__, filename=os.path.join(tmp_dir, f'parse_xcal_tput_to_csv.{now()}.log'))

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

def get_ping_periods(dir_list: list[str]) -> list[tuple[datetime, datetime, str]]:
    all_ping_periods = []
    for dir in dir_list:
        periods_of_ping = collect_periods_of_ping_measurements(base_dir=dir)
        all_ping_periods.extend(periods_of_ping)
    return all_ping_periods

def fuse_rtt_into_xcal_logs(df_xcal_all_logs: pd.DataFrame, df_rtt: pd.DataFrame) -> pd.DataFrame:
    """Fuse RTT data into XCAL logs based on timestamp order.
    
    Args:
        df_xcal_all_logs: DataFrame containing XCAL data
        df_rtt: DataFrame containing RTT measurements
    
    Returns:
        DataFrame with fused XCAL and RTT data, sorted by UTC timestamp
    """
    # Create a copy of xcal df and add new columns with NA
    fused_df = df_xcal_all_logs
    
    # Get unique columns from RTT that aren't in XCAL
    rtt_only_cols = set(df_rtt.columns) - set(fused_df.columns)
    
    # Add RTT-specific columns to XCAL df with NA
    for col in rtt_only_cols:
        fused_df[col] = pd.NA
    
    # Create RTT rows with NA for XCAL-specific columns
    xcal_only_cols = set(fused_df.columns) - set(df_rtt.columns)
    rtt_rows = df_rtt
    for col in xcal_only_cols:
        rtt_rows[col] = pd.NA
    
    # Ensure all columns exist in both dataframes
    rtt_rows = rtt_rows.reindex(columns=fused_df.columns)
    
    # Concatenate and sort
    fused_df = pd.concat([fused_df, rtt_rows], ignore_index=True)
    fused_df = fused_df.sort_values(by=CommonField.LOCAL_DT).reset_index(drop=True)
    
    # fill the missing values for lon and lat with nearby values
    
    fused_df[XcalField.LON] = fused_df[XcalField.LON].ffill().bfill()
    fused_df[XcalField.LAT] = fused_df[XcalField.LAT].ffill().bfill()

    return fused_df

def append_tech_to_rtt_data(operator: str, location: str, output_dir: str):
    dir_list = read_dataset(operator, label=DatasetLabel.NORMAL.value)
    bbr_dir_list = read_dataset(operator, label=DatasetLabel.BBR_TESTING_DATA.value)
    dir_list.extend(bbr_dir_list)
    all_dates = set()
    for dir in dir_list:
        # dir is like /path/to/20240621/153752852
        date = dir.split('/')[-2]
        all_dates.add(date)
    
    logger.info("--Stage 1: extract ping periods")
    all_ping_periods = get_ping_periods(dir_list=dir_list)
    save_extracted_periods_as_csv(all_ping_periods, output_file_path=path.join(output_dir, f'{operator}_ping_periods.csv'))

    logger.info(f"collected {len(all_ping_periods)} periods of ping measurements")
    
    logger.info("-- Stage 2: read all xcal logs related to one location")
    all_xcal_raw_data_csv = path.join(output_dir, f'{operator}_xcal_raw_logs_all_dates.csv')
    # xcal_log_dir = path.join(DATASET_DIR, 'xcal')
    xcal_log_dir = path.join(DATASET_DIR, 'xcal', 'alaska_sizhe_new_data')
    df_xcal_all_logs = pd.DataFrame()
    if path.exists(all_xcal_raw_data_csv):
        logger.info(f"load xcal data (size: {len(df_xcal_all_logs)}) from {all_xcal_raw_data_csv}")
        df_xcal_all_logs = pd.read_csv(all_xcal_raw_data_csv)
    else:
        logger.info(f"xcal data not found, load and concatenate xcal logs")
        all_dates = sorted(all_dates)
        for date in all_dates:
            try:
                df_xcal_daily_data = read_daily_xcal_data(base_dir=xcal_log_dir, date=date, location=location, operator=operator)
                df_xcal_all_logs = pd.concat([df_xcal_all_logs, df_xcal_daily_data])
            except Exception as e:
                logger.info(f"Failed to read or concatenate xcal data for date {date}: {str(e)}")
        logger.info(f"load xcal data (size: {len(df_xcal_all_logs)}) for all dates: {all_dates}")
        df_xcal_all_logs = df_xcal_all_logs.reset_index(drop=True)
        df_xcal_all_logs[XcalField.SRC_IDX] = df_xcal_all_logs.index
        df_xcal_all_logs.to_csv(all_xcal_raw_data_csv)
    
    # Append auxiliary columns
    df_xcal_all_logs[CommonField.LOCAL_DT] = pd.to_datetime(df_xcal_all_logs[XcalField.TIMESTAMP], errors='coerce').dt.tz_localize(
            'US/Eastern', 
            ambiguous='raise', 
            nonexistent='raise'
        ).dt.tz_convert(TIMEZONE)
    df_xcal_all_logs[XcalField.CUSTOM_UTC_TIME] = df_xcal_all_logs[CommonField.LOCAL_DT].dt.tz_convert('UTC')
    df_xcal_all_logs.dropna(subset=[CommonField.LOCAL_DT], inplace=True)
    df_xcal_all_logs[CommonField.UTC_TS] = df_xcal_all_logs[CommonField.LOCAL_DT].apply(lambda x: x.timestamp())

    logger.info('-- Stage 3: fuse xcal logs and ping data')
    rtt_csv_path = path.join(ping_dir, f'{operator}_ping.csv')
    df_rtt = pd.read_csv(rtt_csv_path)
    original_rtt_cols = list(df_rtt.columns)
    df_rtt[CommonField.LOCAL_DT] = pd.to_datetime(df_rtt[CommonField.LOCAL_DT], format='ISO8601')
    df_rtt[XcalField.CUSTOM_UTC_TIME] = df_rtt[CommonField.LOCAL_DT].dt.tz_convert('UTC')
    fused_xcal_all_logs_df = fuse_rtt_into_xcal_logs(df_xcal_all_logs, df_rtt)
    fused_xcal_all_logs_df.to_csv(path.join(output_dir, f'fused_rtt_xcal_logs.{operator}.csv'))

    logger.info("-- Stage 4: filter xcal logs by ping periods")
    try:
        filtered_df = filter_xcal_logs(
            fused_xcal_all_logs_df, 
            periods=all_ping_periods, 
            xcal_timezone='US/Eastern',
            label=f"ping.{operator}_{location}",
        )
        output_csv_path = path.join(output_dir, f'{operator}_xcal_raw_logs_with_rtt.csv')
        filtered_df.to_csv(output_csv_path, index=False)
        logger.info(f"filtered xcal logs (size: {len(filtered_df)}) by ping periods and saved to {output_csv_path}")
    except Exception as e:
        logger.info(f"Failed to filter xcal logs: {str(e)}")
        raise e
    
    logger.info("-- Stage 5: filter rows that have RTT data and save as ping data with tech")
    filtered_df = filtered_df[filtered_df[CommonField.RTT_MS].notna()]
    if len(filtered_df) == 0:
        logger.warn(f"No rows with RTT data found, skip saving")
    output_csv_path = path.join(ping_dir, f'{operator}_ping.csv')

    logger.info('-- Stage 6: patch actual tech column')
    filtered_df = patch_actual_tech(filtered_df, operator, logger)

    logger.info('-- Stage 7: remove unknown tech rows')
    filtered_df = filtered_df[filtered_df[XcalField.ACTUAL_TECH] != 'Unknown']

    # only save the original cols from df_rtt and tech
    # filtered_df = filtered_df[original_rtt_cols + [
    #     XcalField.SEGMENT_ID, 
    #     XcalField.ACTUAL_TECH, 
    #     XcalField.LON, 
    #     XcalField.LAT
    #     ]
    # ]

    filtered_df.to_csv(output_csv_path, index=False)
    logger.info(f"filtered xcal logs (size: {len(filtered_df)}) that have RTT data and saved to {output_csv_path}")

def main():
    # output_dir = path.join(ROOT_DIR, 'xcal')
    output_dir = path.join(ROOT_DIR, 'xcal', 'sizhe_new_data')
    location = 'alaska'

    for dirs in [output_dir, tmp_dir]:
        if not path.exists(dirs):
            os.makedirs(dirs)

    for operator in ['verizon', 'att']:
        logger.info(f"--- Processing {operator}...")
        append_tech_to_rtt_data(operator, location, output_dir)
        logger.info(f"--- Finished processing {operator}")


if __name__ == "__main__":
    main()