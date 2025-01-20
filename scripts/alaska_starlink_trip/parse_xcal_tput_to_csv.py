from datetime import datetime
from os import path
import os
import sys
import pandas as pd

sys.path.append(path.join(path.dirname(__file__), '../..'))

from scripts.time_utils import now
from scripts.logging_utils import create_logger
from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.alaska_starlink_trip.separate_dataset import read_dataset
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.constants import DATASET_DIR, CommonField, XcalField
from scripts.utilities.xcal_processing_utils import collect_periods_of_tput_measurements, filter_xcal_logs, read_daily_xcal_data


tmp_dir = os.path.join(ROOT_DIR, 'tmp')
ping_dir = os.path.join(ROOT_DIR, 'ping')
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
    bbr_testing_data_dir_list = read_dataset(operator, label=DatasetLabel.BBR_TESTING_DATA.value)
    # add bbr testing data into the final dataset
    dir_list.extend(bbr_testing_data_dir_list)

    all_dates = set()
    for dir in dir_list:
        # dir is like /path/to/20240621/153752852
        date = dir.split('/')[-2]
        all_dates.add(date)
    
    logger.info("--Stage 1: save app tput periods as csv")
    app_tput_periods_csv = path.join(output_dir, f'{operator}_app_tput_periods.csv')
    all_tput_periods = get_app_tput_periods(dir_list=dir_list)
    save_extracted_periods_as_csv(all_tput_periods, output_file_path=app_tput_periods_csv)
    logger.info(f"collected {len(all_tput_periods)} periods of tput measurements and saved to {app_tput_periods_csv}")
    
    logger.info("-- Stage 2: read all xcal logs related to one location")
    xcal_log_dir = path.join(DATASET_DIR, 'xcal/alaska_sizhe_new_data')
    # xcal_log_dir = path.join(DATASET_DIR, 'xcal')
    df_xcal_all_logs = pd.DataFrame()
    all_dates = sorted(all_dates)

    for date in all_dates:
        try:
            df_xcal_daily_data = read_daily_xcal_data(base_dir=xcal_log_dir, date=date, location=location, operator=operator)
            df_xcal_all_logs = pd.concat([df_xcal_all_logs, df_xcal_daily_data], ignore_index=True)
        except Exception as e:
            logger.info(f"Failed to read or concatenate xcal data for date {date}: {str(e)}")
    logger.info(f"load xcal data (size: {len(df_xcal_all_logs)}) for all dates: {all_dates}")
    df_xcal_all_logs = df_xcal_all_logs.reset_index(drop=True)

    df_xcal_all_logs[XcalField.SRC_IDX] = df_xcal_all_logs.index
    df_xcal_all_logs.to_csv(path.join(output_dir, f'{operator}_xcal_raw_logs_all_dates.csv'))

    logger.info("-- Stage 3: filter xcal logs by app tput periods")
    try:
        filtered_df = filter_xcal_logs(
            df_xcal_all_logs, 
            periods=all_tput_periods, 
            xcal_timezone='US/Eastern',
            label=f'{operator}_{location}'
        )

        filtered_df.to_csv(path.join(output_dir, f'{operator}_xcal_raw_tput_logs.csv'))
        logger.info(f"filtered xcal logs (size: {len(filtered_df)}) by app tput periods and saved to {path.join(output_dir, f'{operator}_xcal_raw_tput_logs.csv')}")
    except Exception as e:
        logger.info(f"Failed to filter xcal logs: {str(e)}")
        raise e
    
    return filtered_df

def patch_actual_tech(df: pd.DataFrame, operator: str):
    """Patch the actual tech column by special logic"""
    if operator == 'verizon':
        logger.info("-- Patching actual tech column for Alaska Verizon:")
        # Alaska Verizon only has LTE according to Verizon Coverage Map: https://www.verizon.com/coverage-map/
        # Convert all unknown tech to LTE
        unknown_rows = df[df[XcalField.ACTUAL_TECH].str.lower() == 'unknown']
        df.loc[unknown_rows.index, XcalField.ACTUAL_TECH] = 'LTE'
        logger.info(f"Patched {len(unknown_rows)} rows with unknown tech to LTE")
    return df

def process_filtered_xcal_data_for_tput_and_save_to_csv(
        filtered_df: pd.DataFrame,
        operator: str, 
        output_dir: str,
    ):
    logger.info("-- Stage 4: rename and add useful columns")
    df_tput_cols = {
        XcalField.SRC_IDX: filtered_df[XcalField.SRC_IDX],
        XcalField.RUN_ID: filtered_df[XcalField.RUN_ID],
        XcalField.SEGMENT_ID: filtered_df[XcalField.SEGMENT_ID],
        XcalField.CUSTOM_UTC_TIME: filtered_df[XcalField.CUSTOM_UTC_TIME],
        XcalField.LOCAL_TIME: filtered_df[XcalField.CUSTOM_UTC_TIME].dt.tz_convert(TIMEZONE),
        XcalField.TPUT_DL: filtered_df[XCAL_SMART_TPUT_DL],
        XcalField.TPUT_UL: filtered_df[XCAL_SMART_TPUT_UL],
        XcalField.ACTUAL_TECH: filtered_df[XcalField.ACTUAL_TECH],
        XcalField.BAND: filtered_df[XCAL_EVENT_TECHNOLOGY_BAND],
        XcalField.APP_TPUT_PROTOCOL: filtered_df[FIELD_APP_TPUT_PROTOCOL],
        XcalField.APP_TPUT_DIRECTION: filtered_df[FIELD_APP_TPUT_DIRECTION],
        XcalField.LON: filtered_df[XcalField.LON],
        XcalField.LAT: filtered_df[XcalField.LAT],
        XcalField.LTE_EARFCN_DL: filtered_df[XcalField.LTE_EARFCN_DL],
        XcalField.SMART_PHONE_SYSTEM_INFO_NETWORK_TYPE: filtered_df[XcalField.SMART_PHONE_SYSTEM_INFO_NETWORK_TYPE],
    }
    if XcalField.EVENT_5G_LTE in filtered_df.columns:
        df_tput_cols[XcalField.EVENT_5G_LTE] = filtered_df[XcalField.EVENT_5G_LTE]

    df_tput = pd.DataFrame(df_tput_cols)
    xcal_tput_logs_csv = path.join(output_dir, f'{operator}_xcal_renamed_tput_logs.csv')
    df_tput.to_csv(xcal_tput_logs_csv, index=False)
    logger.info(f"Renamed and saved xcal tput logs to {xcal_tput_logs_csv}")
    
    logger.info("-- Stage 5: filter extrem tput for rows of dl and ul")
    df_tput = df_tput.dropna(subset=[XcalField.TPUT_DL, XcalField.TPUT_UL])
    # filter extreme values
    DL_THRESHOLD = 10 * 1000  # 10 Gbps
    UL_THRESHOLD = 10 * 1000  # 10 Gbps
    before_filter_count = len(df_tput)
    df_tput = df_tput[df_tput[XcalField.TPUT_DL] < DL_THRESHOLD]
    after_filter_count = len(df_tput)
    if before_filter_count - after_filter_count > 0:
        logger.info(f"WARNING: Filtered out {before_filter_count - after_filter_count} rows for DOWNLINK due to extreme values")

    before_filter_count = len(df_tput)
    df_tput = df_tput[df_tput[XcalField.TPUT_UL] < UL_THRESHOLD]
    after_filter_count = len(df_tput)
    if before_filter_count - after_filter_count > 0:
        logger.info(f"WARNING: Filtered out {before_filter_count - after_filter_count} rows for UPLINK due to extreme values")

    logger.info("-- Stage 6: patch tech column")
    df_tput = patch_actual_tech(df_tput, operator)

    return df_tput

def main():
    # output_dir = path.join(ROOT_DIR, f'xcal')
    output_dir = path.join(ROOT_DIR, f'xcal/sizhe_new_data')
    location = 'alaska'

    for dirs in [output_dir, tmp_dir]:
        if not path.exists(dirs):
            os.makedirs(dirs)

    for operator in ['att', 'verizon']:
        logger.info(f"--- Processing {operator}...")
        filtered_raw_xcal_df = process_operator_xcal_tput(operator, location, output_dir)
        smart_tput_df = process_filtered_xcal_data_for_tput_and_save_to_csv(filtered_raw_xcal_df, operator, output_dir)

        xcal_smart_tput_csv = path.join(output_dir, f'{operator}_xcal_smart_tput.csv')
        smart_tput_df.to_csv(xcal_smart_tput_csv, index=False)
        logger.info(f"Saved xcal cleaned tput logs (size: {len(smart_tput_df)}) to {xcal_smart_tput_csv}")

        logger.info(f"--- Finished processing {operator}")


if __name__ == "__main__":
    main()