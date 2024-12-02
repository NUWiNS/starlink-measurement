# Plot CDF of throughput
from datetime import datetime
import json
import os
import sys
from typing import List, Tuple
import matplotlib.pyplot as plt
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import CommonField
from scripts.logging_utils import create_logger
from scripts.time_utils import ensure_timezone, now
from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.utilities.xcal_processing_utils import collect_periods_of_tput_measurements
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE

tmp_dir = os.path.join(ROOT_DIR, 'tmp')
starlink_metric_dir = os.path.join(ROOT_DIR, 'starlink')
current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, 'outputs')
logger = create_logger(__name__, filename=os.path.join(tmp_dir, f'parse_xcal_tput_to_csv.{now()}.log'))

class AppTputPeriodExtractor:
    def __init__(self, tmp_data_path: str):
        self.tmp_data_path = tmp_data_path

    def read_dataset(self, category: str, label: str):
        # read merged datasets
        datasets = json.load(open(os.path.join(self.tmp_data_path, f'{category}_merged_datasets.json')))
        return datasets[label]

    def get_app_tput_periods(self, dir_list: List[str], timezone: str):
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
        
        def transform_period(period: tuple[datetime, datetime, str]) -> tuple[datetime, datetime, str, str]:
            start_time, end_time, protocol_direction = period
            protocol, direction = protocol_direction.split('_')
            start_time = ensure_timezone(start_time, timezone)
            end_time = ensure_timezone(end_time, timezone)
            return (start_time, end_time, protocol, direction)

        transformed_periods = list(map(transform_period, all_tput_periods))
        return transformed_periods

    def extract_app_tput_periods(self, operator: str, timezone: str) -> List[Tuple[datetime, datetime, str, str]]:
        dir_list = self.read_dataset(operator, label=DatasetLabel.NORMAL.value)
        bbr_testing_data_dir_list = self.read_dataset(operator, label=DatasetLabel.BBR_TESTING_DATA.value)
        # add bbr testing data into the final dataset
        dir_list.extend(bbr_testing_data_dir_list)

        all_tput_periods = self.get_app_tput_periods(dir_list, timezone=timezone)
        return all_tput_periods

    def save_extracted_periods_as_csv(self, periods: list[tuple[datetime, datetime, str]], output_file_path: str):
        df = pd.DataFrame(periods, columns=['start_time', 'end_time', CommonField.APP_TPUT_PROTOCOL, CommonField.APP_TPUT_DIRECTION])
        df.to_csv(output_file_path, index=False)


def filter_metric_data_by_periods(metric_df: pd.DataFrame, periods: List[Tuple[datetime, datetime, str, str]], source_tz='UTC'):
    metric_df['res_time'] = pd.to_datetime(metric_df['res_time'], format='ISO8601')
    res_utc_ts_series = metric_df['res_time'].apply(lambda x: x.timestamp())
    
    filtered_rows = []
    for period in periods:
        start_time, end_time, protocol, direction = period
        start_ts = start_time.timestamp()
        end_ts = end_time.timestamp()
        period_row_df = metric_df[res_utc_ts_series.between(start_ts, end_ts)].copy()
        period_row_df[CommonField.SRC_IDX] = period_row_df.index
        period_row_df[CommonField.APP_TPUT_PROTOCOL] = protocol
        period_row_df[CommonField.APP_TPUT_DIRECTION] = direction
        filtered_rows.append(period_row_df)
    if len(filtered_rows) > 0:
        filtered_df = pd.concat(filtered_rows, ignore_index=True)
    else:
        filtered_df = pd.DataFrame()
    return filtered_df

def main():
    app_tput_periods_csv = os.path.join(starlink_metric_dir, 'starlink_app_tput_periods.csv')
    all_periods: List[Tuple[datetime, datetime, str, str]] = []
    # if not os.path.exists(app_tput_periods_csv):
    extractor = AppTputPeriodExtractor(tmp_data_path=tmp_dir)
    all_periods = extractor.extract_app_tput_periods(operator='starlink', timezone=TIMEZONE)
    extractor.save_extracted_periods_as_csv(
        all_periods, 
        output_file_path=app_tput_periods_csv
    )
    # else:
    #     df_app_tput_periods = pd.read_csv(app_tput_periods_csv)
    #     df_app_tput_periods['start_time'] = pd.to_datetime(df_app_tput_periods['start_time'], format='ISO8601')
    #     df_app_tput_periods['end_time'] = pd.to_datetime(df_app_tput_periods['end_time'], format='ISO8601')
    #     all_periods = df_app_tput_periods.values.tolist()

    starlink_metric_csv = os.path.join(starlink_metric_dir, 'starlink_metric.csv')
    metric_df = pd.read_csv(starlink_metric_csv)
    
    logger.info(f'Before filtering, data len: {len(metric_df)}')
    filtered_df = filter_metric_data_by_periods(metric_df=metric_df, periods=all_periods)
    logger.info(f'After filtering, data len: {len(filtered_df)}')

    filtered_metric_csv = os.path.join(starlink_metric_dir, 'starlink_metric.app_tput_filtered.csv')
    filtered_df.to_csv(filtered_metric_csv, index=False)
    logger.info(f"Saved filtered metric data to {filtered_metric_csv}")




if __name__ == '__main__':
    main()
