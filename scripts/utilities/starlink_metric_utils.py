
from datetime import datetime
import logging
import os
from typing import List, Tuple
import pandas as pd

from scripts.utilities.AppTputPeriodExtractor import AppTputPeriodExtractor
from scripts.weather_area_type_query_utils import TypeIntervalQueryUtil
from scripts.constants import CommonField

class StarlinkMetricProcessor:
    def __init__(self, root_dir: str, timezone: str, logger: logging.Logger):
        self.operator = 'starlink'
        self.starlink_metric_dir = os.path.join(root_dir, 'starlink')
        self.tmp_dir = os.path.join(root_dir, 'tmp')
        self.others_dir = os.path.join(root_dir, 'others')

        self.timezone = timezone
        self.app_tput_extractor: AppTputPeriodExtractor | None = None
        self.logger = logger

    def process(self):
        self.app_tput_extractor = AppTputPeriodExtractor(tmp_data_path=self.tmp_dir)
        all_periods = self.app_tput_extractor.extract_app_tput_periods(operator=self.operator, timezone=self.timezone)
        starlink_metric_csv = os.path.join(self.starlink_metric_dir, 'starlink_metric.csv')
        metric_df = pd.read_csv(starlink_metric_csv)

        self.logger.info(f'Before filtering, data len: {len(metric_df)}')
        filtered_df = self.filter_metric_data_by_periods(metric_df=metric_df, periods=all_periods)
        self.logger.info(f'After filtering, data len: {len(filtered_df)}')

        weather_csv_path = os.path.join(self.others_dir, 'weather.csv')
        area_csv_path = os.path.join(self.others_dir, 'area.csv')
        
        weather_df = pd.read_csv(weather_csv_path)
        weather_df[CommonField.LOCAL_DT] = pd.to_datetime(weather_df[CommonField.LOCAL_DT], format="ISO8601")
        weather_query_util = TypeIntervalQueryUtil(weather_df[[CommonField.LOCAL_DT, 'value']].values.tolist())

        area_df = pd.read_csv(area_csv_path)
        area_df[CommonField.LOCAL_DT] = pd.to_datetime(area_df[CommonField.LOCAL_DT], format="ISO8601")
        area_query_util = TypeIntervalQueryUtil(area_df[[CommonField.LOCAL_DT, 'value']].values.tolist())

        filtered_df = self.append_weather_area_to_df(
            df=filtered_df, 
            weather_query_util=weather_query_util, 
            area_query_util=area_query_util
        )
        self.logger.info(f'Appended weather and area data to filtered df')

        filtered_metric_csv = os.path.join(self.starlink_metric_dir, 'starlink_metric.app_tput_filtered.csv')
        filtered_df.to_csv(filtered_metric_csv, index=False)
        self.logger.info(f"Saved filtered metric data to {filtered_metric_csv}")

    def save_app_tput_periods(self, all_periods: List[Tuple[datetime, datetime, str, str]]):
        app_tput_periods_csv = os.path.join(self.starlink_metric_dir, 'starlink_app_tput_periods.csv')
        self.app_tput_extractor.save_extracted_periods_as_csv(
            all_periods, 
            output_file_path=app_tput_periods_csv
        )
    
    def filter_metric_data_by_periods(self, metric_df: pd.DataFrame, periods: List[Tuple[datetime, datetime, str, str]]):
        metric_df['res_time'] = pd.to_datetime(metric_df['res_time'], format='ISO8601')
        res_utc_ts_series = metric_df['res_time'].apply(lambda x: x.timestamp())
        
        filtered_rows = []
        for period in periods:
            start_time, end_time, protocol, direction = period
            start_ts = start_time.timestamp()
            end_ts = end_time.timestamp()
            period_row_df = metric_df[res_utc_ts_series.between(start_ts, end_ts)].copy()

            if len(period_row_df) == 0:
                continue

            segment_id = f'{period_row_df.index[0]}:{period_row_df.index[-1]}'
            period_row_df[CommonField.SEGMENT_ID] = segment_id
            period_row_df[CommonField.SRC_IDX] = period_row_df.index
            period_row_df[CommonField.APP_TPUT_PROTOCOL] = protocol
            period_row_df[CommonField.APP_TPUT_DIRECTION] = direction
            filtered_rows.append(period_row_df)
        if len(filtered_rows) > 0:
            filtered_df = pd.concat(filtered_rows, ignore_index=True)
        else:
            filtered_df = pd.DataFrame()
        return filtered_df

    def drop_cols_before_appending(self, df: pd.DataFrame):
        if 'weather' in df.columns:
            df.drop(columns=['weather'], inplace=True)
        if 'area' in df.columns:
            df.drop(columns=['area'], inplace=True)
        return df

    def append_weather_area_to_df(self, df: pd.DataFrame, weather_query_util: TypeIntervalQueryUtil, area_query_util: TypeIntervalQueryUtil):
        df[CommonField.LOCAL_DT] = pd.to_datetime(df[CommonField.LOCAL_DT], format="ISO8601")
        df = self.drop_cols_before_appending(df)
        for idx, row in df.iterrows():
            df.at[idx, 'weather'] = weather_query_util.query(row[CommonField.LOCAL_DT])
            df.at[idx, 'area'] = area_query_util.query(row[CommonField.LOCAL_DT])
        return df
