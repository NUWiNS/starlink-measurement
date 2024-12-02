from abc import ABC
from datetime import datetime
import json
import os
from typing import List, Tuple
import pandas as pd

from scripts.constants import CommonField
from scripts.time_utils import ensure_timezone
from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.utilities.xcal_processing_utils import collect_periods_of_tput_measurements

class AppTputPeriodExtractor(ABC):
    def __init__(self, operator: str):
        self.operator = operator

    def get_all_data_dirs(self):
        raise NotImplementedError

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

    def extract_app_tput_periods(self, timezone: str) -> List[Tuple[datetime, datetime, str, str]]:
        dir_list = self.get_all_data_dirs()
        all_tput_periods = self.get_app_tput_periods(dir_list, timezone=timezone)
        return all_tput_periods

    def save_extracted_periods_as_csv(self, periods: list[tuple[datetime, datetime, str]], output_file_path: str):
        df = pd.DataFrame(periods, columns=['start_time', 'end_time', CommonField.APP_TPUT_PROTOCOL, CommonField.APP_TPUT_DIRECTION])
        df.to_csv(output_file_path, index=False)
