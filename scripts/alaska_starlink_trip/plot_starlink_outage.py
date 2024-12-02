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
from scripts.time_utils import now
from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.utilities.xcal_processing_utils import collect_periods_of_tput_measurements
from scripts.alaska_starlink_trip.configs import ROOT_DIR

tmp_dir = os.path.join(ROOT_DIR, 'tmp')
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

    def get_app_tput_periods(self, dir_list: List[str]):
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

    def extract_app_tput_periods(self, operator: str):
        dir_list = self.read_dataset(operator, label=DatasetLabel.NORMAL.value)
        bbr_testing_data_dir_list = self.read_dataset(operator, label=DatasetLabel.BBR_TESTING_DATA.value)
        # add bbr testing data into the final dataset
        dir_list.extend(bbr_testing_data_dir_list)

        all_tput_periods = self.get_app_tput_periods(dir_list)
        return all_tput_periods

    def save_extracted_periods_as_csv(self, periods: list[tuple[datetime, datetime, str]], output_file_path: str):
        def transform_period(period: tuple[datetime, datetime, str]) -> tuple[datetime, datetime, str, str]:
            protocol, direction = period[2].split('_')
            return (period[0], period[1], protocol, direction)

        transformed_periods = map(transform_period, periods)
        df = pd.DataFrame(transformed_periods, columns=['start_time', 'end_time', CommonField.APP_TPUT_PROTOCOL, CommonField.APP_TPUT_DIRECTION])
        df.to_csv(output_file_path, index=False)


def filter_metric_data_by_periods(metric_data: pd.DataFrame, periods: List[Tuple[datetime, datetime]]):
    pass

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    extractor = AppTputPeriodExtractor(tmp_data_path=tmp_dir)
    all_periods = extractor.extract_app_tput_periods(operator='starlink')
    extractor.save_extracted_periods_as_csv(
        all_periods, 
        output_file_path=os.path.join(output_dir, 'starlink_app_tput_periods.csv')
    )

    


if __name__ == '__main__':
    main()
