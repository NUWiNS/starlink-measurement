from datetime import datetime
import json
import os
import sys
from typing import List, Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.alaska_starlink_trip.labels import DatasetLabel
from scripts.utilities.AppTputPeriodExtractor import AppTputPeriodExtractor
from scripts.utilities.starlink_metric_utils import StarlinkMetricProcessor
from scripts.logging_utils import create_logger
from scripts.time_utils import now
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE

class AlaskaAppTputPeriodExtractor(AppTputPeriodExtractor):
    def __init__(self):
        super().__init__(operator='starlink')

    def read_dataset(self, category: str, label: str):
        # read merged datasets
        tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
        datasets = json.load(open(os.path.join(tmp_data_path, f'{category}_merged_datasets.json')))
        return datasets[label]
    
    def get_all_data_dirs(self):
        dir_list = self.read_dataset(self.operator, label=DatasetLabel.NORMAL.value)
        bbr_testing_data_dir_list = self.read_dataset(self.operator, label=DatasetLabel.BBR_TESTING_DATA.value)
        # add bbr testing data into the final dataset
        dir_list.extend(bbr_testing_data_dir_list)
        return dir_list

def main():
    tmp_dir = os.path.join(ROOT_DIR, 'tmp')
    logger = create_logger(__name__, filename=os.path.join(tmp_dir, f'process_starlink_metric.{now()}.log'))
    processor = StarlinkMetricProcessor(
        root_dir=ROOT_DIR, 
        timezone=TIMEZONE, 
        app_tput_extractor=AlaskaAppTputPeriodExtractor(),
        logger=logger
    )
    processor.process()

if __name__ == '__main__':
    main()
