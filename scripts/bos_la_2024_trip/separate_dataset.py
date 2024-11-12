import glob
import json
import os
import re
import sys
from typing import List
from datetime import datetime
from overrides import overrides

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.time_utils import now
from scripts.logging_utils import create_logger
from scripts.utilities import DatasetSplitter
from scripts.bos_la_2024_trip.configs import ROOT_DIR, DatasetLabel

raw_data_path = os.path.join(ROOT_DIR, 'raw')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')

logger = create_logger(__name__, filename=os.path.join(tmp_data_path, f'separate_dataset_{now()}.log'))

def get_datetime_from_path(path_str: str) -> datetime:
    """
    :param path_str: such as '20240621/094108769/'
    :return:
    """
    date_time_regex = re.compile(r'.*(\d{8})/(\d{9})/*')
    match = date_time_regex.match(path_str)
    if not match:
        raise ValueError('Invalid path string')
    date_str, time_str = match.groups()
    date = datetime.strptime(date_str + time_str, '%Y%m%d%H%M%S%f')
    return date


def get_operator_from_path(path_str: str) -> str:
    """
    :param path_str: such as 'att/20240621/094108769/'
    :return:
    """
    operator_regex = re.compile(r'.*/(\w+)/\d{8}/\d{9}/*')
    match = operator_regex.match(path_str)
    if not match:
        raise ValueError('Invalid path string')
    operator = match.groups()[0]
    return operator


def label_test_data(labels: List[str], _date: datetime, operator: str):
    if _date < datetime(2024, 11, 1, 1):
        labels.append(DatasetLabel.TEST_DATA.value)

    return labels


def get_labels_from_path(path_str: str) -> List[str]:
    datetime = get_datetime_from_path(path_str)
    operator = get_operator_from_path(path_str)
    labels = []
    label_funcs = [
        label_test_data,
    ]
    for label_func in label_funcs:
        labels = label_func(labels, datetime, operator)
    return labels


def save_mapping(mapping: dict, filename: str):
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    with open(filename, 'w') as f:
        json.dump(mapping, f, indent=4)


def get_sub_dirs(pattern: str):
    sub_dirs = glob.glob(pattern)
    sub_dirs = [d for d in sub_dirs if os.path.isdir(d)]
    return sub_dirs


def create_label_mapping_for_raw_data(category: str):
    """
    Create a mapping of raw data path to labels
    :param category:  such as 'att', 'verizon', 'starlink' where it should have sub-dir with date and time
    :return:
    """
    sub_dirs = get_sub_dirs(pattern=os.path.join(raw_data_path, f"{category}/*/*"))
    mapping = {}
    for sub_dir in sub_dirs:
        labels = get_labels_from_path(sub_dir)
        mapping[sub_dir] = labels
    save_mapping(mapping, filename=os.path.join(tmp_data_path, f'{category}_labels.json'))
    logger.info(f'Saved label mapping for {category} to {tmp_data_path}')


def separate_dataset(category: str):
    """
    Separate dataset into normal and labeled data
    :param category:
    :return:
    """
    session_labels = json.load(open(os.path.join(tmp_data_path, f'{category}_labels.json')))
    sub_dirs = get_sub_dirs(pattern=os.path.join(raw_data_path, f"{category}/*/*"))
    datasets = {
        DatasetLabel.NORMAL.value: set(),
    }
    for sub_dir in sub_dirs:
        labels = session_labels[sub_dir]
        if not labels:
            # Not labeled data is considered as normal data
            datasets[DatasetLabel.NORMAL.value].add(sub_dir)
        else:
            for label in labels:
                if label not in datasets:
                    datasets[label] = set()
                datasets[label].add(sub_dir)

    # convert set to list and sort
    for label in datasets:
        datasets[label] = list(datasets[label])
        datasets[label].sort()

    save_mapping(datasets, os.path.join(tmp_data_path, f'{category}_datasets.json'))
    logger.info(f'Separated dataset for {category} to {tmp_data_path}')


def read_dataset(category: str, label: str):
    datasets = json.load(open(os.path.join(tmp_data_path, f'{category}_datasets.json')))
    return datasets[label]

 
class BosLaTripDatasetSplitter(DatasetSplitter):
    def __init__(self, input_dir: str, output_dir: str):
        super().__init__(input_dir, output_dir)

    @overrides
    def get_label_funcs(self):
        return [
            self.label_test_data,
        ]

    def label_test_data(self, labels: List[str], _date: datetime, operator: str):
        if _date < datetime(2024, 11, 1, 14):
            labels.append(DatasetLabel.TEST_DATA.value)
        if datetime(2024, 11, 5, 0) < _date < datetime(2024, 11, 5, 9, 30):
            labels.append(DatasetLabel.TEST_DATA.value)
        return labels
    
    def rename_5g_booster_tests(self, folder: str):
        pattern = 'tcp_downlink*.out'
        files = glob.glob(os.path.join(folder, pattern))
        count = 0
        if len(files) <= 1:
            return count
        # the first file is the tcp_downlink test, the rest are 5g booster tests
        # sort by file name (date) in an asc order
        files.sort()
        booster_tests = files[1:]
        for file in booster_tests:
            new_name = file.replace('tcp_downlink', '5g_booster')
            os.rename(file, new_name)
            count += 1
        return count
        
        
def main():
    categories = ['att', 'verizon', 'starlink', 'tmobile', 'dish_metrics', 'dish_history']
    splitter = BosLaTripDatasetSplitter(input_dir=raw_data_path, output_dir=tmp_data_path)
    for category in categories:
        mapping = splitter.create_label_mapping_for_raw_data(category)
        datasets = splitter.separate_dataset(category)

        for folder in mapping:
            renamed_count = splitter.rename_5g_booster_tests(folder)
            if renamed_count > 0:
                logger.info(f'Renamed {renamed_count} 5g booster tests for {folder}')
        


if __name__ == '__main__':
    main()
