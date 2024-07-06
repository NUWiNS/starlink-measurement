import glob
import json
import os
import re
import sys
from typing import List

from scripts.alaska_starlink_trip.labels import DatasetLabel

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR
import unittest
from datetime import datetime

raw_data_path = os.path.join(DATASET_DIR, 'alaska_starlink_trip/raw')
tmp_data_path = os.path.join(DATASET_DIR, 'alaska_starlink_trip/tmp')


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


def label_udp_1440(labels: List[str], _date: datetime):
    # if date is between 20240621-0940 to 20240621-1030
    if datetime(2024, 6, 21, 9, 40) <= _date <= datetime(2024, 6, 21, 10, 30):
        labels.append(DatasetLabel.UDP_1440.value)
    return labels


def label_small_memory_and_cubic(labels: List[str], _date: datetime):
    if datetime(2024, 6, 21, 9, 40) <= _date <= datetime(2024, 6, 21, 12, 30):
        labels.append(DatasetLabel.SMALL_MEMORY_AND_CUBIC.value)
    return labels


def label_two_traceroute_run(labels: List[str], _date: datetime):
    if datetime(2024, 6, 21, 9, 40) <= _date <= datetime(2024, 6, 21, 15, 37):
        labels.append(DatasetLabel.TWO_TRACEROUTE_RUN.value)
    return labels


def label_test_data(labels: List[str], _date: datetime):
    if _date < datetime(2024, 6, 21, 9, 40):
        labels.append(DatasetLabel.TEST_DATA.value)
    return labels


def get_labels_from_path(path_str: str) -> List[str]:
    datetime = get_datetime_from_path(path_str)
    labels = []
    label_funcs = [
        label_udp_1440,
        label_small_memory_and_cubic,
        label_two_traceroute_run,
        label_test_data,
    ]
    for label_func in label_funcs:
        labels = label_func(labels, datetime)
    return labels


def save_mapping_to_csv(mapping: dict, filename: str):
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    with open(filename, 'w') as f:
        json.dump(mapping, f, indent=4)


def create_label_mapping_for_raw_data(category: str):
    """
    Create a mapping of raw data path to labels
    :param category:  such as 'att', 'verizon', 'starlink' where it should have sub-dir with date and time
    :return:
    """
    sub_dirs = glob.glob(os.path.join(raw_data_path, f"{category}/*/*"))
    mapping = {}
    for sub_dir in sub_dirs:
        labels = get_labels_from_path(sub_dir)
        mapping[sub_dir] = labels
    save_mapping_to_csv(mapping, os.path.join(tmp_data_path, f'{category}_labels.json'))
    print(f'Saved label mapping for {category} to {tmp_data_path}')


def separate_dataset(category: str):
    """
    Separate dataset into normal and labeled data
    :param category:
    :return:
    """
    session_labels = json.load(open(os.path.join(tmp_data_path, f'{category}_labels.json')))
    sub_dirs = glob.glob(os.path.join(raw_data_path, f"{category}/*/*"))
    datasets = {
        DatasetLabel.NORMAL.value: set(),
    }
    for sub_dir in sub_dirs:
        labels = session_labels[sub_dir]
        if not labels:
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

    save_mapping_to_csv(datasets, os.path.join(tmp_data_path, f'{category}_datasets.json'))
    print(f'Separated dataset for {category} to {tmp_data_path}')


def read_dataset(category: str, label: str):
    datasets = json.load(open(os.path.join(tmp_data_path, f'{category}_datasets.json')))
    return datasets[label]


def main():
    categories = ['att', 'verizon', 'starlink', 'tmobile', 'dish_metrics', 'dish_history']
    for category in categories:
        create_label_mapping_for_raw_data(category)
        separate_dataset(category)


# class Unittest(unittest.TestCase):
#     def test_get_datetime_from_path(self):
#         path_str = '20240621/094108769/'
#         date = get_datetime_from_path(path_str)
#         self.assertEqual(date, datetime(2024, 6, 21, 9, 41, 8, 769000))


if __name__ == '__main__':
    main()
