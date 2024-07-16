import glob
import json
import os
import re
import sys
from typing import List

from scripts.maine_starlink_trip.labels import DatasetLabel

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR
import unittest
from datetime import datetime

raw_data_path = os.path.join(DATASET_DIR, 'maine_starlink_trip/raw')
tmp_data_path = os.path.join(DATASET_DIR, 'maine_starlink_trip/tmp')


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


def label_exception(labels: List[str], _date: datetime):
    # starlink did not reboot correctly
    if datetime(2024, 5, 29, 15, 30) <= _date <= datetime(2024, 5, 29, 15, 59):
        labels.append(DatasetLabel.EXCEPTION.value)
    return labels


def label_invalid_iperf_udp_uplink(labels: List[str], _date: datetime):
    if _date < datetime(2024, 5, 28):
        labels.append(DatasetLabel.INVALID_UDP_UL.value)
    return labels


def get_labels_from_path(path_str: str) -> List[str]:
    datetime = get_datetime_from_path(path_str)
    labels = []
    label_funcs = [
        label_invalid_iperf_udp_uplink,
        label_exception,
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
    categories = ['att', 'verizon', 'starlink', 'dish_metrics', 'dish_history']
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
