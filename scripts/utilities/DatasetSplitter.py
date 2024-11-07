from abc import ABC
from datetime import datetime
import glob
import json
import os
import re
from typing import List

from scripts.bos_la_2024_trip.configs import DatasetLabel


class DatasetSplitter(ABC):
    """
    Abstract class, needed for future extension
    Split dataset into normal and labeled data
    """
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def get_datetime_from_path(self, path_str: str) -> datetime:
        """
        :param path_str: such as '20240621/094108769/'
        :return:
        """
        date_time_regex = re.compile(r".*(\d{8})/(\d{9})/*")
        match = date_time_regex.match(path_str)
        if not match:
            raise ValueError("Invalid path string")
        date_str, time_str = match.groups()
        date = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S%f")
        return date


    def get_operator_from_path(self, path_str: str) -> str:
        """
        :param path_str: such as 'att/20240621/094108769/'
        :return:
        """
        operator_regex = re.compile(r".*/(\w+)/\d{8}/\d{9}/*")
        match = operator_regex.match(path_str)
        if not match:
            raise ValueError("Invalid path string")
        operator = match.groups()[0]
        return operator
    

    def label_test_data(self, labels: List[str], _date: datetime, operator: str):
        raise NotImplementedError()

    def get_label_funcs(self):
        raise NotImplementedError()

    def get_labels_from_path(self, path_str: str) -> List[str]:
        datetime = self.get_datetime_from_path(path_str)
        operator = self.get_operator_from_path(path_str)
        labels = []
        label_funcs = self.get_label_funcs()
        for label_func in label_funcs:
            labels = label_func(labels, datetime, operator)
        return labels


    def save_mapping(self, mapping: dict, filename: str):
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        with open(filename, 'w') as f:
            json.dump(mapping, f, indent=4)


    def get_sub_dirs(self, pattern: str):
        sub_dirs = glob.glob(pattern)
        sub_dirs = [d for d in sub_dirs if os.path.isdir(d)]
        return sub_dirs


    def create_label_mapping_for_raw_data(self, category: str):
        """
        Create a mapping of raw data path to labels
        :param category:  such as 'att', 'verizon', 'starlink' where it should have sub-dir with date and time
        :return:
        """
        sub_dirs = self.get_sub_dirs(pattern=os.path.join(self.input_dir, f"{category}/*/*"))
        mapping = {}
        for sub_dir in sub_dirs:
            labels = self.get_labels_from_path(sub_dir)
            mapping[sub_dir] = labels
        self.save_mapping(mapping, filename=os.path.join(self.output_dir, f'{category}_labels.json'))
        print(f'Saved label mapping for {category} to {self.output_dir}')
        return mapping


    def separate_dataset(self, category: str):
        """
        Separate dataset into normal and labeled data
        :param category:
        :return:
        """
        session_labels = json.load(open(os.path.join(self.output_dir, f'{category}_labels.json')))
        sub_dirs = self.get_sub_dirs(pattern=os.path.join(self.input_dir, f"{category}/*/*"))
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

        self.save_mapping(datasets, os.path.join(self.output_dir, f'{category}_datasets.json'))
        print(f'Separated dataset for {category} to {self.output_dir}')
        return datasets


    def read_dataset(self, category: str, label: str):
        datasets = json.load(open(os.path.join(self.output_dir, f'{category}_datasets.json')))
        return datasets[label]

