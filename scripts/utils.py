import os
import re
import unittest
import datetime
from typing import Dict, List

import numpy as np
import pandas as pd


def find_files(base_dir, prefix, suffix):
    target_files = []

    # Walk through the directory structure
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            should_append = False
            if prefix and suffix:
                if file.startswith(prefix) and file.endswith(suffix):
                    should_append = True
            elif prefix:
                if file.startswith(prefix):
                    should_append = True
            elif suffix:
                if file.endswith(suffix):
                    should_append = True
            if should_append:
                target_files.append(os.path.join(root, file))
    return target_files


def count_subfolders(base_dir):
    return len(os.listdir(base_dir))


def safe_get(source, key, default_value=None):
    if isinstance(source, dict):
        return source.get(key, default_value)
    else:
        return getattr(source, key, default_value)


def get_statistics(data_frame: pd.DataFrame, data_stats: Dict = None):
    return {
        'min': data_frame.min(),
        'max': data_frame.max(),
        'median': np.median(data_frame),
        'total_count': safe_get(data_stats, 'total_count', len(data_frame)),
        'filtered_count': safe_get(data_stats, 'filtered_count', len(data_frame)),
    }


def format_statistics(stats, unit: str = ''):
    total_count = safe_get(stats, 'total_count')
    filtered_count = safe_get(stats, 'filtered_count')
    if total_count is None or total_count == 0:
        percentage = 'N/A'
    else:
        # fixed to 2 decimal places
        percentage = f"{(filtered_count / total_count) * 100:.2f}%"
    return f"Median: {stats['median']:.2f} {unit}\nMin: {stats['min']:.2f} {unit}\nMax: {stats['max']:.2f} {unit}\nCount: {filtered_count}/{total_count} ({percentage})"


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
    date = datetime.datetime.strptime(date_str + time_str, '%Y%m%d%H%M%S%f')
    return date
