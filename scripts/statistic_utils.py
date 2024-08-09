from typing import List

import numpy as np
import pandas as pd


def IQR(array: List | pd.DataFrame, threshold: float = 1.5):
    """
    Calculate the interquartile range of the given array
    :param array: list or pandas DataFrame
    :return: IQR
    """
    q1 = np.percentile(array, 25)
    q3 = np.percentile(array, 75)
    iqr = q3 - q1
    lower_bound = q1 - threshold * iqr
    upper_bound = q3 + threshold * iqr
    return lower_bound, upper_bound


def filter_outliers_by_IQR(data_frame: pd.DataFrame, threshold: float = 1.5):
    """
    Filter out the outliers in the given data frame by IQR
    :param data_frame:
    :param threshold:
    :return:
    """
    lower_bound, upper_bound = IQR(data_frame, threshold)
    return data_frame[(data_frame >= lower_bound) & (data_frame <= upper_bound)]
