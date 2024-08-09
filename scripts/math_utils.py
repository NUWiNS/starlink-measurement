from typing import List

import numpy as np
import pandas as pd


def get_cdf(df: List or pd.DataFrame or pd.Series or np.array):
    sorted_data = np.sort(df)
    ranks = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    return sorted_data, ranks
