import os
import sys
from typing import Dict

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from scripts.logging_utils import create_logger
from scripts.statistic_utils import filter_outliers_by_IQR
from scripts.utils import get_statistics, format_statistics

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR, OUTPUT_DIR

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/nslookup')
tmp_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/tmp')
output_dir = os.path.join(OUTPUT_DIR, 'alaska_starlink_trip/plots')

logger = create_logger('plot_cdf_dns_time', filename=os.path.join(tmp_dir, 'plot_cdf_dns_time.log'))

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def cdf(df: pd.DataFrame):
    sorted_data = np.sort(df)
    ranks = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    return sorted_data, ranks


def plot_cdf_dns_time(df: pd.DataFrame, data_stats: Dict, output_dir: str):
    fig, ax = plt.subplots()

    xvals, yvals = cdf(df)

    stats = get_statistics(df, data_stats=data_stats)
    label = format_statistics(stats, unit='ms')
    ax.plot(xvals, yvals, label=label)
    # ax.set_title('CDF of DNS Resolve Time')
    ax.set_xlabel('Resolve Time (ms)')
    ax.set_ylabel('CDF')
    ax.set_yticks(np.arange(0, 1.1, 0.2))
    ax.legend(prop={'size': 10}, loc='lower right')
    ax.grid()
    # fig.tight_layout()
    if (output_dir):
        plt.savefig(os.path.join(output_dir, 'cdf_dns_resolve_time.png'))
    plt.show()


def create_stats(total_count: int = 0, filter_by_count: int = 0):
    return {
        'is_filtered': total_count != filter_by_count,
        'total_count': total_count,
        'filtered_count': filter_by_count,
    }


def show_summary(df: pd.DataFrame):
    stats = df['duration_ms'].describe()
    logger.info(stats)
    dns_server_unique = df['dns_server'].unique()
    logger.info(f'unique dns servers: {dns_server_unique}')
    domain_unique = df['req_domain'].unique()
    logger.info(f'unique domains: {domain_unique}')
    return stats


def main():
    df = pd.read_csv(os.path.join(base_dir, 'starlink_dns_resolve.csv'))
    show_summary(df)
    filtered_df = filter_outliers_by_IQR(df['duration_ms'], threshold=1.5)
    data_stats = create_stats(total_count=len(df), filter_by_count=len(filtered_df))
    plot_cdf_dns_time(filtered_df, data_stats=data_stats, output_dir=output_dir)


if __name__ == '__main__':
    main()
