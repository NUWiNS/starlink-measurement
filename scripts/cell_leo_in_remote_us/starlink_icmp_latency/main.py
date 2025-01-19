import json
import os
from typing import Dict, List
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.cell_leo_in_remote_us.cell_icmp_latency.main import plot_metric_grid_flexible
from scripts.cell_leo_in_remote_us.cell_downlink_tput.main import plot_metric_grid
from scripts.cell_leo_in_remote_us.common import aggregate_latency_data_by_location, operator_conf, location_conf
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('tcp_ul_with_areas', filename=os.path.join(current_dir, 'outputs', 'tcp_ul_with_areas.log'))

def main():
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    latency_data = aggregate_latency_data_by_location(
        locations=['alaska', 'hawaii'], 
        location_conf=location_conf,
    )

    plot_metric_grid_flexible(
        plot_data=latency_data,
        row_conf={
            'latency': {
                'label': '',
                'order': 1,
                'filter_mask': None,
            },
        },
        col_conf={
            'alaska': {
                'label': 'Alaska',
                'order': 1,
                'filter_mask': lambda x: x['location'] == 'alaska',
                'x_field': 'rtt_ms',
                'x_label': 'Round-trip Time (ms)',
                # 'percentile_filter': 95,
            },
            'hawaii': {
                'label': 'Hawaii',
                'order': 2,
                'filter_mask': lambda x: x['location'] == 'hawaii',
                'x_field': 'rtt_ms',
                'x_label': 'Round-trip Time (ms)',
                # 'percentile_filter': 95,
            }
        },
        location_conf=location_conf,
        operator_conf=operator_conf,
        max_xlim=200,
        x_step=50,
        output_filepath=os.path.join(output_dir, 'ak_hi_all_operators.latency.pdf')
    )

    # save_stats_network_kpi(tput_data, latency_data, location_conf, operator_conf, output_dir)

if __name__ == '__main__':
    main()