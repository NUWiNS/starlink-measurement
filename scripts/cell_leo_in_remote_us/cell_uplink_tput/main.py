import json
import os
from typing import Dict, List
import sys



sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.cell_leo_in_remote_us.cell_downlink_tput.main import plot_metric_grid
from scripts.cell_leo_in_remote_us.common import aggregate_xcal_tput_data_by_location, cellular_operator_conf, cellular_location_conf
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('tcp_ul_with_areas', filename=os.path.join(current_dir, 'outputs', 'tcp_ul_with_areas.log'))

def main():
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Collect all data
    tput_data = {}
    for protocol in ['tcp', 'udp']:
        tput_data[protocol] = {}
        for direction in ['uplink']:
            tput_data[protocol][direction] = aggregate_xcal_tput_data_by_location(
                locations=['alaska', 'hawaii'],
                location_conf=cellular_location_conf,
                protocol=protocol,
                direction=direction
            )

    # Plot Uplink Performance
    uplink_metrics = [
        ('tcp_uplink', 'TCP Uplink', 'Throughput (Mbps)', XcalField.TPUT_UL),
        ('udp_uplink', 'UDP Uplink', 'Throughput (Mbps)', XcalField.TPUT_UL),
    ]
    plot_metric_grid(
        data={
            'tcp_uplink': tput_data['tcp']['uplink'],
            'udp_uplink': tput_data['udp']['uplink']
        },
        loc_conf=cellular_location_conf,
        operator_conf=cellular_operator_conf,
        metrics=uplink_metrics,
        max_xlim=175,
        enable_inset=True,
        inset_x_min=0,
        inset_x_max=15,
        inset_x_step=5,
        legend_loc=(0, 1),
        output_filepath=os.path.join(output_dir, 'cellular.uplink.ak_hi.pdf'),
    )

    # save_stats_network_kpi(tput_data, latency_data, location_conf, operator_conf, output_dir)

if __name__ == '__main__':
    main()