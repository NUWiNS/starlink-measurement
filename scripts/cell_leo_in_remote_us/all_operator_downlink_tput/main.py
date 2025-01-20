import json
import os
from typing import Dict, List
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.cell_leo_in_remote_us.cell_downlink_tput.main import plot_metric_grid
from scripts.cell_leo_in_remote_us.common import aggregate_xcal_tput_data_by_location, operator_conf, location_conf, cellular_location_conf, cellular_operator_conf
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger
from scripts.utilities.DatasetHelper import DatasetHelper

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('dl_with_areas', filename=os.path.join(current_dir, 'outputs', 'dl_with_areas.log'))


def get_tput_data_for_alaska_and_hawaii(protocol: str, direction: str):
    al_dataset_helper = DatasetHelper(os.path.join(location_conf['alaska']['root_dir'], 'throughput'))
    hawaii_dataset_helper = DatasetHelper(os.path.join(location_conf['hawaii']['root_dir'], 'throughput'))

    alaska_tput_data = al_dataset_helper.get_tput_data(operator='starlink', protocol=protocol, direction=direction)

    hawaii_tput_data = hawaii_dataset_helper.get_tput_data(operator='starlink', protocol=protocol, direction=direction)

    alaska_tput_data['location'] = 'alaska'
    hawaii_tput_data['location'] = 'hawaii'

    combined_data = pd.concat([alaska_tput_data, hawaii_tput_data])
    combined_data['operator'] = 'starlink'
    return combined_data

def main():
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    tput_data = {}
    for protocol in ['tcp', 'udp']:
        tput_data[protocol] = {}
        for direction in ['downlink']:
            cellular_df = aggregate_xcal_tput_data_by_location(
                locations=['alaska', 'hawaii'],
                location_conf=cellular_location_conf,
                protocol=protocol,
                direction=direction
            )
            starlink_df = get_tput_data_for_alaska_and_hawaii(protocol, direction)
            
            # Rename cellular_df column to match starlink_df
            cellular_df = cellular_df.rename(columns={XcalField.TPUT_DL: CommonField.TPUT_MBPS})
            
            # Keep only the columns we need for the analysis
            columns_to_keep = [CommonField.OPERATOR, CommonField.LOCATION, CommonField.TPUT_MBPS]
            cellular_df = cellular_df[columns_to_keep]
            starlink_df = starlink_df[columns_to_keep]
            
            # Combine the dataframes
            combined_df = pd.concat([cellular_df, starlink_df], ignore_index=True)
            
            # Store in the tput_data dictionary
            tput_data[protocol][direction] = combined_df

    metrics = [
        ('tcp_downlink', 'TCP Downlink', 'Throughput (Mbps)', CommonField.TPUT_MBPS),
        ('udp_downlink', 'UDP Downlink', 'Throughput (Mbps)', CommonField.TPUT_MBPS),
    ]
    plot_metric_grid(
        data={
            'tcp_downlink': tput_data['tcp']['downlink'],
            'udp_downlink': tput_data['udp']['downlink']
        },
        loc_conf=location_conf,
        operator_conf=operator_conf,
        metrics=metrics,
        # max_xlim=175,
        enable_inset=True,
        inset_x_min=0,
        inset_x_max=30,
        inset_x_step=10,
        inset_bbox_to_anchor=(0.5, 0, 0.45, 0.65),
        legend_loc=(0.2, 0.2),
        output_filepath=os.path.join(output_dir, 'all_operators.downlink.ak_hi.pdf'),
    )

    # save_stats_network_kpi(tput_data, latency_data, location_conf, operator_conf, output_dir)

if __name__ == '__main__':
    main()