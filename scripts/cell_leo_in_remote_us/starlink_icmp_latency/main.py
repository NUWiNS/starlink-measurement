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


def save_stats_network_kpi(
        tput_data: Dict[str, Dict[str, pd.DataFrame]],
        latency_data: pd.DataFrame,
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_dir: str,
    ):
    metrics = [
        ('tcp', 'downlink', 'TCP Downlink', 'throughput_mbps'),
        ('tcp', 'uplink', 'TCP Uplink', 'throughput_mbps'),
        ('udp', 'downlink', 'UDP Downlink', 'throughput_mbps'),
        ('udp', 'uplink', 'UDP Uplink', 'throughput_mbps'),
        ('icmp', 'latency', 'Latency', 'rtt_ms'),
    ]
    
    # Define thresholds for each metric type
    thresholds = {
        'downlink': {
            'basic': 3,
            'smooth': 10,
            'ideal': 25
        },
        'uplink': {
            'basic': 1,
            'smooth': 5,
            'ideal': 10
        },
        'latency': {
            'basic': 150,
            'smooth': 100,
            'ideal': 50
        }
    }
    
    for protocol, direction, metric_name, data_field in metrics:
        stats = {}
        for location in loc_conf.keys():
            stats[location] = {}
            
            # Get data for this metric and location
            if direction == 'latency':  # Latency
                location_data = latency_data[latency_data['location'] == location]
                threshold_type = 'latency'
            else:  # Throughput
                location_data = tput_data[protocol][direction][tput_data[protocol][direction]['location'] == location]
                threshold_type = direction
            
            # Calculate stats for each operator
            for operator, op_conf in operator_conf.items():
                operator_label = op_conf['label']
                if operator_label in location_data['operator'].unique():
                    values = location_data[location_data['operator'] == operator_label][data_field]
                    
                    min_val = np.min(values)
                    max_val = np.max(values)
                    min_percentage = (len(values[values == 0]) / len(values)) * 100 if len(values) > 0 else 0

                    stats_dict = {
                        'median': float(np.median(values)),
                        'mean': float(np.mean(values)),
                        'min': float(min_val),
                        'max': float(max_val),
                        'percentile_5': float(np.percentile(values, 5)),
                        'percentile_25': float(np.percentile(values, 25)),
                        'percentile_75': float(np.percentile(values, 75)),
                        'percentile_95': float(np.percentile(values, 95)),
                        'min_percentage': f'{min_percentage:.2f}%',
                        'sample_count': len(values)
                    }

                    # Add threshold-based percentiles
                    if threshold_type in thresholds:
                        sorted_values = np.sort(values)  # ascending order
                        if threshold_type == 'latency':
                            # For latency (lower is better)
                            for level in ['ideal', 'smooth', 'basic']:
                                threshold = thresholds[threshold_type][level]
                                # Find first value that's less than or equal to threshold
                                idx = np.searchsorted(sorted_values, threshold, side='right')
                                pct = (idx / len(values)) * 100
                                stats_dict[f'percentile_{level}'] = f'{pct:.0f}'
                        else:
                            # For throughput (higher is better)
                            for level in ['basic', 'smooth', 'ideal']:
                                threshold = thresholds[threshold_type][level]
                                # Find first value that's greater than or equal to threshold
                                idx = np.searchsorted(sorted_values, threshold)
                                pct = (idx / len(values)) * 100
                                stats_dict[f'percentile_{level}'] = f'{pct:.0f}'

                    stats[location][operator] = stats_dict
        
        # Save stats to JSON file
        output_path = os.path.join(output_dir, f'plot_stats.{metric_name.lower().replace(" ", "_")}.json')
        with open(output_path, 'w') as f:
            json.dump(stats, f, indent=4)

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