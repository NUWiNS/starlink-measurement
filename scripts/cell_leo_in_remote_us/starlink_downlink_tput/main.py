import json
import os
from typing import Any, Dict, List
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.cell_leo_in_remote_us.common import operator_conf, location_conf
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger
from scripts.utilities.DatasetHelper import DatasetHelper

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('dl_with_areas', filename=os.path.join(current_dir, 'outputs', 'dl_with_areas.log'))

# Define area configuration
area_conf = {
    'urban': {'line_style': '-'},
    'rural': {'line_style': '--'}
}

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

def get_latency_data_for_alaska_and_hawaii():
    al_dataset_helper = DatasetHelper(os.path.join(location_conf['alaska']['root_dir'], 'ping'))
    alaska_ping_data = al_dataset_helper.get_ping_data(operator='starlink')

    hawaii_dataset_helper = DatasetHelper(os.path.join(location_conf['hawaii']['root_dir'], 'ping'))
    hawaii_ping_data = hawaii_dataset_helper.get_ping_data(operator='starlink')

    alaska_ping_data['location'] = 'alaska'
    hawaii_ping_data['location'] = 'hawaii'
    combined_data = pd.concat([alaska_ping_data, hawaii_ping_data])
    combined_data['operator'] = 'starlink'
    return combined_data

def plot_starlink_tput_comparison_by_location_and_area(
    plot_data: Dict[str, pd.DataFrame],
    location_conf: Dict[str, Any],
    area_conf: Dict[str, Any],
    output_filepath: str,
):
    
    # Define metrics for each subplot
    metrics = ['tcp_downlink', 'tcp_uplink', 'icmp_ping']
    # Create figure with smaller spacing between subplots
    fig, axes = plt.subplots(1, len(metrics), figsize=(len(metrics) * 3.2, 3))

    metrics_conf = {
        'tcp_downlink': {
            'xlabel': 'DL Throughput (Mbps)',
            'max_xlim': 250,
            'interval_x': 50,
            'data_field': CommonField.TPUT_MBPS,
        },
        'tcp_uplink': {
            'xlabel': 'UL Throughput (Mbps)',
            'max_xlim': 20,
            'interval_x': 5,
            'data_field': CommonField.TPUT_MBPS,
        },
        'icmp_ping': {
            'xlabel': 'RTT (ms)',
            'max_xlim': 200,
            'interval_x': 50,
            'data_field': CommonField.RTT_MS,
        }
    }
    
    legend_lines = []
    legend_labels = []
    
    # Process each subplot
    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        df = plot_data[metric]
        metric_conf = metrics_conf[metric]
        
        # Plot CDF for each location-area combination
        for location in ['alaska', 'hawaii']:
            location_color = location_conf[location]['color']
            location_df = df[df['location'] == location]
            
            for area_type in ['urban', 'rural']:
                if area_type == 'urban':
                    mask = (location_df[CommonField.AREA_TYPE] == 'urban') | (location_df[CommonField.AREA_TYPE] == 'suburban')
                else:
                    # rural
                    mask = (location_df[CommonField.AREA_TYPE] == 'rural')
                location_area_df = location_df[mask]
                
                data = location_area_df[metric_conf['data_field']]

                if len(data) == 0:
                    logger.warning(f'No data for {location} {area_type}')
                    continue
                
                # Sort data for CDF
                sorted_data = np.sort(data)
                # Calculate cumulative probabilities
                cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
                
                # Plot the CDF line
                line = ax.plot(
                    sorted_data,
                    cdf,
                    color=location_color,
                    linestyle=area_conf[area_type]['line_style'],
                    linewidth=2
                )[0]
                
                # Only add to legend for the first subplot
                if idx == 0:
                    legend_lines.append(line)
                    legend_labels.append(f"{location.capitalize()} {area_type.capitalize()}")
        
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.set_xlabel(metrics_conf[metric]['xlabel'])
        ax.set_ylabel('CDF' if idx == 0 else '')
        ax.set_ylim(0, 1)
        ax.set_xlim(0, metric_conf['max_xlim'])
        ax.xaxis.set_ticks(np.arange(0, metric_conf['max_xlim'] + 1, metric_conf['interval_x']))
        ax.yaxis.set_ticks(np.arange(0, 1.1, 0.2))

        # Hide y-ticks for second subplot
        if idx != 0:
            ax.set_yticklabels([])
        
    # Add legend to the first subplot at lower right
    axes[0].legend(
        legend_lines,
        legend_labels,
        loc='lower right',
        bbox_to_anchor=(0.98, 0.02),  # Fine-tune position
        borderaxespad=0.
    )
    
    plt.tight_layout()
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    logger.info(f'Saved plot to {output_filepath}')
    plt.close()

def main():
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get data for both protocols
    plot_data = {}
    for protocol in ['tcp']:
        for direction in ['downlink', 'uplink']:
            key = f'{protocol}_{direction}'
            starlink_df = get_tput_data_for_alaska_and_hawaii(protocol, direction)
            
            # Keep necessary columns and add area information
            columns_to_keep = [CommonField.LOCATION, CommonField.AREA_TYPE, CommonField.TPUT_MBPS]
            starlink_df = starlink_df[columns_to_keep]
            
            # Store in the plot_data dictionary
            plot_data[key] = starlink_df

    starlink_ping_data = get_latency_data_for_alaska_and_hawaii()
    plot_data['icmp_ping'] = starlink_ping_data

    # Create the plot
    plot_starlink_tput_comparison_by_location_and_area(
        plot_data=plot_data,
        location_conf=location_conf,
        area_conf=area_conf,
        output_filepath=os.path.join(output_dir, 'starlink_performance_with_areas.png')
    )

if __name__ == '__main__':
    main()