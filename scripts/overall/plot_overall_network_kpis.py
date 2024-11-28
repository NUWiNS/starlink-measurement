import os
import sys
from typing import Dict, List

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.constants import operator_color_map

location_conf = {
    'alaska': {
        'label': 'AK',
        'root_dir': ALASKA_ROOT_DIR,
        'operators': ['starlink', 'verizon', 'att'],
        'order': 1
    },
    'hawaii': {
        'label': 'HI',
        'root_dir': HAWAII_ROOT_DIR,
        'operators': ['starlink', 'verizon', 'att', 'tmobile'],
        'order': 2
    }
}

operator_conf = {
    'att': {
        'label': 'AT&T',
        'order': 1,
        'color': operator_color_map['att'],
        'linestyle': '-'
    },
    'verizon': {
        'label': 'Verizon',
        'order': 2,
        'color': operator_color_map['verizon'],
        'linestyle': '--'
    },
    'tmobile': {
        'label': 'T-Mobile',
        'order': 3,
        'color': operator_color_map['tmobile'],
        'linestyle': ':'
    },
    'starlink': {
        'label': 'Starlink',
        'order': 4,
        'color': operator_color_map['starlink'],
        'linestyle': '-.',
    },
}

def read_tput_data(root_dir: str, operator: str, protocol: str, direction: str):
    tput_data_path = os.path.join(root_dir, 'throughput', f'{operator}_{protocol}_{direction}.csv')
    if not os.path.exists(tput_data_path):
        raise FileNotFoundError(f'Throughput data file not found: {tput_data_path}')
    return pd.read_csv(tput_data_path)

def aggregate_operator_tput_data(root_dir: str, operators: List[str], protocol: str, direction: str):
    df = pd.DataFrame()
    for operator in operators:
        tput_data = read_tput_data(root_dir, operator, protocol, direction)
        tput_data['operator'] = operator_conf[operator]['label']
        df = pd.concat([df, tput_data], ignore_index=True)
    return df

def read_latency_data(root_dir: str, operator: str):
    latency_data_path = os.path.join(root_dir, 'ping', f'{operator}_ping.csv')
    if not os.path.exists(latency_data_path):
        raise FileNotFoundError(f'Latency data file not found: {latency_data_path}')
    return pd.read_csv(latency_data_path)

def aggregate_operator_latency_data(root_dir: str, operators: List[str]):
    df = pd.DataFrame()
    for operator in operators:
        latency_data = read_latency_data(root_dir, operator)
        latency_data['operator'] = operator_conf[operator]['label']
        df = pd.concat([df, latency_data], ignore_index=True)
    return df

def aggregate_latency_data_by_location(locations: List[str]):
    combined_data = pd.DataFrame()
    for location in locations:
        conf = location_conf[location]
        latency_data = aggregate_operator_latency_data(conf['root_dir'], conf['operators'])
        latency_data['location'] = location
        combined_data = pd.concat([combined_data, latency_data], ignore_index=True)
    return combined_data

def plot_network_kpis_comparison(
        df: pd.DataFrame, 
        title: str,
        data_field: str,
        data_label: str,
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_filepath: str,
        show_location_label: bool = False,
        show_legend: bool = False,
        show_ylabel: bool = False,
        max_percentile: float = 100,
    ):
    # Calculate number of locations for subplot layout
    n_locations = len(loc_conf)
    fig, axes = plt.subplots(n_locations, 1, figsize=(4, 3*n_locations))
    if n_locations == 1:
        axes = [axes]

    # Sort locations by order
    locations_sorted = sorted(df['location'].unique(), key=lambda x: loc_conf[x]['order'])

    # Track max x value for consistent axis limits
    max_x_value = 0

    # First pass to determine consistent x-axis limit
    if max_percentile < 100:
        for location in locations_sorted:
            sub_df = df[df['location'] == location]
            for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
                operator_label = op_conf['label']
                if operator_label in sub_df['operator'].unique():
                    operator_data = sub_df[sub_df['operator'] == operator_label][data_field]
                    percentile_value = np.percentile(operator_data, max_percentile)
                    max_x_value = max(max_x_value, percentile_value)

    # Add padding for better visualization
    plot_max_x = max_x_value * 1.05  # 5% padding

    # Create subplot for each location
    for idx, location in enumerate(locations_sorted):
        location_label = loc_conf[location]['label']
        sub_df = df[df['location'] == location]
        
        # Plot CDF for each operator
        for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
            operator_label = op_conf['label']
            if operator_label in sub_df['operator'].unique():
                operator_data = sub_df[sub_df['operator'] == operator_label][data_field]
                
                if max_percentile < 100:
                    # Filter data based on max_percentile
                    percentile_value = np.percentile(operator_data, max_percentile)
                    operator_data = operator_data[operator_data <= percentile_value]
                
                # Calculate CDF
                data_sorted = np.sort(operator_data)
                cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                
                # Plot CDF line
                axes[idx].plot(
                    data_sorted,
                    cdf,
                    label=operator_label,
                    color=op_conf['color'],
                    linewidth=2
                )
        
        # Set x-axis limits
        if max_percentile < 100:
            axes[idx].set_xlim(0, plot_max_x)
        
        if show_location_label:
            axes[idx].text(-0.15, 0.5, location_label, 
                          transform=axes[idx].transAxes,
                          rotation=0,
                          verticalalignment='center',
                          horizontalalignment='right',
                          fontsize=14,
                          fontweight='bold')
            
        axes[idx].grid(True, alpha=0.3)
        axes[idx].set_xlabel(data_label, fontsize=12)
        if show_ylabel:
            axes[idx].set_ylabel('CDF', fontsize=12)
        
        axes[idx].set_yticks(np.arange(0, 1.1, 0.25))
        axes[idx].tick_params(axis='both', labelsize=10)
        
        if show_legend and idx == 0:
            legend = axes[idx].legend(fontsize=12)
            for text in legend.get_texts():
                text.set_fontweight('bold')
    
    # Add overall title
    fig.suptitle(title, y=1.0, fontsize=16, fontweight='bold')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save plot
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    plt.close()


def aggregate_tput_data_by_location(locations: List[str], protocol: str, direction: str):
    combined_data = pd.DataFrame()
    for location in locations:
        conf = location_conf[location]
        tcp_dl_tput_data = aggregate_operator_tput_data(
            root_dir=conf['root_dir'], 
            operators=conf['operators'], 
            protocol=protocol, 
            direction=direction
        )
        tcp_dl_tput_data['location'] = location
        combined_data = pd.concat([combined_data, tcp_dl_tput_data], ignore_index=True)
    return combined_data



def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for protocol in ['tcp', 'udp']:
        for direction in ['downlink', 'uplink']:
            combined_data = aggregate_tput_data_by_location(locations=['alaska', 'hawaii'], protocol=protocol, direction=direction)

            output_filepath = os.path.join(output_dir, f'all_locations_cdf.{protocol}_{direction}.png')
            if protocol == 'tcp' and direction == 'downlink':
                show_ylabel = True
                show_legend = True
            else:
                show_ylabel = False
                show_legend = False

            plot_network_kpis_comparison(
                df=combined_data, 
                title=f'{protocol.upper()} {direction.capitalize()}',
                data_field='throughput_mbps',
                data_label='Throughput (Mbps)',
                loc_conf=location_conf,
                operator_conf=operator_conf,
                show_location_label=True,
                show_legend=show_legend,
                show_ylabel=show_ylabel,
                output_filepath=output_filepath,
                max_percentile=95
            )
    
    combined_latency_data = aggregate_latency_data_by_location(locations=['alaska', 'hawaii'])
    output_filepath = os.path.join(output_dir, f'all_locations_cdf.latency.png')
    plot_network_kpis_comparison(
        df=combined_latency_data, 
        title='Latency (95th Percentile)',
        data_field='rtt_ms',
        data_label='Round-Trip Time (ms)',
        loc_conf=location_conf,
        operator_conf=operator_conf,
        output_filepath=output_filepath,
        show_location_label=False,
        show_legend=False,
        show_ylabel=False,
        max_percentile=95,
    )


if __name__ == '__main__':
    main()