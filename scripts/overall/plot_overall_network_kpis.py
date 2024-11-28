import os
import sys
from typing import Dict, List

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json

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

def plot_all_network_kpis(
        tput_data: Dict[str, Dict[str, pd.DataFrame]],
        latency_data: pd.DataFrame,
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_filepath: str,
    ):
    n_locations = len(loc_conf)
    n_metrics = 5
    
    fig, axes = plt.subplots(n_locations, n_metrics, figsize=(24, 3*n_locations))
    if n_locations == 1:
        axes = axes.reshape(1, -1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2)
    
    metrics = [
        ('tcp', 'downlink', 'TCP DL', 'Throughput (Mbps)', 'throughput_mbps'),
        ('tcp', 'uplink', 'TCP UL', 'Throughput (Mbps)', 'throughput_mbps'),
        ('udp', 'downlink', 'UDP DL', 'Throughput (Mbps)', 'throughput_mbps'),
        ('udp', 'uplink', 'UDP UL', 'Throughput (Mbps)', 'throughput_mbps'),
        (None, None, 'Latency (95th percentile)', 'RTT (ms)', 'rtt_ms'),
    ]
    
    locations_sorted = sorted(loc_conf.keys(), key=lambda x: loc_conf[x]['order'])
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for col, (protocol, direction, _, _, data_field) in enumerate(metrics):
        max_val = 0
        min_val = float('inf')
        for location in locations_sorted:
            if protocol is None:  # Latency
                plot_df = latency_data[latency_data['location'] == location]
                for op_label in plot_df['operator'].unique():
                    op_data = plot_df[plot_df['operator'] == op_label][data_field]
                    max_val = max(max_val, np.percentile(op_data, 95))  # 95th for latency
                    min_val = min(min_val, np.min(op_data))
            else:  # Throughput
                plot_df = tput_data[protocol][direction][tput_data[protocol][direction]['location'] == location]
                for op_label in plot_df['operator'].unique():
                    op_data = plot_df[plot_df['operator'] == op_label][data_field]
                    max_val = max(max_val, np.max(op_data))  # All data for throughput
                    min_val = min(min_val, np.min(op_data))
        
        # Add padding (5% of range)
        padding = (max_val - min_val) * 0.05
        col_max_values.append(max_val + padding)
        col_min_values.append(min_val - padding)
    # Create empty lines for legend with all operators
    first_ax = axes[0, 0]
    for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
        first_ax.plot([], [], 
                     label=op_conf['label'],
                     color=op_conf['color'],
                     linewidth=2)
    
    # Second pass: create plots
    for row, location in enumerate(locations_sorted):
        for col, (protocol, direction, title, xlabel, data_field) in enumerate(metrics):
            ax = axes[row, col]
            
            if protocol is None:  # Latency
                plot_df = latency_data[latency_data['location'] == location]
                percentile = 95
                
                # Set x-ticks for latency
                step = 50
                max_tick = int(np.ceil(col_max_values[col] / step)) * step
                ax.set_xticks(np.arange(0, max_tick + step, step))
            else:  # Throughput
                mask = tput_data[protocol][direction]['location'] == location
                plot_df = tput_data[protocol][direction][mask]
                percentile = 100
            
            for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
                operator_label = op_conf['label']
                if operator_label in plot_df['operator'].unique():
                    operator_data = plot_df[plot_df['operator'] == operator_label][data_field]
                    
                    if percentile < 100:
                        operator_data = operator_data[operator_data <= np.percentile(operator_data, percentile)]
                    
                    data_sorted = np.sort(operator_data)
                    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                    
                    ax.plot(
                        data_sorted,
                        cdf,
                        color=op_conf['color'],
                        linewidth=2
                    )
            
            # Set consistent x-axis limits for each column
            ax.set_xlim(col_min_values[col], col_max_values[col])
            
            ax.grid(True, alpha=0.3)
            ax.set_yticks(np.arange(0, 1.1, 0.25))
            ax.tick_params(axis='both', labelsize=10)
            
            # Only set location label and y-label for the first column
            if col == 0:
                ax.text(-0.25, 0.5, loc_conf[location]['label'],
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14,
                       fontweight='bold')
                ax.set_ylabel('CDF', fontsize=10)
            
            # Only set title for the first row
            if row == 0:
                ax.set_title(title, fontsize=12, fontweight='bold')
            
            # Only set legend in the first plot
            if row == 0 and col == 0:
                legend = ax.legend(fontsize=10, 
                                 loc='best',
                                 framealpha=0.9,
                                 edgecolor='black')
                for text in legend.get_texts():
                    text.set_fontweight('bold')
            
            # Only set x-label for the last row
            if row == n_locations - 1:
                ax.set_xlabel(xlabel, fontsize=10)

    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    plt.close()

def save_stats_network_kpi(
        tput_data: Dict[str, Dict[str, pd.DataFrame]],
        latency_data: pd.DataFrame,
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_dir: str,
    ):
    """Save statistics for all network KPIs (throughput and latency).
    
    Stats include: median, mean, min, max, 5th, 25th, 75th, and 95th percentiles, and sample count.
    """
    metrics = [
        ('tcp', 'downlink', 'TCP Downlink', 'throughput_mbps'),
        ('tcp', 'uplink', 'TCP Uplink', 'throughput_mbps'),
        ('udp', 'downlink', 'UDP Downlink', 'throughput_mbps'),
        ('udp', 'uplink', 'UDP Uplink', 'throughput_mbps'),
        (None, None, 'Latency', 'rtt_ms'),
    ]
    
    for protocol, direction, metric_name, data_field in metrics:
        stats = {}
        for location in loc_conf.keys():
            stats[location] = {}
            
            # Get data for this metric and location
            if protocol is None:  # Latency
                location_data = latency_data[latency_data['location'] == location]
            else:  # Throughput
                location_data = tput_data[protocol][direction][tput_data[protocol][direction]['location'] == location]
            
            # Calculate stats for each operator
            for operator, op_conf in operator_conf.items():
                operator_label = op_conf['label']
                if operator_label in location_data['operator'].unique():
                    values = location_data[location_data['operator'] == operator_label][data_field]
                    
                    stats[location][operator] = {
                        'median': float(np.median(values)),
                        'mean': float(np.mean(values)),
                        'min': float(np.min(values)),
                        'max': float(np.max(values)),
                        'percentile_5': float(np.percentile(values, 5)),
                        'percentile_25': float(np.percentile(values, 25)),
                        'percentile_75': float(np.percentile(values, 75)),
                        'percentile_95': float(np.percentile(values, 95)),
                        'sample_count': len(values)
                    }
        
        # Save stats to JSON file
        output_path = os.path.join(output_dir, f'plot_stats.{metric_name.lower().replace(" ", "_")}.json')
        with open(output_path, 'w') as f:
            json.dump(stats, f, indent=4)

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Collect all throughput data
    tput_data = {}
    for protocol in ['tcp', 'udp']:
        tput_data[protocol] = {}
        for direction in ['downlink', 'uplink']:
            tput_data[protocol][direction] = aggregate_tput_data_by_location(
                locations=['alaska', 'hawaii'],
                protocol=protocol,
                direction=direction
            )
    
    # Collect latency data
    latency_data = aggregate_latency_data_by_location(locations=['alaska', 'hawaii'])
    
    # Create combined plot
    plot_all_network_kpis(
        tput_data=tput_data,
        latency_data=latency_data,
        loc_conf=location_conf,
        operator_conf=operator_conf,
        output_filepath=os.path.join(output_dir, 'all_network_kpis.png'),
    )

    save_stats_network_kpi(tput_data, latency_data, location_conf, operator_conf, output_dir)


if __name__ == '__main__':
    main()