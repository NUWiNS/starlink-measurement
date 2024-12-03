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

cellular_location_conf = {
    'alaska': {
        'label': 'AK',
        'root_dir': ALASKA_ROOT_DIR,
        'operators': ['verizon', 'att'],
        'order': 1
    },
    'hawaii': {
        'label': 'HI',
        'root_dir': HAWAII_ROOT_DIR,
        'operators': ['verizon', 'att', 'tmobile'],
        'order': 2
    }
}

cellular_operator_conf = {
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

def plot_metric_grid(
        data: Dict[str, pd.DataFrame],
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        metrics: List[tuple],
        output_filepath: str,
        title: str = None,
        percentile_filter: Dict[str, float] = None,  # e.g., {'latency': 95}
    ):
    """Generic function to plot grid of CDF plots.
    
    Args:
        data: Dictionary mapping metric name to its DataFrame
        loc_conf: Location configuration
        operator_conf: Operator configuration
        metrics: List of tuples (metric_name, column_title, xlabel, data_field)
        output_filepath: Where to save the plot
        title: Optional overall title for the plot
        percentile_filter: Dictionary mapping metric name to percentile filter
    """
    n_locations = len(loc_conf)
    n_metrics = len(metrics)
    
    fig, axes = plt.subplots(n_locations, n_metrics, figsize=(4.8*n_metrics, 3*n_locations))
    if n_locations == 1:
        axes = axes.reshape(1, -1)
    if n_metrics == 1:
        axes = axes.reshape(-1, 1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2)
    
    locations_sorted = sorted(loc_conf.keys(), key=lambda x: loc_conf[x]['order'])
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for col, (metric_name, _, _, data_field) in enumerate(metrics):
        max_val = 0
        min_val = float('inf')
        percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
        
        for location in locations_sorted:
            plot_df = data[metric_name][data[metric_name]['location'] == location]

            needed_operators = list(map(lambda op: operator_conf[op]['label'], loc_conf[location]['operators']))
            plot_df = plot_df[plot_df['operator'].isin(needed_operators)]

            for op_label in plot_df['operator'].unique():
                op_data = plot_df[plot_df['operator'] == op_label][data_field]
                if percentile < 100:
                    max_val = max(max_val, np.percentile(op_data, percentile))
                else:
                    max_val = max(max_val, np.max(op_data))
                min_val = min(min_val, np.min(op_data))
        
        padding = (max_val - min_val) * 0.05
        col_max_values.append(max_val + padding)
        col_min_values.append(min_val - padding)
    
    # Create empty lines for legend
    first_ax = axes[0, 0]
    for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
        first_ax.plot([], [], 
                     label=op_conf['label'],
                     color=op_conf['color'],
                     linewidth=2)
    
    # Second pass: create plots
    for row, location in enumerate(locations_sorted):
        for col, (metric_name, column_title, xlabel, data_field) in enumerate(metrics):
            ax = axes[row, col]
            plot_df = data[metric_name][data[metric_name]['location'] == location]

            needed_operators = list(map(lambda op: operator_conf[op]['label'], loc_conf[location]['operators']))
            plot_df = plot_df[plot_df['operator'].isin(needed_operators)]

            percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
            
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
            
            # Set x-axis limits and ticks
            ax.set_xlim(col_min_values[col], col_max_values[col])
            if metric_name == 'latency':
                step = 50
            elif 'downlink' in metric_name:
                step = 100
            elif 'uplink' in metric_name:
                step = 25
            if step:
                max_tick = int(np.ceil(col_max_values[col] / step)) * step
                ax.set_xticks(np.arange(0, max_tick + step, step))
        
            
            ax.grid(True, alpha=0.3)
            ax.set_yticks(np.arange(0, 1.1, 0.25))
            ax.tick_params(axis='both', labelsize=10)
            
            if col == 0:
                ax.text(-0.2, 0.5, loc_conf[location]['label'],
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14,
                       fontweight='bold')
                ax.set_ylabel('CDF', fontsize=10)
            
            if row == 0:
                ax.set_title(column_title, fontsize=12, fontweight='bold')
            
            if row == 0 and col == 0:
                legend = ax.legend(fontsize=10, 
                                 loc='best',
                                 framealpha=0.9,
                                 edgecolor='black')
                for text in legend.get_texts():
                    text.set_fontweight('bold')
            
            if row == n_locations - 1:
                ax.set_xlabel(xlabel, fontsize=10)
    
    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
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
                    
                    min_val = np.min(values)
                    max_val = np.max(values)
                    min_percentage = (len(values[values == 0]) / len(values)) * 100 if len(values) > 0 else 0

                    stats[location][operator] = {
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
        
        # Save stats to JSON file
        output_path = os.path.join(output_dir, f'plot_stats.{metric_name.lower().replace(" ", "_")}.json')
        with open(output_path, 'w') as f:
            json.dump(stats, f, indent=4)

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Collect all data
    tput_data = {}
    for protocol in ['tcp', 'udp']:
        tput_data[protocol] = {}
        for direction in ['downlink', 'uplink']:
            tput_data[protocol][direction] = aggregate_tput_data_by_location(
                locations=['alaska', 'hawaii'],
                protocol=protocol,
                direction=direction
            )
    
    latency_data = aggregate_latency_data_by_location(locations=['alaska', 'hawaii'])
    
    # Plot TCP throughput
    tcp_metrics = [
        ('tcp_downlink', 'TCP DL', 'Throughput (Mbps)', 'throughput_mbps'),
        ('tcp_uplink', 'TCP UL', 'Throughput (Mbps)', 'throughput_mbps'),
    ]
    # Cellular Only
    plot_metric_grid(
        data={
            'tcp_downlink': tput_data['tcp']['downlink'],
            'tcp_uplink': tput_data['tcp']['uplink']
        },
        loc_conf=cellular_location_conf,
        operator_conf=cellular_operator_conf,
        metrics=tcp_metrics,
        output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators_tcp_tput.png'),
    )
    # plot_metric_grid(
    #     data={
    #         'tcp_downlink': tput_data['tcp']['downlink'],
    #         'tcp_uplink': tput_data['tcp']['uplink']
    #     },
    #     loc_conf=location_conf,
    #     operator_conf=operator_conf,
    #     metrics=tcp_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators_tcp_tput.png'),
    # )
    
    # Plot UDP throughput
    udp_metrics = [
        ('udp_downlink', 'UDP DL', 'Throughput (Mbps)', 'throughput_mbps'),
        ('udp_uplink', 'UDP UL', 'Throughput (Mbps)', 'throughput_mbps'),
    ]
    # Cellular Only
    plot_metric_grid(
        data={
            'udp_downlink': tput_data['udp']['downlink'],
            'udp_uplink': tput_data['udp']['uplink']
        },
        loc_conf=cellular_location_conf,
        operator_conf=cellular_operator_conf,
        metrics=udp_metrics,
        output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators_udp_tput.png'),
    )
    # plot_metric_grid(
    #     data={
    #         'udp_downlink': tput_data['udp']['downlink'],
    #         'udp_uplink': tput_data['udp']['uplink']
    #     },
    #     loc_conf=location_conf,
    #     operator_conf=operator_conf,
    #     metrics=udp_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators_udp_tput.png'),
    # )
    
    # Plot latency
    latency_metrics = [
        ('latency', 'Latency (95th percentile)', 'RTT (ms)', 'rtt_ms'),
    ]
    # Cellular Only
    plot_metric_grid(
        data={'latency': latency_data},
        loc_conf=cellular_location_conf,
        operator_conf=cellular_operator_conf,
        metrics=latency_metrics,
        output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators_latency.png'),
        percentile_filter={'latency': 95}
    )
    # plot_metric_grid(
    #     data={'latency': latency_data},
    #     loc_conf=location_conf,
    #     operator_conf=operator_conf,
    #     metrics=latency_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators_latency.png'),
    #     percentile_filter={'latency': 95}
    # )

    save_stats_network_kpi(tput_data, latency_data, location_conf, operator_conf, output_dir)


if __name__ == '__main__':
    main()