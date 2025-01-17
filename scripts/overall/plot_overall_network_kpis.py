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
        max_xlim: float = None,
        x_step: float = None,
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
        
        col_max_values.append(max_val)
        col_min_values.append(min_val)
    
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
            x_min = col_min_values[col] - 5
            if max_xlim:
                x_max = max_xlim
            else:
                x_max = col_max_values[col]
            ax.set_xlim(x_min, x_max)

            if not x_step:
                if metric_name == 'latency':
                    x_step = 50
                elif 'downlink' in metric_name:
                    x_step = 100
                elif 'uplink' in metric_name:
                    x_step = 25

            # round to the nearest x_step
            ax.set_xticks(np.arange(
                round(x_min / x_step) * x_step, 
                round(x_max / x_step) * x_step + 1, 
                x_step
            ))
        
            
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
    print(f'Saved plot to {output_filepath}')
    plt.close()

def plot_metric_grid_flexible(
        plot_data: Dict[str, pd.DataFrame],
        row_conf: Dict[str, Dict],  # Configuration for rows
        col_conf: Dict[str, Dict],  # Configuration for columns
        location_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_filepath: str,
        title: str = None,
        max_xlim: float = None,
        x_step: float = None,
    ):
    """Flexible function to plot grid of CDF plots with custom row and column configurations.
    """
    n_rows = len(row_conf)
    n_cols = len(col_conf)
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4.5*n_cols, 3*n_rows))
    if n_rows == 1 and n_cols == 1:
        axes = np.array([[axes]])
    elif n_rows == 1:
        axes = axes.reshape(1, -1)
    elif n_cols == 1:
        axes = axes.reshape(-1, 1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2)
    
    rows_sorted = sorted(row_conf.keys(), key=lambda x: row_conf[x]['order'])
    cols_sorted = sorted(col_conf.keys(), key=lambda x: col_conf[x]['order'])
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for col_id in cols_sorted:
        max_val = 0
        min_val = float('inf')
        col_filter_mask = col_conf[col_id]['filter_mask']
        x_field = col_conf[col_id]['x_field']
        percentile_filter = col_conf[col_id].get('percentile_filter', None)
        if col_filter_mask:
            col_data = plot_data[col_filter_mask]
        else:
            col_data = plot_data
        
        for row_id in rows_sorted:
            row_filter_mask = row_conf[row_id]['filter_mask']
            if row_filter_mask:
                row_data = col_data[row_filter_mask]
            else:
                row_data = col_data
            
            operator_labels = list(map(lambda op: operator_conf[op]['label'], operator_conf.keys()))
            row_data = row_data[row_data['operator'].isin(operator_labels)]

            for op_label in operator_labels:
                op_data = row_data[row_data['operator'] == op_label][x_field]
                if percentile_filter and len(op_data) > 0:
                    max_val = max(max_val, np.percentile(op_data, percentile_filter))
                else:
                    max_val = max(max_val, np.max(op_data))
                min_val = min(min_val, np.min(op_data))
        
        col_max_values.append(max_val)
        col_min_values.append(min_val)
    
    # Create empty lines for legend
    first_ax = axes[0, 0]
    for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
        first_ax.plot([], [], 
                     label=op_conf['label'],
                     color=op_conf['color'],
                     linewidth=2)
    
    # Second pass: create plots
    for row_idx, row_id in enumerate(rows_sorted):
        for col_idx, col_id in enumerate(cols_sorted):
            ax = axes[row_idx, col_idx]
            col_filter_mask = col_conf[col_id]['filter_mask']
            x_field = col_conf[col_id]['x_field']
            if col_filter_mask:
                col_data = plot_data[col_filter_mask]
            else:
                col_data = plot_data

            operator_labels = list(map(lambda op: operator_conf[op]['label'], operator_conf.keys()))
            row_data = col_data[col_data['operator'].isin(operator_labels)]
            
            for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
                operator_label = op_conf['label']
                if operator_label in row_data['operator'].unique():
                    operator_data = row_data[row_data['operator'] == operator_label][x_field]
                    
                    if percentile_filter:
                        operator_data = operator_data[operator_data <= np.percentile(operator_data, percentile_filter)]
                    
                    data_sorted = np.sort(operator_data)
                    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                    
                    ax.plot(
                        data_sorted,
                        cdf,
                        color=op_conf['color'],
                        linewidth=2
                    )
            
            # Set x-axis limits and ticks
            x_min = col_min_values[col_idx] - 5
            if max_xlim:
                x_max = max_xlim
            else:
                x_max = col_max_values[col_idx]
            ax.set_xlim(x_min, x_max)

            if x_step:
                ax.set_xticks(np.arange(
                    round(x_min / x_step) * x_step, 
                    round(x_max / x_step) * x_step + 1, 
                    x_step
                ))
            
            ax.grid(True, alpha=0.3)
            ax.set_yticks(np.arange(0, 1.1, 0.25))
            ax.tick_params(axis='both', labelsize=10)
            
            if col_idx == 0:
                ax.text(-0.2, 0.5, row_conf[row_id]['label'],
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14,
                       fontweight='bold')
                ax.set_ylabel('CDF', fontsize=10)
            
            if row_idx == 0:
                ax.set_title(col_conf[col_id]['label'], fontsize=12, fontweight='bold')
            
            if row_idx == 0 and col_idx == 0:
                legend = ax.legend(fontsize=10, 
                                 loc='best',
                                 framealpha=0.9,
                                 edgecolor='black')
                for text in legend.get_texts():
                    text.set_fontweight('bold')
            
            if row_idx == n_rows - 1:
                ax.set_xlabel(col_conf[col_id]['x_label'], fontsize=10)
    
    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    print(f'Saved plot to {output_filepath}')
    plt.close()

def save_stats_network_kpi(
        tput_data: Dict[str, Dict[str, pd.DataFrame]],
        latency_data: pd.DataFrame,
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_dir: str,
    ):
    """Save statistics for all network KPIs (throughput and latency).
    
    Stats include: median, mean, min, max, 5th, 25th, 75th, and 95th percentiles, sample count,
    and percentage of measurements meeting different quality thresholds:
    
    Downlink thresholds:
    - Basic: 3 Mbps
    - Smooth: 10 Mbps
    - Ideal: 25 Mbps
    
    Uplink thresholds:
    - Basic: 1 Mbps
    - Smooth: 5 Mbps
    - Ideal: 10 Mbps
    
    Latency thresholds:
    - Basic: 150 ms
    - Smooth: 100 ms
    - Ideal: 50 ms
    """
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
    
    # Plot Downlink Performance
    # downlink_metrics = [
    #     ('tcp_downlink', 'TCP Downlink', 'Throughput (Mbps)', 'throughput_mbps'),
    #     ('udp_downlink', 'UDP Downlink', 'Throughput (Mbps)', 'throughput_mbps'),
    # ]
    # plot_metric_grid(
    #     data={
    #         'tcp_downlink': tput_data['tcp']['downlink'],
    #         'udp_downlink': tput_data['udp']['downlink']
    #     },
    #     loc_conf=cellular_location_conf,
    #     operator_conf=cellular_operator_conf,
    #     metrics=downlink_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators.downlink_tput.png'),
    # )
    # downlink_metrics = [
    #     ('tcp_downlink', 'TCP Downlink', 'Throughput (Mbps)', 'throughput_mbps'),
    #     # ('udp_downlink', 'UDP Downlink', 'Throughput (Mbps)', 'throughput_mbps'),
    # ]
    # plot_metric_grid(
    #     data={
    #         'tcp_downlink': tput_data['tcp']['downlink'],
    #         # 'udp_downlink': tput_data['udp']['downlink']
    #     },
    #     loc_conf=location_conf,
    #     operator_conf=operator_conf,
    #     metrics=downlink_metrics,
    #     max_xlim=300,
    #     x_step=50,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators.downlink_tput.png'),
    # )

    # plot_metric_grid_flexible(
    #     plot_data=tput_data['tcp']['downlink'],
    #     row_conf={
    #         'tcp_downlink': {
    #             'label': '',
    #             'order': 1,
    #             'filter_mask': None,
    #         },
    #     },
    #     col_conf={
    #         'alaska': {
    #             'label': 'Alaska',
    #             'order': 1,
    #             'filter_mask': lambda x: x['location'] == 'alaska',
    #             'x_field': 'throughput_mbps',
    #             'x_label': 'Throughput (Mbps)',
    #         },
    #         'hawaii': {
    #             'label': 'Hawaii',
    #             'order': 2,
    #             'filter_mask': lambda x: x['location'] == 'hawaii',
    #             'x_field': 'throughput_mbps',
    #             'x_label': 'Throughput (Mbps)',
    #         }
    #     },
    #     location_conf=location_conf,
    #     operator_conf=operator_conf,
    #     max_xlim=300,
    #     x_step=50,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators.tcp_dl.png')
    # )


    # plot_metric_grid_flexible(
    #     plot_data=tput_data['tcp']['uplink'],
    #     row_conf={
    #         'tcp_uplink': {
    #             'label': '',
    #             'order': 1,
    #             'filter_mask': None,
    #         },
    #     },
    #     col_conf={
    #         'alaska': {
    #             'label': 'Alaska',
    #             'order': 1,
    #             'filter_mask': lambda x: x['location'] == 'alaska',
    #             'x_field': 'throughput_mbps',
    #             'x_label': 'Throughput (Mbps)',
    #         },
    #         'hawaii': {
    #             'label': 'Hawaii',
    #             'order': 2,
    #             'filter_mask': lambda x: x['location'] == 'hawaii',
    #             'x_field': 'throughput_mbps',
    #             'x_label': 'Throughput (Mbps)',
    #         }
    #     },
    #     location_conf=location_conf,
    #     operator_conf=operator_conf,
    #     max_xlim=50,
    #     x_step=10,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators.tcp_ul.png')
    # )

    # downlink_metrics = [
    #     ('tcp_downlink', 'TCP Downlink', 'Throughput (Mbps)', 'throughput_mbps'),
    #     # ('udp_downlink', 'UDP Downlink', 'Throughput (Mbps)', 'throughput_mbps'),
    # ]
    # plot_metric_grid(
    #     data={
    #         'tcp_downlink': tput_data['tcp']['downlink'],
    #         # 'udp_downlink': tput_data['udp']['downlink']
    #     },
    #     loc_conf=location_conf,
    #     operator_conf=operator_conf,
    #     metrics=downlink_metrics,
    #     max_xlim=300,
    #     x_step=50,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators.downlink_tput.png'),
    # )

    
    # Plot TCP throughput
    # downlink_metrics = [
    #     ('tcp_downlink', 'TCP DL', 'Throughput (Mbps)', 'throughput_mbps'),
    #     ('tcp_uplink', 'TCP UL', 'Throughput (Mbps)', 'throughput_mbps'),
    # ]
    # # Cellular Only
    # plot_metric_grid(
    #     data={
    #         'tcp_downlink': tput_data['tcp']['downlink'],
    #         'tcp_uplink': tput_data['tcp']['uplink']
    #     },
    #     loc_conf=cellular_location_conf,
    #     operator_conf=cellular_operator_conf,
    #     metrics=downlink_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators_tcp_tput.png'),
    # )
    # plot_metric_grid(
    #     data={
    #         'tcp_downlink': tput_data['tcp']['downlink'],
    #         'tcp_uplink': tput_data['tcp']['uplink']
    #     },
    #     loc_conf=location_conf,
    #     operator_conf=operator_conf,
    #     metrics=downlink_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators_tcp_tput.png'),
    # )

    # TCP UL and UDP UL
    # uplink_metrics = [
    #     ('tcp_uplink', 'TCP UL', 'Throughput (Mbps)', 'throughput_mbps'),
    #     ('udp_uplink', 'UDP UL', 'Throughput (Mbps)', 'throughput_mbps'),
    # ]
    # # Cellular Only
    # plot_metric_grid(
    #     data={
    #         'tcp_uplink': tput_data['tcp']['uplink'],
    #         'udp_uplink': tput_data['udp']['uplink']
    #     },
    #     loc_conf=cellular_location_conf,
    #     operator_conf=cellular_operator_conf,
    #     metrics=uplink_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators.uplink_tput.png'),
    # )
    # uplink_metrics = [
    #     ('tcp_uplink', 'TCP Uplink', 'Throughput (Mbps)', 'throughput_mbps'),
    #     # ('udp_uplink', 'UDP Uplink', 'Throughput (Mbps)', 'throughput_mbps'),
    # ]
    # plot_metric_grid(
    #     data={
    #         'tcp_uplink': tput_data['tcp']['uplink'],
    #         # 'udp_uplink': tput_data['udp']['uplink']
    #     },
    #     loc_conf=location_conf,
    #     operator_conf=operator_conf,
    #     metrics=uplink_metrics,
    #     max_xlim=50,
    #     x_step=10,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_all_operators.uplink_tput.png'),
    # )
    
    # Plot UDP throughput
    # udp_metrics = [
    #     ('udp_downlink', 'UDP DL', 'Throughput (Mbps)', 'throughput_mbps'),
    #     ('udp_uplink', 'UDP UL', 'Throughput (Mbps)', 'throughput_mbps'),
    # ]
    # # Cellular Only
    # plot_metric_grid(
    #     data={
    #         'udp_downlink': tput_data['udp']['downlink'],
    #         'udp_uplink': tput_data['udp']['uplink']
    #     },
    #     loc_conf=cellular_location_conf,
    #     operator_conf=cellular_operator_conf,
    #     metrics=udp_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators_udp_tput.png'),
    # )
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
    
    latency_data = aggregate_latency_data_by_location(locations=['alaska', 'hawaii'])

    # # Plot latency
    # latency_metrics = [
    #     ('latency', 'Latency (95th percentile)', 'RTT (ms)', 'rtt_ms'),
    # ]
    # # Cellular Only
    # plot_metric_grid(
    #     data={'latency': latency_data},
    #     loc_conf=cellular_location_conf,
    #     operator_conf=cellular_operator_conf,
    #     metrics=latency_metrics,
    #     output_filepath=os.path.join(output_dir, 'ak_hi_cellular_operators_latency.png'),
    #     percentile_filter={'latency': 95}
    # )
    
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
        x_step=25,
        output_filepath=os.path.join(output_dir, 'ak_hi_all_operators.latency.png')
    )

    save_stats_network_kpi(tput_data, latency_data, location_conf, operator_conf, output_dir)


if __name__ == '__main__':
    main()