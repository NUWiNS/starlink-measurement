import os
import json
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Any, List, Dict


sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from scripts.constants import CommonField
from scripts.logging_utils import create_logger


from scripts.alaska_starlink_trip.configs import ROOT_DIR as AL_DATASET_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HI_DATASET_DIR

current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, './outputs')
logger = create_logger('sl_by_area', filename=os.path.join(output_dir, 'plot_starlink_comparison_by_area.log'))

location_conf = {
    'alaska': {
        'label': 'AK',
        'root_dir': AL_DATASET_DIR,
        'operators': ['starlink'],
        'color': '#2E86C1',  # blue
        'order': 1
    },
    'hawaii': {
        'label': 'HI',
        'root_dir': HI_DATASET_DIR,
        'operators': ['starlink'],
        'color': '#E74C3C',  # red
        'order': 2
    }
}

area_conf = {
    'urban': {
        'label': 'Urban',
        'order': 1
    },
    'rural': {
        'label': 'Rural',
        'order': 2
    }
}

operator_conf = {
    'att': {
        'label': 'AT&T',
        'order': 1,
        'linestyle': 'solid'
    },
    'verizon': {
        'label': 'Verizon',
        'order': 2,
        'linestyle': 'dashed'
    },
    'tmobile': {
        'label': 'T-Mobile',
        'order': 3,
        'linestyle': 'dotted'
    },
}

tech_conf = {
    'LTE': {
        'label': 'LTE',
        'color': '#326f21',
        'order': 1
    },
    'LTE-A': {
        'label': 'LTE-A',
        'color': '#86c84d',
        'order': 2
    },
    '5G-low': {
        'label': '5G Low',
        'color': '#ffd700',
        'order': 3
    },
    '5G-mid': {
        'label': '5G Mid',
        'color': '#ff9900',
        'order': 4
    },
}

protocol_conf = {
    'tcp': {
        'label': 'TCP',
        'linestyle': 'solid',
        'order': 1
    },
    'udp': {
        'label': 'UDP',
        'linestyle': 'dashed',
        'order': 2
    }
}

def read_tput_data(root_dir: str, operator: str, protocol: str = None, direction: str = None):
    input_csv_path = os.path.join(root_dir, 'throughput', f'{operator}_{protocol}_{direction}.csv')
    df = pd.read_csv(input_csv_path)
    df[CommonField.APP_TPUT_PROTOCOL] = protocol
    df[CommonField.APP_TPUT_DIRECTION] = direction
    return df

def aggregate_tput_data(
        root_dir: str, 
        operators: List[str], 
        protocol: str = None, 
        direction: str = None, 
    ):
    data = pd.DataFrame()
    for operator in operators:
        df = read_tput_data(
            root_dir=root_dir, 
            operator=operator,
            protocol=protocol, 
            direction=direction, 
        )
        df['operator'] = operator
        data = pd.concat([data, df])
    return data

def aggregate_tput_data_by_location(
        locations: List[str], 
        protocol: str = None, 
        direction: str = None, 
    ):
    data = pd.DataFrame()
    for location in locations:
        conf = location_conf[location]
        df = aggregate_tput_data(
            root_dir=conf['root_dir'], 
            operators=conf['operators'], 
            protocol=protocol, 
            direction=direction, 
        )
        df['location'] = location
        data = pd.concat([data, df])
    return data

def read_latency_data(root_dir: str, operator: str, protocol: str = 'icmp'):
    if protocol == 'icmp':
        input_csv_path = os.path.join(root_dir, 'ping', f'{operator}_ping.csv')
    else:
        raise ValueError(f'Unsupported protocol: {protocol}')

    df = pd.read_csv(input_csv_path)
    return df

def aggregate_latency_data_by_operator(
        root_dir: str,
        operators: List[str], 
        protocol: str = 'icmp',
    ):
    data = pd.DataFrame()
    for operator in operators:
        df = read_latency_data(
            root_dir=root_dir, 
            operator=operator, 
            protocol=protocol
        )
        df[CommonField.OPERATOR] = operator
        data = pd.concat([data, df])
    return data

def aggregate_latency_data_by_location(
        locations: List[str], 
        protocol: str = 'icmp',
    ):
    data = pd.DataFrame()
    for location in locations:
        df = aggregate_latency_data_by_operator(
            root_dir=location_conf[location]['root_dir'],
            operators=location_conf[location]['operators'], 
            protocol=protocol
        )
        df[CommonField.LOCATION] = location
        data = pd.concat([data, df])
    return data

def filter_data_by_area(df: pd.DataFrame, area_type: str):
    if area_type == 'urban':
        mask = (df[CommonField.AREA_TYPE] == 'urban') | (df[CommonField.AREA_TYPE] == 'suburban')
    elif area_type == 'rural':
        mask = df[CommonField.AREA_TYPE] == 'rural'
    else:
        raise ValueError(f'Unsupported area type: {area_type}')
    return df[mask]

def plot_tput_metric_grid(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        area_conf: Dict[str, Any],
        location_conf: Dict[str, Any],
        protocol_conf: Dict[str, Any],
        output_filepath: str,
        title: str = None,
        percentile_filter: Dict[str, float] = None,
        max_xlim: float = None,
        data_sample_threshold: int = 240,
        x_step: float = None,
    ):
    """
    Plot a grid of metrics for each area type
    """
    area_types = list(sorted(area_conf.keys(), key=lambda x: area_conf[x]['order']))
    n_areas = len(area_types)
    n_metrics = len(metrics)
    
    fig, axes = plt.subplots(n_areas, n_metrics, figsize=(4.8*n_metrics, 3*n_areas))
    if n_areas == 1:
        axes = axes.reshape(1, -1)
    if n_metrics == 1:
        axes = axes.reshape(-1, 1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2, right=0.85)
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for col, (metric_name, _, _, data_field) in enumerate(metrics):
        max_val = 0
        min_val = float('inf')
        percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
        
        for area in area_types:
            df = data[metric_name]
            plot_df = filter_data_by_area(df, area)
            for _, proto_df in plot_df.groupby([CommonField.LOCATION, CommonField.APP_TPUT_PROTOCOL]):
                filtered_data = proto_df[data_field]
                
                if len(filtered_data) >= data_sample_threshold:
                    if percentile < 100:
                        max_val = max(max_val, np.percentile(filtered_data, percentile))
                    else:
                        max_val = max(max_val, np.max(filtered_data))
                    min_val = min(min_val, np.min(filtered_data))
        
        col_max_values.append(max_val)
        col_min_values.append(min_val)
    
    # Create location legend (colors)
    location_lines = []
    for _, loc_conf in sorted(location_conf.items(), key=lambda x: x[1]['order']):
        line = axes[0, 0].plot([], [],
                            color=loc_conf['color'],
                            label=loc_conf['label'],
                            linewidth=2)[0]
        location_lines.append(line)
    
    # Create protocol legend (linestyles)
    protocol_lines = []
    for _, proto_conf in sorted(protocol_conf.items(), key=lambda x: x[1]['order']):
        line = axes[1, 0].plot([], [],
                            color='gray',
                            label=proto_conf['label'],
                            linestyle=proto_conf['linestyle'],
                            linewidth=2)[0]
        protocol_lines.append(line)
    
    # Add legends
    location_legend = axes[0, 0].legend(handles=location_lines,
                                    title='Region',
                                    loc='lower right',
                                    fontsize=8,
                                    framealpha=0.9,
                                    edgecolor='black')
    protocol_legend = axes[1, 0].legend(handles=protocol_lines,
                                    title='Protocol',
                                    loc='lower right',
                                    fontsize=8,
                                    framealpha=0.9,
                                    edgecolor='black')
    
    # Add legends to their respective subplots
    axes[0, 0].add_artist(location_legend)
    axes[1, 0].add_artist(protocol_legend)
    
    # Clear any automatic legends
    for ax in axes.flat:
        ax.get_legend().remove() if ax.get_legend() else None
    
    # Second pass: create plots
    for row, area_type in enumerate(area_types):
        for col, (metric_name, column_title, xlabel, data_field) in enumerate(metrics):
            ax = axes[row, col]
            df = data[metric_name]
            plot_df = filter_data_by_area(df, area_type)
            percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
            
            # Plot each location+protocol combination
            for location in sorted(location_conf.keys(), key=lambda x: location_conf[x]['order']):
                for protocol in sorted(protocol_conf.keys(), key=lambda x: protocol_conf[x]['order']):
                    mask = (plot_df[CommonField.LOCATION] == location) & (plot_df[CommonField.APP_TPUT_PROTOCOL] == protocol)
                    filtered_data = plot_df[mask][data_field]

                    if len(filtered_data) < data_sample_threshold:
                        logger.warn(f'{area_type}-{location}-{protocol} data sample is less than required threshold, skip plotting: {len(filtered_data)} < {data_sample_threshold}')
                        continue
                    
                    if percentile < 100:
                        filtered_data = filtered_data[filtered_data <= np.percentile(filtered_data, percentile)]
                    
                    data_sorted = np.sort(filtered_data)
                    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                    
                    ax.plot(
                        data_sorted,
                        cdf,
                        color=location_conf[location]['color'],
                        linestyle=protocol_conf[protocol]['linestyle'],
                        linewidth=2
                    )
            
            # Set axis properties
            padding = (max_val - min_val) * 0.05
            if max_xlim:
                ax.set_xlim(col_min_values[col] - padding, max_xlim + padding)
            else:
                ax.set_xlim(col_min_values[col] - padding, col_max_values[col] + padding)
            
            if x_step:
                ax.set_xticks(np.arange(col_min_values[col], col_max_values[col] + x_step, x_step))
            
            ax.grid(True, alpha=0.3)
            ax.set_yticks(np.arange(0, 1.1, 0.25))
            ax.tick_params(axis='both', labelsize=10)
            
            if col == 0:
                ax.text(-0.2, 0.5, area_type.capitalize(),
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14,
                       fontweight='bold')
                ax.set_ylabel('CDF', fontsize=10)
            
            if row == 0:
                ax.set_title(column_title, fontsize=12, fontweight='bold')
            
            if row == n_areas - 1:
                ax.set_xlabel(xlabel, fontsize=10)
    
    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    plt.close()

def plot_tput_metric_grid_reversed(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        area_conf: Dict[str, Any],
        location_conf: Dict[str, Any],
        protocol_conf: Dict[str, Any],
        output_filepath: str,
        title: str = None,
        percentile_filter: Dict[str, float] = None,
        max_xlim: float = None,
        data_sample_threshold: int = 240,
        x_step: float = None,
    ):
    """
    Plot a grid of metrics with area types as columns
    """
    area_types = list(sorted(area_conf.keys(), key=lambda x: area_conf[x]['order']))
    n_areas = len(area_types)
    n_metrics = len(metrics)
    
    # Create 1xN grid where N is number of area types
    fig, axes = plt.subplots(n_metrics, n_areas, figsize=(4.8*n_areas, 3*n_metrics))
    if n_metrics == 1:
        axes = axes.reshape(1, -1)
    if n_areas == 1:
        axes = axes.reshape(-1, 1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2, right=0.85)
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for metric_name, _, _, data_field in metrics:
        max_val = 0
        min_val = float('inf')
        percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
        
        df = data[metric_name]
        for area in area_types:
            plot_df = filter_data_by_area(df, area)
            for _, proto_df in plot_df.groupby([CommonField.LOCATION, CommonField.APP_TPUT_PROTOCOL]):
                filtered_data = proto_df[data_field]
                
                if len(filtered_data) >= data_sample_threshold:
                    if percentile < 100:
                        max_val = max(max_val, np.percentile(filtered_data, percentile))
                    else:
                        max_val = max(max_val, np.max(filtered_data))
                    min_val = min(min_val, np.min(filtered_data))
        
        col_max_values.extend([max_val] * n_areas)
        col_min_values.extend([min_val] * n_areas)
    
    # Create location legend (colors)
    location_lines = []
    for _, loc_conf in sorted(location_conf.items(), key=lambda x: x[1]['order']):
        line = axes[0, 0].plot([], [],
                            color=loc_conf['color'],
                            label=loc_conf['label'],
                            linewidth=2)[0]
        location_lines.append(line)
    
    # Create protocol legend (linestyles)
    protocol_lines = []
    for _, proto_conf in sorted(protocol_conf.items(), key=lambda x: x[1]['order']):
        line = axes[0, 0].plot([], [],
                            color='gray',
                            label=proto_conf['label'],
                            linestyle=proto_conf['linestyle'],
                            linewidth=2)[0]
        protocol_lines.append(line)
    
    # Add legends to first subplot
    location_legend = axes[0, 0].legend(handles=location_lines,
                                    title='Region',
                                    loc='lower right',
                                    fontsize=8,
                                    framealpha=0.9,
                                    edgecolor='black')
    protocol_legend = axes[0, 1].legend(handles=protocol_lines,
                                    title='Protocol',
                                    loc='lower right',
                                    fontsize=8,
                                    framealpha=0.9,
                                    edgecolor='black')
    
    axes[0, 0].add_artist(location_legend)
    axes[0, 1].add_artist(protocol_legend)
    
    # Clear any automatic legends
    for ax in axes.flat:
        ax.get_legend().remove() if ax.get_legend() else None
    
    # Second pass: create plots
    for row, (metric_name, column_title, xlabel, data_field) in enumerate(metrics):
        df = data[metric_name]
        percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
        
        for col, area_type in enumerate(area_types):
            ax = axes[row, col]
            plot_df = filter_data_by_area(df, area_type)
            
            # Plot each location+protocol combination
            for location in sorted(location_conf.keys(), key=lambda x: location_conf[x]['order']):
                for protocol in sorted(protocol_conf.keys(), key=lambda x: protocol_conf[x]['order']):
                    mask = (plot_df[CommonField.LOCATION] == location) & (plot_df[CommonField.APP_TPUT_PROTOCOL] == protocol)
                    filtered_data = plot_df[mask][data_field]

                    if len(filtered_data) < data_sample_threshold:
                        logger.warn(f'{area_type}-{location}-{protocol} data sample is less than required threshold, skip plotting: {len(filtered_data)} < {data_sample_threshold}')
                        continue
                    
                    if percentile < 100:
                        filtered_data = filtered_data[filtered_data <= np.percentile(filtered_data, percentile)]
                    
                    data_sorted = np.sort(filtered_data)
                    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                    
                    ax.plot(
                        data_sorted,
                        cdf,
                        color=location_conf[location]['color'],
                        linestyle=protocol_conf[protocol]['linestyle'],
                        linewidth=2
                    )
            
            # Set axis properties
            padding = (col_max_values[col] - col_min_values[col]) * 0.05
            if max_xlim:
                ax.set_xlim(col_min_values[col] - padding, max_xlim + padding)
            else:
                ax.set_xlim(col_min_values[col] - padding, col_max_values[col] + padding)
            
            if x_step:
                ax.set_xticks(np.arange(col_min_values[col], col_max_values[col] + x_step, x_step))
            
            ax.grid(True, alpha=0.3)
            ax.set_yticks(np.arange(0, 1.1, 0.25))
            ax.tick_params(axis='both', labelsize=10)
            
            if col == 0:
                ax.set_ylabel('CDF', fontsize=10)
            
            # Add area type as column title
            ax.set_title(f'{area_type.capitalize()}', fontsize=12, fontweight='bold')
            
            # Add x-label only on the bottom row
            if row == n_metrics - 1:
                ax.set_xlabel(xlabel, fontsize=10)
    
    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    plt.close()

def save_tput_stats_to_json(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        area_conf: Dict[str, Any],
        filepath: str,
    ):
    """Save hierarchical statistics for each metric.
    
    Structure:
    metric_name:
            area_type:
                location:
                    protocol:
                        stats (min, max, median, percentiles, etc.)
    """
    stats = {}
    
    for metric_name, _, _, data_field in metrics:
        stats[metric_name] = {}
        df = data[metric_name]
        grouped_df = df.groupby([CommonField.LOCATION, CommonField.APP_TPUT_PROTOCOL, CommonField.APP_TPUT_DIRECTION])
        
        # First level: Area Type
        for area_type in area_conf.keys():
            stats[metric_name][area_type] = {}
            area_df = filter_data_by_area(df, area_type)
            
            # Second level: Location
            for location in area_df[CommonField.LOCATION].unique():
                stats[metric_name][area_type][location] = {}
                loc_df = area_df[area_df[CommonField.LOCATION] == location]

                
                # Third level: Protocol
                for protocol in loc_df[CommonField.APP_TPUT_PROTOCOL].unique():
                    proto_df = loc_df[loc_df[CommonField.APP_TPUT_PROTOCOL] == protocol]
                    proto_data = proto_df[data_field]
                    if len(proto_data) == 0:
                        continue
                    
                    total_count = len(grouped_df.get_group((location, protocol, metric_name)))
                    stats[metric_name][area_type][location][protocol] = {
                        'min': float(np.min(proto_data)),
                        'max': float(np.max(proto_data)),
                        'median': float(np.median(proto_data)),
                        'mean': float(np.mean(proto_data)),
                        'percentile_5': float(np.percentile(proto_data, 5)),
                        'percentile_25': float(np.percentile(proto_data, 25)),
                        'percentile_75': float(np.percentile(proto_data, 75)),
                        'percentile_95': float(np.percentile(proto_data, 95)),
                        'sample_count': len(proto_data),
                        'percent_of_total': len(proto_data) / total_count * 100,
                    }
    
    # Save to JSON file
    with open(filepath, 'w') as f:
        json.dump(stats, f, indent=4)

def plot_tput_direction_by_area(
        direction: str, direction_label: str,
        x_step: float = None,
        reversed_layout: bool = False,
    ):
    tcp_tput_df = aggregate_tput_data_by_location(
        locations=['alaska', 'hawaii'],
        protocol='tcp',
        direction=direction
    )
    udp_tput_df = aggregate_tput_data_by_location(
        locations=['alaska', 'hawaii'],
        protocol='udp',
        direction=direction
    )
    downlink_tput_df = pd.concat([tcp_tput_df, udp_tput_df])
    plot_data = {
        direction: downlink_tput_df
    }
    metrics = [
        (direction, direction_label, 'Throughput (Mbps)', CommonField.TPUT_MBPS),
    ]
    output_filepath = os.path.join(
        output_dir,
        f'starlink_{direction}_tput_by_area.png',
    )
    if reversed_layout:
        plot_tput_metric_grid_reversed(
            data=plot_data,
            metrics=metrics,
            area_conf=area_conf,
            location_conf=location_conf,
            protocol_conf=protocol_conf,
            x_step=x_step,
            output_filepath=output_filepath,
        )
    else:
        plot_tput_metric_grid(
            data=plot_data,
            metrics=metrics,
            area_conf=area_conf,
            location_conf=location_conf,
            protocol_conf=protocol_conf,
            x_step=x_step,
            output_filepath=output_filepath,
        )
    save_tput_stats_to_json(
        data=plot_data,
        metrics=metrics,
        area_conf=area_conf,
        filepath=os.path.join(output_dir, f'starlink_{direction}_tput_by_area.json'),
    )

def plot_latency_metric_grid(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        area_conf: Dict[str, Any],
        location_conf: Dict[str, Any],
        output_filepath: str,
        title: str = None,
        percentile_filter: Dict[str, float] = None,
        max_xlim: float = None,
        data_sample_threshold: int = 240,
        x_step: float = None,
    ):
    """
    Plot a grid of latency metrics for each area type
    """
    area_types = list(sorted(area_conf.keys(), key=lambda x: area_conf[x]['order']))
    n_areas = len(area_types)
    n_metrics = len(metrics)
    
    fig, axes = plt.subplots(n_areas, n_metrics, figsize=(4.8*n_metrics, 3*n_areas))
    if n_areas == 1:
        axes = axes.reshape(1, -1)
    if n_metrics == 1:
        axes = axes.reshape(-1, 1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2, right=0.85)
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for col, (metric_name, _, _, data_field) in enumerate(metrics):
        max_val = 0
        min_val = float('inf')
        percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
        
        for area in area_types:
            df = data[metric_name]
            plot_df = filter_data_by_area(df, area)
            for _, loc_df in plot_df.groupby([CommonField.LOCATION]):
                filtered_data = loc_df[data_field]
                
                if len(filtered_data) >= data_sample_threshold:
                    if percentile < 100:
                        max_val = max(max_val, np.percentile(filtered_data, percentile))
                    else:
                        max_val = max(max_val, np.max(filtered_data))
                    min_val = min(min_val, np.min(filtered_data))
        
        col_max_values.append(max_val)
        col_min_values.append(min_val)
    
    # Create location legend (colors)
    location_lines = []
    for _, loc_conf in sorted(location_conf.items(), key=lambda x: x[1]['order']):
        line = axes[0, 0].plot([], [],
                            color=loc_conf['color'],
                            label=loc_conf['label'],
                            linewidth=2)[0]
        location_lines.append(line)
    
    # Add single legend for locations
    location_legend = axes[0, 0].legend(handles=location_lines,
                                    title='Region',
                                    loc='lower right',
                                    fontsize=8,
                                    framealpha=0.9,
                                    edgecolor='black')
    
    # Add legend to the first subplot
    axes[0, 0].add_artist(location_legend)
    
    # Clear any automatic legends
    for ax in axes.flat:
        ax.get_legend().remove() if ax.get_legend() else None
    
    # Second pass: create plots
    for row, area_type in enumerate(area_types):
        for col, (metric_name, column_title, xlabel, data_field) in enumerate(metrics):
            ax = axes[row, col]
            df = data[metric_name]
            plot_df = filter_data_by_area(df, area_type)
            percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
            
            # Plot each location
            for location in sorted(location_conf.keys(), key=lambda x: location_conf[x]['order']):
                mask = plot_df[CommonField.LOCATION] == location
                filtered_data = plot_df[mask][data_field]

                if len(filtered_data) < data_sample_threshold:
                    logger.warn(f'{area_type}-{location} data sample is less than required threshold, skip plotting: {len(filtered_data)} < {data_sample_threshold}')
                    continue
                
                if percentile < 100:
                    filtered_data = filtered_data[filtered_data <= np.percentile(filtered_data, percentile)]
                
                data_sorted = np.sort(filtered_data)
                cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                
                ax.plot(
                    data_sorted,
                    cdf,
                    color=location_conf[location]['color'],
                    linewidth=2
                )
            
            # Set axis properties
            padding = (col_max_values[col] - col_min_values[col]) * 0.05
            if max_xlim:
                ax.set_xlim(col_min_values[col] - padding, max_xlim + padding)
            else:
                ax.set_xlim(col_min_values[col] - padding, col_max_values[col] + padding)
            
            if x_step:
                ax.set_xticks(np.arange(col_min_values[col], col_max_values[col] + x_step, x_step))
            
            ax.grid(True, alpha=0.3)
            ax.set_yticks(np.arange(0, 1.1, 0.25))
            ax.tick_params(axis='both', labelsize=10)
            
            if col == 0:
                ax.text(-0.2, 0.5, area_type.capitalize(),
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14,
                       fontweight='bold')
                ax.set_ylabel('CDF', fontsize=10)
            
            if row == 0:
                ax.set_title(column_title, fontsize=12, fontweight='bold')
            
            if row == n_areas - 1:
                ax.set_xlabel(xlabel, fontsize=10)
    
    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    plt.close()

def save_latency_stats_to_json(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        area_conf: Dict[str, Any],
        filepath: str,
    ):
    """Save hierarchical statistics for latency metrics.
    
    Structure:
    metric_name:
        area_type:
            location:
                stats (min, max, median, percentiles, etc.)
    """
    stats = {}
    
    for metric_name, _, _, data_field in metrics:
        stats[metric_name] = {}
        df = data[metric_name]
        grouped_df = df.groupby([CommonField.LOCATION])
        
        # First level: Area Type
        for area_type in area_conf.keys():
            stats[metric_name][area_type] = {}
            area_df = filter_data_by_area(df, area_type)
            
            # Second level: Location
            for location in area_df[CommonField.LOCATION].unique():
                loc_df = area_df[area_df[CommonField.LOCATION] == location]
                loc_data = loc_df[data_field]
                
                if len(loc_data) == 0:
                    continue
                
                total_count = len(grouped_df.get_group((location,)))
                stats[metric_name][area_type][location] = {
                    'min': float(np.min(loc_data)),
                    'max': float(np.max(loc_data)),
                    'median': float(np.median(loc_data)),
                    'mean': float(np.mean(loc_data)),
                    'percentile_5': float(np.percentile(loc_data, 5)),
                    'percentile_25': float(np.percentile(loc_data, 25)),
                    'percentile_75': float(np.percentile(loc_data, 75)),
                    'percentile_95': float(np.percentile(loc_data, 95)),
                    'sample_count': len(loc_data),
                    'percent_of_total': len(loc_data) / total_count * 100,
                }
    
    # Save to JSON file
    with open(filepath, 'w') as f:
        json.dump(stats, f, indent=4)

def plot_latency_by_area():
    latency_df = aggregate_latency_data_by_location(
        locations=['alaska', 'hawaii'],
        protocol='icmp',
    )
    plot_data = {
        'latency': latency_df
    }
    metrics = [
        ('latency', 'Latency (Up to P95)', 'Round-Trip Time (ms)', CommonField.RTT_MS),
    ]
    output_filepath = os.path.join(
        output_dir,
        f'starlink_latency_by_area.png',
    )
    plot_latency_metric_grid(
        data=plot_data,
        metrics=metrics,
        area_conf=area_conf,
        location_conf=location_conf,
        output_filepath=output_filepath,
        percentile_filter={
            'latency': 95,
        },
    )
    save_latency_stats_to_json(
        data=plot_data,
        metrics=metrics,
        area_conf=area_conf,
        filepath=os.path.join(output_dir, f'starlink_latency_by_area.json'),
    )

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    plot_tput_direction_by_area(
        direction='downlink',
        direction_label='Downlink',
        x_step=50,
        reversed_layout=True,
    )
    plot_tput_direction_by_area(
        direction='uplink',
        direction_label='Uplink',
        x_step=10,
        reversed_layout=True,
    )
    plot_latency_by_area()

if __name__ == '__main__':
    main()
