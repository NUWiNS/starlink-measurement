import json
import os
from typing import Dict, List, Tuple
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

from scripts.cell_leo_in_remote_us.common import aggregate_xcal_tput_data_by_location, cellular_operator_conf, cellular_location_conf
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('tcp_dl_with_areas', filename=os.path.join(current_dir, 'outputs', 'tcp_dl_with_areas.log'))

def plot_metric_grid(
        data: Dict[str, pd.DataFrame],
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        metrics: List[tuple],
        output_filepath: str,
        title: str = None,
        max_xlim: float = None,
        x_step: float = None,
        enable_inset: bool = False,
        inset_x_min: float = 0,
        inset_x_max: float = 50,
        inset_x_step: float = 10,
        legend_loc: str = 'upper right',
        inset_bbox_to_anchor: Tuple[float, float, float, float] = (0.5, 0, 0.45, 0.65),
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

            needed_operators = loc_conf[location]['operators']
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

            needed_operators = loc_conf[location]['operators']
            plot_df = plot_df[plot_df['operator'].isin(needed_operators)]

            percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
            
            if enable_inset:
                # Create inset axes - tall rectangle in lower right
                axins = inset_axes(ax, 
                                 width="60%",  # increased width
                                 height="85%",  # increased height
                                 bbox_to_anchor=inset_bbox_to_anchor,  # (x, y, width, height) - adjusted size and position
                                 bbox_transform=ax.transAxes,
                                 borderpad=0)
            else:
                axins = None
            
            for op_key, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
                if op_key in plot_df['operator'].unique():
                    operator_data = plot_df[plot_df['operator'] == op_key][data_field]
                    
                    if percentile < 100:
                        operator_data = operator_data[operator_data <= np.percentile(operator_data, percentile)]
                    
                    data_sorted = np.sort(operator_data)
                    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                    
                    # Main plot
                    ax.plot(
                        data_sorted,
                        cdf,
                        color=op_conf['color'],
                        linewidth=2
                    )
                    if axins:
                        # Inset plot
                        axins.plot(
                            data_sorted,
                        cdf,
                        color=op_conf['color'],
                        linewidth=2
                    )
            
            # Set x-axis limits and ticks for main plot
            if max_xlim:
                x_max = max_xlim
            else:
                x_max = col_max_values[col]
            ax.set_xlim(0, x_max)

            if axins:
                # Configure inset axes
                axins.set_xlim(inset_x_min, inset_x_max)  # Focus on 0-50 Mbps range
                axins.set_ylim(0.0, 1.0)  # Show full CDF range
                axins.grid(True, alpha=0.3)
                axins.tick_params(axis='both', labelsize=10)
                axins.set_xticks(np.arange(0, inset_x_max + 1, inset_x_step))
                axins.set_yticks(np.arange(0, 1.1, 0.2))
                
                # Add frame around the inset
                for spine in axins.spines.values():
                    spine.set_color('black')
                    spine.set_linewidth(1.0)
                
                # Update connecting lines positions for lower right placement
                from mpl_toolkits.axes_grid1.inset_locator import mark_inset
                mark_inset(ax, axins, loc1=1, loc2=4, fc="none", ec="0.5")

            if not x_step:
                if metric_name == 'latency':
                    x_step = 50
                elif 'downlink' in metric_name:
                    x_step = 200
                elif 'uplink' in metric_name:
                    x_step = 25

            # round to the nearest x_step
            ax.set_xticks(np.arange(
                0, 
                max_xlim + 1, 
                x_step
            ))
        
            
            ax.grid(True, alpha=0.3)
            # Set axis limits and ticks
            ax.set_ylim(0, 1)
            ax.set_yticks(np.arange(0, 1.1, 0.2))
            ax.tick_params(axis='both')
            
            if col == 0:
                ax.text(-0.2, 0.5, loc_conf[location]['label'],
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14)
                ax.set_ylabel('CDF')
            
            if row == 0:
                ax.set_title(column_title)
            
            if row == 0 and col == 0:
                legend = ax.legend(loc=legend_loc,
                                 framealpha=0.5,
                                 edgecolor='black',
                                 fontsize=10)
            
            if row == n_locations - 1:
                ax.set_xlabel(xlabel)
    
    # Adjust layout to prevent overlapping - increase spacing further
    plt.subplots_adjust(wspace=0.5, hspace=0)  # Increased wspace more to accommodate larger insets

    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    print(f'Saved plot to {output_filepath}')
    plt.close()


def main():
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Collect all data
    tput_data = {}
    for protocol in ['tcp', 'udp']:
        tput_data[protocol] = {}
        for direction in ['downlink']:
            tput_data[protocol][direction] = aggregate_xcal_tput_data_by_location(
                locations=['alaska', 'hawaii'],
                location_conf=cellular_location_conf,
                protocol=protocol,
                direction=direction,
            )

    # Plot Downlink Performance
    downlink_metrics = [
        ('tcp_downlink', 'TCP Downlink', 'Throughput (Mbps)', XcalField.TPUT_DL),
        ('udp_downlink', 'UDP Downlink', 'Throughput (Mbps)', XcalField.TPUT_DL),
    ]
    plot_metric_grid(
        data={
            'tcp_downlink': tput_data['tcp']['downlink'],
            'udp_downlink': tput_data['udp']['downlink']
        },
        loc_conf=cellular_location_conf,
        operator_conf=cellular_operator_conf,
        metrics=downlink_metrics,
        max_xlim=350,
        x_step=50,
        # enable_inset=True,
        # inset_x_min=0,
        # inset_x_max=30,
        # inset_x_step=10,
        output_filepath=os.path.join(output_dir, 'cellular.downlink.ak_hi.pdf'),
    )

    # save_stats_network_kpi(tput_data, latency_data, location_conf, operator_conf, output_dir)

if __name__ == '__main__':
    main()