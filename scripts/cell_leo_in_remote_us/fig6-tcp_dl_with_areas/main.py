import json
import os
from typing import Dict, List
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.cell_leo_in_remote_us.common import aggregate_xcal_tput_data_by_location, location_conf, operator_conf, tech_conf
from scripts.constants import XcalField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('tcp_dl_with_areas', filename=os.path.join(current_dir, 'outputs', 'tcp_dl_with_areas.log'))


def plot_metric_grid(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        tech_conf: Dict[str, Dict],
        output_filepath: str,
        title: str = None,
        percentile_filter: Dict[str, float] = None,
        max_xlim: float = None,
        x_step: int = None,
        data_sample_threshold: int = 240, # one round data
    ):
    n_locations = len(loc_conf)
    n_metrics = len(metrics)
    
    fig, axes = plt.subplots(n_locations, n_metrics, figsize=(4.8*n_metrics, 3*n_locations))
    if n_locations == 1:
        axes = axes.reshape(1, -1)
    if n_metrics == 1:
        axes = axes.reshape(-1, 1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2, right=0.85)
    
    locations_sorted = sorted(loc_conf.keys(), key=lambda x: loc_conf[x]['order'])
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for col, (metric_name, _, _, data_field) in enumerate(metrics):
        max_val = 0
        min_val = float('inf')
        
        for location in locations_sorted:
            plot_df = data[metric_name][data[metric_name]['location'] == location]
            for op_tech in plot_df[['operator', XcalField.ACTUAL_TECH]].drop_duplicates().itertuples():
                op_data = plot_df[
                    (plot_df['operator'] == op_tech.operator) & 
                    (plot_df[XcalField.ACTUAL_TECH] == op_tech.actual_tech)
                ][data_field]

                max_val = np.max(op_data)
                if max_val > 1000:
                    pass
                
                # Apply percentile filter if specified
                if percentile_filter and metric_name in percentile_filter:
                    percentile = percentile_filter[metric_name]
                    op_data = op_data[op_data <= np.percentile(op_data, percentile)]
                
                if len(op_data) > 0:  # Only update if we have data after filtering
                    max_val = max(max_val, np.max(op_data))
                    if max_val > 1000:
                        pass
                    min_val = min(min_val, np.min(op_data))
        
        col_max_values.append(max_val)
        col_min_values.append(min_val)
    
    operator_lines = []
    for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
        line = axes[1, 0].plot([], [], 
                            color='gray',
                            label=op_conf['label'],
                            linestyle=op_conf['linestyle'],
                            linewidth=2)[0]
        operator_lines.append(line)
    
    # Create tech legend (colors)
    tech_lines = []
    for _, t_conf in sorted(tech_conf.items(), key=lambda x: x[1]['order']):
        line = axes[0, 0].plot([], [],
                            color=t_conf['color'],
                            label=t_conf['label'],
                            linestyle='-',
                            linewidth=2)[0]
        tech_lines.append(line)
    
    # Place tech legend in first subplot (0,0)
    tech_legend = axes[0, 0].legend(handles=tech_lines,
                                title='Technology',
                                loc='lower right',
                                fontsize=8,
                                framealpha=0.9,
                                edgecolor='black')
    tech_legend.get_title().set_fontweight('bold')
    tech_legend.get_title().set_fontsize(8)
    
    # Place operator legend in subplot below (1,0)
    operator_legend = axes[1, 0].legend(handles=operator_lines, 
                                    title='Operator',
                                    loc='lower right',
                                    fontsize=8,
                                    framealpha=0.9,
                                    edgecolor='black')
    operator_legend.get_title().set_fontweight('bold')
    operator_legend.get_title().set_fontsize(8)
    
    # Add both legends to their respective subplots
    axes[0, 0].add_artist(tech_legend)
    axes[1, 0].add_artist(operator_legend)
    
    # Clear any automatic legend that might appear
    for ax in axes.flat:
        ax.get_legend().remove() if ax.get_legend() else None
    
    # Second pass: create plots
    for row, location in enumerate(locations_sorted):
        for col, (metric_name, column_title, xlabel, data_field) in enumerate(metrics):
            ax = axes[row, col]
            mask = data[metric_name]['location'] == location
            plot_df = data[metric_name][mask]
            
            # Plot each operator+tech combination
            for op_key, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
                for t_key, t_conf in sorted(tech_conf.items(), key=lambda x: x[1]['order']):
                    mask = (plot_df['operator'] == op_key) & (plot_df[XcalField.ACTUAL_TECH] == t_key)
                    operator_data = plot_df[mask][data_field]

                    # Skip if not enough samples
                    if len(operator_data) < data_sample_threshold:
                        logger.warn(f'{location}-{op_key}-{t_key} data sample is less than required threshold, skip plotting: {len(operator_data)} < {data_sample_threshold}')
                        continue
                    
                    # Apply percentile filter if specified
                    if percentile_filter and metric_name in percentile_filter:
                        percentile = percentile_filter[metric_name]
                        operator_data = operator_data[operator_data <= np.percentile(operator_data, percentile)]
                    
                    data_sorted = np.sort(operator_data)
                    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                    
                    ax.plot(
                        data_sorted,
                        cdf,
                        color=t_conf['color'],
                        linestyle=op_conf['linestyle'],
                        linewidth=2
                    )
            
            # Set x-axis limits and ticks
            x_min = col_min_values[col] - 5
            if max_xlim:
                x_max = max_xlim
            else:
                x_max = col_max_values[col]
            ax.set_xlim(x_min, x_max)
            if x_step:
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
                ax.text(-0.25, 0.5, loc_conf[location]['label'],
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14,
                       fontweight='bold')
                ax.set_ylabel('CDF', fontsize=10)
            
            if row == 0:
                ax.set_title(column_title, fontsize=12, fontweight='bold')
            
            if row == n_locations - 1:
                ax.set_xlabel(xlabel, fontsize=10)
    
    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    logger.info(f'Saved plot to {output_filepath}')
    plt.close()

def save_stats_to_json(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        filepath: str,
    ):
    """Save hierarchical statistics for each metric.
    
    Structure:
    metric_name:
        location:
            operator:
                area_type:
                    tech:
                        stats (min, max, median, percentiles, etc.)
    """
    stats = {}
    
    for metric_name, _, _, data_field in metrics:
        stats[metric_name] = {}
        df = data[metric_name]
        
        # First level: Location
        for location in df['location'].unique():
            stats[metric_name][location] = {}
            loc_df = df[df['location'] == location]
            
            # Second level: Operator
            for operator in loc_df['operator'].unique():
                stats[metric_name][location][operator] = {}
                op_df = loc_df[loc_df['operator'] == operator]

                if XcalField.SEGMENT_ID in op_df.columns:
                    grouped_df = op_df.groupby([XcalField.SEGMENT_ID])
                    tech_distance_mile_map, total_distance_miles = calculate_tech_coverage_in_miles(grouped_df)
                
                # Fourth level: Technology
                tech_breakdown = {}
                for tech in op_df[XcalField.ACTUAL_TECH].unique():
                    tech_df = op_df[op_df[XcalField.ACTUAL_TECH] == tech]
                    tech_data = tech_df[data_field]
                    operator_total = len(op_df)
                    if len(tech_data) == 0:
                        continue
                        
                    tech_breakdown[tech] = {
                        'min': float(np.min(tech_data)),
                        'max': float(np.max(tech_data)),
                        'median': float(np.median(tech_data)),
                        'mean': float(np.mean(tech_data)),
                        'percentile_5': float(np.percentile(tech_data, 5)),
                        'percentile_25': float(np.percentile(tech_data, 25)),
                        'percentile_75': float(np.percentile(tech_data, 75)),
                        'percentile_95': float(np.percentile(tech_data, 95)),
                        'sample_count': len(tech_data),
                        'percentage_occurence': float(len(tech_data) / operator_total * 100 if operator_total > 0 else 0),
                    }
                    if XcalField.SEGMENT_ID in op_df.columns:
                        tech_breakdown[tech]['percentage_miles'] = float(tech_distance_mile_map[tech] / total_distance_miles * 100 if total_distance_miles > 0 else 0)
                
                stats[metric_name][location][operator] = {
                    'tech_breakdown': tech_breakdown,
                    'total_samples': operator_total
                }
    
    # Save to JSON file
    with open(filepath, 'w') as f:
        json.dump(stats, f, indent=4)

def plot_tech_breakdown_cdfs_in_a_row(
        df: pd.DataFrame,
        data_field: str,
        output_filepath: str,
        operators: List[str],
        operator_conf: Dict[str, Dict],
        tech_conf: Dict[str, Dict],
        category: str | None = None,
        title: str | None = None,
    ):
    # Create figure with horizontal subplots (one per operator)
    n_operators = len(operators)

    show_category = category is not None
    show_title = title is not None
    
    # Adjust figure size and left margin based on whether we're showing category
    left_margin = 0.1 if show_category else 0.05
    fig = plt.figure(figsize=(4.8 * n_operators, 4))
    gs = fig.add_gridspec(1, n_operators, left=left_margin, bottom=0.2, top=0.85)
    axes = [fig.add_subplot(gs[0, i]) for i in range(n_operators)]
    
    # Ensure axes is always an array even with single operator
    if n_operators == 1:
        axes = np.array([axes])
    
    # Adjust spacing between subplots
    plt.subplots_adjust(wspace=0.2)
    
    # Add category text to the left middle of the entire figure if requested
    if show_category:
        fig.text(0.02, 0.5, category, 
                rotation=0,
                verticalalignment='center',
                horizontalalignment='right',
                size=16,
                weight='bold')
    
    # Add title at the top middle of the figure if requested
    if show_title:
        fig.suptitle(title, y=0.98, size=16, weight='bold')
    
    # Set up common y-axis label for all subplots
    fig.text(0.035, 0.5, 'CDF', 
             rotation=90,
             verticalalignment='center',
             size=14,
             weight='bold')
    
    # Set up common x-axis label for all subplots
    fig.text(0.5, 0.08, 'Throughput (Mbps)', 
             horizontalalignment='center',
             size=14,
             weight='bold')
    
    # TODO: Implement subplot content for each operator
    for idx, operator in enumerate(operators):
        ax = axes[idx]
        # Set operator name as subplot title
        ax.set_title(operator_conf[operator]['label'])
        
        # Basic axis setup
        ax.grid(True, alpha=0.3)
        ax.set_yticks(np.arange(0, 1.1, 0.25))
        ax.tick_params(axis='both')
    
    # Save the figure
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    logger.info(f'Saved plot to {output_filepath}')
    plt.close()


def plot_tput_tech_breakdown_by_area_by_operator(
        locations: List[str], 
        protocol: str, 
        direction: str,
        max_xlim: float = None,
        percentile_filter: Dict[str, float] = None,
        data_sample_threshold: int = 240, # 1 rounds data (~2min)
    ):
    # tput_df = aggregate_xcal_tput_data_by_location(
    #     locations=locations,
    #     protocol=protocol,
    #     direction=direction
    # )
    tput_df = pd.DataFrame()

    output_filepath = os.path.join(current_dir, 'outputs', f'tech_breakdown_by_area.{protocol}_{direction}.png')

    tput_field_map = {
        'downlink': XcalField.TPUT_DL,
        'uplink': XcalField.TPUT_UL,
    }

    tput_field = tput_field_map[direction]
    metrics = [
        ('urban', 'Urban', 'Throughput (Mbps)', tput_field),
        ('rural', 'Rural', 'Throughput (Mbps)', tput_field),
    ]

    plot_tech_breakdown_cdfs_in_a_row(
        category='AK',
        title='Urban',
        df=tput_df,
        data_field=tput_field,
        output_filepath=output_filepath,
        operators=['att', 'verizon', 'tmobile'],
        operator_conf=operator_conf,
        tech_conf=tech_conf,
    )

    # plot_metric_grid(
    #     data=plot_data, 
    #     metrics=metrics, 
    #     loc_conf=location_conf, 
    #     operator_conf=operator_conf, 
    #     tech_conf=tech_conf, 
    #     output_filepath=output_filepath,
    #     max_xlim=max_xlim,
    #     percentile_filter=percentile_filter,
    #     data_sample_threshold=data_sample_threshold,
    # )
    # save_stats_to_json(
    #     data=plot_data,
    #     metrics=metrics, 
    #     filepath=output_filepath.replace('.png', '.json'),
    # )


def main():
    if not os.path.exists(os.path.join(current_dir, 'outputs')):
        os.makedirs(os.path.join(current_dir, 'outputs'))

    plot_tput_tech_breakdown_by_area_by_operator(
        locations=['alaska', 'hawaii'],
        protocol='tcp',
        direction='downlink',
    )

if __name__ == '__main__':
    main()