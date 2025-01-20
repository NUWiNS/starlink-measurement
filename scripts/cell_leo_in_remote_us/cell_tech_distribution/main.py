import math
import os
import json
import numpy as np
import matplotlib.pyplot as plt
from typing import Callable, Dict
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.cell_leo_in_remote_us.common import calculate_tech_coverage_in_miles, cellular_operator_conf, cellular_location_conf, tech_order, tech_conf
from scripts.constants import XcalField, CommonField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('cell_tech_distribution', filename=os.path.join(current_dir, 'outputs', 'cell_tech_distribution.log'))


def plot_tech_dist_stack(
        dfs: dict, 
        output_dir: str, 
        location_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        tech_conf: Dict[str, Dict],
        title: str = 'Technology Distribution by Operator',
        fig_name: str = 'fig',
        df_mask: Callable | None = None, 
    ):
    """Plot a stack plot of the tech distribution for each operator
    
    Args:
        dfs (dict): Dictionary mapping operator names to their dataframes
        output_dir (str): Directory to save the plot
        location (str): Location name (e.g., 'alaska' or 'hawaii')
    """
    # Sort operators by the order of Verizon, T-Mobile, ATT
    operators = sorted(list(dfs.keys()), key=lambda x: operator_conf[x]['order'])
    
    # Dynamically adjust figure width based on number of operators while maintaining bar width consistency
    bar_width = 0.3  # Reduced from 0.35
    spacing_factor = 2.8  # Reduced from 2.8
    max_operator_length = 3
    total_width_needed = max_operator_length * bar_width * spacing_factor
    fig_width = total_width_needed + 1.9
    fig_height = fig_width * 0.6  # Make height 60% of width for rectangular shape
    
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    
    tech_fractions = []
    stats = {}

    # Calculate fractions for each operator
    for operator in operators:
        print(f'Calculate fractions for {operator}')
        df = dfs[operator]
        sub_stats = {}

        if df_mask is not None:
            data = df[df_mask(df)]
        else:
            data = df
        
        # Calculate total distance for each tech
        tech_distance_mile_map = {tech: 0 for tech in tech_order}
        
        grouped_df = data.groupby([XcalField.SEGMENT_ID])
        tech_distance_mile_map, total_distance_miles = calculate_tech_coverage_in_miles(grouped_df)
        
        sub_stats['tech_distance_miles'] = tech_distance_mile_map
        sub_stats['total_distance_miles'] = total_distance_miles
        sub_stats['tech_fractions'] = {tech: tech_distance_mile_map[tech] / total_distance_miles 
                                      for tech in tech_order}
        
        if total_distance_miles > 0:
            tech_fractions.append({tech: tech_distance_mile_map[tech] / total_distance_miles 
                                 for tech in tech_order})
            
        stats[operator] = sub_stats
    
    # Find which technologies are actually present in the data
    present_techs = set()
    for fractions in tech_fractions:
        for tech, fraction in fractions.items():
            if fraction > 0:
                present_techs.add(tech)
    
    # Sort present_techs according to tech_order
    present_techs = sorted(list(present_techs), key=lambda x: tech_order.index(x))
    
    # Plot stacked bars with consistent width
    x = np.arange(len(operators)) * spacing_factor * bar_width  # Evenly space the bars
    bottom = np.zeros(len(operators))
    
    # Only plot technologies that are present
    for i, tech in enumerate(present_techs):
        values = [f[tech] if tech in f else 0 for f in tech_fractions]
        ax.bar(x, values, bottom=bottom, label=tech, color=tech_conf[tech]['color'], 
               width=bar_width)
        bottom += values
    
    # ax.set_title(title)
    # ax.set_xlabel('Operator')
    ax.set_ylabel('Fraction of Miles')
    ax.set_yticks(np.arange(0, 1.1, 0.2))
    ax.set_xticks(x)
    ax.set_xticklabels(map(lambda x: operator_conf[x]['label'], operators))
    
    # Set x-axis limits to maintain consistent spacing
    ax.set_xlim(x[0] - bar_width * 1.5, x[-1] + bar_width * 1.5)
    
    # Move legend above the plot
    handles, labels = ax.get_legend_handles_labels()
    # Calculate number of columns based on available width
    # Use max 3 columns to ensure readability
    ncol = min(3, len(present_techs))
    nrow = math.ceil(len(present_techs) / ncol)
    legend_height = 0.15 * nrow  # Adjust height based on number of rows

    ax.legend(handles[::-1], labels[::-1],
             bbox_to_anchor=(0, 1.02, 1, legend_height), 
             loc='lower left',
             mode='expand',
             borderaxespad=0,
             ncol=ncol,
             fontsize=9)
    
    ax.grid(True, axis='y', alpha=0.3)  # Reduced grid line opacity
    
    # Adjust layout to be more compact
    plt.tight_layout()
    figure_path = os.path.join(output_dir, f'{fig_name}.pdf')
    plt.savefig(figure_path, bbox_inches='tight')
    plt.close()
    
    # Save stats to json
    stats_json_path = os.path.join(output_dir, f'{fig_name}.stats.json')
    with open(stats_json_path, 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.info(f"Saved technology distribution plot to {figure_path}")
    logger.info(f"Saved technology distribution stats to {stats_json_path}")


def main():
    # for location in ['alaska']:
    for location in ['alaska', 'hawaii']:
        logger.info(f'-- Processing dataset: {location}')
        base_dir = cellular_location_conf[location]['root_dir']
        output_dir = os.path.join(current_dir, 'outputs/sizhe_new_data', location)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Store dataframes for all operators
        operator_dfs = {}

        for operator in sorted(cellular_location_conf[location]['operators'], key=lambda x: cellular_operator_conf[x]['order']):
            logger.info(f'---- Processing operator: {operator}')
            smart_tput_csv_path = os.path.join(base_dir, 'xcal/sizhe_new_data', f'{operator}_xcal_smart_tput.csv')
            rtt_csv_path = os.path.join(base_dir, 'ping/sizhe_new_data', f'{operator}_ping.csv')

            smart_tput_df = pd.read_csv(smart_tput_csv_path)
            rtt_df = pd.read_csv(rtt_csv_path)
            
            # Merge the dataframes to create a common structure
            df = pd.merge(
                smart_tput_df[[CommonField.LOCAL_DT, XcalField.SEGMENT_ID, XcalField.ACTUAL_TECH, XcalField.LON, XcalField.LAT]],
                rtt_df[[CommonField.LOCAL_DT, XcalField.SEGMENT_ID, XcalField.ACTUAL_TECH, XcalField.LON, XcalField.LAT]],
                on=XcalField.SEGMENT_ID,
                how='inner'
            )

            operator_dfs[operator] = df

        loc_label = cellular_location_conf[location]['label']

        # All Areas
        plot_tech_dist_stack(
            dfs=operator_dfs,
            output_dir=output_dir, 
            location_conf=cellular_location_conf,
            operator_conf=cellular_operator_conf,
            tech_conf=tech_conf,
            title=f'Technology Distribution ({loc_label}-All Areas)',
            fig_name=f'tech_dist_stack_all_areas.{location}',
        )

        # Urban (Urban + suburban)
        plot_tech_dist_stack(
            dfs=operator_dfs, 
            df_mask=lambda df: (df[XcalField.AREA] == 'urban') | (df[XcalField.AREA] == 'suburban'), 
            output_dir=output_dir, 
            location_conf=cellular_location_conf,
            operator_conf=cellular_operator_conf,
            tech_conf=tech_conf,
            title=f'Technology Distribution ({loc_label}-Urban)',
            fig_name=f'tech_dist_stack_urban.{location}',
        )

        # Rural
        plot_tech_dist_stack(
            dfs=operator_dfs, 
            df_mask=lambda df: df[XcalField.AREA] == 'rural', 
            output_dir=output_dir, 
            location_conf=cellular_location_conf,
            operator_conf=cellular_operator_conf,
            tech_conf=tech_conf,
            title=f'Technology Distribution ({loc_label}-Rural)',
            fig_name=f'tech_dist_stack_rural.{location}',
        )


if __name__ == '__main__':
    main()
