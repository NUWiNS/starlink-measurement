import os
import json
import numpy as np
import matplotlib.pyplot as plt
from typing import Callable
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.cell_leo_in_remote_us.common import calculate_tech_coverage_in_miles, operator_conf, location_conf
from scripts.constants import XcalField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('cell_tech_distribution', filename=os.path.join(current_dir, 'outputs', 'cell_tech_distribution.log'))


def plot_tech_dist_stack(
        dfs: dict, 
        output_dir: str, 
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
    bar_width = 0.35  # Desired bar width
    spacing_factor = 2.8  # Space between bars relative to bar width
    max_operator_length = 3
    total_width_needed = max_operator_length * bar_width * spacing_factor
    fig_width = total_width_needed + 2
    fig_height = fig_width * 0.6  # Make height 60% of width for rectangular shape
    
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    
    # Colors for different technologies - from grey (NO SERVICE) to rainbow gradient
    colors = ['#808080', '#326f21', '#86c84d', '#f5ff61', '#f4b550']
    tech_order = ['NO SERVICE', 'LTE', 'LTE-A', '5G-low', '5G-mid']
    
    tech_fractions = []
    stats = {}

    # Calculate fractions for each operator
    for operator in operators:
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
        ax.bar(x, values, bottom=bottom, label=tech, color=colors[tech_order.index(tech)], 
               width=bar_width)
        bottom += values
    
    # ax.set_title(title)
    # ax.set_xlabel('Operator')
    ax.set_ylabel('Fraction of Miles')
    ax.set_xticks(x)
    ax.set_xticklabels(map(lambda x: operator_conf[x]['label'], operators))
    
    # Set x-axis limits to maintain consistent spacing
    ax.set_xlim(x[0] - bar_width * 1.5, x[-1] + bar_width * 1.5)
    
    # Adjust legend position and size
    ax.legend(title='Technology', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize='small')
    
    ax.grid(True, axis='y', alpha=0.3)  # Reduced grid line opacity
    
    # Adjust layout to be more compact
    plt.tight_layout()
    figure_path = os.path.join(output_dir, f'{fig_name}.png')
    plt.savefig(figure_path, bbox_inches='tight', dpi=300)
    plt.close()
    
    # Save stats to json
    stats_json_path = os.path.join(output_dir, f'{fig_name}.stats.json')
    with open(stats_json_path, 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.info(f"Saved technology distribution plot to {figure_path}")
    logger.info(f"Saved technology distribution stats to {stats_json_path}")


def main():
    for location in ['alaska', 'hawaii']:
        logger.info(f'-- Processing dataset: {location}')
        base_dir = location_conf[location]['root_dir']
        output_dir = os.path.join(current_dir, 'outputs', location)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Store dataframes for all operators
        operator_dfs = {}

        for operator in sorted(location_conf[location]['operators'], key=lambda x: operator_conf[x]['order']):
            logger.info(f'---- Processing operator: {operator}')
            input_csv_path = os.path.join(base_dir, 'xcal', f'{operator}_xcal_smart_tput.csv')
            df = pd.read_csv(input_csv_path)
            operator_dfs[operator] = df

        loc_label = location_conf[location]['label']

        # Urban + suburban tech breakdown
        plot_tech_dist_stack(
            dfs=operator_dfs, 
            df_mask=lambda df: (df[XcalField.AREA] == 'urban') | (df[XcalField.AREA] == 'suburban'), 
            output_dir=output_dir, 
            title=f'Technology Distribution ({loc_label}-Urban)',
            fig_name=f'tech_dist_stack_urban.{location}',
        )

        # Plot location-wide tech breakdown by area
        plot_tech_dist_stack(
            dfs=operator_dfs, 
            df_mask=lambda df: df[XcalField.AREA] == 'rural', 
            output_dir=output_dir, 
            title=f'Technology Distribution ({loc_label}-Rural)',
            fig_name=f'tech_dist_stack_rural.{location}',
        )


if __name__ == '__main__':
    main()
