import json
import os
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import sys
from typing import Callable, List, Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.utilities.distance_utils import DistanceUtils
from scripts.celllular_analysis.TechBreakdown import Segment, TechBreakdown
from scripts.utilities.list_utils import replace_with_elements
from scripts.constants import XcalField, XcallHandoverEvent
from scripts.logging_utils import create_logger
from scripts.alaska_starlink_trip.configs import ROOT_DIR as AL_DATASET_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HI_DATASET_DIR

current_dir = os.path.dirname(os.path.abspath(__file__))

logger = create_logger('handover_split', filename=os.path.join(current_dir, 'outputs'   , 'handover_process.log'))

# Colors for different technologies - from grey (NO SERVICE) to rainbow gradient (green->yellow->orange->red) for increasing tech
colors = ['#808080', '#326f21', '#86c84d', '#f5ff61', '#f4b550', '#eb553a', '#ba281c']  # Grey, green, light green, yellow, amber, orange, red
tech_order = ['NO SERVICE', 'LTE', 'LTE-A', '5G-low', '5G-mid', '5G-mmWave (28GHz)', '5G-mmWave (39GHz)']

def calculate_tech_coverage_in_miles(grouped_df: pd.DataFrame) -> Tuple[dict, float]:
    # Initialize mile fractions for each tech
    tech_distance_mile_map = {}
    for tech in tech_order:
        tech_distance_mile_map[tech] = 0

    # calculate cumulative distance for each segment
    for segment_id, segment_df in grouped_df:
        unique_techs = segment_df[XcalField.ACTUAL_TECH].unique()
        if len(unique_techs) > 1:
            raise ValueError(f"Segment ({segment_id}) should only have one tech: {unique_techs}")
        tech = unique_techs[0]

        tech_distance_miles = DistanceUtils.calculate_cumulative_miles(segment_df[XcalField.LON].tolist(), segment_df[XcalField.LAT].tolist())
        # add to total distance for this tech
        tech_distance_mile_map[tech] += tech_distance_miles

    total_distance_miles = sum(tech_distance_mile_map.values())

    return tech_distance_mile_map, total_distance_miles


def plot_tech_by_protocol_direction(df: pd.DataFrame, operator: str, output_dir: str):
    # Plot a stack plot of the tput for each tech grouped by protocol + direction
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Colors for different technologies - from grey (NO SERVICE) to rainbow gradient (green->yellow->orange->red) for increasing tech
    colors = ['#808080', '#326f21', '#86c84d', '#f5ff61', '#f4b550', '#eb553a', '#ba281c']  # Grey, green, light green, yellow, amber, orange, red
    tech_order = ['NO SERVICE', 'LTE', 'LTE-A', '5G-low', '5G-mid', '5G-mmWave (28GHz)', '5G-mmWave (39GHz)']
    
    # Create groups for protocol + direction combinations
    groups = []
    fractions = []

    stats = {}
    
    for protocol in ['tcp', 'udp']:
        for direction in ['downlink', 'uplink']:
            sub_stats = {}

            # Filter data for this protocol and direction
            mask = (df[XcalField.APP_TPUT_PROTOCOL] == protocol) & \
                  (df[XcalField.APP_TPUT_DIRECTION] == direction)
            data = df[mask]
            if len(data) == 0:
                continue

            # Calculate fractions
            grouped_df = data.groupby([XcalField.SEGMENT_ID])

            tech_distance_mile_map, total_distance_miles = calculate_tech_coverage_in_miles(grouped_df)

            sub_stats['tech_distance_miles'] = tech_distance_mile_map
            sub_stats['total_distance_miles'] = total_distance_miles

            if total_distance_miles > 0:  # Avoid division by zero
                tech_fractions = {tech: tech_distance_mile_map.get(tech, 0) / total_distance_miles for tech in tech_order}
                sub_stats['tech_fractions'] = tech_fractions
                fractions.append(tech_fractions)
                groups.append(f"{protocol}\n{direction}")

            stats[f"{protocol}_{direction}"] = sub_stats
    
    # Plot stacked bars
    x = range(len(groups))
    bottom = np.zeros(len(groups))
    bar_width = 0.5
    
    for i, tech in enumerate(tech_order):
        values = [f[tech] if tech in f else 0 for f in fractions]
        ax.bar(x, values, bottom=bottom, label=tech, color=colors[i], 
               width=bar_width)
        bottom += values
    
    ax.set_title(f'Technology Distribution in miles by Protocol and Direction ({operator})')
    ax.set_xlabel('Protocol + Direction')
    ax.set_ylabel('Fraction of Total Distance (Miles)')
    ax.set_xticks(x)
    ax.set_xticklabels(groups)
    ax.legend(title='Technology', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, axis='y')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'tech_distribution_{operator}.png'), bbox_inches='tight')
    plt.close()
    logger.info(f"Saved technology distribution plot to {os.path.join(output_dir, f'{operator}_tech_distribution.png')}")

    # save stats to json
    stats_json_path = os.path.join(output_dir, f'tech_distribution_{operator}.stats.json')
    with open(stats_json_path, 'w') as f:
        json.dump(stats, f, indent=4)
    logger.info(f"Saved technology distribution stats to {stats_json_path}")


def plot_tech_by_operator(
        dfs: dict, 
        df_mask: Callable, 
        output_dir: str, 
        title: str = 'Technology Distribution by Operator',
        fig_name: str = 'fig',
    ):
    """Plot a stack plot of the tech distribution for each operator
    
    Args:
        dfs (dict): Dictionary mapping operator names to their dataframes
        output_dir (str): Directory to save the plot
        location (str): Location name (e.g., 'alaska' or 'hawaii')
    """
    # Set up the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Colors for different technologies - from grey (NO SERVICE) to rainbow gradient
    colors = ['#808080', '#326f21', '#86c84d', '#f5ff61', '#f4b550', '#eb553a', '#ba281c']
    tech_order = ['NO SERVICE', 'LTE', 'LTE-A', '5G-low', '5G-mid', '5G-mmWave (28GHz)', '5G-mmWave (39GHz)']
    
    # Sort operators by the order of Verizon, T-Mobile, ATT
    operators = sorted(list(dfs.keys()), key=lambda x: {'verizon': 0, 'tmobile': 1, 'att': 2}.get(x.lower(), 999))
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
        
        if total_distance_miles > 0:
            tech_fractions.append({tech: tech_distance_mile_map[tech] / total_distance_miles 
                                 for tech in tech_order})
            
        stats[operator] = sub_stats
    
    # Plot stacked bars
    x = range(len(operators))
    bottom = np.zeros(len(operators))
    bar_width = 0.5
    
    for i, tech in enumerate(tech_order):
        values = [f[tech] if tech in f else 0 for f in tech_fractions]
        ax.bar(x, values, bottom=bottom, label=tech, color=colors[i], 
               width=bar_width)
        bottom += values
    
    ax.set_title(title)
    ax.set_xlabel('Operator')
    ax.set_ylabel('Fraction of Total Distance (Miles)')
    ax.set_xticks(x)
    ax.set_xticklabels(operators)
    ax.legend(title='Technology', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, axis='y')
    
    plt.tight_layout()
    figure_path = os.path.join(output_dir, f'{fig_name}.png')
    plt.savefig(figure_path, bbox_inches='tight')
    plt.close()
    
    # Save stats to json
    stats_json_path = os.path.join(output_dir, f'{fig_name}.stats.json')
    with open(stats_json_path, 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.info(f"Saved technology distribution plot to {figure_path}")
    logger.info(f"Saved technology distribution stats to {stats_json_path}")


def main():
    location_dir = {
        'alaska': AL_DATASET_DIR,
        'hawaii': HI_DATASET_DIR
    }

    for location in ['alaska', 'hawaii']:
        logger.info(f'-- Processing dataset: {location}')
        base_dir = location_dir[location]
        output_dir = os.path.join(current_dir, 'outputs', location)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Store dataframes for all operators
        operator_dfs = {}

        for operator in ['att', 'tmobile', 'verizon']:
            logger.info(f'---- Processing operator: {operator}')
            input_csv_path = os.path.join(base_dir, 'xcal', f'{operator}_xcal_smart_tput.csv')
            df = pd.read_csv(input_csv_path)
            operator_dfs[operator] = df
        
        # for operator in ['att', 'tmobile', 'verizon']:
            # df = operator_dfs[operator]
            # plot_tech_by_protocol_direction(df, operator, output_dir)
            # logger.info(f'---- Finished processing operator: {operator}')

        # # Create combined plot for all operators
        # plot_tech_by_operator(
        #     dfs=operator_dfs, 
        #     output_dir=output_dir, 
        #     title=f'Technology Distribution by Operator ({location})',
        #     fig_name=f'tech_dist_by_operator.{location}'
        # )

        # Filter by direction
        # Downlink
        logger.info('-- plot tech dist by downlink with all operators')
        df_mask = lambda df: (df[XcalField.APP_TPUT_DIRECTION] == 'downlink')
        plot_tech_by_operator(
            dfs=operator_dfs, 
            df_mask=df_mask,
            output_dir=output_dir, 
            title=f'Technology Distribution by Downlink ({location})',
            fig_name=f'tech_dist_by_downlink.{location}'
        )

        # Uplink
        logger.info('-- plot tech dist by uplink with all operators')
        df_mask = lambda df: (df[XcalField.APP_TPUT_DIRECTION] == 'uplink')
        plot_tech_by_operator(
            dfs=operator_dfs, 
            df_mask=df_mask,
            output_dir=output_dir, 
            title=f'Technology Distribution by Uplink ({location})',
            fig_name=f'tech_dist_by_uplink.{location}'
        )

        logger.info(f'-- Finished processing dataset: {location}')

if __name__ == "__main__":
    main()