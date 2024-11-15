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
colors = ['#808080', '#326f21', '#86c84d', '#ffd700', '#ff9900', '#ff4500', '#ba281c']  # Grey, green, light green, yellow, amber, orange, red
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

def plot_tech_by_area_for_one_operator(df: pd.DataFrame, operator: str, location: str, output_dir: str):
    # Plot a stack plot of the tput for each tech grouped by protocol + direction
    if location == 'alaska':
        figsize = (10, 8)
    else:
        figsize = (12, 8)
    fig, ax = plt.subplots(figsize=figsize)

    # Create groups for protocol + direction combinations
    groups = []
    fractions = []

    stats = {}
    
    for area in ['urban', 'suburban', 'rural']:
        sub_stats = {}

        # Filter data for this area
        mask = (df[XcalField.AREA] == area)
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
            groups.append(area)
        
        stats[area] = sub_stats

    if location == 'alaska':
        # Merge rural with suburban
        if 'rural' in stats and 'suburban' in stats:
            logger.info('Merging suburban with rural for Alaska')
            stats['suburban+rural'] = {
                'tech_distance_miles': {},
                'total_distance_miles': 0,
                'tech_fractions': {}
            }
            # Add rural miles to suburban
            for tech in tech_order:
                stats['suburban+rural']['tech_distance_miles'][tech] = stats['suburban']['tech_distance_miles'].get(tech, 0) + stats['rural']['tech_distance_miles'].get(tech, 0)
            stats['suburban+rural']['total_distance_miles'] = stats['suburban']['total_distance_miles'] + stats['rural']['total_distance_miles']
            
            # Recalculate suburban fractions
            if stats['suburban+rural']['total_distance_miles'] > 0:
                stats['suburban+rural']['tech_fractions'] = {
                    tech: stats['suburban+rural']['tech_distance_miles'][tech] / stats['suburban+rural']['total_distance_miles'] 
                    for tech in tech_order
                }
            
            # Update groups and fractions lists (add suburban+rural, remove rural and suburban  )
            groups.append('suburban+rural')
            fractions.append(stats['suburban+rural']['tech_fractions'])

            rural_idx = groups.index('rural') if 'rural' in groups else -1
            suburban_idx = groups.index('suburban') if 'suburban' in groups else -1
            if rural_idx != -1:
                groups.pop(rural_idx)
                fractions.pop(rural_idx)
            if suburban_idx != -1:
                groups.pop(suburban_idx)
                fractions.pop(suburban_idx)
    
    # Plot stacked bars
    x = range(len(groups))
    bottom = np.zeros(len(groups))
    bar_width = 0.35
    
    for i, tech in enumerate(tech_order):
        values = [f[tech] if tech in f else 0 for f in fractions]
        ax.bar(x, values, bottom=bottom, label=tech, color=colors[i], 
               width=bar_width)
        bottom += values
    
    ax.set_title(f'Tech Dist in miles by Area ({operator})')
    ax.set_xlabel('Area')
    ax.set_ylabel('Fraction of Total Distance (Miles)')
    ax.set_xticks(x)
    ax.set_xticklabels(groups)
    ax.legend(title='Technology', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, axis='y')
    
    plt.tight_layout()
    figure_path = os.path.join(output_dir, f'tech_dist_by_area.{operator}.png')
    plt.savefig(figure_path, bbox_inches='tight')
    plt.close()
    logger.info(f"Saved technology distribution plot to {figure_path}")

    # save stats to json
    stats_json_path = os.path.join(output_dir, f'tech_dist_by_area.{operator}.stats.json')
    with open(stats_json_path, 'w') as f:
        json.dump(stats, f, indent=4)
    logger.info(f"Saved technology distribution stats to {stats_json_path}")

def plot_tech_with_all_operators(
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

def plot_tcp_tput_cdf_by_tech_for_one_operator(
        df: pd.DataFrame, 
        operator: str, 
        output_dir: str,
        dl_xlim: Tuple[float, float] = (0, 500),
        ul_xlim: Tuple[float, float] = (0, 200),
    ):
    """Plot CDF of throughput for each technology for a given operator.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data
        operator (str): Name of the operator
        output_dir (str): Directory to save the plots
    """
    # Create separate plots for downlink and uplink
    for direction in ['downlink', 'uplink']:
        fig, ax = plt.subplots(figsize=(6, 3))
        
        # Initialize stats dictionary
        stats = {}
        
        # Filter data for this direction
        mask = (df[XcalField.APP_TPUT_PROTOCOL] == 'tcp') & \
               (df[XcalField.APP_TPUT_DIRECTION] == direction)
        direction_df = df[mask]
        unique_techs = direction_df[XcalField.ACTUAL_TECH].unique()
        # sort techs by the order of tech_order
        unique_techs = sorted(unique_techs, key=lambda x: tech_order.index(x))
        
        # Plot CDF for each technology
        for tech in unique_techs:
            # Skip NO SERVICE
            if tech.lower() == 'no service':
                continue
            
            # Filter data for this technology
            tech_df = direction_df[direction_df[XcalField.ACTUAL_TECH] == tech]
            if len(tech_df) == 0:
                continue
                
            tput_field = XcalField.TPUT_DL if direction == 'downlink' else XcalField.TPUT_UL
            # Get throughput values and sort them
            tput_values = tech_df[tput_field].sort_values()
            
            # Calculate statistics
            stats[tech] = {
                'median': float(np.median(tput_values)),
                'mean': float(np.mean(tput_values)),
                'min': float(np.min(tput_values)),
                'max': float(np.max(tput_values)),
                'percentile_25': float(np.percentile(tput_values, 25)),
                'percentile_75': float(np.percentile(tput_values, 75)),
                'sample_count': len(tput_values)
            }
            
            # Calculate CDF
            cdf = np.arange(1, len(tput_values) + 1) / len(tput_values)
            
            if tech not in tech_order:
                raise ValueError(f"Tech ({tech}) not in tech_order")
            
            # Plot CDF
            idx = tech_order.index(tech)
            ax.plot(tput_values, cdf, label=tech, color=colors[idx])
        
        ax.set_title(f'Throughput CDF by Technology - {operator} ({direction})')
        ax.set_xlabel('Throughput (Mbps)')
        ax.set_ylabel('CDF')
        if direction == 'downlink':
            ax.set_xlim(dl_xlim)
        else:
            ax.set_xlim(ul_xlim)
        ax.grid(True, which="both", ls="-", alpha=0.2)
        ax.legend(title='Technology', loc='lower right', reverse=True)
        
        plt.tight_layout()
        figure_path = os.path.join(output_dir, f'tput_cdf_by_tech.{operator}.{direction}.png')
        plt.savefig(figure_path, bbox_inches='tight')
        plt.close()
        
        # Save stats to json
        stats_json_path = os.path.join(output_dir, f'tput_cdf_by_tech.{operator}.{direction}.stats.json')
        with open(stats_json_path, 'w') as f:
            json.dump(stats, f, indent=4)
        
        logger.info(f"Saved throughput CDF plot to {figure_path}")
        logger.info(f"Saved throughput statistics to {stats_json_path}")

def plot_tcp_tput_cdf_with_tech_by_area_alaska(
        df: pd.DataFrame, 
        operator: str, 
        output_dir: str, 
        dl_xlim: Tuple[float, float] = None,
        ul_xlim: Tuple[float, float] = None,
    ):
    """Plot CDF of throughput for each technology grouped by area type for Alaska.
    Suburban and rural areas are merged into one category.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data
        operator (str): Name of the operator
        output_dir (str): Directory to save the plots
        dl_xlim (Tuple[float, float]): X-axis limits for downlink plot
        ul_xlim (Tuple[float, float]): X-axis limits for uplink plot
    """
    # Create separate plots for downlink and uplink
    for direction in ['downlink', 'uplink']:
        fig, ax = plt.subplots(figsize=(10, 5))
        
        # Initialize stats dictionary
        stats = {}
        
        # Filter data for TCP and this direction
        mask = (df[XcalField.APP_TPUT_PROTOCOL] == 'tcp') & \
               (df[XcalField.APP_TPUT_DIRECTION] == direction)
        direction_df = df[mask]
        
        # Define area groups for Alaska (merging suburban and rural)
        area_groups = {
            'urban': ['urban'],
            'suburban+rural': ['suburban', 'rural']
        }
        
        # Plot CDF for each area and technology
        for area_label, area_types in area_groups.items():
            stats[area_label] = {}
            
            # Filter data for this area group
            area_df = direction_df[direction_df[XcalField.AREA].isin(area_types)]
            unique_techs = area_df[XcalField.ACTUAL_TECH].unique()
            # Sort techs by the order of tech_order
            unique_techs = sorted(unique_techs, key=lambda x: tech_order.index(x))
            
            for tech in unique_techs:
                # Skip NO SERVICE
                if tech.lower() == 'no service':
                    continue
                
                # Filter data for this technology
                tech_df = area_df[area_df[XcalField.ACTUAL_TECH] == tech]
                if len(tech_df) == 0:
                    continue
                
                tput_field = XcalField.TPUT_DL if direction == 'downlink' else XcalField.TPUT_UL
                tput_values = tech_df[tput_field].sort_values()
                
                # Calculate statistics
                stats[area_label][tech] = {
                    'median': float(np.median(tput_values)),
                    'mean': float(np.mean(tput_values)),
                    'min': float(np.min(tput_values)),
                    'max': float(np.max(tput_values)),
                    'percentile_25': float(np.percentile(tput_values, 25)),
                    'percentile_75': float(np.percentile(tput_values, 75)),
                    'sample_count': len(tput_values)
                }
                
                # Calculate CDF
                cdf = np.arange(1, len(tput_values) + 1) / len(tput_values)
                
                # Get color and linestyle based on area and tech
                idx = tech_order.index(tech)
                color = colors[idx]
                linestyle = '--' if area_label == 'suburban+rural' else '-'
                
                # Plot CDF
                label = f'{area_label}-{tech}'
                ax.plot(tput_values, cdf, label=label, color=color, linestyle=linestyle)
        
        ax.set_title(f'Throughput CDF by Area and Technology - {operator} ({direction})')
        ax.set_xlabel('Throughput (Mbps)')
        ax.set_ylabel('CDF')
        if direction == 'downlink':
            if dl_xlim is not None:
                ax.set_xlim(dl_xlim)
        else:
            if ul_xlim is not None:
                ax.set_xlim(ul_xlim)
        ax.grid(True, which="both", ls="-", alpha=0.2)
        ax.legend(title='Area-Technology', loc='lower right', bbox_to_anchor=(1.35, 0))
        
        plt.tight_layout()
        figure_path = os.path.join(output_dir, f'tput_cdf_by_area_tech.{operator}.{direction}.png')
        plt.savefig(figure_path, bbox_inches='tight')
        plt.close()
        
        # Save stats to json
        stats_json_path = os.path.join(output_dir, f'tput_cdf_by_area_tech.{operator}.{direction}.stats.json')
        with open(stats_json_path, 'w') as f:
            json.dump(stats, f, indent=4)
        
        logger.info(f"Saved throughput CDF plot to {figure_path}")
        logger.info(f"Saved throughput statistics to {stats_json_path}")

def plot_tcp_tput_cdf_with_tech_by_area_hawaii(
        df: pd.DataFrame, 
        operator: str, 
        output_dir: str, 
        dl_xlim: Tuple[float, float] = None,
        ul_xlim: Tuple[float, float] = None,
    ):
    """Plot CDF of throughput for each technology grouped by area type for Hawaii.
    Uses all three area types: urban, suburban, and rural.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data
        operator (str): Name of the operator
        output_dir (str): Directory to save the plots
        dl_xlim (Tuple[float, float]): X-axis limits for downlink plot
        ul_xlim (Tuple[float, float]): X-axis limits for uplink plot
    """
    # Create separate plots for downlink and uplink
    for direction in ['downlink', 'uplink']:
        fig, ax = plt.subplots(figsize=(10, 5))
        
        # Initialize stats dictionary
        stats = {}
        
        # Filter data for TCP and this direction
        mask = (df[XcalField.APP_TPUT_PROTOCOL] == 'tcp') & \
               (df[XcalField.APP_TPUT_DIRECTION] == direction)
        direction_df = df[mask]
        
        # Define area types for Hawaii
        areas = ['urban', 'suburban', 'rural']
        linestyles = ['-', '--', ':']  # Different line styles for each area
        
        # Plot CDF for each area and technology
        for area, linestyle in zip(areas, linestyles):
            stats[area] = {}
            
            # Filter data for this area
            area_df = direction_df[direction_df[XcalField.AREA] == area]
            unique_techs = area_df[XcalField.ACTUAL_TECH].unique()
            # Sort techs by the order of tech_order
            unique_techs = sorted(unique_techs, key=lambda x: tech_order.index(x))
            
            for tech in unique_techs:
                # Skip NO SERVICE
                if tech.lower() == 'no service':
                    continue
                
                # Filter data for this technology
                tech_df = area_df[area_df[XcalField.ACTUAL_TECH] == tech]
                if len(tech_df) == 0:
                    continue
                
                tput_field = XcalField.TPUT_DL if direction == 'downlink' else XcalField.TPUT_UL
                tput_values = tech_df[tput_field].sort_values()
                
                # Calculate statistics
                stats[area][tech] = {
                    'median': float(np.median(tput_values)),
                    'mean': float(np.mean(tput_values)),
                    'min': float(np.min(tput_values)),
                    'max': float(np.max(tput_values)),
                    'percentile_25': float(np.percentile(tput_values, 25)),
                    'percentile_75': float(np.percentile(tput_values, 75)),
                    'sample_count': len(tput_values)
                }
                
                # Calculate CDF
                cdf = np.arange(1, len(tput_values) + 1) / len(tput_values)
                
                # Get color based on tech
                idx = tech_order.index(tech)
                color = colors[idx]
                
                # Plot CDF
                label = f'{area}-{tech}'
                ax.plot(tput_values, cdf, label=label, color=color, linestyle=linestyle)
        
        ax.set_title(f'Throughput CDF by Area and Technology - {operator} ({direction})')
        ax.set_xlabel('Throughput (Mbps)')
        ax.set_ylabel('CDF')
        if direction == 'downlink':
            ax.set_xlim(dl_xlim)
        else:
            ax.set_xlim(ul_xlim)
        ax.grid(True, which="both", ls="-", alpha=0.2)
        ax.legend(title='Area-Technology', loc='lower right', bbox_to_anchor=(1.35, 0))
        
        plt.tight_layout()
        figure_path = os.path.join(output_dir, f'tput_cdf_by_area_tech.{operator}.{direction}.png')
        plt.savefig(figure_path, bbox_inches='tight')
        plt.close()
        
        # Save stats to json
        stats_json_path = os.path.join(output_dir, f'tput_cdf_by_area_tech.{operator}.{direction}.stats.json')
        with open(stats_json_path, 'w') as f:
            json.dump(stats, f, indent=4)
        
        logger.info(f"Saved throughput CDF plot to {figure_path}")
        logger.info(f"Saved throughput statistics to {stats_json_path}")

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
        
        for operator in ['att', 'tmobile', 'verizon']:
            df = operator_dfs[operator]
            # plot_tech_by_protocol_direction(df, operator, output_dir)
            # plot_tech_by_area_for_one_operator(df, operator, location, output_dir)

            if location == 'alaska':
                dl_xlim = (0, 400)
                ul_xlim = (0, 125)
            elif location == 'hawaii':
                dl_xlim = (0, 1000)
                ul_xlim = (0, 175)

            # plot_tcp_tput_cdf_by_tech_for_one_operator(df, operator, output_dir, dl_xlim, ul_xlim)
            
            if location == 'alaska':
                plot_tcp_tput_cdf_with_tech_by_area_alaska(df, operator, output_dir)
            elif location == 'hawaii':
                plot_tcp_tput_cdf_with_tech_by_area_hawaii(df, operator, output_dir)

            logger.info(f'---- Finished processing operator: {operator}')

        logger.info(f'-- Finished processing dataset: {location}')

if __name__ == "__main__":
    main()