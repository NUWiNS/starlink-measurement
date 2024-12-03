import json
import os
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import sys
from typing import Callable, Dict, List, Tuple

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
    # Sort operators by the order of Verizon, T-Mobile, ATT
    operators = sorted(list(dfs.keys()), key=lambda x: operator_conf[x]['order'])
    
    # Dynamically adjust figure width based on number of operators while maintaining bar width consistency
    bar_width = 0.35  # Desired bar width
    spacing_factor = 3  # Space between bars relative to bar width
    total_width_needed = len(operators) * bar_width * spacing_factor
    fig_width = max(6, total_width_needed + 2)  # Add padding for margins
    
    fig, ax = plt.subplots(figsize=(fig_width, 4))
    
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
    
    ax.set_title(title)
    ax.set_xlabel('Operator')
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

def plot_latency_cdf_by_tech_for_one_operator(
        df: pd.DataFrame, 
        operator: str, 
        output_dir: str,
    ):
    """Plot CDF of latency (RTT) for each technology for a given operator.
    
    Args:
        df (pd.DataFrame): DataFrame containing the latency data with fields 'rtt_ms' and 'actual_tech'
        operator (str): Name of the operator
        output_dir (str): Directory to save the plots
    """
    fig, ax = plt.subplots(figsize=(6, 3))
    
    # Initialize stats dictionary
    stats = {}
    
    # Get unique technologies and sort them according to tech_order
    unique_techs = df[XcalField.ACTUAL_TECH].unique()
    unique_techs = sorted(unique_techs, key=lambda x: tech_order.index(x))

    x_lim_min = np.inf
    x_lim_max = -np.inf
    
    # Plot CDF for each technology
    for tech in unique_techs:
        # Skip NO SERVICE
        if tech.lower() == 'no service':
            continue
        
        # Filter data for this technology
        tech_df = df[df[XcalField.ACTUAL_TECH] == tech]
        if len(tech_df) == 0:
            continue
            
        # Get RTT values and sort them
        rtt_values = tech_df['rtt_ms'].sort_values()
        
        # Calculate statistics
        stats[tech] = {
            'median': float(np.median(rtt_values)),
            'mean': float(np.mean(rtt_values)),
            'min': float(np.min(rtt_values)),
            'max': float(np.max(rtt_values)),
            'percentile_25': float(np.percentile(rtt_values, 25)),
            'percentile_75': float(np.percentile(rtt_values, 75)),
            'percentile_90': float(np.percentile(rtt_values, 90)),
            'sample_count': len(rtt_values)
        }

        x_lim_min = min(x_lim_min, stats[tech]['min'])
        x_lim_max = max(x_lim_max, stats[tech]['percentile_90'])

        # Calculate CDF
        cdf = np.arange(1, len(rtt_values) + 1) / len(rtt_values)
        
        # Get color based on tech
        idx = tech_order.index(tech)
        
        # Plot CDF
        ax.plot(rtt_values, cdf, label=tech, color=colors[idx])
    
    # Add small padding to x-axis limits to prevent occlusion
    x_padding = (x_lim_max - x_lim_min) * 0.02  # 2% padding
    ax.set_xlim(x_lim_min - x_padding, x_lim_max + x_padding)
    
    ax.set_title(f'Latency CDF by Technology - {operator}')
    ax.set_xlabel('RTT (ms)')
    ax.set_ylabel('CDF')
    ax.set_yticks(np.arange(0, 1.1, 0.25))
    ax.grid(True, which="both", ls="-", alpha=0.2)
    ax.legend(title='Technology', loc='lower right', reverse=True)
    
    plt.tight_layout()
    figure_path = os.path.join(output_dir, f'latency_cdf_by_tech.{operator}.png')
    plt.savefig(figure_path, bbox_inches='tight')
    plt.close()
    
    # Save stats to json
    stats_json_path = os.path.join(output_dir, f'latency_cdf_by_tech.{operator}.stats.json')
    with open(stats_json_path, 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.info(f"Saved latency CDF plot to {figure_path}")
    logger.info(f"Saved latency statistics to {stats_json_path}")

location_conf = {
    'alaska': {
        'label': 'AK',
        'root_dir': AL_DATASET_DIR,
        'operators': ['verizon', 'att'],
        'order': 1
    },
    'hawaii': {
        'label': 'HI',
        'root_dir': HI_DATASET_DIR,
        'operators': ['verizon', 'att', 'tmobile'],
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

def read_xcal_tput_data(root_dir: str, operator: str, protocol: str = None, direction: str = None):
    input_csv_path = os.path.join(root_dir, 'xcal', f'{operator}_xcal_smart_tput.csv')
    df = pd.read_csv(input_csv_path)
    if protocol:
        df = df[df[XcalField.APP_TPUT_PROTOCOL] == protocol]
    if direction:
        df = df[df[XcalField.APP_TPUT_DIRECTION] == direction]
    return df

def aggregate_xcal_tput_data(
        root_dir: str, 
        operators: List[str], 
        protocol: str = None, 
        direction: str = None, 
    ):
    data = pd.DataFrame()
    for operator in operators:
        df = read_xcal_tput_data(
            root_dir=root_dir, 
            operator=operator, 
            protocol=protocol, 
            direction=direction, 
        )
        df['operator'] = operator
        data = pd.concat([data, df])
    return data

def aggregate_xcal_tput_data_by_location(
        locations: List[str], 
        protocol: str = None, 
        direction: str = None, 

    ):
    data = pd.DataFrame()
    for location in locations:
        conf = location_conf[location]
        df = aggregate_xcal_tput_data(
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
        df['operator'] = operator
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
        df['location'] = location
        data = pd.concat([data, df])
    return data

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
        data_sample_threshold: int = 240, # one round data
    ):
    n_locations = len(loc_conf)
    n_metrics = len(metrics)
    
    # Increased figure width to accommodate legends
    fig, axes = plt.subplots(n_locations, n_metrics, figsize=(4.8*n_metrics, 3*n_locations))
    if n_locations == 1:
        axes = axes.reshape(1, -1)
    if n_metrics == 1:
        axes = axes.reshape(-1, 1)
    
    # Adjusted right margin to leave more space for legends
    plt.subplots_adjust(wspace=0.2, hspace=0.2, right=0.85)  # Reduced right margin from 0.8 to 0.75
    
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
            for op_tech in plot_df[['operator', XcalField.ACTUAL_TECH]].drop_duplicates().itertuples():
                op_data = plot_df[
                    (plot_df['operator'] == op_tech.operator) & 
                    (plot_df[XcalField.ACTUAL_TECH] == op_tech.actual_tech)
                ][data_field]
                
                if percentile < 100:
                    max_val = max(max_val, np.percentile(op_data, percentile))
                else:
                    max_val = max(max_val, np.max(op_data))
                min_val = min(min_val, np.min(op_data))
        
        padding = (max_val - min_val) * 0.05
        col_max_values.append(max_val + padding)
        col_min_values.append(min_val - padding)
    
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
            percentile = percentile_filter.get(metric_name, 100) if percentile_filter else 100
            
            # Plot each operator+tech combination
            for op_key, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
                for t_key, t_conf in sorted(tech_conf.items(), key=lambda x: x[1]['order']):
                    mask = (plot_df['operator'] == op_key) & (plot_df[XcalField.ACTUAL_TECH] == t_key)
                    operator_data = plot_df[mask][data_field]

                    # NOTE: IF DATA SAMPLE IS LESS THAN THRESHOLD, DO NOT PLOT IT!
                    if len(operator_data) < data_sample_threshold:
                        logger.warn(f'{location}-{op_key}-{t_key} data sample is less than required threshold, skip plotting: {len(operator_data)} < {data_sample_threshold}')
                        continue
                    
                    if percentile < 100:
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
            if max_xlim:
                ax.set_xlim(col_min_values[col], max_xlim)
            else:
                ax.set_xlim(col_min_values[col], col_max_values[col])
        
            
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
                        'percentage_in_area': float(len(tech_data) / operator_total * 100 if operator_total > 0 else 0)
                    }
                
                stats[metric_name][location][operator] = {
                    'tech_breakdown': tech_breakdown,
                    'total_samples': operator_total
                }
    
    # Save to JSON file
    with open(filepath, 'w') as f:
        json.dump(stats, f, indent=4)

def plot_tput_tech_breakdown_by_area_by_operator(
        locations: List[str], 
        protocol: str, 
        direction: str,
        max_xlim: float = None,
        data_sample_threshold: int = 240, # 1 rounds data (~2min)
    ):
    tput_df = aggregate_xcal_tput_data_by_location(
        locations=locations,
        protocol=protocol,
        direction=direction
    )

    plot_data = {
        'urban': tput_df[
            (tput_df[XcalField.AREA] == 'urban') | (tput_df[XcalField.AREA] == 'suburban')
        ],
        'rural': tput_df[tput_df[XcalField.AREA] == 'rural'],
    }
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
    plot_metric_grid(
        data=plot_data, 
        metrics=metrics, 
        loc_conf=location_conf, 
        operator_conf=operator_conf, 
        tech_conf=tech_conf, 
        output_filepath=output_filepath,
        max_xlim=max_xlim,
        data_sample_threshold=data_sample_threshold,
    )
    save_stats_to_json(
        data=plot_data,
        metrics=metrics, 
        filepath=output_filepath.replace('.png', '.json'),
    )

def plot_latency_tech_breakdown_by_area_by_operator(
        locations: List[str], 
        protocol: str = 'icmp', 
        max_xlim: float = None,
        data_sample_threshold: int = 240, # 1 rounds data (~2min)
    ):

    latency_df = aggregate_latency_data_by_location(
        locations=locations,
        protocol=protocol,
    )

    plot_data = {
        'urban': latency_df[
            (latency_df[XcalField.AREA] == 'urban') | (latency_df[XcalField.AREA] == 'suburban')
        ],
        'rural': latency_df[latency_df[XcalField.AREA] == 'rural'],
    }

    # Set output filepath for the plot
    output_filepath = os.path.join(current_dir, 'outputs', f'latency_tech_breakdown_by_area.{protocol}.png')

    latency_field = 'rtt_ms'
    
    # Define metrics for plotting - similar structure but with latency labels
    metrics = [
        ('urban', 'Urban', 'Round-Trip Time (ms)', latency_field),
        ('rural', 'Rural', 'Round-Trip Time (ms)', latency_field),
    ]

    # Generate the plot using the same plotting function
    plot_metric_grid(
        data=plot_data, 
        metrics=metrics, 
        loc_conf=location_conf, 
        operator_conf=operator_conf, 
        tech_conf=tech_conf, 
        output_filepath=output_filepath,
        max_xlim=max_xlim,
        data_sample_threshold=data_sample_threshold,
    )

    # Save statistics to JSON file
    save_stats_to_json(
        data=plot_data,
        metrics=metrics, 
        filepath=output_filepath.replace('.png', '.json'),
    )

def main():
    # plot_tput_tech_breakdown_by_area_by_operator(
    #     locations=['alaska', 'hawaii'],
    #     protocol='tcp',
    #     direction='downlink',
    #     max_xlim=400,
    #     data_sample_threshold=480, # 2 rounds data (~4min)
    # )

    # plot_tput_tech_breakdown_by_area_by_operator(
    #     locations=['alaska', 'hawaii'],
    #     protocol='tcp',
    #     direction='uplink',
    #     max_xlim=150,
    #     data_sample_threshold=480, # 2 rounds data (~4min)
    # )

    # plot_latency_tech_breakdown_by_area_by_operator(
    #     locations=['alaska', 'hawaii'],
    #     protocol='icmp',
    #     # max_xlim=100,
    #     # data_sample_threshold=480, # 2 rounds data (~4min)
    # )

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
        plot_tech_with_all_operators(
            dfs=operator_dfs, 
            df_mask=lambda df: (df[XcalField.AREA] == 'urban') | (df[XcalField.AREA] == 'suburban'), 
            output_dir=output_dir, 
            title=f'Technology Distribution ({loc_label}-Urban)',
            fig_name=f'tech_breakdown_by_urban_area',
        )

        # Plot location-wide tech breakdown by area
        plot_tech_with_all_operators(
            dfs=operator_dfs, 
            df_mask=lambda df: df[XcalField.AREA] == 'rural', 
            output_dir=output_dir, 
            title=f'Technology Distribution ({loc_label}-Rural)',
            fig_name=f'tech_breakdown_by_rural_area',
        )


    # Throughput relevant plots
    # for location in ['alaska', 'hawaii']:
    #     logger.info(f'-- Processing dataset: {location}')
    #     base_dir = location_dir[location]
    #     output_dir = os.path.join(current_dir, 'outputs', location)
    #     if not os.path.exists(output_dir):
    #         os.makedirs(output_dir)

    #     # Store dataframes for all operators
    #     operator_dfs = {}

    #     for operator in ['att', 'tmobile', 'verizon']:
    #         logger.info(f'---- Processing operator: {operator}')
    #         input_csv_path = os.path.join(base_dir, 'xcal', f'{operator}_xcal_smart_tput.csv')
    #         df = pd.read_csv(input_csv_path)
    #         operator_dfs[operator] = df
        
    #     for operator in ['att', 'tmobile', 'verizon']:
    #         df = operator_dfs[operator]
    #         # plot_tech_by_protocol_direction(df, operator, output_dir)
    #         # plot_tech_by_area_for_one_operator(df, operator, location, output_dir)

    #         if location == 'alaska':
    #             dl_xlim = (0, 400)
    #             ul_xlim = (0, 125)
    #         elif location == 'hawaii':
    #             dl_xlim = (0, 1000)
    #             ul_xlim = (0, 175)

    #         # plot_tcp_tput_cdf_by_tech_for_one_operator(df, operator, output_dir, dl_xlim, ul_xlim)
            
    #         if location == 'alaska':
    #             plot_tcp_tput_cdf_with_tech_by_area_alaska(df, operator, output_dir)
    #         elif location == 'hawaii':
    #             plot_tcp_tput_cdf_with_tech_by_area_hawaii(df, operator, output_dir)

    #         logger.info(f'---- Finished processing operator: {operator}')

    #     logger.info(f'-- Finished processing dataset: {location}')

    # Latency relevant plots
    # for location in ['alaska', 'hawaii']:
    #     logger.info(f'-- Processing dataset: {location}')
    #     base_dir = location_dir[location]
    #     output_dir = os.path.join(current_dir, 'outputs', location)
    #     if not os.path.exists(output_dir):
    #         os.makedirs(output_dir)

    #     for operator in ['att', 'tmobile', 'verizon']:
    #         logger.info(f'---- Processing operator: {operator}')
    #         input_csv_path = os.path.join(base_dir, 'ping', f'{operator}_ping.csv')
    #         df = pd.read_csv(input_csv_path)
    #         plot_latency_cdf_by_tech_for_one_operator(df, operator, output_dir)
    #         logger.info(f'---- Finished processing operator: {operator}')

    #     logger.info(f'-- Finished processing dataset: {location}')

if __name__ == "__main__":
    main()