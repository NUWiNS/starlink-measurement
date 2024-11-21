import os
import sys
from typing import Dict, Tuple
import numpy as np
import matplotlib.pyplot as plt
import json
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.hawaii_starlink_trip.configs import ROOT_DIR, OUTPUT_DIR
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
tput_dir = os.path.join(ROOT_DIR, 'throughput')
output_dir = os.path.join(OUTPUT_DIR, 'plots')

logger = create_logger('tput_by_area', filename=os.path.join(ROOT_DIR, 'tmp', 'tput_by_area.log'))

# Define colors for each operator
operator_colors = {
    'att': '#0088CE',      # AT&T Blue
    'verizon': '#CD040B',  # Verizon Red
    'starlink': '#000000'  # Starlink Black
}

# Define line styles for each area type
area_styles = {
    'urban': {'linestyle': '-', 'label_suffix': '(Urban)'},
    'suburban': {'linestyle': '--', 'label_suffix': '(Suburban)'},
    'rural': {'linestyle': ':', 'label_suffix': '(Rural)'}
}

def read_tput_data_by_area_type(operator: str, protocol: str, direction: str):
    file_path = os.path.join(tput_dir, f'{operator}_{protocol}_{direction}.csv')
    return pd.read_csv(file_path)

def plot_tput_by_area_type(
        dfs: Dict[str, pd.DataFrame],
        dl_xlim: Tuple[float, float] = (0, 400),
        ul_xlim: Tuple[float, float] = (0, 125)
    ):
    """Plot throughput CDF for each operator grouped by area type.
    
    Args:
        dfs: Dictionary mapping operator_protocol_direction to DataFrame
        dl_xlim: X-axis limits for downlink plots
        ul_xlim: X-axis limits for uplink plots
    """
    # Create separate plots for downlink and uplink
    for direction in ['downlink', 'uplink']:
        fig, ax = plt.subplots(figsize=(6, 3))
        
        # Initialize stats dictionary
        stats = {}
        
        # Process each operator's data
        for key, df in dfs.items():
            operator, protocol, dir = key.split('_')
            if dir != direction:
                continue
                
            stats[operator] = {}
            
            # Plot CDF for each area type
            for area in ['urban', 'suburban', 'rural']:
                # Filter data for this area
                area_df = df[df['area'] == area]
                if len(area_df) == 0:
                    continue
                
                # Get throughput values and sort them
                tput_values = area_df['throughput_mbps'].sort_values()
                
                # Calculate statistics
                stats[operator][area] = {
                    'median': float(np.median(tput_values)),
                    'mean': float(np.mean(tput_values)),
                    'min': float(np.min(tput_values)),
                    'max': float(np.max(tput_values)),
                    'percentile_25': float(np.percentile(tput_values, 25)),
                    'percentile_75': float(np.percentile(tput_values, 75)),
                    'percentile_90': float(np.percentile(tput_values, 90)),
                    'sample_count': len(tput_values)
                }
                
                # Calculate CDF
                cdf = np.arange(1, len(tput_values) + 1) / len(tput_values)
                
                # Plot CDF with operator color and area line style
                label = f'{operator.upper()} {area_styles[area]["label_suffix"]}'
                ax.plot(tput_values, cdf, 
                       label=label,
                       color=operator_colors[operator],
                       linestyle=area_styles[area]['linestyle'])
        
        # Set plot attributes
        ax.set_title(f'Throughput CDF by Area Type ({direction})')
        ax.set_xlabel('Throughput (Mbps)')
        ax.set_ylabel('CDF')
        ax.set_yticks(np.arange(0, 1.1, 0.25))
        
        # Set x-axis limits based on direction
        if direction == 'downlink':
            ax.set_xlim(dl_xlim)
        else:
            ax.set_xlim(ul_xlim)
        
        ax.grid(True, which="both", ls="-", alpha=0.2)
        ax.legend(title='Operator (Area)', bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        
        # Save plot
        figure_path = os.path.join(output_dir, f'tput_cdf_by_area.{direction}.png')
        plt.savefig(figure_path, bbox_inches='tight')
        plt.close()
        
        # Save stats
        stats_path = os.path.join(output_dir, f'tput_cdf_by_area.{direction}.stats.json')
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=4)
        
        logger.info(f"Saved {direction} throughput CDF plot to {figure_path}")
        logger.info(f"Saved {direction} statistics to {stats_path}")

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    dfs = {}
    for operator in ['att', 'verizon', 'starlink']:
        for direction in ['downlink', 'uplink']:
            protocol = 'tcp'
            df = read_tput_data_by_area_type(operator, protocol, direction)
            key = f'{operator}_{protocol}_{direction}'
            dfs[key] = df
    plot_tput_by_area_type(dfs)

if __name__ == '__main__':
    main()