# Plot CDF of throughput
import json
import os
import sys
from typing import Dict

from matplotlib import pyplot as plt
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from scripts.hawaii_starlink_trip.configs import ROOT_DIR
from scripts.ping_plotting_utils import plot_boxplot_of_rtt, plot_cdf_of_rtt_with_all_operators, plot_all_cdf_for_rtt


from scripts.constants import OUTPUT_DIR
import pandas as pd

base_dir = os.path.join(ROOT_DIR, 'ping')
output_dir = os.path.join(OUTPUT_DIR, "hawaii_starlink_trip/plots")


# Define colors for each operator
operator_colors = {
    'att': '#0088CE',      # AT&T Blue
    'verizon': '#CD040B',  # Verizon Red
    'starlink': '#000000',  # Starlink Black
    'tmobile': 'deeppink'    # T-Mobile Pink
}

# Define line styles for each area type
area_styles = {
    'urban': {'linestyle': '-', 'label_suffix': '(Urban)'},
    'suburban': {'linestyle': '--', 'label_suffix': '(Suburban)'},
    'rural': {'linestyle': ':', 'label_suffix': '(Rural)'}
}

def get_data_frame_by_operator(operator: str):
    file_path = os.path.join(base_dir, f'{operator}_ping.csv')
    return pd.read_csv(file_path)


def plot_rtt_by_area_type(
    dfs: Dict[str, pd.DataFrame],
    max_percentile: float = 100,
    x_scale: str = 'linear'
):
    """Plot RTT CDF for all operators grouped by area type.
    
    Args:
        dfs: Dictionary mapping operator names to their DataFrames
        max_percentile: Only plot values up to this percentile (default: 90)
    """
    fig, ax = plt.subplots(figsize=(6, 3))
    
    # Initialize stats dictionary
    stats = {}

    area_conf = {
        'urban': {
            'label': 'Urban',
            'order': 1,
        },
        'rural': {
            'label': 'Rural',
            'order': 2,
        }
    }
    
    # Process each operator's data
    for operator, df in dfs.items():
        stats[operator] = {}

        # merge urban and suburban
        total_count = len(df)
        data = {
            'urban': df[(df['area'] == 'urban') | (df['area'] == 'suburban')],
            'rural': df[df['area'] == 'rural'],
        }
        
        # Plot CDF for each area type
        for area in sorted(area_conf.keys(), key=lambda x: area_conf[x]['order']):
            # Filter data for this area
            area_df = data[area]
            if len(area_df) == 0:
                continue
            
            # Get RTT values and sort them
            rtt_values = area_df['rtt_ms'].sort_values()
            
            # Calculate percentile threshold
            max_value = np.percentile(rtt_values, max_percentile)
            
            # Filter values up to the specified percentile
            rtt_values = rtt_values[rtt_values <= max_value]
            
            # Calculate statistics
            stats[operator][area] = {
                'median': float(np.median(rtt_values)),
                'mean': float(np.mean(rtt_values)),
                'min': float(np.min(rtt_values)),
                'max': float(max_value),  # Use the percentile value as max
                'percentile_5': float(np.percentile(rtt_values, 5)),
                'percentile_25': float(np.percentile(rtt_values, 25)),
                'percentile_75': float(np.percentile(rtt_values, 75)),
                'percentile_95': float(np.percentile(rtt_values, 95)),
                'sample_count': len(rtt_values),
                'percent_of_total': (len(rtt_values) / total_count) * 100,
            }
            
            # Calculate CDF
            cdf = np.arange(1, len(rtt_values) + 1) / len(area_df)  # Use original length for correct CDF
            
            # Plot CDF with operator color and area line style
            label = f'{operator.upper()} - {area_conf[area]["label"]}'
            ax.plot(rtt_values, cdf, 
                   label=label,
                   color=operator_colors[operator],
                   linestyle=area_styles[area]['linestyle'])
    
    # Set plot attributes
    ax.set_title(f'RTT CDF by Area Type (up to {max_percentile}th percentile)')
    ax.set_xlabel('RTT (ms)')
    ax.set_ylabel('CDF')
    ax.set_yticks(np.arange(0, 1.1, 0.25))
    
    # Set x-axis to log scale since RTT values can vary widely
    ax.set_xscale(x_scale)
    
    ax.grid(True, which="both", ls="-", alpha=0.2)
    ax.legend(title='Operator (Area)', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    plt.tight_layout()
    
    # Save plot
    figure_path = os.path.join(output_dir, f'rtt_cdf_by_area_p{int(max_percentile)}.png')
    plt.savefig(figure_path, bbox_inches='tight')
    plt.close()
    
    # Save stats
    stats_path = os.path.join(output_dir, f'rtt_cdf_by_area_p{int(max_percentile)}.stats.json')
    with open(stats_path, 'w') as f:
        json.dump(stats, f, indent=4)


def main():
    dataset_dir = os.path.join(base_dir, 'csv')
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir, exist_ok=True)

    # Create dictionary of dataframes
    dfs = {}
    for operator in ['att', 'verizon', 'tmobile', 'starlink']:
        dfs[operator] = get_data_frame_by_operator(operator)

    # Create combined dataframe for other plots
    combined_df = pd.concat(dfs.values(), ignore_index=True)

    # Plot the CDF of throughput
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    print('Plotting boxplot of RTT...')
    plot_boxplot_of_rtt(df=combined_df, output_dir=output_dir, yscale='linear')
    print("Plot is saved to: ", output_dir)

    print('Plotting CDF of RTT...')
    plot_all_cdf_for_rtt(df=combined_df, output_dir=output_dir, xscale='linear')
    plot_all_cdf_for_rtt(df=combined_df, output_dir=output_dir, xscale='log')

    plot_rtt_by_area_type(dfs, max_percentile=100, x_scale='log')
    plot_rtt_by_area_type(dfs, max_percentile=95, x_scale='linear')
    print("Plot is saved to: ", output_dir)


if __name__ == '__main__':
    main()
