import sys
import os

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.logging_utils import create_logger
from scripts.cell_leo_in_remote_us.common import location_conf, operator_conf, cellular_location_conf
from scripts.alaska_starlink_trip.configs import ROOT_DIR as AK_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HI_ROOT_DIR

current_dir = os.path.abspath(os.path.dirname(__file__))
output_dir = os.path.join(current_dir, 'outputs')
logger = create_logger('zero_tput_diff', filename=os.path.join(output_dir, 'zero_tput_diff.log'))

def get_zero_tput_diff_path(base_dir: str, operator_a: str, operator_b: str, trace_type: str):
    return os.path.join(base_dir, f'zero_tput_diff.{operator_a}_{operator_b}.{trace_type}.csv')

def plot_cdf_of_zero_tput_diff(
        df: pd.DataFrame, 
        operator_conf: dict,
        title: str, 
        output_filename: str
    ):
    plt.figure(figsize=(6, 4))
    
    # Group by based_operator
    groups = df.groupby('based')
    
    for operator, group in groups:
        # Sort time differences for CDF
        sorted_diffs = np.sort(group['time_diff_sec'].values)
        sample_size = len(sorted_diffs)
        # Calculate cumulative probabilities
        cumulative_prob = np.arange(1, len(sorted_diffs) + 1) / len(sorted_diffs)
        
        # Plot CDF line with color from operator_conf
        plt.plot(sorted_diffs, cumulative_prob, 
                color=operator_conf[operator]['color'],
                label=f'{operator} based (samples: {sample_size})')
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.title(title)
    plt.xlabel('Time Difference (sec)')
    plt.ylabel('CDF')
    plt.legend(loc='lower right')
    
    # Save the figure
    plt.savefig(output_filename, bbox_inches='tight', dpi=300)
    plt.close()

def plot_zero_tput_diff_distribution(
        location: str, 
        location_conf: dict,
        operator_conf: dict,
        trace_type: str,
        operator_a: str, 
        operator_b: str,
        output_dir: str
      ):
    loc_conf = location_conf[location]
    mptcp_dir = os.path.join(loc_conf['root_dir'], 'mptcp')
    zero_tput_diff_path = get_zero_tput_diff_path(mptcp_dir, operator_a, operator_b, trace_type)
    df = pd.read_csv(zero_tput_diff_path)

    title = f'zero-tput diff: {location} {trace_type} ({operator_a}-{operator_b})'
    output_filename = os.path.join(output_dir, f'zero_tput_diff.{location}.{operator_a}_{operator_b}.{trace_type}.pdf')
    plot_cdf_of_zero_tput_diff(
        df=df, 
        operator_conf=operator_conf,
        title=title,
        output_filename=output_filename
    )
    logger.info(f"Saved figure [{title}] to {output_filename}")


def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Plot operator combinations
    for location in ['alaska', 'hawaii']:
        for trace_type in ['tcp_downlink', 'tcp_uplink']:
            cell_operators = cellular_location_conf[location]['operators']
            operators = ['starlink'] + cell_operators
            for i, operator_a in enumerate(operators):
                for operator_b in operators[i+1:]:  # Start from i+1 to avoid duplicates
                    plot_zero_tput_diff_distribution(
                        location=location, 
                        location_conf=location_conf,
                        operator_conf=operator_conf,
                        trace_type=trace_type,
                        operator_a=operator_a, 
                        operator_b=operator_b,
                        output_dir=output_dir
                    )

if __name__ == "__main__":
    main()