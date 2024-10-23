import os
import sys
import logging
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.math_utils import get_cdf
from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.utilities.DatasetHelper import DatasetHelper
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

CURR_DIR = os.path.dirname(__file__)
ALASKA_PING_DIR = os.path.join(ALASKA_ROOT_DIR, 'ping')
HAWAII_PING_DIR = os.path.join(HAWAII_ROOT_DIR, 'ping')
OUTPUT_DIR = os.path.join(CURR_DIR, 'outputs')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def save_plot_statistics(data, title, output_filename):
    logger.info(f"Saving statistics for {title}")
    locations = ['alaska', 'hawaii']
    stats = {}

    for location in locations:
        subset = data[data['location'] == location]
        key = f"{location.capitalize()}"
        stats[key] = {
            'count': len(subset),
            'percentiles': {
                '0': subset['rtt_ms'].min(),
                '25': subset['rtt_ms'].quantile(0.25),
                '50': subset['rtt_ms'].median(),
                '75': subset['rtt_ms'].quantile(0.75),
                '100': subset['rtt_ms'].max()
            },
            'min': subset['rtt_ms'].min(),
            'max': subset['rtt_ms'].max(),
            'median': subset['rtt_ms'].median(),
            'mean': subset['rtt_ms'].mean(),
            'std': subset['rtt_ms'].std(),
        }

    with open(output_filename, 'w') as f:
        json.dump(stats, f, indent=4)
    logger.info(f"Statistics saved as {output_filename}")

def create_cdf_plot(
        data, 
        title, 
        output_filename,
        xlim: tuple[float, float] = None,
      ):
    logger.info(f"Creating CDF plot for {title}")
    locations = ['alaska', 'hawaii']
    colors = ['#1f77b4', '#ff7f0e']  # Blue for Alaska, Orange for Hawaii

    plt.figure(figsize=(5, 4))
    ax = plt.gca()

    for i, location in enumerate(locations):
        subset = data[data['location'] == location]
        logger.info(f"Plotting data for {location}. Data points: {len(subset)}")
        
        # Sort the data and calculate CDF
        sorted_data, yvals = get_cdf(subset['rtt_ms'])
        
        ax.plot(sorted_data, yvals, color=colors[i], label=f"{location.capitalize()}")

    ax.set_title(title)
    ax.set_xlabel('RTT (ms)')
    ax.set_ylabel('CDF')
    ax.set_ylim(0, 1)
    if xlim:
        ax.set_xlim(xlim)
    ax.set_yticks(np.arange(0, 1.25, 0.25))
    ax.grid(True, linestyle='--', alpha=0.7)
    ax.tick_params(axis='both', which='major')

    ax.legend(loc='lower right')

    plt.tight_layout()
    plt.savefig(output_filename)
    logger.info(f"Plot saved as {output_filename}")
    plt.close()

    # Save statistics
    stats_filename = os.path.splitext(output_filename)[0] + '_stats.json'
    save_plot_statistics(data, title, stats_filename)

def save_comparison_stats(data, output_filename):
    logger.info("Saving comparison statistics")
    locations = ['alaska', 'hawaii']
    stats = {}

    for metric in [ 'mean', 'median', 'min', 'max']:
        hi_value = data[data['location'] == 'hawaii']['rtt_ms'].agg(metric)
        al_value = data[data['location'] == 'alaska']['rtt_ms'].agg(metric)
        stats[metric] = f"HI {hi_value:.2f}ms vs AL {al_value:.2f}ms"

    with open(output_filename, 'w') as f:
        f.write("Comparison Statistics:\n\n")
        for metric in ['mean', 'median', 'min', 'max']:
            f.write(f"- {metric.capitalize()}: {stats[metric]}\n")

    logger.info(f"Comparison statistics saved as {output_filename}")

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    logger.info("Starting Starlink Alaska vs Hawaii latency comparison")

    logger.info("Initializing DatasetHelpers")
    al_dataset_helper = DatasetHelper(ALASKA_PING_DIR)
    hawaii_dataset_helper = DatasetHelper(HAWAII_PING_DIR)

    logger.info("Fetching ping data for Alaska")
    alaska_ping_data = al_dataset_helper.get_ping_data(operator='starlink')

    logger.info("Fetching ping data for Hawaii")
    hawaii_ping_data = hawaii_dataset_helper.get_ping_data(operator='starlink')

    logger.info("Combining Alaska and Hawaii data")
    alaska_ping_data['location'] = 'alaska'
    hawaii_ping_data['location'] = 'hawaii'
    combined_data = pd.concat([alaska_ping_data, hawaii_ping_data])

    logger.info("Creating latency plot")
    create_cdf_plot(combined_data, 
                    title='CDF ofStarlink Round-Trip Time', 
                    output_filename=os.path.join(OUTPUT_DIR, 'starlink_latency_cdf_al_vs_hi.png'))
    

    create_cdf_plot(combined_data, 
                    title='CDF of Starlink Round-Trip Time (Zoomed)', 
                    xlim=(0, 200),
                    output_filename=os.path.join(OUTPUT_DIR, 'starlink_latency_cdf_al_vs_hi_zoomed.png'),
                    )

    logger.info("Saving comparison statistics")
    save_comparison_stats(combined_data, os.path.join(OUTPUT_DIR, 'latency_comparison_stats.txt'))

    logger.info("Plotting and statistics generation completed")

if __name__ == '__main__':
    main()
