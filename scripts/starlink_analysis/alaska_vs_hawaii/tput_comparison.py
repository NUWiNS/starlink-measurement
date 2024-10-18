import os
import sys
import logging


sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.math_utils import get_cdf
from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.utilities.DatasetHelper import DatasetHelper
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

CURR_DIR = os.path.dirname(__file__)
ALASKA_TPUT_DIR = os.path.join(ALASKA_ROOT_DIR, 'throughput')
HAWAII_TPUT_DIR = os.path.join(HAWAII_ROOT_DIR, 'throughput')
OUTPUT_DIR = os.path.join(CURR_DIR, 'plots')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_cdf_plot(data, title, output_filename):
    logger.info(f"Creating CDF plot for {title}")
    protocols = ['tcp', 'udp']
    locations = ['alaska', 'hawaii']
    colors = ['#1f77b4', '#ff7f0e']  # Blue for Alaska, Orange for Hawaii
    linestyles = ['-', '--']  # Solid for TCP, dashed for UDP

    plt.figure(figsize=(5, 4))
    ax = plt.gca()

    for i, protocol in enumerate(protocols):
        for j, location in enumerate(locations):
            subset = data[(data['protocol'] == protocol.lower()) & (data['location'] == location)]
            logger.info(f"Plotting {protocol} data for {location}. Data points: {len(subset)}")
            
            # Sort the data and calculate CDF
            sorted_data, yvals = get_cdf(subset['throughput_mbps'])
            
            ax.plot(sorted_data, yvals, color=colors[j], linestyle=linestyles[i],
                    label=f"{location.capitalize()} {protocol.upper()}")

    ax.set_title(title)
    ax.set_xlabel('Throughput (Mbps)')
    ax.set_ylabel('CDF')
    ax.set_ylim(0, 1)
    ax.set_yticks(np.arange(0, 1.25, 0.25))
    ax.grid(True, linestyle='--', alpha=0.7)
    ax.tick_params(axis='both', which='major')

    ax.legend(loc='lower right')

    plt.tight_layout()
    plt.savefig(output_filename)
    logger.info(f"Plot saved as {output_filename}")
    plt.close()

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    logger.info("Starting Starlink Alaska vs Hawaii throughput comparison")

    logger.info("Initializing DatasetHelpers")
    al_dataset_helper = DatasetHelper(ALASKA_TPUT_DIR)
    hawaii_dataset_helper = DatasetHelper(HAWAII_TPUT_DIR)

    logger.info("Fetching throughput data for Alaska")
    alaska_tput_data = al_dataset_helper.get_tput_data(operator='starlink')
    logger.info(f"Alaska data shape: {alaska_tput_data.shape}")

    logger.info("Fetching throughput data for Hawaii")
    hawaii_tput_data = hawaii_dataset_helper.get_tput_data(operator='starlink')
    logger.info(f"Hawaii data shape: {hawaii_tput_data.shape}")

    logger.info("Combining Alaska and Hawaii data")
    alaska_tput_data['location'] = 'alaska'
    hawaii_tput_data['location'] = 'hawaii'
    combined_data = pd.concat([alaska_tput_data, hawaii_tput_data])
    logger.info(f"Combined data shape: {combined_data.shape}")

    logger.info("Creating downlink throughput plot")
    create_cdf_plot(combined_data[combined_data['direction'] == 'downlink'], 
                    title='Downlink Throughput', 
                    output_filename=os.path.join(OUTPUT_DIR, 'starlink_dl_tput_cdf_al_vs_hi.png'))

    logger.info("Creating uplink throughput plot")
    create_cdf_plot(combined_data[combined_data['direction'] == 'uplink'], 
                    title='Uplink Throughput', 
                    output_filename=os.path.join(OUTPUT_DIR, 'starlink_ul_tput_cdf_al_vs_hi.png'))

    logger.info("Plotting completed")
    

if __name__ == '__main__':
    main()