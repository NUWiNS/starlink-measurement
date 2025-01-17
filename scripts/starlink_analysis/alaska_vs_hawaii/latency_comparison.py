import os
import sys
import logging
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.math_utils import get_cdf
from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.maine_starlink_trip.configs import ROOT_DIR as MAINE_ROOT_DIR
from scripts.utilities.DatasetHelper import DatasetHelper
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

CURR_DIR = os.path.dirname(__file__)
ALASKA_PING_DIR = os.path.join(ALASKA_ROOT_DIR, 'ping')
HAWAII_PING_DIR = os.path.join(HAWAII_ROOT_DIR, 'ping')
MAINE_PING_DIR = os.path.join(MAINE_ROOT_DIR, 'ping')
OUTPUT_DIR = os.path.join(CURR_DIR, 'outputs')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def save_plot_statistics(data, title, output_filename):
    logger.info(f"Saving statistics for {title}")
    locations = ['alaska', 'hawaii', 'maine']
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
        x_step: float = None,
      ):
    logger.info(f"Creating CDF plot for {title}")
    locations = ['alaska', 'hawaii', 'maine']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']  # Blue for Alaska, Orange for Hawaii, Green for Maine

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

    min_val = np.min(sorted_data)
    max_val = np.max(sorted_data)
    if xlim:
        min_val = xlim[0]
        max_val = xlim[1]   
    ax.set_xlim(min_val, max_val)
    if x_step:
        ax.set_xticks(np.arange(
            round(min_val / x_step) * x_step, 
            round(max_val / x_step) * x_step + 1, 
            x_step
        ))
        
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
    locations = ['alaska', 'hawaii', 'maine']
    stats = {}

    for metric in [ 'mean', 'median', 'min', 'max']:
        hi_value = data[data['location'] == 'hawaii']['rtt_ms'].agg(metric)
        al_value = data[data['location'] == 'alaska']['rtt_ms'].agg(metric)
        me_value = data[data['location'] == 'maine']['rtt_ms'].agg(metric)
        stats[metric] = f"HI {hi_value:.2f}ms vs AL {al_value:.2f}ms vs ME {me_value:.2f}ms"

    with open(output_filename, 'w') as f:
        f.write("Comparison Statistics:\n\n")
        for metric in ['mean', 'median', 'min', 'max']:
            f.write(f"- {metric.capitalize()}: {stats[metric]}\n")

    logger.info(f"Comparison statistics saved as {output_filename}")


def plot_latency_comparison_by_weather(df: pd.DataFrame, output_file_path: str = None):
    logger.info("Starting Starlink Alaska vs Hawaii latency comparison by weather")

    fig, ax = plt.subplots(figsize=(12, 8))
    all_weather = ['sunny', 'cloudy', 'rainy', 'snowy']
    df['weather'] = pd.Categorical(df['weather'], categories=all_weather, ordered=True)
    
    color_map = {
        'alaska': 'blue',
        'hawaii': 'orange',
        'maine': 'green'
    }
    
    locations = ['alaska', 'hawaii']
    line_styles = {
        'sunny': '-',
        'cloudy': '--',
        'rainy': ':',
        'snowy': '-.'
    }

    for weather in all_weather:
        for location in locations:
            weather_loc_df = df[(df['weather'] == weather) & (df['location'] == location)]['rtt_ms']
            if not weather_loc_df.empty:
                xvals, yvals = get_cdf(weather_loc_df)
                ax.plot(
                    xvals,
                    yvals,
                    color=color_map[location],
                    linestyle=line_styles[weather],
                    label=f'{weather.capitalize()} - {location.capitalize()}'
                )

    ax.set_xlabel('RTT (ms)')
    ax.set_ylabel('CDF')
    ax.set_yticks(np.arange(0, 1.25, 0.25))
    ax.legend(loc='lower right')
    ax.grid(True)

    plt.title(f'CDF of RTT (Alaska vs Hawaii, By Weather)')
    plt.tight_layout()

    if output_file_path:
        plt.savefig(output_file_path, bbox_inches='tight')
        # save_tput_by_weather_comparison_stats(df, full_path.replace('.png', '_stats.json'))
        logger.info(f"Saved plot to {output_file_path}")
    else:
        plt.show()
    plt.close()
            

def save_tput_by_weather_comparison_stats(df: pd.DataFrame, output_file_path: str):
    stats = {}
    all_weather = ['sunny', 'cloudy', 'rainy', 'snowy']
    locations = ['alaska', 'hawaii', 'maine']

    for weather in all_weather:
        stats[weather] = {}
        for location in locations:
            location_df = df[df['location'] == location]
            weather_loc_df = location_df[location_df['weather'] == weather]['throughput_mbps']
            if not weather_loc_df.empty:
                stats[weather][location] = {
                    'count': len(weather_loc_df),
                    'percentage': f"{len(weather_loc_df) / len(location_df) * 100:.2f}%",
                    'mean': weather_loc_df.mean(),
                    'median': weather_loc_df.median(),
                    'std': weather_loc_df.std(),
                    'min': weather_loc_df.min(),
                    'max': weather_loc_df.max(),
                    '25th': weather_loc_df.quantile(0.25),
                    '75th': weather_loc_df.quantile(0.75)
                }
            else:
                stats[weather][location] = None

    with open(output_file_path, 'w') as f:
        json.dump(stats, f, indent=4)

    logger.info(f"Saved throughput comparison stats to {output_file_path}")

def get_latency_data_for_alaska_and_hawaii():
    logger.info("Initializing DatasetHelpers")

    al_dataset_helper = DatasetHelper(ALASKA_PING_DIR)
    logger.info("Fetching ping data for Alaska")
    alaska_ping_data = al_dataset_helper.get_ping_data(operator='starlink')

    hawaii_dataset_helper = DatasetHelper(HAWAII_PING_DIR)
    logger.info("Fetching ping data for Hawaii")
    hawaii_ping_data = hawaii_dataset_helper.get_ping_data(operator='starlink')

    maine_dataset_helper = DatasetHelper(MAINE_PING_DIR)
    logger.info("Fetching ping data for Maine")
    maine_ping_data = maine_dataset_helper.get_ping_data(operator='starlink')

    logger.info("Combining Alaska and Hawaii data")
    alaska_ping_data['location'] = 'alaska'
    hawaii_ping_data['location'] = 'hawaii'
    maine_ping_data['location'] = 'maine'
    combined_data = pd.concat([alaska_ping_data, hawaii_ping_data, maine_ping_data])
    return combined_data

def plot_overall_latency_comparison(df: pd.DataFrame, output_file_path: str = None):
    logger.info("Creating latency plot")
    # create_cdf_plot(df, 
    #                 title='CDF ofStarlink Round-Trip Time', 
    #                 output_filename=os.path.join(OUTPUT_DIR, 'starlink_latency_cdf_al_vs_hi.png'))
    

    create_cdf_plot(df, 
                    title='Latency', 
                    xlim=(0, 200),
                    x_step=25,
                    output_filename=os.path.join(OUTPUT_DIR, 'starlink_latency_cdf_al_vs_hi_zoomed.png'),
                    )

    logger.info("Saving comparison statistics")
    save_comparison_stats(df, os.path.join(OUTPUT_DIR, 'latency_comparison_stats.txt'))

    logger.info("Plotting and statistics generation completed")

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    df = get_latency_data_for_alaska_and_hawaii()
    plot_overall_latency_comparison(df, os.path.join(OUTPUT_DIR, 'starlink_latency_cdf_al_vs_hi.png'))
    
    # starlink_latency_by_weather_comparison_fig = os.path.join(OUTPUT_DIR, 'starlink_latency_by_weather_comparison_al_vs_hi.png')
    # plot_latency_comparison_by_weather(df, output_file_path=starlink_latency_by_weather_comparison_fig)

if __name__ == '__main__':
    main()
