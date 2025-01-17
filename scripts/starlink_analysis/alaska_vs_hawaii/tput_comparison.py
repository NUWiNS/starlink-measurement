import os
import sys
import logging
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.logging_utils import create_logger
from scripts.math_utils import get_cdf
from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.maine_starlink_trip.configs import ROOT_DIR as MAINE_ROOT_DIR
from scripts.utilities.DatasetHelper import DatasetHelper
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

CURR_DIR = os.path.dirname(__file__)
ALASKA_TPUT_DIR = os.path.join(ALASKA_ROOT_DIR, 'throughput')
HAWAII_TPUT_DIR = os.path.join(HAWAII_ROOT_DIR, 'throughput')
MAINE_TPUT_DIR = os.path.join(MAINE_ROOT_DIR, 'throughput')
OUTPUT_DIR = os.path.join(CURR_DIR, 'outputs')

# Set up logging
logger = create_logger(__name__, console_output=True)

def save_plot_statistics(data, title, output_filename):
    logger.info(f"Saving statistics for {title}")
    protocols = ['tcp', 'udp']
    locations = ['alaska', 'hawaii', 'maine']
    stats = {}

    for protocol in protocols:
        for location in locations:
            subset = data[(data['protocol'] == protocol.lower()) & (data['location'] == location)]
            key = f"{location.capitalize()} {protocol.upper()}"
            stats[key] = {
                'count': len(subset),
                'percentiles': {
                    '0': subset['throughput_mbps'].min(),
                    '25': subset['throughput_mbps'].quantile(0.25),
                    '50': subset['throughput_mbps'].median(),
                    '75': subset['throughput_mbps'].quantile(0.75),
                    '100': subset['throughput_mbps'].max()
                },
                'min': subset['throughput_mbps'].min(),
                'max': subset['throughput_mbps'].max(),
                'median': subset['throughput_mbps'].median(),
                'mean': subset['throughput_mbps'].mean(),
                'std': subset['throughput_mbps'].std(),
                'zero_tput': {
                    'count': len(subset[subset['throughput_mbps'] == 0]),
                    'percentage': (len(subset[subset['throughput_mbps'] == 0]) / len(subset)) * 100
                },
            }

    with open(output_filename, 'w') as f:
        json.dump(stats, f, indent=4)
    logger.info(f"Statistics saved as {output_filename}")

def create_cdf_plot(data, title, output_filename, x_step: float = None):
    logger.info(f"Creating CDF plot for {title}")
    protocols = ['tcp', 'udp']
    locations = ['alaska', 'hawaii', 'maine']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
    linestyles = ['-', '--', '-.']

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
    min_val = np.min(sorted_data)
    max_val = np.max(sorted_data)
    if x_step:
        min_val = round(min_val / x_step) * x_step
        max_val = round(max_val / x_step) * x_step + 1
    ax.set_xlim(min_val, max_val)
    if x_step:
        ax.set_xticks(np.arange(
            round(min_val / x_step) * x_step, 
            round(max_val / x_step) * x_step + 1, 
            x_step
        ))
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
    protocols = ['tcp', 'udp']
    locations = ['alaska', 'hawaii', 'maine']
    directions = ['downlink', 'uplink']
    stats = {}

    for direction in directions:
        stats[direction] = {}
        for protocol in protocols:
            stats[direction][protocol] = {}
            filtered_data = data[(data['protocol'] == protocol) & (data['direction'] == direction)]
            for metric in ['max', 'mean', 'median']:
                hi_value = filtered_data[filtered_data['location'] == 'hawaii']['throughput_mbps'].agg(metric)
                al_value = filtered_data[filtered_data['location'] == 'alaska']['throughput_mbps'].agg(metric)
                maine_value = filtered_data[filtered_data['location'] == 'maine']['throughput_mbps'].agg(metric)
                stats[direction][protocol][metric] = f"HI {hi_value:.2f}Mbps vs AL {al_value:.2f}Mbps vs ME {maine_value:.2f}Mbps"
            
            # Calculate zero throughput distribution
            hi_zero_tput = (filtered_data[filtered_data['location'] == 'hawaii']['throughput_mbps'] == 0).mean() * 100
            al_zero_tput = (filtered_data[filtered_data['location'] == 'alaska']['throughput_mbps'] == 0).mean() * 100
            maine_zero_tput = (filtered_data[filtered_data['location'] == 'maine']['throughput_mbps'] == 0).mean() * 100
            stats[direction][protocol]['zero_tput'] = f"HI {hi_zero_tput:.2f}% vs AL {al_zero_tput:.2f}% vs ME {maine_zero_tput:.2f}%"

    with open(output_filename, 'w') as f:
        f.write("Comparison Statistics:\n\n")
        for direction in directions:
            f.write(f"{direction.capitalize()}:\n")
            for metric in ['max', 'mean', 'median', 'zero_tput']:
                f.write(f"- {metric.capitalize()}:\n")
                for protocol in protocols:
                    f.write(f"  - {protocol.upper()}: {stats[direction][protocol][metric]}\n")
            f.write("\n")

    logger.info(f"Comparison statistics saved as {output_filename}")

def plot_overall_tput_comparison(df: pd.DataFrame, base_dir: str):
    logger.info("Starting Starlink Alaska vs Hawaii throughput comparison")

    logger.info("Creating downlink throughput plot")
    create_cdf_plot(df[df['direction'] == 'downlink'], 
                    title='Downlink Throughput', 
                    output_filename=os.path.join(base_dir, 'starlink_dl_tput_cdf_al_vs_hi.png'))

    logger.info("Creating uplink throughput plot")
    create_cdf_plot(df[df['direction'] == 'uplink'], 
                    title='Uplink Throughput', 
                    x_step=10,
                    output_filename=os.path.join(base_dir, 'starlink_ul_tput_cdf_al_vs_hi.png'))

    logger.info("Saving comparison statistics")
    save_comparison_stats(df, os.path.join(base_dir, 'comparison_stats.txt'))

    logger.info("Plotting and statistics generation completed")

def get_tput_data_for_alaska_and_hawaii():
    logger.info("Initializing DatasetHelpers")
    al_dataset_helper = DatasetHelper(ALASKA_TPUT_DIR)
    hawaii_dataset_helper = DatasetHelper(HAWAII_TPUT_DIR)
    maine_dataset_helper = DatasetHelper(MAINE_TPUT_DIR)

    logger.info("Fetching throughput data for Alaska")
    alaska_tput_data = al_dataset_helper.get_tput_data(operator='starlink')
    logger.info(f"Alaska data shape: {alaska_tput_data.shape}")

    logger.info("Fetching throughput data for Hawaii")
    hawaii_tput_data = hawaii_dataset_helper.get_tput_data(operator='starlink')
    logger.info(f"Hawaii data shape: {hawaii_tput_data.shape}")

    logger.info("Fetching throughput data for Maine")
    maine_tput_data = maine_dataset_helper.get_tput_data(operator='starlink')
    logger.info(f"Maine data shape: {maine_tput_data.shape}")

    logger.info("Combining Alaska and Hawaii data")
    alaska_tput_data['location'] = 'alaska'
    hawaii_tput_data['location'] = 'hawaii'
    maine_tput_data['location'] = 'maine'

    combined_data = pd.concat([alaska_tput_data, hawaii_tput_data, maine_tput_data])
    logger.info(f"Combined data shape: {combined_data.shape}")
    return combined_data

def plot_tput_comparison_by_weather(df: pd.DataFrame, output_file_path: str = None):
    logger.info("Starting Starlink throughput comparison by weather")

    protocols = ['tcp', 'udp']
    directions = ['downlink', 'uplink']

    for protocol in protocols:
        for direction in directions:
            fig, ax = plt.subplots(figsize=(12, 8))

            all_weather = ['sunny', 'cloudy', 'rainy', 'snowy']
            df['weather'] = pd.Categorical(df['weather'], categories=all_weather, ordered=True)
            
            color_map = {
                'alaska': 'blue',
                'hawaii': 'orange',
                'maine': 'green',
            }
            
            locations = ['alaska', 'hawaii', 'maine']
            line_styles = {
                'sunny': '-',
                'cloudy': '--',
                'rainy': ':',
                'snowy': '-.'
            }

            filtered_df = df[(df['protocol'] == protocol) & (df['direction'] == direction)]

            for weather in all_weather:
                for location in locations:
                    weather_loc_df = filtered_df[(filtered_df['weather'] == weather) & (filtered_df['location'] == location)]['throughput_mbps']
                    if not weather_loc_df.empty:
                        xvals, yvals = get_cdf(weather_loc_df)
                        ax.plot(
                            xvals,
                            yvals,
                            color=color_map[location],
                            linestyle=line_styles[weather],
                            label=f'{weather.capitalize()} - {location.capitalize()}'
                        )

            ax.set_xlabel('Throughput (Mbps)')
            ax.set_ylabel('CDF')
            ax.set_yticks(np.arange(0, 1.25, 0.25))
            ax.legend(loc='lower right')
            ax.grid(True)

            plt.title(f'CDF of {direction.capitalize()} Throughput - {protocol.upper()} (Alaska vs Hawaii, By Weather)')
            plt.tight_layout()

            if output_file_path:
                file_name = f'starlink_tput_by_weather_comparison_al_vs_hi_{protocol}_{direction}.png'
                full_path = os.path.join(os.path.dirname(output_file_path), file_name)
                plt.savefig(full_path, bbox_inches='tight')
                save_tput_by_weather_comparison_stats(filtered_df, full_path.replace('.png', '_stats.json'))
                logger.info(f"Saved plot to {full_path}")
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

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    df = get_tput_data_for_alaska_and_hawaii()

    plot_overall_tput_comparison(df, base_dir=OUTPUT_DIR)

    # tput_by_weather_comparison_fig = os.path.join(OUTPUT_DIR, 'starlink_tput_by_weather_comparison_al_vs_hi.png')
    # plot_tput_comparison_by_weather(df, output_file_path=tput_by_weather_comparison_fig)

if __name__ == '__main__':
    main()
