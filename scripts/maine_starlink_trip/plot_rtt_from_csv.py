# Plot CDF of throughput
import os
import sys

from scripts.ping_plotting_utils import plot_boxplot_of_rtt

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR, OUTPUT_DIR
import pandas as pd

base_dir = os.path.join(DATASET_DIR, "maine_starlink_trip/ping")
output_dir = os.path.join(OUTPUT_DIR, "maine_starlink_trip/plots")


def get_data_frame_by_operator(operator: str):
    file_path = os.path.join(base_dir, f'{operator}_ping.csv')
    return pd.read_csv(file_path)


def main():
    dataset_dir = os.path.join(base_dir, 'csv')
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir, exist_ok=True)

    att_ping_df = get_data_frame_by_operator('att')
    verizon_ping_df = get_data_frame_by_operator('verizon')
    starlink_ping_df = get_data_frame_by_operator('starlink')
    combined_df = pd.concat([att_ping_df, verizon_ping_df, starlink_ping_df], ignore_index=True)

    # Plot the CDF of throughput
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    print('Plotting boxplot of RTT...')
    plot_boxplot_of_rtt(df=combined_df, output_dir=output_dir)
    print("Plot is saved to: ", output_dir)


if __name__ == '__main__':
    main()
