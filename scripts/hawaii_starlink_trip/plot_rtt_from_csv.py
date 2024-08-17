# Plot CDF of throughput
import os
import sys

from scripts.ping_plotting_utils import plot_boxplot_of_rtt, plot_cdf_of_rtt_with_all_operators, plot_all_cdf_for_rtt

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR, OUTPUT_DIR
import pandas as pd

base_dir = os.path.join(DATASET_DIR, "alaska_starlink_trip/ping")
output_dir = os.path.join(OUTPUT_DIR, "alaska_starlink_trip/plots")


def get_data_frame_by_operator(operator: str):
    file_path = os.path.join(base_dir, f'{operator}_ping.csv')
    return pd.read_csv(file_path)


def main():
    dataset_dir = os.path.join(base_dir, 'csv')
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir, exist_ok=True)

    combined_df = pd.DataFrame()
    # for operator in ['att', 'verizon', 'starlink', 'tmobile']:
    for operator in ['att', 'verizon', 'starlink']:
        operator_df = get_data_frame_by_operator(operator)
        combined_df = pd.concat([combined_df, operator_df], ignore_index=True)

    # Plot the CDF of throughput
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    print('Plotting boxplot of RTT...')
    plot_boxplot_of_rtt(df=combined_df, output_dir=output_dir, yscale='linear')
    print("Plot is saved to: ", output_dir)

    print('Plotting CDF of RTT...')
    plot_all_cdf_for_rtt(df=combined_df, output_dir=output_dir, xscale='linear')
    plot_all_cdf_for_rtt(df=combined_df, output_dir=output_dir, xscale='log')
    print("Plot is saved to: ", output_dir)


if __name__ == '__main__':
    main()
