import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.cdf_tput_plotting_utils import get_data_frame_from_all_csv, plot_cdf_of_throughput_with_all_operators, \
    plot_cdf_of_throughput

from scripts.constants import DATASET_DIR, OUTPUT_DIR

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput')
output_dir = os.path.join(OUTPUT_DIR, 'alaska_starlink_trip/plots')

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def get_data_frame_from_all_csv(operator: str, protocol: str, direction: str):
    file_path = os.path.join(base_dir, f'{operator}_{protocol}_{direction}.csv')
    try:
        return pd.read_csv(file_path)
    except Exception:
        return pd.DataFrame()


def plot_tcp_downlink_data(df: pd.DataFrame, output_dir='.'):
    # plot one CDF of throughput for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        title='CDF of TCP Downlink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_tcp_downlink_all.png')
    )


def read_and_plot_throughput_data(protocol: str, direction: str, output_dir: str):
    print(f"Reading and plotting {protocol}_{direction} with all operator data...")
    att_df = get_data_frame_from_all_csv('att', protocol, direction)
    att_df['operator'] = 'att'
    verizon_df = get_data_frame_from_all_csv('verizon', protocol, direction)
    verizon_df['operator'] = 'verizon'
    starlink_df = get_data_frame_from_all_csv('starlink', protocol, direction)
    starlink_df['operator'] = 'starlink'
    tmobile_df = get_data_frame_from_all_csv('tmobile', protocol, direction)
    tmobile_df['operator'] = 'tmobile'
    combined_df = pd.concat([att_df, verizon_df, starlink_df, tmobile_df], ignore_index=True)

    plot_cdf_of_throughput_with_all_operators(
        combined_df,
        title=f'CDF of {protocol.upper()} {direction.capitalize()} Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_{protocol}_{direction}_all.png')
    )
    print('Done!')

def read_and_plot_starlink_throughput_data(output_dir: str):
    data_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/starlink')
    sl_metric_df = pd.read_csv(os.path.join(data_dir, 'starlink_metric.csv'))

    # convert the throughput to Mbps
    tput_df = sl_metric_df['tput_dl_bps'] / 1e6

    plot_cdf_of_throughput(
        tput_df,
        title=f'CDF of Starlink Downlink Throughput',
        output_file_path=os.path.join(output_dir, f'cdf_starlink_metric_downlink_tput.png')
    )
    print('Done!')


def main():
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"Dataset folder does not exist: {base_dir} ")

    # Plot the CDF of throughput
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    read_and_plot_throughput_data('tcp', 'downlink', output_dir)
    print("--------------")
    read_and_plot_throughput_data('tcp', 'uplink', output_dir)
    print("--------------")
    read_and_plot_throughput_data('udp', 'downlink', output_dir)
    print("--------------")
    read_and_plot_throughput_data('udp', 'uplink', output_dir)
    print("--------------")

    read_and_plot_starlink_throughput_data(output_dir)
    print("--------------")


if __name__ == '__main__':
    main()
