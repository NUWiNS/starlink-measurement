import os
import sys

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from scripts.logging_utils import create_logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.cdf_tput_plotting_utils import get_data_frame_from_all_csv, plot_cdf_of_throughput_with_all_operators, \
    plot_cdf_of_throughput, plot_cdf_of_starlink_throughput_by_weather

from scripts.constants import DATASET_DIR, OUTPUT_DIR

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput')
tput_cubic_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput_cubic')
tmp_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/tmp')
output_dir = os.path.join(OUTPUT_DIR, 'alaska_starlink_trip/plots')

logger = create_logger('plot_cdf_throughput', filename=os.path.join(tmp_dir, 'plot_cdf_throughput.log'))

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def get_data_frame_from_all_csv(operator: str, protocol: str, direction: str, base_dir: str = base_dir):
    csv_filename = f'{operator}_{protocol}_{direction}.csv'
    file_path = os.path.join(base_dir, csv_filename)
    df = pd.read_csv(file_path)
    logger.info(f'{csv_filename} count: {df.count()}')
    return df


def plot_tcp_downlink_data(df: pd.DataFrame, output_dir='.'):
    # plot one CDF of throughput_cubic for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        title='CDF of TCP Downlink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_tcp_downlink_all.png')
    )


def plot_cdf_tput_tcp_vs_udp_for_starlink_and_cellular(tcp_dl_df: pd.DataFrame, udp_dl_df: pd.DataFrame,
                                                       direction: str = 'downlink',
                                                       output_dir='.'):
    config = {
        'legends': ['Starlink-TCP', 'Cellular-TCP', 'Starlink-UDP', 'Cellular-UDP'],
        'filename': f'tcp_vs_udp_{direction}.png'
    }
    all_throughputs = []

    cmap20 = plt.cm.tab20

    starlink_tcp = tcp_dl_df[tcp_dl_df['operator'] == 'starlink']['throughput_mbps']
    cellular_tcp = tcp_dl_df[tcp_dl_df['operator'] != 'starlink']['throughput_mbps']

    starlink_udp = udp_dl_df[udp_dl_df['operator'] == 'starlink']['throughput_mbps']
    cellular_udp = udp_dl_df[udp_dl_df['operator'] != 'starlink']['throughput_mbps']

    logger.info('Starlink TCP: %s', starlink_tcp.describe())
    logger.info('Cellular TCP: %s', cellular_tcp.describe())
    logger.info('Starlink UDP: %s', starlink_udp.describe())
    logger.info('Cellular UDP: %s', cellular_udp.describe())

    all_throughputs.extend([starlink_tcp, cellular_tcp, starlink_udp, cellular_udp])
    colors = [cmap20(0), cmap20(4), cmap20(0), cmap20(4)]
    linestyles = ['--', '--', '-', '-']

    fig, ax = plt.subplots(figsize=(6.4, 4.8))

    for idx, data in enumerate(all_throughputs):
        sorted_data = np.sort(data)
        count, bins_count = np.histogram(sorted_data, bins=np.unique(sorted_data).shape[0])
        cdf = np.cumsum(count) / len(sorted_data)
        plt.plot(bins_count[1:], cdf, label=config['legends'][idx], color=colors[idx],
                 linestyle=linestyles[idx], linewidth=4)

    fzsize = 22
    ax.tick_params(axis='y', labelsize=fzsize)
    ax.tick_params(axis='x', labelsize=fzsize)
    ax.set_xlabel('Throughput (Mbps)', fontsize=fzsize)
    ax.set_ylabel('CDF', fontsize=fzsize)
    ax.legend(prop={'size': 20}, loc='lower right')
    if direction == 'uplink':
        plt.xlim(0, 150)
        ax.set_xticks(range(0, 151, 25))
        ax.set_xticklabels(list(map(lambda x: str(x), range(0, 151, 25))))
    else:
        plt.xlim(0, 600)
        ax.set_xticks([0, 100, 200, 300, 400, 500, 600])
        ax.set_xticklabels(['0', '100', '200', '300', '400', '500', '600'])
    plt.ylim(0, 1.02)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, config['filename']))


def plot_cdf_tcp_tput_with_cubic_vs_bbr(
        cubic_df: pd.DataFrame,
        bbr_df: pd.DataFrame,
        protocol: str,
        direction: str = 'downlink',
        output_dir='.'
):
    config = {
        'legends': ['Starlink-CUBIC', 'Cellular-CUBIC', 'Starlink-BBR', 'Cellular-BBR'],
        'filename': f'cubic_vs_bbr_{protocol}_{direction}.png'
    }
    all_throughputs = []

    cmap20 = plt.cm.tab20

    # Compare Urban performance only
    starlink_cubic = cubic_df[
        cubic_df['operator'] == 'starlink' & cubic_df['area'] == 'urban'
    ]['throughput_mbps']
    cellular_cubic = cubic_df[
        cubic_df['operator'] != 'starlink' & cubic_df['area'] == 'urban'
    ]['throughput_mbps']

    starlink_bbr = bbr_df[
        bbr_df['operator'] == 'starlink' & bbr_df['area'] == 'urban'
    ]['throughput_mbps']
    cellular_bbr = bbr_df[
        bbr_df['operator'] != 'starlink' & bbr_df['area'] == 'urban'
    ]['throughput_mbps']

    logger.info('Starlink CUBIC: %s', starlink_cubic.describe())
    logger.info('Cellular CUBIC: %s', cellular_cubic.describe())
    logger.info('Starlink BBR: %s', starlink_bbr.describe())
    logger.info('Cellular BBR: %s', cellular_bbr.describe())

    all_throughputs.extend([starlink_cubic, cellular_cubic, starlink_bbr, cellular_bbr])
    colors = [cmap20(0), cmap20(4), cmap20(0), cmap20(4)]
    linestyles = ['--', '--', '-', '-']

    fig, ax = plt.subplots(figsize=(6.4, 4.8))

    for idx, data in enumerate(all_throughputs):
        sorted_data = np.sort(data)
        count, bins_count = np.histogram(sorted_data, bins=np.unique(sorted_data).shape[0])
        cdf = np.cumsum(count) / len(sorted_data)
        plt.plot(bins_count[1:], cdf, label=config['legends'][idx], color=colors[idx],
                 linestyle=linestyles[idx], linewidth=4)

    fzsize = 22
    ax.tick_params(axis='y', labelsize=fzsize)
    ax.tick_params(axis='x', labelsize=fzsize)
    ax.set_xlabel('Throughput (Mbps)', fontsize=fzsize)
    ax.set_ylabel('CDF', fontsize=fzsize)
    ax.legend(prop={'size': 20}, loc='lower right')
    if direction == 'uplink':
        plt.xlim(0, 150)
        ax.set_xticks(range(0, 151, 25))
        ax.set_xticklabels(list(map(lambda x: str(x), range(0, 151, 25))))
    else:
        plt.xlim(0, 400)
        ax.set_xticks(range(0, 401, 50))
        ax.set_xticklabels(list(map(lambda x: str(x), range(0, 401, 50))))
    plt.ylim(0, 1.02)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, config['filename']))


def read_and_plot_throughput_data(
        protocol: str,
        direction: str,
        output_dir: str,
):
    print(f"Reading and plotting {protocol}_{direction} with all operator data...")
    combined_df, _ = read_all_throughput_data(direction, protocol)

    plot_cdf_of_throughput_with_all_operators(
        combined_df,
        title=f'CDF of {protocol.upper()} {direction.capitalize()} Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_{protocol}_{direction}_all.png')
    )
    print('Done!')


def read_and_plot_throughput_data_by_area(
        protocol: str,
        direction: str,
        output_dir: str,
        area_type: str,
):
    print(f"Reading and plotting {protocol}_{direction} with all operator data...")
    combined_df, stats = read_all_throughput_data(direction, protocol, filter_by=('area', area_type))

    by_area_output_dir = os.path.join(output_dir, 'by_area')
    if not os.path.exists(by_area_output_dir):
        os.makedirs(by_area_output_dir, exist_ok=True)

    plot_cdf_of_throughput_with_all_operators(
        combined_df,
        data_stats=stats,
        title=f'CDF of {protocol.upper()} {direction.capitalize()} Throughput ({area_type.capitalize()} Area)',
        output_file_path=os.path.join(by_area_output_dir, f'cdf_{protocol}_{direction}_{area_type}.png')
    )
    print('Done!')


def read_and_plot_throughput_data_by_weather(
        protocol: str,
        direction: str,
        output_dir: str,
):
    print(f"Reading and plotting {protocol}_{direction} with all operator data...")

    all_df = pd.DataFrame()
    all_data_stats = {}
    for weather in ['sunny', 'cloudy', 'rainy', 'snowy']:
        weather_df, weather_data_stats = read_throughput_data('starlink', direction, protocol,
                                                              filter_by=('weather', weather))
        all_df = pd.concat([all_df, weather_df], ignore_index=True)
        all_data_stats[weather] = weather_data_stats

    by_weather_output_dir = os.path.join(output_dir, 'by_weather')
    if not os.path.exists(by_weather_output_dir):
        os.makedirs(by_weather_output_dir, exist_ok=True)

    plot_cdf_of_starlink_throughput_by_weather(
        all_df,
        data_stats=all_data_stats,
        title=f'CDF of {protocol.upper()} {direction.capitalize()} Throughput (By Weather)',
        output_file_path=os.path.join(by_weather_output_dir,
                                      f'cdf_{protocol}_{direction}_starlink_by_weather.png')
    )
    print('Done!')


def read_throughput_data(operator: str, direction: str, protocol: str, filter_by: (str, str) = None):
    def create_stats(total_count: int = 0, filter_by_count: int = 0):
        return {
            'is_filtered': filter_by is not None,
            'total_count': total_count,
            'filtered_count': filter_by_count,
        }

    df = get_data_frame_from_all_csv(operator, protocol, direction)
    df['operator'] = operator
    stats = create_stats(total_count=len(df))
    if filter_by:
        df = df[df[filter_by[0]] == filter_by[1]]
        stats['filtered_count'] = len(df)
    return df, stats


def read_all_throughput_data(direction: str, protocol: str, filter_by: (str, str) = None):
    combined_df = pd.DataFrame()
    all_stats = {}
    for operator in ['att', 'verizon', 'starlink', 'tmobile']:
        df, stats = read_throughput_data(operator, direction, protocol, filter_by=filter_by)
        all_stats[operator] = stats
        if operator == 'att':
            combined_df = df
        else:
            combined_df = pd.concat([combined_df, df], ignore_index=True)
    return combined_df, all_stats


def read_and_plot_starlink_throughput_data(output_dir: str, filter_by: str = None):
    data_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/starlink')
    sl_metric_df = pd.read_csv(os.path.join(data_dir, 'starlink_metric.csv'))

    # convert the throughput_cubic to Mbps
    tput_df = sl_metric_df['tput_dl_bps'] / 1e6

    plot_cdf_of_throughput(
        tput_df,
        title=f'CDF of Starlink Downlink Throughput',
        output_file_path=os.path.join(output_dir, f'cdf_starlink_metric_downlink_tput.png')
    )
    print('Done!')


def plot_cdf_tput_starlink_vs_cellular(direction: str = 'downlink'):
    tcp_tput_df = pd.DataFrame()
    udp_tput_df = pd.DataFrame()
    for operator in ['att', 'verizon', 'starlink', 'tmobile']:
        sub_tcp_tput_df = get_data_frame_from_all_csv(operator, 'tcp', direction)
        sub_tcp_tput_df['operator'] = operator
        tcp_tput_df = pd.concat([tcp_tput_df, sub_tcp_tput_df], ignore_index=True)

        sub_udp_df = get_data_frame_from_all_csv(operator, 'udp', direction)
        sub_udp_df['operator'] = operator
        udp_tput_df = pd.concat([udp_tput_df, sub_udp_df], ignore_index=True)

    plot_cdf_tput_tcp_vs_udp_for_starlink_and_cellular(
        tcp_tput_df,
        udp_tput_df,
        direction=direction,
        output_dir=output_dir
    )


def read_and_plot_cdf_tcp_tput_with_cubic_vs_bbr(protocol: str, direction: str, output_dir: str):
    cubic_df = pd.DataFrame()
    bbr_df = pd.DataFrame()
    for operator in ['att', 'verizon', 'starlink', 'tmobile']:
        sub_cubic_df = get_data_frame_from_all_csv(operator, protocol, direction, base_dir=tput_cubic_dir)
        sub_cubic_df['operator'] = operator
        cubic_df = pd.concat([cubic_df, sub_cubic_df], ignore_index=True)

        sub_bbr_df = get_data_frame_from_all_csv(operator, protocol, direction)
        sub_bbr_df['operator'] = operator
        bbr_df = pd.concat([bbr_df, sub_bbr_df], ignore_index=True)

    plot_cdf_tcp_tput_with_cubic_vs_bbr(
        cubic_df,
        bbr_df,
        protocol=protocol,
        direction=direction,
        output_dir=output_dir
    )


def main():
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"Dataset folder does not exist: {base_dir} ")

    # Plot the CDF of throughput
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # read_and_plot_throughput_data('tcp', 'downlink', output_dir)
    # print("--------------")
    # read_and_plot_throughput_data('tcp', 'uplink', output_dir)
    # print("--------------")
    # read_and_plot_throughput_data('udp', 'downlink', output_dir)
    # print("--------------")
    # read_and_plot_throughput_data('udp', 'uplink', output_dir)
    # print("--------------")

    # By area
    # for area_type in ['urban', 'suburban', 'rural']:
    #     read_and_plot_throughput_data_by_area('tcp', 'downlink', output_dir, area_type=area_type)
    #     print("--------------")
    #     read_and_plot_throughput_data_by_area('tcp', 'uplink', output_dir, area_type=area_type)
    #     print("--------------")
    #     read_and_plot_throughput_data_by_area('udp', 'downlink', output_dir, area_type=area_type)
    #     print("--------------")
    #     read_and_plot_throughput_data_by_area('udp', 'uplink', output_dir, area_type=area_type)
    #     print("--------------")
    #
    # # By weather
    # read_and_plot_throughput_data_by_weather('tcp', 'downlink', output_dir)
    # read_and_plot_throughput_data_by_weather('tcp', 'uplink', output_dir)
    # read_and_plot_throughput_data_by_weather('udp', 'downlink', output_dir)
    # read_and_plot_throughput_data_by_weather('udp', 'uplink', output_dir)

    # read_and_plot_starlink_throughput_data(output_dir)
    # print("--------------")

    # Starlink vs Cellular
    # plot_cdf_tput_starlink_vs_cellular('downlink')
    # plot_cdf_tput_starlink_vs_cellular('uplink')

    # Cubic vs BBR
    read_and_plot_cdf_tcp_tput_with_cubic_vs_bbr('tcp', 'downlink', output_dir)
    read_and_plot_cdf_tcp_tput_with_cubic_vs_bbr('tcp', 'uplink', output_dir)


if __name__ == '__main__':
    main()
