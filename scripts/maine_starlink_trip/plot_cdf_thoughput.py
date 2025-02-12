import os
import sys
from typing import List

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.logging_utils import create_logger
from scripts.cdf_tput_plotting_utils import get_data_frame_from_all_csv, plot_cdf_of_throughput_with_all_operators, \
    plot_cdf_of_throughput, plot_cdf_of_starlink_throughput_by_weather, \
    plot_cdf_tput_tcp_vs_udp_for_starlink_and_cellular, plot_cdf_xcal_vs_app_tput_combined
from scripts.maine_starlink_trip.configs import ROOT_DIR
from scripts.constants import OUTPUT_DIR

base_dir = os.path.join(ROOT_DIR, 'throughput')
tmp_dir = os.path.join(ROOT_DIR, 'tmp')
output_dir = os.path.join(OUTPUT_DIR, 'maine_starlink_trip/plots')

logger = create_logger('plot_cdf_throughput', filename=os.path.join(tmp_dir, 'plot_cdf_throughput.log'))

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def get_data_frame_from_all_csv(operator: str, protocol: str, direction: str):
    csv_filename = f'{operator}_{protocol}_{direction}.csv'
    file_path = os.path.join(base_dir, csv_filename)
    try:
        df = pd.read_csv(file_path)
        logger.info(f'{csv_filename} count: {df.count()}')
        return df
    except Exception:
        return pd.DataFrame()


def plot_tcp_downlink_data(df: pd.DataFrame, output_dir='.'):
    # plot one CDF of throughput_cubic for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        all_operators=['starlink', 'att', 'verizon'],
        title='CDF of TCP Downlink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_tcp_downlink_all.png')
    )


def read_and_plot_throughput_data(
        protocol: str,
        direction: str,
        output_dir: str,
):
    print(f"Reading and plotting {protocol}_{direction} with all operator data...")
    combined_df, _ = read_all_throughput_data(direction, protocol)

    plot_cdf_of_throughput_with_all_operators(
        combined_df,
        all_operators=['starlink', 'att', 'verizon'],
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
        all_operators=['starlink', 'att', 'verizon'],
        data_stats=stats,
        title=f'CDF of {protocol.upper()} {direction.capitalize()} Throughput ({area_type.capitalize()} Area)',
        output_file_path=os.path.join(by_area_output_dir, f'cdf_{protocol}_{direction}_{area_type}.png')
    )
    print('Done!')


def read_and_plot_throughput_data_by_area_2(
        protocol: str,
        direction: str,
        output_dir: str,
):
    def merge_stats(stats1, stats2):
        res = {}
        for operator in stats1:
            item1 = stats1[operator]
            item2 = stats2[operator]
            res[operator] = {
                'is_filtered': item1['is_filtered'] or item2['is_filtered'],
                'total_count': item1['total_count'],
                'filtered_count': item1['filtered_count'] + item2['filtered_count'],
            }
        return res

    print(f"Reading and plotting {protocol}_{direction} with all operator data...")
    urban_df, urban_stats = read_all_throughput_data(direction, protocol, filter_by=('area', 'urban'))
    suburban_df, suburban_stats = read_all_throughput_data(direction, protocol, filter_by=('area', 'suburban'))
    rural_df, rural_stats = read_all_throughput_data(direction, protocol, filter_by=('area', 'rural'))

    by_area_output_dir = os.path.join(output_dir, 'by_area')
    if not os.path.exists(by_area_output_dir):
        os.makedirs(by_area_output_dir, exist_ok=True)

    # We merge suburban to rural because the number of samples in suburban is very low
    # Rural and suburban
    rural_suburban_df = pd.concat([rural_df, suburban_df], ignore_index=True)
    rural_suburban_stats = merge_stats(rural_stats, suburban_stats)
    area_type = 'rural'
    plot_cdf_of_throughput_with_all_operators(
        rural_suburban_df,
        all_operators=['starlink', 'att', 'verizon'],
        data_stats=rural_suburban_stats,
        title=f'CDF of {protocol.upper()} {direction.capitalize()} Throughput ({area_type.capitalize()} Area)',
        output_file_path=os.path.join(by_area_output_dir, f'cdf_{protocol}_{direction}_{area_type}.png')
    )

    # Urban
    area_type = 'urban'
    plot_cdf_of_throughput_with_all_operators(
        urban_df,
        all_operators=['starlink', 'att', 'verizon'],
        data_stats=urban_stats,
        title=f'CDF of {protocol.upper()} {direction.capitalize()} Throughput ({area_type.capitalize()} Area)',
        output_file_path=os.path.join(by_area_output_dir, f'cdf_{protocol}_{direction}_{area_type}.png')
    )

    print('Done!')


def read_and_plot_throughput_data_by_weather(
        protocol: str,
        direction: str,
        output_dir: str,
        all_weathers: List[str],
):
    print(f"Reading and plotting {protocol}_{direction} with all operator data...")

    all_df = pd.DataFrame()
    all_data_stats = {}
    for weather in all_weathers:
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
    for operator in ['att', 'verizon', 'starlink']:
        df, stats = read_throughput_data(operator, direction, protocol, filter_by=filter_by)
        all_stats[operator] = stats
        if operator == 'att':
            combined_df = df
        else:
            combined_df = pd.concat([combined_df, df], ignore_index=True)
    return combined_df, all_stats


def read_and_plot_starlink_throughput_data(output_dir: str, filter_by: str = None):
    data_dir = os.path.join(ROOT_DIR, 'starlink')
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
    for operator in ['att', 'verizon', 'starlink']:
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
        output_dir=output_dir,
        logger=logger
    )

def read_and_plot_xcal_tput_data(output_dir: str):
    operators = ['att', 'verizon']

    for operator in operators:
        df = pd.read_csv(os.path.join(ROOT_DIR, f'xcal/{operator}_xcal_smart_tput.csv'))
        
        # Collect all data first
        app_tput_dfs = {}
        xcal_tput_dfs = {}
        
        for protocol in ['tcp', 'udp']:
            for direction in ['downlink', 'uplink']:
                sub_df = df[(df['app_tput_protocol'] == protocol) & (df['app_tput_direction'] == direction)]
                
                # Get XCAL throughput
                if direction == 'downlink':
                    xcal_tput = sub_df['dl']
                else:
                    xcal_tput = sub_df['ul']
                
                # Get application throughput
                app_tput = get_data_frame_from_all_csv(operator, protocol, direction)['throughput_mbps']
                
                # Store in dictionaries
                key = f"{protocol}_{direction}"
                app_tput_dfs[key] = app_tput
                xcal_tput_dfs[key] = xcal_tput

        # Plot combined figure
        fig_path = os.path.join(output_dir, f'cdf_xcal_smart_tput_{operator}_combined.png')
        plot_cdf_xcal_vs_app_tput_combined(
            app_tput_dfs,
            xcal_tput_dfs,
            output_file_path=fig_path,
            title=f'CDF of Smart Tput and App Throughput: {operator}'
        )
        print(f'Saved combined XCAL vs application throughput plot to {fig_path}')




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

    # for area_type in ['urban', 'suburban', 'rural']:
    #     read_and_plot_throughput_data_by_area('tcp', 'downlink', output_dir, area_type=area_type)
    #     print("--------------")
    #     read_and_plot_throughput_data_by_area('tcp', 'uplink', output_dir, area_type=area_type)
    #     print("--------------")
    #     read_and_plot_throughput_data_by_area('udp', 'downlink', output_dir, area_type=area_type)
    #     print("--------------")
    #     read_and_plot_throughput_data_by_area('udp', 'uplink', output_dir, area_type=area_type)
    #     print("--------------")

    # all_weathers = ['sunny', 'cloudy', 'rainy', 'snowy']
    # read_and_plot_throughput_data_by_weather('tcp', 'downlink', output_dir, all_weathers=all_weathers)
    # read_and_plot_throughput_data_by_weather('tcp', 'uplink', output_dir, all_weathers=all_weathers)
    # read_and_plot_throughput_data_by_weather('udp', 'downlink', output_dir, all_weathers=all_weathers)
    # read_and_plot_throughput_data_by_weather('udp', 'uplink', output_dir, all_weathers=all_weathers)
    # #
    # # # read_and_plot_starlink_throughput_data(output_dir)
    # # # print("--------------")
    # #
    # plot_cdf_tput_starlink_vs_cellular('downlink')
    # plot_cdf_tput_starlink_vs_cellular('uplink')

    read_and_plot_xcal_tput_data(output_dir)

if __name__ == '__main__':
    main()
