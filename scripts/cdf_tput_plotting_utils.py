# Plot CDF of throughput_cubic
import logging
import os
from typing import Dict, List, Tuple

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from scripts.math_utils import get_cdf
from scripts.utils import safe_get

base_directory = os.path.join(os.getcwd(), "../outputs/maine_starlink_trip/")


def get_data_frame_from_all_csv(protocol, direction):
    file_path = os.path.join(base_directory, f'csv/{protocol}_{direction}_all.csv')
    return pd.read_csv(file_path)


def extract_throughput_df(data_frame, operator=None):
    df = data_frame.copy()
    if operator:
        df = df[df['operator'] == operator]
    return df['throughput_mbps']


def save_throughput_metric_to_csv(data_frame, protocol, direction, output_dir='.'):
    """
    :param data_frame:
    :param protocol: tcp | udp
    :param direction:  uplink | downlink
    :param output_dir:
    :return:
    """
    prefix = f"{protocol}_{direction}"
    csv_filepath = os.path.join(output_dir, f'{prefix}_all.csv')
    data_frame.to_csv(csv_filepath, index=False)
    print(f'save all the {prefix} data to csv file: {csv_filepath}')


def get_statistics(data_frame: pd.DataFrame or np.array, data_stats: Dict = None):
    return {
        'min': data_frame.min(),
        'max': data_frame.max(),
        'median': np.median(data_frame),
        'mean': np.mean(data_frame),
        'total_count': safe_get(data_stats, 'total_count', len(data_frame)),
        'filtered_count': safe_get(data_stats, 'filtered_count', len(data_frame)),
    }


def format_statistics(stats):
    total_count = safe_get(stats, 'total_count')
    filtered_count = safe_get(stats, 'filtered_count')

    result = f"Median: {stats['median']:.1f} Mbps\nMean: {stats['mean']:.1f} Mbps\nMax: {stats['max']:.1f} Mbps"

    if total_count == filtered_count:
        return result

    if total_count is None or total_count == 0:
        percentage = 'N/A'
    else:
        # fixed to 2 decimal places
        percentage = f"{(filtered_count / total_count) * 100:.0f}%"
    return f"{result}\nCount: {filtered_count}/{total_count} ({percentage})"


def plot_cdf_of_throughput(
        data_frame,
        xlabel='Throughput (Mbps)',
        ylabel='CDF',
        title='CDF of Throughput',
        output_file_path=None
):
    xvals, yvals = get_cdf(data_frame)

    stats = get_statistics(data_frame)
    label = format_statistics(stats)

    plt.figure(figsize=(10, 6))
    plt.plot(xvals, yvals, linestyle='-', label=label)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    else:
        plt.show()
    plt.close()


def plot_cdf_of_throughput_with_all_operators(
        data_frame,
        all_operators: List[str],
        data_stats=None,
        xlabel='Throughput (Mbps)',
        ylabel='CDF',
        title='CDF of Throughput with all Operators',
        output_file_path=None
):
    """

    :param data_frame:
    :param all_operators: list of all operators,  = ['starlink', 'att', 'verizon', 'tmobile']
    :param data_stats: containing data stats of all operators, keyed by operator name
    :param xlabel:
    :param ylabel:
    :param title:
    :param output_file_path:
    :return:
    """
    fig, ax = plt.subplots(figsize=(6, 4))
    # ensure the order by making Categorical
    data_frame['operator'] = pd.Categorical(data_frame['operator'], categories=all_operators, ordered=True)
    # make sure the colors are consistent for each operator
    color_map = {
        'starlink': 'black',
        'att': 'b',
        'verizon': 'r',
        'tmobile': 'deeppink'
    }
    unique_operators = data_frame['operator'].unique()

    for operator in all_operators:
        if operator not in unique_operators:
            continue

        operator_data = extract_throughput_df(data_frame, operator)
        color = color_map[operator]

        data_sorted, cdf = get_cdf(operator_data)

        stats = get_statistics(
            data_sorted,
            data_stats=(safe_get(data_stats, operator, None))
        )
        label = format_statistics(stats)
        ax.plot(
            data_sorted,
            cdf,
            color=color,
            linestyle='-',
            label=f'{operator}\n{label}'
        )

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_yticks(np.arange(0, 1.1, 0.25))
    ax.set_title(title)
    ax.legend(fontsize=8)
    ax.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    # plt.show()
    plt.close(fig)


def plot_cdf_of_starlink_throughput_by_weather(
        df,
        data_stats: Dict = None,
        xlabel='Throughput (Mbps)',
        ylabel='CDF',
        title='CDF of Throughput with all Operators',
        output_file_path=None
):
    plt.figure(figsize=(10, 6))

    all_weather = ['sunny', 'cloudy', 'rainy', 'snowy']
    df['weather'] = pd.Categorical(df['weather'], categories=all_weather, ordered=True)
    # make sure the colors are consistent for each operator
    color_map = {
        'sunny': 'r',
        'cloudy': 'g',
        'rainy': 'b',
        'snowy': 'orange'
    }
    weathers = df['weather'].unique()

    for index, weather in enumerate(weathers):
        weather_df = df[df['weather'] == weather]['throughput_mbps']
        data_sorted = np.sort(weather_df)
        color = color_map[weather]
        cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)

        weather_data_stats = safe_get(data_stats, weather)
        stats = get_statistics(data_sorted, data_stats=weather_data_stats)
        label = format_statistics(stats)
        plt.plot(
            data_sorted,
            cdf,
            color=color,
            linestyle='-',
            label=f'{weather}\n{label}'
        )

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    plt.show()


def plot_tcp_uplink_data(df: pd.DataFrame, output_dir='.'):
    operators = df['operator'].unique()

    # plot CDF of throughput_cubic for each operator
    for operator in operators:
        throughput_df = extract_throughput_df(df, operator)
        print(f"Describe throughput_normal data of {operator}")
        print(throughput_df.describe())

        plot_freq_distribution_of_throughput(
            throughput_df,
            operator,
            output_dir,
            plot_title=f'Frequency Distribution of TCP Uplink Throughput ({operator})'
        )

    #     plot_cdf_of_throughput(
    #         throughput_df,
    #         title=f'CDF of TCP Uplink Throughput ({operator.capitalize()})',
    #         output_file_path=os.path.join(output_dir, f'cdf_tcp_uplink_{operator}.png')
    #     )
    #
    # # plot one CDF of throughput_cubic for all operators
    # plot_cdf_of_throughput_with_all_operators(
    #     df,
    #     title='CDF of TCP Uplink Throughput (All Operators)',
    #     output_file_path=os.path.join(output_dir, f'cdf_tcp_uplink_all.png')
    # )


def plot_freq_distribution_of_throughput(
        single_col_df: pd.DataFrame,
        operator: str,
        output_dir: str,
        plot_title: str = 'Frequency Distribution of Throughput'
):
    print('count df', single_col_df.count())

    unique_values = single_col_df.unique()
    print(f"Number of unique values: {unique_values}")

    frequency_distribution = single_col_df.value_counts()
    print("Frequency distribution:\n", frequency_distribution)

    # Calculate the total number of observations
    total_observations = frequency_distribution.sum()
    print("Total number of observations:", total_observations)

    # Calculate the percentage distribution
    percentage_distribution = (frequency_distribution / total_observations) * 100

    # Plot a bar chart for frequency distribution
    plt.figure(figsize=(15, 9))
    percentage_distribution.sort_index().head(100).plot(kind='bar', edgecolor='black')
    plt.xlabel('Throughput (Mbps)')
    plt.ylabel('Percentage (%)')
    plt.title(plot_title)
    plt.grid(True)

    # Save the plot
    plot_path = os.path.join(output_dir, f'frequency_distribution_{operator}.png')
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")

    # Show the plot
    plt.show()


def plot_udp_uplink_data(df: pd.DataFrame, output_dir='.'):
    operators = df['operator'].unique()

    # plot CDF of throughput_cubic for each operator
    for operator in operators:
        throughput_df = extract_throughput_df(df, operator)
        plot_cdf_of_throughput(
            throughput_df,
            title=f'CDF of UDP Uplink Throughput ({operator.capitalize()})',
            output_file_path=os.path.join(output_dir, f'cdf_udp_uplink_{operator}.png')
        )

    # plot one CDF of throughput_cubic for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        title='CDF of UDP Uplink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_udp_uplink_all.png')
    )


def plot_udp_downlink_data(df: pd.DataFrame, output_dir='.'):
    operators = df['operator'].unique()

    # plot CDF of throughput_cubic for each operator
    for operator in operators:
        throughput_df = extract_throughput_df(df, operator)
        plot_cdf_of_throughput(
            throughput_df,
            title=f'CDF of UDP Downlink Throughput ({operator.capitalize()})',
            output_file_path=os.path.join(output_dir, f'cdf_udp_downlink_{operator}.png')
        )

    # plot one CDF of throughput_cubic for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        all_operators=['starlink', 'att', 'verizon', 'tmobile'],
        title='CDF of UDP Downlink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_udp_downlink_all.png')
    )


def plot_cdf_tput_tcp_vs_udp_for_starlink_and_cellular(
        tcp_dl_df: pd.DataFrame,
        udp_dl_df: pd.DataFrame,
        direction: str = 'downlink',
        output_dir='.',
        logger: logging.Logger = logging.getLogger(),
):
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
    ax.set_yticks(np.arange(0, 1.1, 0.25))
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, config['filename']))
    plt.show()
    plt.close(fig)

def plot_cdf_xcal_vs_app_tput_combined(
        app_tput_dfs: dict, 
        xcal_tput_dfs: dict, 
        output_file_path: str, 
        title: str = None
    ):
    """Plot CDF comparing XCAL and application throughput measurements in a 2x2 subplot.
    
    Args:
        app_tput_dfs: Dict containing app throughput Series for each protocol/direction
        xcal_tput_dfs: Dict containing XCAL throughput Series for each protocol/direction
        output_file_path: Path to save the output figure
    """
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))
    cmap20 = plt.cm.tab20
    
    # Add title for whole figure
    if title:
        fig.suptitle(title)
    
    # Subplot positions for each combination
    positions = {
        ('tcp', 'downlink'): (0, 0),
        ('tcp', 'uplink'): (0, 1),
        ('udp', 'downlink'): (1, 0),
        ('udp', 'uplink'): (1, 1)
    }
    
    # Plot each subplot
    for protocol in ['tcp', 'udp']:
        for direction in ['downlink', 'uplink']:
            row, col = positions[(protocol, direction)]
            ax = axs[row, col]
            
            key = f"{protocol}_{direction}"
            app_data = app_tput_dfs[key]
            xcal_data = xcal_tput_dfs[key]
            
            # Plot CDFs
            for idx, data in enumerate([app_data, xcal_data]):
                xvals, yvals = get_cdf(data)
                ax.plot(xvals, yvals,
                       label=['Application', 'XCAL'][idx],
                       color=[cmap20(0), cmap20(4)][idx],
                       linewidth=3)
            
            # Formatting each subplot
            # fzsize = 16
            # ax.tick_params(axis='both', labelsize=fzsize-2)
            # ax.set_xlabel('Throughput (Mbps)', fontsize=fzsize)
            # ax.set_ylabel('CDF', fontsize=fzsize)
            # ax.set_yticks(np.arange(0, 1.1, 0.25))
            
            # # Set axis limits
            # max_tput = max(app_data.max(), xcal_data.max())
            # ax.set_xlim(0, max_tput)
            # if max_tput <= 100:
            #     ax.set_xticks(range(0, int(max_tput) + 1, 25))
            # else:
            #     ax.set_xticks(range(0, int(max_tput) + 1, 50))
            # ax.set_ylim(0, 1.02)
            
            # Add subplot title
            ax.set_title(f'{protocol.upper()} {direction.capitalize()}')
            ax.grid(True)
            
            # Only add legend to first subplot
            if row == 0 and col == 0:
                ax.legend(loc='lower right')
    
    plt.tight_layout()
    plt.savefig(output_file_path)
    plt.close()
