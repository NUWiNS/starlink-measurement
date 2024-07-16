# Plot CDF of throughput_cubic
import os
from typing import Dict

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

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


def get_statistics(data_frame: pd.DataFrame, data_stats: Dict = None):
    return {
        'min': data_frame.min(),
        'max': data_frame.max(),
        'median': np.median(data_frame),
        'total_count': safe_get(data_stats, 'total_count', len(data_frame)),
        'filtered_count': safe_get(data_stats, 'filtered_count', len(data_frame)),
    }


def format_statistics(stats):
    total_count = safe_get(stats, 'total_count')
    filtered_count = safe_get(stats, 'filtered_count')
    if total_count is None or total_count == 0:
        percentage = 'N/A'
    else:
        # fixed to 2 decimal places
        percentage = f"{(filtered_count / total_count) * 100:.2f}%"
    return f"Median: {stats['median']:.2f} Mbps\nMin: {stats['min']:.2f} Mbps\nMax: {stats['max']:.2f} Mbps\nCount: {filtered_count}/{total_count} ({percentage})"


def plot_cdf_of_throughput(
        data_frame,
        xlabel='Throughput (Mbps)',
        ylabel='CDF',
        title='CDF of Throughput',
        output_file_path=None
):
    data_sorted = np.sort(data_frame)
    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)

    stats = get_statistics(data_frame)
    label = format_statistics(stats)

    plt.figure(figsize=(10, 6))
    plt.plot(data_sorted, cdf, linestyle='-', label=label)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    plt.show()


def plot_cdf_of_throughput_with_all_operators(
        data_frame,
        data_stats=None,
        xlabel='Throughput (Mbps)',
        ylabel='CDF',
        title='CDF of Throughput with all Operators',
        output_file_path=None
):
    """

    :param data_frame:
    :param data_stats: containing data stats of all operators, keyed by operator name
    :param xlabel:
    :param ylabel:
    :param title:
    :param output_file_path:
    :return:
    """
    plt.figure(figsize=(10, 6))

    all_operators = ['starlink', 'att', 'verizon', 'tmobile']
    # ensure the order by making Categorical
    data_frame['operator'] = pd.Categorical(data_frame['operator'], categories=all_operators, ordered=True)
    # make sure the colors are consistent for each operator
    color_map = {
        'starlink': 'r',
        'att': 'g',
        'verizon': 'b',
        'tmobile': 'orange'
    }
    operators = data_frame['operator'].unique()

    for index, operator in enumerate(operators):
        operator_data = extract_throughput_df(data_frame, operator)
        data_sorted = np.sort(operator_data)
        color = color_map[operator]
        cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)

        stats = get_statistics(
            data_sorted,
            data_stats=(safe_get(data_stats, operator, None))
        )
        label = format_statistics(stats)
        plt.plot(
            data_sorted,
            cdf,
            color=color,
            linestyle='-',
            label=f'{operator}\n{label}'
        )

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    plt.show()


def plot_cdf_of_starlink_throughput_by_weather(
        df,
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

        stats = get_statistics(data_sorted)
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
        title='CDF of UDP Downlink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_udp_downlink_all.png')
    )
