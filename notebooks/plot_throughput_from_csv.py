# Plot CDF of throughput
import os

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

base_directory = os.path.join(os.getcwd(), "../outputs/maine_starlink_trip/")


def get_data_frame_from_all_csv(protocol, direction):
    file_path = os.path.join(base_directory, f'datasets/{protocol}_{direction}_all.csv')
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


def plot_cdf_of_throughput(
        data_frame,
        xlabel='Throughput (Mbps)',
        ylabel='CDF',
        title='CDF of Throughput',
        output_file_path=None
):
    data_sorted = np.sort(data_frame)
    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)

    plt.figure(figsize=(10, 6))
    plt.plot(data_sorted, cdf, linestyle='-')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    plt.show()


def plot_cdf_of_throughput_with_all_operators(
        df,
        xlabel='Throughput (Mbps)',
        ylabel='CDF',
        title='CDF of Throughput with all Operators',
        output_file_path=None
):
    plt.figure(figsize=(10, 6))

    operators = ['starlink', 'att', 'verizon']
    colors = ['r', 'g', 'b']

    for index, operator in enumerate(operators):
        operator_data = extract_throughput_df(df, operator)
        data_sorted = np.sort(operator_data)
        color = colors[index]
        cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
        plt.plot(
            data_sorted,
            cdf,
            color=color,
            linestyle='-',
            label=f'{operator}'
        )

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    plt.show()


def plot_tcp_downlink_data(df, output_dir='.'):
    operators = df['operator'].unique()

    # plot CDF of throughput for each operator
    for operator in operators:
        throughput_df = extract_throughput_df(df, operator)
        plot_cdf_of_throughput(
            throughput_df,
            title=f'CDF of TCP Downlink Throughput ({operator.capitalize()})',
            output_file_path=os.path.join(output_dir, f'cdf_tcp_downlink_{operator}.png')
        )

    # plot one CDF of throughput for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        title='CDF of TCP Downlink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_tcp_downlink_all.png')
    )


def plot_tcp_uplink_data(df: pd.DataFrame, output_dir='.'):
    operators = df['operator'].unique()

    # plot CDF of throughput for each operator
    for operator in operators:
        throughput_df = extract_throughput_df(df, operator)
        plot_cdf_of_throughput(
            throughput_df,
            title=f'CDF of TCP Uplink Throughput ({operator.capitalize()})',
            output_file_path=os.path.join(output_dir, f'cdf_tcp_uplink_{operator}.png')
        )

    # plot one CDF of throughput for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        title='CDF of TCP Uplink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_tcp_uplink_all.png')
    )


def plot_udp_uplink_data(df: pd.DataFrame, output_dir='.'):
    operators = df['operator'].unique()

    # plot CDF of throughput for each operator
    for operator in operators:
        throughput_df = extract_throughput_df(df, operator)
        plot_cdf_of_throughput(
            throughput_df,
            title=f'CDF of UDP Uplink Throughput ({operator.capitalize()})',
            output_file_path=os.path.join(output_dir, f'cdf_udp_uplink_{operator}.png')
        )

    # plot one CDF of throughput for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        title='CDF of UDP Uplink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_udp_uplink_all.png')
    )


def plot_udp_downlink_data(df: pd.DataFrame, output_dir='.'):
    operators = df['operator'].unique()

    # plot CDF of throughput for each operator
    for operator in operators:
        throughput_df = extract_throughput_df(df, operator)
        plot_cdf_of_throughput(
            throughput_df,
            title=f'CDF of UDP Downlink Throughput ({operator.capitalize()})',
            output_file_path=os.path.join(output_dir, f'cdf_udp_downlink_{operator}.png')
        )

    # plot one CDF of throughput for all operators
    plot_cdf_of_throughput_with_all_operators(
        df,
        title='CDF of UDP Downlink Throughput (All Operators)',
        output_file_path=os.path.join(output_dir, f'cdf_udp_downlink_all.png')
    )


def main():
    dataset_dir = os.path.join(base_directory, 'datasets')
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir, exist_ok=True)

    all_udp_downlink_df = get_data_frame_from_all_csv('udp', 'downlink')

    # Plot the CDF of throughput
    output_plots_dir = os.path.join(base_directory, 'plots')
    if not os.path.exists(output_plots_dir):
        os.makedirs(output_plots_dir, exist_ok=True)

    # plot_tcp_downlink_data(all_tcp_downlink_df, output_dir=output_plots_dir)
    # plot_tcp_uplink_data(all_tcp_uplink_df, output_dir=output_plots_dir)
    # plot_udp_uplink_data(df=all_udp_uplink_df, output_dir=output_plots_dir)
    plot_udp_downlink_data(df=all_udp_downlink_df, output_dir=output_plots_dir)


if __name__ == '__main__':
    main()
