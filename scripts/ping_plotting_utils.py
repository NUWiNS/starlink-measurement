import os

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from scripts.utils import get_statistics, format_statistics


def extract_rtt_df(data_frame, operator=None):
    df = data_frame.copy()
    if operator:
        df = df[df['operator'] == operator]
    return df['rtt_ms']


def plot_cdf_of_rtt(
        data_frame,
        xlabel='RTT (ms)',
        ylabel='CDF',
        title='CDF of RTT',
        output_file_path=None,
        xscale="linear"
):
    data_sorted = np.sort(data_frame)
    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)

    plt.figure(figsize=(10, 6))
    plt.plot(data_sorted, cdf, linestyle='-')
    plt.xscale(xscale)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    plt.show()


def plot_cdf_of_rtt_with_all_operators(
        df,
        xlabel='RTT (ms)',
        ylabel='CDF',
        title='CDF of RTT with all Operators',
        output_file_path=None,
        xscale="linear",
        max_percentile=100,
):
    plt.figure(figsize=(10, 6))

    all_operators = ['starlink', 'att', 'verizon', 'tmobile']
    df['operator'] = pd.Categorical(df['operator'], categories=all_operators, ordered=True)
    operators = filter(lambda x: x in df['operator'].unique(), df['operator'].cat.categories)

    color_map = {
        'starlink': 'black',
        'att': 'b',
        'verizon': 'r',
        'tmobile': 'deeppink'
    }

    x_max = float('-inf')
    for index, operator in enumerate(operators):
        operator_data = extract_rtt_df(df, operator)
        max_value = np.percentile(operator_data, max_percentile)
        x_max = max(x_max, max_value)
        filtered_data = operator_data[operator_data <= max_value]
        data_sorted = np.sort(filtered_data)
        color = color_map[operator]
        cdf = np.arange(1, len(data_sorted) + 1) / len(operator_data)

        plt.plot(
            data_sorted,
            cdf,
            color=color,
            label=operator,
            linestyle='-',
        )

    plt.yticks(np.arange(0, 1.1, 0.25))
    plt.xscale(xscale)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    plt.xlim(0, x_max)    
    plt.title(title)
    plt.legend()
    plt.grid(True)
    if output_file_path:
        plt.savefig(output_file_path)
    plt.close()


def plot_boxplot_of_rtt(df: pd.DataFrame, output_dir='.', yscale='linear'):
    fig, ax = plt.subplots(figsize=(10, 6))
    all_operators = filter(lambda x: x in df['operator'].unique(), ['starlink', 'att', 'verizon', 'tmobile'])
    df['operator'] = pd.Categorical(df['operator'], categories=all_operators, ordered=True)
    sns.boxplot(data=df, ax=ax, x='operator', y='rtt_ms', showfliers=False)
    ax.set_xlabel('Operator')
    ax.set_ylabel('RTT (ms)')
    ax.set_title('Boxplot of RTT by operator')

    plt.grid(True)
    plt.yscale(yscale)
    if output_dir:
        plt.savefig(os.path.join(output_dir, 'boxplot_rtt_all_operators.png'))
    else:
        plt.show()
    plt.close(fig)


def plot_all_cdf_for_rtt(
        df: pd.DataFrame,
        output_dir='.',
        max_percentile=100,
        xscale="linear"
):
    operators = df['operator'].unique()

    filename_prefix = 'cdf_rtt'
    if xscale:
        filename_prefix = f'{filename_prefix}_{xscale}'

    # plot CDF of throughput for each operator
    # for operator in operators:
    #     throughput_df = extract_rtt_df(df, operator)
    #     print(f"Describe rtt data of {operator}")
    #     print(throughput_df.describe())
    #     print('---')
    #     plot_cdf_of_rtt(
    #         throughput_df,
    #         title=f'CDF of Round-Trip Time ({operator.capitalize()})',
    #         output_file_path=os.path.join(output_dir, f'{filename_prefix}_{operator}.png'),
    #         xscale=xscale,
    #     )

    # plot one CDF of throughput for all operators
    plot_cdf_of_rtt_with_all_operators(
        df,
        title='CDF of Round-Trip Time (All Operators)',
        output_file_path=os.path.join(output_dir, f'{filename_prefix}_all.png'),
        xscale=xscale,
        max_percentile=max_percentile,
    )
