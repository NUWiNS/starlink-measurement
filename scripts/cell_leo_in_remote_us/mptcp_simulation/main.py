import os
import sys
from typing import Any, Dict, List

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.cell_leo_in_remote_us.common import location_conf, operator_conf, cellular_location_conf
from scripts.constants import CommonField

def read_throughput_data_for_all_operators(
    base_dir: str,
    protocol: str,
    direction: str,
    operators: List[str],
):
    operator_dfs = {}
    for operator in operators:
        operator_dfs[operator] = pd.read_csv(os.path.join(base_dir, f'{operator}_{protocol}_{direction}.csv'))
    return operator_dfs

def read_mptcp_tput_data_from_two_operators(
    base_dir: str,
    operator_a: str,
    operator_b: str,
    protocol: str,
    direction: str,
):
    return pd.read_csv(os.path.join(base_dir, f'fused_trace.{protocol}_{direction}.{operator_a}_{operator_b}.csv'))

def read_mptcp_ping_data_from_two_operators(
    base_dir: str,
    operator_a: str,
    operator_b: str,
):
    return pd.read_csv(os.path.join(base_dir, f'fused_trace.ping.{operator_a}_{operator_b}.csv'))


def read_ping_data_for_all_operators(
    base_dir: str,
    operators: List[str],
):
    operator_dfs = {}
    for operator in operators:
        operator_dfs[operator] = pd.read_csv(os.path.join(base_dir, f'{operator}_ping.csv'))
    return operator_dfs

def read_mptcp_tput_data_for_starlink_with_cellular_operators(
        base_dir: str,
        protocol: str,
        direction: str,
        cellular_operators: List[str],
):
    mptcp_dfs = {}
    for cellular_operator in cellular_operators:
        operator_a = 'starlink'
        key = f'{operator_a}_{cellular_operator}'
        df = read_mptcp_tput_data_from_two_operators(
            base_dir=base_dir,
            operator_a='starlink',
            operator_b=cellular_operator,
            protocol=protocol,
            direction=direction,
        )
        df = generate_max_and_sum_tput_trace_of_two_operators(df)
        mptcp_dfs[key] = df
    return mptcp_dfs


def read_mptcp_ping_data_for_starlink_with_cellular_operators(
        base_dir: str,
        cellular_operators: List[str],
):
    mptcp_dfs = {}
    for cellular_operator in cellular_operators:
        operator_a = 'starlink'
        key = f'{operator_a}_{cellular_operator}'
        df = read_mptcp_ping_data_from_two_operators(
            base_dir=base_dir,
            operator_a='starlink',
            operator_b=cellular_operator,
        )
        df = generate_min_ping_trace_of_two_operators(df)
        mptcp_dfs[key] = df
    return mptcp_dfs


def generate_max_and_sum_tput_trace_of_two_operators(df: pd.DataFrame):
    df['max_tput'] = df.apply(lambda row: max(row[f'A_{CommonField.TPUT_MBPS}'], row[f'B_{CommonField.TPUT_MBPS}']), axis=1)
    df['sum_tput'] = df.apply(lambda row: sum([row[f'A_{CommonField.TPUT_MBPS}'], row[f'B_{CommonField.TPUT_MBPS}']]), axis=1)
    return df


def generate_min_ping_trace_of_two_operators(df: pd.DataFrame):
    df['min_rtt'] = df.apply(lambda row: min(row[f'A_{CommonField.RTT_MS}'], row[f'B_{CommonField.RTT_MS}']), axis=1)
    return df

mptcp_operator_conf = {
    'starlink': {
        'label': 'SL',
        'color': operator_conf['starlink']['color'],
    },
    'att': {
        'label': 'AT',
        'color': operator_conf['att']['color'],
    },
    'verizon': {
        'label': 'VZ',
        'color': operator_conf['verizon']['color'],
    },
    'tmobile': {
        'label': 'TM',
        'color': operator_conf['tmobile']['color'],
    },
    'starlink_att': {
        'label': 'SL + AT',
        'color': operator_conf['att']['color'],
        'order': 0, 
    },
    'starlink_verizon': {
        'label': 'SL + VZ',
        'color': operator_conf['verizon']['color'],
        'order': 1,
    },
    'starlink_tmobile': {
        'label': 'SL + TM',
        'color': operator_conf['tmobile']['color'],
        'order': 2,
    },
}


def plot_mptcp_boxplots_for_tput(
        dfs: Dict[str, pd.DataFrame],
        mptcp_dfs: Dict[str, pd.DataFrame],
        mptcp_operator_conf: Dict[str, Any],
        max_ylim: int | None = None,
        y_step: int = None,
        output_fig_filename: str = './figure.pdf',
):
    # Create three subplots sharing y axis
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(10, 3), sharey=True)
    
    # Plot original throughput
    data1 = []
    labels1 = []
    colors1 = []
    for operator, df in dfs.items():
        data1.append(df['throughput_mbps'])
        labels1.append(mptcp_operator_conf[operator]['label'])
        colors1.append(mptcp_operator_conf[operator]['color'])
    
    bp1 = ax1.boxplot(data1, labels=labels1, patch_artist=True, flierprops={'marker': None})
    for patch, color in zip(bp1['boxes'], colors1):
        patch.set_facecolor(color)
    ax1.set_title('Single-Path TCP')
    ax1.set_ylabel('Throughput (Mbps)')
    ax1.grid(True, axis='y', linestyle='--', alpha=0.7)

    if max_ylim is not None:
        ax1.set_ylim(0, max_ylim)
        if y_step is not None:
            ax1.set_yticks(range(0, max_ylim + 1, y_step))
    
    # Plot max throughput
    data2 = []
    labels2 = []
    colors2 = []
    for operator, df in mptcp_dfs.items():
        data2.append(df['max_tput'])
        labels2.append(mptcp_operator_conf[operator]['label'])
        colors2.append(mptcp_operator_conf[operator]['color'])
    
    bp2 = ax2.boxplot(data2, labels=labels2, patch_artist=True, flierprops={'marker': None})
    for patch, color in zip(bp2['boxes'], colors2):
        patch.set_facecolor(color)
    ax2.set_title('MPTCP (Max)')
    ax2.grid(True, axis='y', linestyle='--', alpha=0.7)
    if max_ylim is not None:
        ax2.set_ylim(0, max_ylim)
        if y_step is not None:
            ax2.set_yticks(range(0, max_ylim + 1, y_step))
    
    # Plot sum throughput
    data3 = []
    labels3 = []
    colors3 = []
    for operator, df in mptcp_dfs.items():
        data3.append(df['sum_tput'])
        labels3.append(mptcp_operator_conf[operator]['label'])
        colors3.append(mptcp_operator_conf[operator]['color'])
    
    bp3 = ax3.boxplot(data3, labels=labels3, patch_artist=True, flierprops={'marker': None})
    for patch, color in zip(bp3['boxes'], colors3):
        patch.set_facecolor(color)
    ax3.set_title('MPTCP (Sum)')
    ax3.grid(True, axis='y', linestyle='--', alpha=0.7)
    if max_ylim is not None:
        ax3.set_ylim(0, max_ylim)
        if y_step is not None:
            ax3.set_yticks(range(0, max_ylim + 1, y_step))
    
    plt.tight_layout()
    plt.savefig(output_fig_filename, bbox_inches='tight')
    plt.close()


def plot_mptcp_boxplots_for_rtt(
        dfs: Dict[str, pd.DataFrame],
        mptcp_dfs: Dict[str, pd.DataFrame],
        mptcp_operator_conf: Dict[str, Any],
        max_ylim: int | None = None,
        y_step: int = None,
        output_fig_filename: str = './figure.pdf',
):
    # Create three subplots sharing y axis
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7, 3), sharey=True)
    
    # Plot original throughput
    data1 = []
    labels1 = []
    colors1 = []
    for operator, df in dfs.items():
        data1.append(df['rtt_ms'])
        labels1.append(mptcp_operator_conf[operator]['label'])
        colors1.append(mptcp_operator_conf[operator]['color'])
    
    bp1 = ax1.boxplot(data1, labels=labels1, patch_artist=True, flierprops={'marker': None})
    for patch, color in zip(bp1['boxes'], colors1):
        patch.set_facecolor(color)
    ax1.set_title('Single-Path')
    ax1.set_ylabel('RTT (ms)')
    ax1.grid(True, axis='y', linestyle='--', alpha=0.7)

    if max_ylim is not None:
        ax1.set_ylim(0, max_ylim)
        if y_step is not None:
            ax1.set_yticks(range(0, max_ylim + 1, y_step))
    
    # Plot max throughput
    data2 = []
    labels2 = []
    colors2 = []
    for operator, df in mptcp_dfs.items():
        data2.append(df['min_rtt'])
        labels2.append(mptcp_operator_conf[operator]['label'])
        colors2.append(mptcp_operator_conf[operator]['color'])
    
    bp2 = ax2.boxplot(data2, labels=labels2, patch_artist=True, flierprops={'marker': None})
    for patch, color in zip(bp2['boxes'], colors2):
        patch.set_facecolor(color)
    ax2.set_title('MPTCP (Min)')
    ax2.grid(True, axis='y', linestyle='--', alpha=0.7)
    if max_ylim is not None:
        ax2.set_ylim(0, max_ylim)
        if y_step is not None:
            ax2.set_yticks(range(0, max_ylim + 1, y_step))

    plt.tight_layout()
    plt.savefig(output_fig_filename, bbox_inches='tight')
    plt.close()


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    proto_dir_conf = {
        'alaska': {
            'tcp_downlink': {
            'max_ylim': 350,
            'y_step': 50,
            },
            'tcp_uplink': {
                'max_ylim': 40,
                'y_step': 10,
            },
            'ping': {
                'max_ylim': 200,
                'y_step': 50,
            },
        },
        'hawaii': {
            'tcp_downlink': {
                'max_ylim': 750,
                'y_step': 100,
            },
            'tcp_uplink': {
                'max_ylim': 70,
                'y_step': 10,
            },
            'ping': {
                'max_ylim': 200,
                'y_step': 25,
            },
        },
    }

    for location in ['alaska', 'hawaii']:
        for proto_dir in [('tcp', 'downlink'), ('tcp', 'uplink')]:
            loc_conf = location_conf[location]
            operators = loc_conf['operators']
            protocol = proto_dir[0]
            direction = proto_dir[1]
            _proto_dir_conf = proto_dir_conf[location][f'{protocol}_{direction}']

            tput_dfs = read_throughput_data_for_all_operators(
                base_dir=os.path.join(loc_conf['root_dir'], 'throughput'),
                protocol=protocol,
                direction=direction,
                operators=operators,
            )
            mptcp_dfs = read_mptcp_tput_data_for_starlink_with_cellular_operators(
                base_dir=os.path.join(loc_conf['root_dir'], 'mptcp'),
                protocol=protocol,
                direction=direction,
                cellular_operators=cellular_location_conf[location]['operators'],
            )
            for key, item in mptcp_dfs.items():
                output_mptcp_csv_path = os.path.join(loc_conf['root_dir'], 'mptcp', f'mptcp_trace.{protocol}_{direction}.{key}.csv')
                item.to_csv(output_mptcp_csv_path, index=False)
                print(f'Saved mptcp trace for {key} to dir: {output_mptcp_csv_path}')

            plot_mptcp_boxplots_for_tput(
                dfs=tput_dfs,
                mptcp_dfs=mptcp_dfs,    
                mptcp_operator_conf=mptcp_operator_conf,
                max_ylim=_proto_dir_conf['max_ylim'],
                y_step=_proto_dir_conf['y_step'],
                output_fig_filename=os.path.join(output_dir, f'mptcp_result.{protocol}_{direction}.{location}.pdf'),
        )

    for location in ['alaska', 'hawaii']:
        loc_conf = location_conf[location]
        operators = loc_conf['operators']
        protocol = 'ping'
        _proto_dir_conf = proto_dir_conf[location][f'{protocol}']

        tput_dfs = read_ping_data_for_all_operators(
            base_dir=os.path.join(loc_conf['root_dir'], 'ping', 'sizhe_new_data'),
            operators=operators,
        )
        mptcp_dfs = read_mptcp_ping_data_for_starlink_with_cellular_operators(
            base_dir=os.path.join(loc_conf['root_dir'], 'mptcp'),
            cellular_operators=cellular_location_conf[location]['operators'],
        )
        for key, item in mptcp_dfs.items():
            output_mptcp_csv_path = os.path.join(loc_conf['root_dir'], 'mptcp', f'mptcp_trace.ping.{key}.csv')
            item.to_csv(output_mptcp_csv_path, index=False)
            print(f'Saved mptcp trace for {key} to dir: {output_mptcp_csv_path}')

        plot_mptcp_boxplots_for_rtt(
            dfs=tput_dfs,
            mptcp_dfs=mptcp_dfs,    
            mptcp_operator_conf=mptcp_operator_conf,
            max_ylim=_proto_dir_conf['max_ylim'],
            y_step=_proto_dir_conf['y_step'],
            output_fig_filename=os.path.join(output_dir, f'mptcp_result.ping.{location}.pdf'),
        )


if __name__ == '__main__':
    main()