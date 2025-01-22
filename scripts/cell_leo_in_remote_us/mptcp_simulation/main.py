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

def read_mptcp_tput_data_with_all_operators(
        base_dir: str,
        protocol: str,
        direction: str,
        operators: List[str],
):
    mptcp_dfs = {}
    for i in range(len(operators) - 1):
        for j in range(i+1, len(operators)):
            operator_a = operators[i]
            operator_b = operators[j]
            key = f'{operator_a}_{operator_b}'
            df = read_mptcp_tput_data_from_two_operators(
                base_dir=base_dir,
                operator_a=operator_a,
                operator_b=operator_b,
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

def read_mptcp_ping_data_with_all_operators(
        base_dir: str,
        operators: List[str],
):
    mptcp_dfs = {}
    for i in range(len(operators) - 1):
        for j in range(i+1, len(operators)):
            operator_a = operators[i]
            operator_b = operators[j]
            key = f'{operator_a}_{operator_b}'
            df = read_mptcp_ping_data_from_two_operators(
                base_dir=base_dir,
                operator_a=operator_a,
                operator_b=operator_b,
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
        'hatch': None,
    },
    'att': {
        'label': 'AT',
        'color': operator_conf['att']['color'],
        'hatch': None,
    },
    'verizon': {
        'label': 'VZ',
        'color': operator_conf['verizon']['color'],
        'hatch': None,
    },
    'tmobile': {
        'label': 'TM',
        'color': operator_conf['tmobile']['color'],
        'hatch': None,
    },
    'starlink_att': {
        'label': 'SL+AT',
        'color': operator_conf['att']['color'],
        'hatch': None,
        'order': 0, 
    },
    'starlink_verizon': {
        'label': 'SL+VZ',
        'color': operator_conf['verizon']['color'],
        'hatch': None,
        'order': 1,
    },
    'starlink_tmobile': {
        'label': 'SL+TM',
        'color': operator_conf['tmobile']['color'],
        'hatch': None,
        'order': 2,
    },
    'verizon_att': {
        'label': 'VZ+AT',
        'color': operator_conf['att']['color'],
        'hatch': '///',
        'order': 3,
    },
    'verizon_tmobile': {
        'label': 'TM+VZ',
        'color': operator_conf['verizon']['color'],
        'hatch': '///',
        'order': 4,
    },
    'att_tmobile': {
        'label': 'AT+TM',
        'color': operator_conf['tmobile']['color'],
        'hatch': '///',
        'order': 5,
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
    num_of_combinations = len(mptcp_dfs.keys())
    if num_of_combinations > 3:
        fig_width = 18
    else:
        fig_width = 10

    # Create three subplots sharing y axis
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(fig_width, 3), sharey=True)
    
    # Plot original throughput
    data1 = []
    labels1 = []
    colors1 = []
    for operator, df in dfs.items():
        op_conf = mptcp_operator_conf[operator]
        data1.append(df['throughput_mbps'])
        labels1.append(op_conf['label'])
        colors1.append(op_conf['color'])
    
    bp1 = ax1.boxplot(
        data1,
        labels=labels1,
        patch_artist=True,
        whis=[5, 95],  # Set whiskers to 5th and 95th percentile
        # flierprops={'marker': None},
        showfliers=False,
    )
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
    hatchs2 = []
    for operator, df in mptcp_dfs.items():
        op_conf = mptcp_operator_conf[operator]
        data2.append(df['max_tput'])
        labels2.append(op_conf['label'])
        colors2.append(op_conf['color'])
        hatchs2.append(op_conf['hatch'])
    
    bp2 = ax2.boxplot(
        data2,
        labels=labels2,
        whis=[5, 95],  # Set whiskers to 5th and 95th percentile
        patch_artist=True,
        showfliers=False,
        # flierprops={'marker': None},
    )
    for patch, color, hatch in zip(bp2['boxes'], colors2, hatchs2):
        patch.set_facecolor(color)
        patch.set_hatch(hatch)

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
    hatchs3 = []
    for operator, df in mptcp_dfs.items():
        op_conf = mptcp_operator_conf[operator]
        data3.append(df['sum_tput'])
        labels3.append(op_conf['label'])
        colors3.append(op_conf['color'])
        hatchs3.append(op_conf['hatch'])

    bp3 = ax3.boxplot(
        data3, 
        labels=labels3, 
        patch_artist=True, 
        showfliers=False,
        # flierprops={'marker': None},
        whis=[5, 95],
    )
    for patch, color, hatch in zip(bp3['boxes'], colors3, hatchs3):
        patch.set_facecolor(color)
        patch.set_hatch(hatch)

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
    num_of_combinations = len(mptcp_dfs.keys())
    if num_of_combinations > 3:
        fig_width = 12
    else:
        fig_width = 10

    # Create three subplots sharing y axis
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(fig_width, 3), sharey=True)
    
    # Plot original throughput
    data1 = []
    labels1 = []
    colors1 = []
    for operator, df in dfs.items():
        data1.append(df['rtt_ms'])
        labels1.append(mptcp_operator_conf[operator]['label'])
        colors1.append(mptcp_operator_conf[operator]['color'])
    
    bp1 = ax1.boxplot(
        data1, 
        labels=labels1, 
        patch_artist=True, 
        flierprops={'marker': None},
        whis=[5, 95],
    )
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
    
    bp2 = ax2.boxplot(
        data2, 
        labels=labels2, 
        patch_artist=True, 
        flierprops={'marker': None},
        whis=[5, 95],
    )
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


def append_tput_series_to_dfs(dfs: Dict[str, pd.DataFrame], operator: str, sub_df: pd.DataFrame):
    if operator not in dfs:
        dfs[operator] = pd.DataFrame(columns=['throughput_mbps', 'time'])
    # Only concat rows with new timestamps
    sub_df = sub_df.drop_duplicates(subset=['time'])
    dfs[operator] = pd.concat([dfs[operator], sub_df], ignore_index=True)
    dfs[operator] = dfs[operator].drop_duplicates(subset=['time']).reset_index(drop=True)
    return dfs

def generate_single_path_dfs(mptcp_dfs: Dict[str, pd.DataFrame], data_field: str):
    dfs = {}
    for key, df in mptcp_dfs.items():
        operator_a, operator_b = key.split('_')
        sub_df_a = df[[f'A_{data_field}', 'A_time']].rename(columns={f'A_{data_field}': data_field, 'A_time': 'time'})
        sub_df_b = df[[f'B_{data_field}', 'B_time']].rename(columns={f'B_{data_field}': data_field, 'B_time': 'time'})

        dfs = append_tput_series_to_dfs(dfs, operator_a, sub_df_a)
        dfs = append_tput_series_to_dfs(dfs, operator_b, sub_df_b)
    return dfs

def test_generate_single_path_tput_dfs():
    # Create mock MPTCP dataframe with time column
    mock_data = {
        'A_throughput_mbps': [10, 20, 30, 30],
        'B_throughput_mbps': [15, 25, 35, 35],
        'A_time': [1000, 2000, 3000, 3000],
        'B_time': [1000, 2000, 3000, 3000]
    }
    mock_df = pd.DataFrame(mock_data)
    mock_mptcp_dfs = {'starlink_att': mock_df}

    # Generate single path dfs
    result_dfs = generate_single_path_dfs(mock_mptcp_dfs)

    # Verify the results
    assert 'starlink' in result_dfs, "starlink should be in result"
    assert 'att' in result_dfs, "att should be in result"
    
    # Check dataframe structure
    assert set(result_dfs['starlink'].columns) == {'throughput_mbps', 'time'}, "starlink df should have throughput_mbps and time columns"
    assert set(result_dfs['att'].columns) == {'throughput_mbps', 'time'}, "att df should have throughput_mbps and time columns"
    
    # Check data length
    assert len(result_dfs['starlink']) == 3, "starlink df should have 3 rows"
    assert len(result_dfs['att']) == 3, "att df should have 3 rows"
    
    # Check values
    assert result_dfs['starlink']['time'].tolist() == [1000, 2000, 3000], "starlink time values incorrect"
    assert result_dfs['att']['time'].tolist() == [1000, 2000, 3000], "att time values incorrect"
    assert result_dfs['starlink']['throughput_mbps'].tolist() == [10, 20, 30], "starlink throughput values incorrect"
    assert result_dfs['att']['throughput_mbps'].tolist() == [15, 25, 35], "att throughput values incorrect"
    
    print("All tests passed!")

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
                'max_ylim': 75,
                'y_step': 25,
            },
            'ping': {
                'max_ylim': 200,
                'y_step': 25,
            },
        },
        'hawaii': {
            'tcp_downlink': {
                'max_ylim': 750,
                'y_step': 100,
            },
            'tcp_uplink': {
                'max_ylim': 125,
                'y_step': 25,
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

            mptcp_dfs = read_mptcp_tput_data_with_all_operators(
                base_dir=os.path.join(loc_conf['root_dir'], 'mptcp'),
                protocol=protocol,
                direction=direction,
                operators=operators,
            )

            single_path_tcp_dfs = generate_single_path_dfs(mptcp_dfs, data_field=CommonField.TPUT_MBPS)

            for key, item in mptcp_dfs.items():
                output_mptcp_csv_path = os.path.join(loc_conf['root_dir'], 'mptcp', f'mptcp_trace.{protocol}_{direction}.{key}.csv')
                item.to_csv(output_mptcp_csv_path, index=False)
                print(f'Saved mptcp trace for {key} to dir: {output_mptcp_csv_path}')

            plot_mptcp_boxplots_for_tput(
                dfs=single_path_tcp_dfs,
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

        mptcp_dfs = read_mptcp_ping_data_with_all_operators(
            base_dir=os.path.join(loc_conf['root_dir'], 'mptcp'),
            operators=operators,
        )
        single_path_ping_dfs = generate_single_path_dfs(mptcp_dfs, data_field=CommonField.RTT_MS)

        for key, item in mptcp_dfs.items():
            output_mptcp_csv_path = os.path.join(loc_conf['root_dir'], 'mptcp', f'mptcp_trace.ping.{key}.csv')
            item.to_csv(output_mptcp_csv_path, index=False)
            print(f'Saved mptcp trace for {key} to dir: {output_mptcp_csv_path}')

        plot_mptcp_boxplots_for_rtt(
            dfs=single_path_ping_dfs,
            mptcp_dfs=mptcp_dfs,    
            mptcp_operator_conf=mptcp_operator_conf,
            max_ylim=_proto_dir_conf['max_ylim'],
            y_step=_proto_dir_conf['y_step'],
            output_fig_filename=os.path.join(output_dir, f'mptcp_result.ping.{location}.pdf'),
        )


if __name__ == '__main__':
    main()
    # test_generate_single_path_tput_dfs()