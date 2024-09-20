import os
import sys
import unittest

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

DATA_DIR = os.path.join(os.getcwd(), '../../datasets')
OUTPUT_DIR = os.path.join(os.getcwd(), '../../outputs/overall')


def get_rtt_diff_between_hops(df: pd.DataFrame, hop1: int, hop2: int):
    if hop1 <= 0:
        raise ValueError('hop1 must be greater than 0')
    if hop2 < hop1:
        raise ValueError('hop2 must be greater than hop1')
    _df = df[df['rtt_ms'].notna() & df['exception'].isna()]
    hop_df = _df[(_df['hop_number'] == hop1) | (_df['hop_number'] == hop2)]
    hop_df = hop_df.groupby(['start_time', 'hop_number']).agg({'rtt_ms': 'mean'}).reset_index()
    result = hop_df.pivot(index='start_time', columns='hop_number', values='rtt_ms')
    result['rtt_diff'] = result[hop2] - result[hop1]
    return result['rtt_diff'].dropna().astype(float)


def get_dishy_to_gs_series(df: pd.DataFrame, dishy_hop: int, gs_hop: int):
    return get_rtt_diff_between_hops(df, dishy_hop, gs_hop)


def get_gs_to_pop_series(df: pd.DataFrame, gs_hop: int, pop_hop: int):
    return get_rtt_diff_between_hops(df, gs_hop, pop_hop)


def get_largest_hop_number_of_given_ip_prefix(df: pd.DataFrame, ip_prefix: str):
    _df = df.dropna(subset=['ip'])
    res_df = _df[_df['ip'].str.startswith(ip_prefix)]
    if res_df.empty:
        return -1
    return res_df['hop_number'].max()


# class UnitTest(unittest.TestCase):
#     def test_get_largest_hop_number_of_given_ip_prefix(self):
#         df = pd.DataFrame({
#             'ip': ['206.224.64.190', '206.224.64.185', '206.224.64.63'],
#             'hop_number': [4, 5, 6],
#         })
#         self.assertEqual(6, get_largest_hop_number_of_given_ip_prefix(df, '206.224'))
#

def get_datasource():
    # ALASKA
    ak_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'alaska_starlink_trip/traceroute/starlink_traceroute.csv'))
    ak_d2g = get_dishy_to_gs_series(ak_tr_df, dishy_hop=1, gs_hop=2)
    ak_pop_hop = get_largest_hop_number_of_given_ip_prefix(ak_tr_df, '206.224')
    ak_g2p = get_gs_to_pop_series(ak_tr_df, gs_hop=2, pop_hop=ak_pop_hop)

    # HAWAII
    hi_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'hawaii_starlink_trip/traceroute/starlink_traceroute.csv'))
    hi_d2g = get_dishy_to_gs_series(hi_tr_df, dishy_hop=1, gs_hop=2)
    hi_pop_hop = get_largest_hop_number_of_given_ip_prefix(hi_tr_df, '206.224')
    hi_g2p = get_gs_to_pop_series(hi_tr_df, gs_hop=2, pop_hop=hi_pop_hop)

    # MAINE
    me_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'maine_starlink_trip/traceroute/starlink_traceroute.csv'))
    me_d2g = get_dishy_to_gs_series(me_tr_df, dishy_hop=1, gs_hop=2)
    me_pop_hop = get_largest_hop_number_of_given_ip_prefix(me_tr_df, '206.224')
    me_g2p = get_gs_to_pop_series(me_tr_df, gs_hop=2, pop_hop=me_pop_hop)

    return {
        'alaska': {
            'dishy_to_gs': ak_d2g,
            'gs_to_pop': ak_g2p
        },
        'hawaii': {
            'dishy_to_gs': hi_d2g,
            'gs_to_pop': hi_g2p
        },
        'maine': {
            'dishy_to_gs': me_d2g,
            'gs_to_pop': me_g2p
        },
    }


config = {
    'alaska-dishy_to_gs': {
        'label': 'Dishy<->GS',
    },
    'alaska-gs_to_pop': {
        'label': 'GS<->PoP',
    },
    'hawaii-dishy_to_gs': {
        'label': 'Dishy<->GS',
    },
    'hawaii-gs_to_pop': {
        'label': 'GS<->PoP',
    },
    'maine-dishy_to_gs': {
        'label': 'Dishy<->GS',
    },
    'maine-gs_to_pop': {
        'label': 'GS<->PoP',
    },
}


def plot_swimlane_chart(datasource: dict, config: dict, output_path: str = 'swimlane.png'):
    cmap20 = plt.cm.tab20
    colors = [cmap20(0), cmap20(4)]
    group_colors = ['#f0f0f0', '#e0e0e0', '#d0d0d0']  # Light gray colors for alternating groups
    fig, ax = plt.subplots(figsize=(5, 3))

    group_idx = 0
    idx = 0
    position = 0
    ylabels = []
    group_positions = []

    for name, val in datasource.items():
        group_start = position
        # Add a background rectangle for the group
        rect = Rectangle((-25, position - 0.5),
                         200,
                         2.75,  # Height to cover two lanes plus spacing
                         facecolor=group_colors[group_idx % 3],
                         edgecolor='none',
                         zorder=0)  # Ensure it's drawn behind the boxplots

        ax.add_patch(rect)

        for direction in ['gs_to_pop', 'dishy_to_gs']:
            series = val[direction]
            box = ax.boxplot(series,
                             positions=[position],
                             widths=0.5,
                             patch_artist=True,
                             vert=False,
                             zorder=3,
                             showfliers=False,
                             )  # Ensure boxplots are drawn on top
            color = colors[idx % 2]
            ylabels.append(config[f'{name}-{direction}']['label'])

            plt.setp(box['boxes'], facecolor=color)
            plt.setp(box['medians'], color='black', linewidth=1.5)

            idx += 1
            position += 1.5
        group_positions.append((group_start, position - 1.5))
        group_idx += 1
    # Customize the plot
    ax.set_ylim(-0.5, position - 0.5)
    # ax.set_xlim(0, 260)
    ax.set_yticks(np.arange(0, position, 1.5))
    ax.set_yticklabels(ylabels)
    ax.set_xlabel('RTT (ms)')
    ax.set_title('Bent-Pipe Latency Breakdown')

    # Add vertical lines
    for x in range(-25, 175 + 1, 25):
        ax.axvline(x=x, color='gray', linestyle='--', alpha=0.5)

    for i, (start, end) in enumerate(group_positions):
        # mid = (start + end) / 2
        names = list(datasource.keys())
        ax.text(-0.05, end + 0.5, f'{names[i].upper()}',
                verticalalignment='center',
                horizontalalignment='right',
                transform=ax.get_yaxis_transform(),
                fontweight='bold')

    plt.tight_layout()
    # plt.show()
    plt.savefig(output_path)


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    data = get_datasource()
    plot_swimlane_chart(data, config=config, output_path=os.path.join(OUTPUT_DIR, 'bent_pipe_breakdown.png'))


if __name__ == '__main__':
    main()
