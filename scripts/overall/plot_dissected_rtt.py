import json
import os
import sys
import unittest
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

current_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(current_dir, '../../datasets')
OUTPUT_DIR = os.path.join(current_dir, './outputs')


def get_rtt_diff_series_between_hops(df: pd.DataFrame, hop1: int, hop2: int):
    """
    Get the RTT difference series between two hops
    :param df:
    :param hop1:
    :param hop2: -1 means the last hop
    :return:
    """
    if hop1 <= 0:
        raise ValueError('hop1 must be greater than 0')
    if hop2 != -1 and hop2 < hop1:
        raise ValueError('hop2 must be greater than hop1')

    _df = df[df['rtt_ms'].notna() & df['exception'].isna()]
    hop_df = _df.groupby(['start_time', 'hop_number']).agg({'rtt_ms': 'mean'}).reset_index()
    result = []
    for _, group in hop_df.groupby('start_time'):
        if hop2 == -1:
            hop2 = group['hop_number'].max()
        hop1_rtt_values = group[group['hop_number'] == hop1]['rtt_ms'].values
        hop2_rtt_values = group[group['hop_number'] == hop2]['rtt_ms'].values
        if len(hop1_rtt_values) == 0 or len(hop2_rtt_values) == 0:
            continue
        rtt_diff = hop2_rtt_values[0] - hop1_rtt_values[0]
        result.append(rtt_diff)
    return pd.Series(result).dropna().astype(float)


def get_largest_hop_number_of_given_ip_prefix(df: pd.DataFrame, ip_prefix: str):
    _df = df.dropna(subset=['ip'])
    res_df = _df[_df['ip'].str.startswith(ip_prefix)]
    if res_df.empty:
        return -1
    return res_df['hop_number'].max()


# class UnitTest(unittest.TestCase):
#     def test_get_largest_hop_number_of_given_ip_prefix(self):
#         df = pd.DataFrame({
#             'start_time': ['2021-08-01 00:00:00', '2021-08-01 00:00:00', '2021-08-01 00:00:00'],
#             'ip': ['206.224.64.190', '206.224.64.185', '206.224.64.63'],
#             'hop_number': [4, 5, 6],
#             'exception': [None, None, None],
#         })
#         self.assertEqual(6, get_largest_hop_number_of_given_ip_prefix(df, '206.224'))
#
#     def test_get_rtt_diff_with_last_hop(self):
#         df = pd.DataFrame({
#             'start_time': ['2021-08-01 00:00:00', '2021-08-01 00:00:00', '2021-08-01 00:00:00'],
#             'hop_number': [1, 2, 3],
#             'rtt_ms': [3, 9, 13],
#             'exception': [None, None, None],
#         })
#         rtt_diff_series = get_rtt_diff_series_between_hops(df, hop1=1, hop2=-1)
#         self.assertEqual(10, rtt_diff_series[0])


def get_datasource_of_bent_pipe_rtt_breakdown() -> dict:
    """
    Get the datasource of bent-pipe rtt breakdown
    - d2g: dishy to gs
    - g2p: gs to pop
    :return:
    """
    # ALASKA
    ak_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'alaska_starlink_trip/traceroute/starlink_traceroute.csv'))
    ak_d2g = get_rtt_diff_series_between_hops(ak_tr_df, hop1=1, hop2=2)
    ak_g2p = get_rtt_diff_series_between_hops(ak_tr_df, hop1=2, hop2=3)

    # HAWAII
    hi_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'hawaii_starlink_trip/traceroute/starlink_traceroute.csv'))
    hi_d2g = get_rtt_diff_series_between_hops(hi_tr_df, hop1=1, hop2=2)
    hi_g2p = get_rtt_diff_series_between_hops(hi_tr_df, hop1=2, hop2=3)

    # MAINE
    me_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'maine_starlink_trip/traceroute/starlink_traceroute.csv'))
    me_d2g = get_rtt_diff_series_between_hops(me_tr_df, hop1=1, hop2=2)
    me_g2p = get_rtt_diff_series_between_hops(me_tr_df, hop1=2, hop2=3)

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


def get_datasource_of_overall_rtt_breakdown() -> dict:
    """
    Get the datasource of overall rtt breakdown
    - d2g: dishy to gs
    - g2p: gs to pop
    - p2p: pop to endpoint
    :return:
    """
    # ALASKA
    ak_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'alaska_starlink_trip/traceroute/starlink_traceroute.csv'))
    ak_d2g = get_rtt_diff_series_between_hops(ak_tr_df, hop1=1, hop2=2)
    ak_g2p = get_rtt_diff_series_between_hops(ak_tr_df, hop1=2, hop2=3)
    ak_p2p = get_rtt_diff_series_between_hops(ak_tr_df, hop1=3, hop2=-1)

    # HAWAII
    hi_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'hawaii_starlink_trip/traceroute/starlink_traceroute.csv'))
    hi_d2g = get_rtt_diff_series_between_hops(hi_tr_df, hop1=1, hop2=2)
    hi_g2p = get_rtt_diff_series_between_hops(hi_tr_df, hop1=2, hop2=3)
    hi_p2p = get_rtt_diff_series_between_hops(hi_tr_df, hop1=3, hop2=-1)

    # MAINE
    me_tr_df = pd.read_csv(os.path.join(DATA_DIR, 'maine_starlink_trip/traceroute/starlink_traceroute.csv'))
    me_d2g = get_rtt_diff_series_between_hops(me_tr_df, hop1=1, hop2=2)
    me_g2p = get_rtt_diff_series_between_hops(me_tr_df, hop1=2, hop2=3)
    me_p2p = get_rtt_diff_series_between_hops(me_tr_df, hop1=3, hop2=-1)

    return {
        'alaska': {
            'dishy_to_gs': ak_d2g,
            'gs_to_pop': ak_g2p,
            'pop_to_endpoint': ak_p2p
        },
        'hawaii': {
            'dishy_to_gs': hi_d2g,
            'gs_to_pop': hi_g2p,
            'pop_to_endpoint': hi_p2p
        },
        'maine': {
            'dishy_to_gs': me_d2g,
            'gs_to_pop': me_g2p,
            'pop_to_endpoint': me_p2p
        },
    }


config = {
    'alaska-dishy_to_gs': {
        'label': 'Dishy<->GS',
        'color': 'g',
    },
    'alaska-gs_to_pop': {
        'label': 'GS<->PoP',
        'color': 'b',
    },
    'alaska-pop_to_endpoint': {
        'label': 'PoP<->Endpoint',
        'color': 'purple',
    },
    'hawaii-dishy_to_gs': {
        'label': 'Dishy<->GS',
        'color': 'g',
    },
    'hawaii-gs_to_pop': {
        'label': 'GS<->PoP',
        'color': 'b',
    },
    'hawaii-pop_to_endpoint': {
        'label': 'PoP<->Endpoint',
        'color': 'purple',
    },
    'maine-dishy_to_gs': {
        'label': 'Dishy<->GS',
        'color': 'g',
    },
    'maine-gs_to_pop': {
        'label': 'GS<->PoP',
        'color': 'b',
    },
    'maine-pop_to_endpoint': {
        'label': 'PoP<->Endpoint',
        'color': 'purple',
    },
}


def plot_rtt_breakdown_swimlane_chart(datasource: dict, directions: List[str], config: dict,
                                      output_path: str = 'swimlane.png', x_step: int = None):
    """
    Plot the swimlane chart of RTT breakdown and return statistics for each boxplot
    :param datasource:
    :param directions: can be ['pop_to_endpoint', 'gs_to_pop'] or ['pop_to_endpoint', 'gs_to_pop', 'dishy_to_gs']
    :param config:
    :param output_path:
    :param x_step: Step size for x-axis grid lines
    :return: Dictionary containing statistics for each location and direction
    """
    group_colors = ['#f0f0f0', '#e0e0e0']
    fig, ax = plt.subplots(figsize=(5, 3.5))

    stats = {}  # Dictionary to store statistics
    
    group_idx = 0
    idx = 0
    position = 0
    ylabels = []
    group_positions = []

    group_item_count = len(directions)

    for name, val in datasource.items():
        stats[name] = {}  # Initialize stats for this location
        group_start = position
        
        # Add background rectangle for the group
        group_height = group_item_count * 1.38
        rect = Rectangle((-25, position - 0.5),
                         200,
                         group_height,
                         facecolor=group_colors[group_idx % 2],
                         edgecolor='none',
                         zorder=0)  # Ensure it's drawn behind the boxplots

        ax.add_patch(rect)

        # reverse the directions to make the chart more readable
        reversed_directions = directions[::-1]
        for direction in reversed_directions:
            series = val[direction]
            box = ax.boxplot(series,
                             positions=[position],
                             widths=0.5,
                             patch_artist=True,
                             vert=False,
                             zorder=3,
                             showfliers=False,
                             )  # Ensure boxplots are drawn on top
            
            # Calculate and store statistics
            stats[name][direction] = {
                'median': np.median(series),
                'min': np.min(series),
                'max': np.max(series),
                'mean': np.mean(series),
                'std': np.std(series),
                'p5': np.percentile(series, 5),
                'p25': np.percentile(series, 25),
                'p75': np.percentile(series, 75),
                'p95': np.percentile(series, 95),
                'count': len(series)
            }
            
            conf = config[f'{name}-{direction}']
            ylabels.append(conf['label'])

            plt.setp(box['boxes'], facecolor=conf['color'])
            plt.setp(box['medians'], color='black', linewidth=1.5)

            idx += 1
            position += 1.5
        group_positions.append((group_start, position - 1.5))
        group_idx += 1
    # Customize the plot
    ax.set_ylim(-0.5, position - 0.5)
    
    # Set both grid lines and x-axis ticks using x_step
    if x_step:
        # Set vertical grid lines
        for x in range(-25, 175 + 1, x_step):
            ax.axvline(x=x, color='gray', linestyle='--', alpha=0.5)
        
        # Set x-axis ticks
        xticks = np.arange(-25, 175 + 1, x_step)
        ax.set_xticks(xticks)
        ax.set_xticklabels([str(x) for x in xticks])
    
    ax.set_yticks(np.arange(0, position, 1.5))
    ax.set_yticklabels(ylabels)
    ax.set_xlabel('RTT (ms)')
    ax.set_title('Bent-Pipe Latency Breakdown')

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
    
    return stats

def save_stats_to_json(stats: dict, output_path: str, indent: int = 4):
    with open(output_path, 'w') as f:
        json.dump(stats, f, indent=indent)

def plot_bent_pipe_rtt_breakdown(x_step: int = 25):
    data = get_datasource_of_bent_pipe_rtt_breakdown()
    output_filepath = os.path.join(OUTPUT_DIR, 'bent_pipe_latency_breakdown.png')
    stats = plot_rtt_breakdown_swimlane_chart(data,
                                              directions=['dishy_to_gs', 'gs_to_pop'],
                                              config=config,
                                              output_path=output_filepath,
                                              x_step=x_step)
    save_stats_to_json(stats, output_filepath.replace('.png', '.json'))
    return stats


def plot_overall_rtt_breakdown(x_step: int = 25):
    data = get_datasource_of_overall_rtt_breakdown()
    output_filepath = os.path.join(OUTPUT_DIR, 'overall_latency_breakdown.png')
    stats = plot_rtt_breakdown_swimlane_chart(data,
                                              directions=['dishy_to_gs', 'gs_to_pop', 'pop_to_endpoint'],
                                              config=config,
                                              output_path=output_filepath,
                                              x_step=x_step)
    save_stats_to_json(stats, output_filepath.replace('.png', '.json'))
    return stats


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    bent_pipe_stats = plot_bent_pipe_rtt_breakdown()
    overall_stats = plot_overall_rtt_breakdown()
    
    # You can now use these stats as needed
    # Example of accessing stats:
    # print(bent_pipe_stats['alaska']['dishy_to_gs']['median'])


if __name__ == '__main__':
    main()
