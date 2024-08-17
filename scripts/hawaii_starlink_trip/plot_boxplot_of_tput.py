import os
import sys

import pandas as pd
from matplotlib import pyplot as plt

from scripts.logging_utils import create_logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR, OUTPUT_DIR

base_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/throughput')
tmp_dir = os.path.join(DATASET_DIR, 'alaska_starlink_trip/tmp')
output_dir = os.path.join(OUTPUT_DIR, 'alaska_starlink_trip/plots')

logger = create_logger('plot_cdf_throughput', filename=os.path.join(tmp_dir, 'plot_cdf_throughput.log'))


def get_data_frame_from_all_csv(operator: str, protocol: str, direction: str):
    csv_filename = f'{operator}_{protocol}_{direction}.csv'
    file_path = os.path.join(base_dir, csv_filename)
    try:
        df = pd.read_csv(file_path)
        logger.info(f'{csv_filename} count: {df.count()}')
        return df
    except Exception:
        return pd.DataFrame()


def plot_boxplot_of_throughput(df: pd.DataFrame, output_dir='.'):
    data = [
        df[(df['area'] == 'urban') & (df['operator'] != 'starlink')]['throughput_mbps'],
        df[(df['area'] == 'urban') & (df['operator'] == 'starlink')]['throughput_mbps'],
        df[(df['area'] == 'suburban') & (df['operator'] != 'starlink')]['throughput_mbps'],
        df[(df['area'] == 'suburban') & (df['operator'] == 'starlink')]['throughput_mbps'],
        df[(df['area'] == 'rural') & (df['operator'] != 'starlink')]['throughput_mbps'],
        df[(df['area'] == 'rural') & (df['operator'] == 'starlink')]['throughput_mbps']
    ]

    fig, ax = plt.subplots(figsize=(6.5, 4.8))
    x_positions = [0.9, 1.1, 1.5, 1.7, 2.1, 2.3]
    labels = ['Cellular', 'Starlink', 'Cellular', 'Starlink', 'Cellular', 'Starlink']
    cmap20 = plt.cm.tab20
    colors = [cmap20(i) for i in [1, 5]]

    boxes = []
    for idx, ele in enumerate(data):
        box = ax.boxplot(
            ele,
            positions=[x_positions[idx]],
            patch_artist=True,
            widths=0.2,
            labels=[labels[idx]],
            boxprops=dict(facecolor=colors[idx % 2]),
            showfliers=False
        )
        boxes.append(box)

    xtick_lables = ['Urban', 'Suburban', 'Rural']
    xticks = [1, 1.6, 2.2]
    ax.set_xticks(xticks)
    ax.set_xticklabels(xtick_lables)
    ax.set_xlabel('Area Type')
    ax.set_ylabel('Throughput (Mbps)')

    legend_boxes = [boxes[0]['boxes'][0], boxes[1]['boxes'][0]]
    lend_labels = ['Cellular', 'Starlink']
    ax.legend(legend_boxes, lend_labels, loc='upper left')

    plt.xlim(0.7, 2.5)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'boxplot_throughput.png'))
    plt.close(fig)


def read_and_plot_throughput_data(protocol: str, direction: str, output_dir: str):
    print(f"Reading and plotting {protocol}_{direction} with all operator data...")
    att_df = get_data_frame_from_all_csv('att', protocol, direction)
    att_df['operator'] = 'att'
    verizon_df = get_data_frame_from_all_csv('verizon', protocol, direction)
    verizon_df['operator'] = 'verizon'
    starlink_df = get_data_frame_from_all_csv('starlink', protocol, direction)
    starlink_df['operator'] = 'starlink'
    tmobile_df = get_data_frame_from_all_csv('tmobile', protocol, direction)
    tmobile_df['operator'] = 'tmobile'
    combined_df = pd.concat([att_df, verizon_df, starlink_df, tmobile_df], ignore_index=True)

    plot_boxplot_of_throughput(combined_df, output_dir=output_dir)


def main():
    read_and_plot_throughput_data('udp', 'downlink', output_dir=output_dir)


if __name__ == '__main__':
    main()
