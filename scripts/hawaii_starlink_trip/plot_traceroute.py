import logging
import os
import sys

from scripts.logging_utils import create_logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR, OUTPUT_DIR
import pandas as pd

base_dir = os.path.join(DATASET_DIR, "hawaii_starlink_trip/traceroute")
output_dir = os.path.join(OUTPUT_DIR, "hawaii_starlink_trip/plots")

logger = create_logger(
    'traceroute_plotting',
    filename=os.path.join(output_dir, 'plot_traceroute.log'),
    filemode='w',
    formatter=logging.Formatter()
)


def main():
    df = pd.read_csv(os.path.join(base_dir, 'starlink_traceroute.csv'))
    # find groups that have less than 3 hops
    df_groups = df.groupby('start_time')
    for start_time, group_df in df_groups:
        if group_df['hop_number'].max() < 3:
            logger.info(f'[CAUTION] group {start_time} has less than 3 hops')

    for hop in range(1, 8):
        hop_df = df[df['hop_number'] == hop]
        # exclude exceptional cases
        hop_df = hop_df[hop_df['exception'].isna()]

        unique_ip = hop_df["ip"].unique()
        results = []
        for ip in unique_ip:
            if pd.isna(ip):
                ip_df = hop_df[hop_df['ip'].isna()].copy()
            else:
                ip_df = hop_df[hop_df['ip'] == ip].copy()
            # get freq of this ip
            ip_freq = ip_df.shape[0]
            mean_rtt = pd.to_numeric(ip_df['rtt_ms'], errors='coerce').dropna()
            results.append(f'{ip} ({ip_freq}) - {mean_rtt.mean():.2f} ms')
        logger.info(f'hop {hop} count ({len(hop_df["ip"])}), unique: {results}')


if __name__ == '__main__':
    main()
