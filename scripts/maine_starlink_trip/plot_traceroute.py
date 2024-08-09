import os
import sys

from scripts.logging_utils import create_logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import DATASET_DIR, OUTPUT_DIR
import pandas as pd

base_dir = os.path.join(DATASET_DIR, "maine_starlink_trip/traceroute")
output_dir = os.path.join(OUTPUT_DIR, "maine_starlink_trip/plots")

logger = create_logger('traceroute_plotting', filename=os.path.join(output_dir, 'plot_traceroute.log'))


def main():
    df = pd.read_csv(os.path.join(base_dir, 'starlink_traceroute.csv'))
    for hop in range(1, 6):
        hop_df = df[df['hop_number'] == hop]

        unique_ip = hop_df["ip"].unique()
        results = []
        for ip in unique_ip:
            ip_df = hop_df[hop_df['ip'] == ip]
            mean_rtt = pd.to_numeric(ip_df['rtt_ms'], errors='coerce').dropna()
            results.append(f'{ip} - {mean_rtt.mean():.2f} ms')
        logger.info(f'hop {hop} ip unique: {results}')


if __name__ == '__main__':
    main()
