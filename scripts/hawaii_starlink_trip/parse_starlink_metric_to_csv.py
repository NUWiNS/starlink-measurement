import os
import sys

import pandas as pd

from scripts.hawaii_starlink_trip.configs import ROOT_DIR

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.starlink_metric_utils import find_starlink_metric_files, parse_starlink_metric_logs

base_dir = os.path.join(ROOT_DIR, 'raw/dish_metrics')


def main():
    all_metric_files = find_starlink_metric_files(base_dir)
    print(f"Found {len(all_metric_files)} metric files.")

    excluded_files = []
    total_df = pd.DataFrame()

    for file in all_metric_files:
        try:
            with open(file) as f:
                content = f.read()
                extracted_data = parse_starlink_metric_logs(content)
                if not extracted_data:
                    print(f"Error reading {file}: No data extracted.")
                    excluded_files.append(file)
                    continue

                df = pd.DataFrame(extracted_data)

                csv_file_path = file.replace('.out', '.csv')
                df.to_csv(csv_file_path, index=False)
                print(f"Extracted data is saved to {csv_file_path}")

                total_df = pd.concat([total_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print('Total files:', len(all_metric_files))
    output_dir = os.path.join(ROOT_DIR, 'starlink')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    total_csv_file = os.path.join(output_dir, 'starlink_metric.csv')
    total_df.to_csv(total_csv_file, index=False)
    print(f'Saved all the metric data to csv file: {output_dir}')


if __name__ == '__main__':
    main()
