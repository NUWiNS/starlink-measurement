import glob
import os
import sys

import pandas as pd

from scripts.weather_area_type_query_utils import TypeIntervalQueryUtil

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from scripts.logging_utils import create_logger
from scripts.constants import DATASET_DIR

tput_dir = os.path.join(DATASET_DIR, 'maine_starlink_trip/throughput')
others_dir = os.path.join(DATASET_DIR, 'maine_starlink_trip/others')
tmp_data_path = os.path.join(DATASET_DIR, 'maine_starlink_trip/tmp')

logger = create_logger('append_weather_area_to_tput_dataset',
                       filename=os.path.join(tmp_data_path, 'append_weather_area_to_tput_dataset.log'))


def main():
    all_tput_csv_files = glob.glob(os.path.join(tput_dir, '*.csv'))
    logger.info(f'Found {len(all_tput_csv_files)} throughput CSV files')

    weather_csv_path = os.path.join(others_dir, 'weather.csv')
    logger.info(f'Loading weather data from {weather_csv_path}')
    area_csv_path = os.path.join(others_dir, 'area.csv')
    logger.info(f'Loading area data from {area_csv_path}')

    for tput_csv_file in all_tput_csv_files:
        logger.info(f'Processing {tput_csv_file}')

        tput_df = pd.read_csv(tput_csv_file)
        tput_df['time'] = pd.to_datetime(tput_df['time'], format="ISO8601")

        weather_df = pd.read_csv(weather_csv_path)
        weather_df['time'] = pd.to_datetime(weather_df['time'], format="ISO8601")
        weatherIntervalQueryUtil = TypeIntervalQueryUtil(weather_df[['time', 'value']].values.tolist())

        area_df = pd.read_csv(area_csv_path)
        area_df['time'] = pd.to_datetime(area_df['time'], format="ISO8601")
        areaIntervalQueryUtil = TypeIntervalQueryUtil(area_df[['time', 'value']].values.tolist())

        for idx, row in tput_df.iterrows():
            tput_df.at[idx, 'weather'] = weatherIntervalQueryUtil.query(row['time'])
            tput_df.at[idx, 'area'] = areaIntervalQueryUtil.query(row['time'])

        tput_df.to_csv(tput_csv_file, index=False)
        logger.info(f'Finished processing {tput_csv_file}, weather and area data appended')


if __name__ == '__main__':
    main()
