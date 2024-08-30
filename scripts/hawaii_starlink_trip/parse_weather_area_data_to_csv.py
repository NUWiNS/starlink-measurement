import glob
import os
import sys

import pandas as pd

from scripts.hawaii_starlink_trip.configs import ROOT_DIR

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.logging_utils import create_logger
from scripts.weather_area_type_query_utils import parse_weather_area_type_log

base_dir = os.path.join(ROOT_DIR, 'raw/weather_area')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
others_dataset_dir = os.path.join(ROOT_DIR, 'others')

logger = create_logger('weather_area_parsing',
                       filename=os.path.join(tmp_data_path, 'parse_weather_area_data_to_csv.log'))


def main():
    if not os.path.exists(others_dataset_dir):
        os.makedirs(others_dataset_dir)

    logs = glob.glob(os.path.join(base_dir, '**/weather_area_record.out'))
    logger.info(f'Found {len(logs)} logs')

    weather_data = []
    area_data = []
    for log in logs:
        result = parse_weather_area_type_log(log)
        weather_data.extend(result['weather'])
        area_data.extend(result['area'])

    header = ['time', 'value']
    weather_csv_path = os.path.join(others_dataset_dir, 'weather.csv')
    weather_df = pd.DataFrame(weather_data, columns=header).sort_values(by='time')
    weather_df.to_csv(weather_csv_path, index=False)
    logger.info(f'Saved weather data to CSV files: {weather_csv_path}')

    area_csv_path = os.path.join(others_dataset_dir, 'area.csv')
    area_df = pd.DataFrame(area_data, columns=header).sort_values(by='time')
    area_df.to_csv(area_csv_path, index=False)
    logger.info(f'Saved area data to CSV files: {area_csv_path}')


if __name__ == '__main__':
    main()
