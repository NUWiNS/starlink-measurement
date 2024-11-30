import glob
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from scripts.weather_area_type_query_utils import TypeIntervalQueryUtil
from scripts.logging_utils import create_logger
from scripts.constants import CommonField
from scripts.hawaii_starlink_trip.configs import ROOT_DIR

others_dir = os.path.join(ROOT_DIR, 'others')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
ping_dir = os.path.join(ROOT_DIR, 'ping')

logger = create_logger('append_weather_area_to_latency_dataset',
                       filename=os.path.join(tmp_data_path, 'append_weather_area_to_latency_dataset.log'))

def drop_cols_before_appending(df: pd.DataFrame):
    if 'weather' in df.columns:
        df.drop(columns=['weather'], inplace=True)
    if 'area' in df.columns:
        df.drop(columns=['area'], inplace=True)
    return df

def append_weather_area_to_rtt_traces(df: pd.DataFrame, weather_query_util: TypeIntervalQueryUtil, area_query_util: TypeIntervalQueryUtil):
    df[CommonField.LOCAL_DT] = pd.to_datetime(df[CommonField.LOCAL_DT], format="ISO8601")
    df = drop_cols_before_appending(df)
    for idx, row in df.iterrows():
        df.at[idx, 'weather'] = weather_query_util.query(row[CommonField.LOCAL_DT])
        df.at[idx, 'area'] = area_query_util.query(row[CommonField.LOCAL_DT])
    return df   

def main():
    weather_csv_path = os.path.join(others_dir, 'weather.csv')
    logger.info(f'Loading weather data from {weather_csv_path}')
    area_csv_path = os.path.join(others_dir, 'area.csv')
    logger.info(f'Loading area data from {area_csv_path}')
    
    weather_df = pd.read_csv(weather_csv_path)
    weather_df[CommonField.LOCAL_DT] = pd.to_datetime(weather_df[CommonField.LOCAL_DT], format="ISO8601")
    weather_query_util = TypeIntervalQueryUtil(weather_df[[CommonField.LOCAL_DT, 'value']].values.tolist())

    area_df = pd.read_csv(area_csv_path)
    area_df[CommonField.LOCAL_DT] = pd.to_datetime(area_df[CommonField.LOCAL_DT], format="ISO8601")
    area_query_util = TypeIntervalQueryUtil(area_df[[CommonField.LOCAL_DT, 'value']].values.tolist())

    for rtt_csv_file in glob.glob(os.path.join(ping_dir, '*_ping.csv')):
        logger.info(f'Appending weather and area data to {rtt_csv_file}')
        rtt_df = pd.read_csv(rtt_csv_file)
        rtt_df = append_weather_area_to_rtt_traces(rtt_df, weather_query_util, area_query_util)
        rtt_df.to_csv(rtt_csv_file, index=False)
        logger.info(f'Finished processing {rtt_csv_file}, weather and area data appended')

if __name__ == '__main__':
    main()
