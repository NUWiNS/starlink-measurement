import glob
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import CommonField, XcalField
from scripts.hawaii_starlink_trip.configs import ROOT_DIR
from scripts.weather_area_type_query_utils import TypeIntervalQueryUtil
from scripts.logging_utils import create_logger

tput_dir = os.path.join(ROOT_DIR, 'throughput')
xcal_tput_dir = os.path.join(ROOT_DIR, 'xcal')

others_dir = os.path.join(ROOT_DIR, 'others')
tmp_data_path = os.path.join(ROOT_DIR, 'tmp')
ping_dir = os.path.join(ROOT_DIR, 'ping')
logger = create_logger('append_weather_area_to_tput_dataset',
                       filename=os.path.join(tmp_data_path, 'append_weather_area_to_tput_dataset.log'))

def drop_cols_before_appending(tput_df: pd.DataFrame):
    if 'weather' in tput_df.columns:
        tput_df.drop(columns=['weather'], inplace=True)
    if 'area' in tput_df.columns:
        tput_df.drop(columns=['area'], inplace=True)
    return tput_df

def append_weather_area_to_xcal_tput_traces(
    xcal_tput_dir: str, 
    weather_query_util: TypeIntervalQueryUtil, 
    area_query_util: TypeIntervalQueryUtil
):
    all_tput_csv_files = glob.glob(os.path.join(xcal_tput_dir, '*_xcal_smart_tput.csv'))
    logger.info(f'Found {len(all_tput_csv_files)} XCAL throughput CSV files')

    for tput_csv_file in all_tput_csv_files:
        logger.info(f'Processing {tput_csv_file}')
        tput_df = pd.read_csv(tput_csv_file)

        tput_df = drop_cols_before_appending(tput_df)

        for idx, row in tput_df.iterrows():
            row_time = pd.to_datetime(row[XcalField.LOCAL_TIME], format="ISO8601")
            tput_df.at[idx, 'weather'] = weather_query_util.query(row_time)
            tput_df.at[idx, 'area'] = area_query_util.query(row_time)

        tput_df.to_csv(tput_csv_file, index=False)
        logger.info(f'Finished processing {tput_csv_file}, weather and area data appended')



def append_weather_area_to_app_tput_traces(
    tput_dir: str, 
    weather_query_util: TypeIntervalQueryUtil, 
    area_query_util: TypeIntervalQueryUtil
):
    all_tput_csv_files = glob.glob(os.path.join(tput_dir, '*.csv'))
    logger.info(f'Found {len(all_tput_csv_files)} throughput CSV files')

    for tput_csv_file in all_tput_csv_files:
        logger.info(f'Processing {tput_csv_file}')

        tput_df = pd.read_csv(tput_csv_file)
        tput_df['time'] = pd.to_datetime(tput_df['time'], format="ISO8601")

        tput_df = drop_cols_before_appending(tput_df)
        for idx, row in tput_df.iterrows():
            tput_df.at[idx, 'weather'] = weather_query_util.query(row['time'])
            tput_df.at[idx, 'area'] = area_query_util.query(row['time'])

        tput_df.to_csv(tput_csv_file, index=False)
        logger.info(f'Finished processing {tput_csv_file}, weather and area data appended')


def append_weather_area_to_rtt_traces(
        df: pd.DataFrame, 
        weather_query_util: TypeIntervalQueryUtil, 
        area_query_util: TypeIntervalQueryUtil
    ):
    df['time'] = pd.to_datetime(df['time'], format="ISO8601")
    df = drop_cols_before_appending(df)
    for idx, row in df.iterrows():
        df.at[idx, 'weather'] = weather_query_util.query(row['time'])
        df.at[idx, 'area'] = area_query_util.query(row['time'])
    return df   

def main():
    weather_csv_path = os.path.join(others_dir, 'weather.csv')
    logger.info(f'Loading weather data from {weather_csv_path}')
    area_csv_path = os.path.join(others_dir, 'area.csv')
    logger.info(f'Loading area data from {area_csv_path}')
    
    weather_df = pd.read_csv(weather_csv_path)
    weather_df[CommonField.LOCAL_DT] = pd.to_datetime(weather_df[CommonField.UTC_TS])
    weather_query_util = TypeIntervalQueryUtil(weather_df[[CommonField.LOCAL_DT, 'value']].values.tolist())

    area_df = pd.read_csv(area_csv_path)
    area_df[CommonField.LOCAL_DT] = pd.to_datetime(area_df[CommonField.UTC_TS])
    area_query_util = TypeIntervalQueryUtil(area_df[[CommonField.LOCAL_DT, 'value']].values.tolist())

    append_weather_area_to_app_tput_traces(
        tput_dir=tput_dir, 
        weather_query_util=weather_query_util, 
        area_query_util=area_query_util
    )

    append_weather_area_to_xcal_tput_traces(
        xcal_tput_dir=xcal_tput_dir, 
        weather_query_util=weather_query_util, 
        area_query_util=area_query_util
    )

    for rtt_csv_file in glob.glob(os.path.join(ping_dir, '*_ping.csv')):
        logger.info(f'Appending weather and area data to {rtt_csv_file}')
        rtt_df = pd.read_csv(rtt_csv_file)
        rtt_df = append_weather_area_to_rtt_traces(rtt_df, weather_query_util, area_query_util)
        rtt_df.to_csv(rtt_csv_file, index=False)
        logger.info(f'Finished processing {rtt_csv_file}, weather and area data appended')

if __name__ == '__main__':
    main()
