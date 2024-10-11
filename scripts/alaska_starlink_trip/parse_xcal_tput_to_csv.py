# Filter out the rows where the timestamp are in the range of each tput measurement


import glob
from os import path
import re
import pandas as pd
from scripts.alaska_starlink_trip.configs import ROOT_DIR
from scripts.constants import DATASET_DIR, OUTPUT_DIR


def collect_periods_of_tput_measurements(base_dir: str, protocol: str, direction: str) -> list[tuple[str, str]]:
    # find all the files starting with {protocol}_{direction}*.out
    pattern = f'{protocol}_{direction}*.out'
    files = glob.glob(path.join(base_dir, pattern))

    def extract_period_from_file(file: str) -> tuple[str, str]:
        with open(file, 'r') as f:
            lines = f.readlines()
            start_time_row = lines[0].strip()
            end_time_row = lines[-1].strip() 
            # use regex to extract the timestamp
            start_time = re.search(r'Start time: (\d+)', start_time_row).group(1)
            end_time = re.search(r'End time: (\d+)', end_time_row).group(1)
            if start_time and end_time:
                return (start_time, end_time)
            else:
                raise ValueError(f'Failed to extract start and end time from {file}')
    periods = []
    # read each file and extract the first and last line as the start and end timestamp
    # e.g., Start time: 1718727965324\n * End time: 1718727984122
    for file in files:
        period = extract_period_from_file(file)
        periods.append(period)

    return periods

def read_daily_xcal_data(base_dir: str, date: str, location: str, operator: str) -> pd.DataFrame:
    pass

def filter_xcal_logs(df_xcal_logs: pd.DataFrame, periods: list[tuple[str, str]]) -> pd.DataFrame:
    pass

def main():
    # deal with one location and one operator per time
    operator = 'starlink'
    location = 'alaska'
    dates = ['20240618']

    xcal_log_dir = path.join(DATASET_DIR, 'xcal')
    measurement_log_dir = path.join(ROOT_DIR, f'raw/{operator}_merged')
    output_dir = path.join(ROOT_DIR, f'xcal')

    periods_of_tcp_dl = collect_periods_of_tput_measurements(base_dir=measurement_log_dir, protocol='tcp', direction='downlink')
    periods_of_tcp_ul = collect_periods_of_tput_measurements(base_dir=measurement_log_dir, protocol='tcp', direction='uplink')
    periods_of_udp_dl = collect_periods_of_tput_measurements(base_dir=measurement_log_dir, protocol='udp', direction='downlink')
    periods_of_udp_ul = collect_periods_of_tput_measurements(base_dir=measurement_log_dir, protocol='udp', direction='uplink')

    df_xcal_all_logs = pd.DataFrame()
    for date in dates:
        df_xcal_daily_data = read_daily_xcal_data(base_dir=xcal_log_dir, date=date, location=location, operator=operator)
        df_xcal_all_logs = pd.concat([df_xcal_all_logs, df_xcal_daily_data])

    all_tput_periods = periods_of_tcp_dl + periods_of_tcp_ul + periods_of_udp_dl + periods_of_udp_ul
    df_xcal_tput_logs = filter_xcal_logs(df_xcal_all_logs, periods=all_tput_periods)
    df_xcal_tput_logs.to_csv(path.join(output_dir, f'{operator}_smart_tput.csv'), index=False)


if __name__ == "__main__":
    main()