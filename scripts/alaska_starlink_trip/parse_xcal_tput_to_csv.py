# Filter out the rows where the timestamp are in the range of each tput measurement


from os import path
import pandas as pd

def collect_periods_of_tput_measurements(base_dir: str, protocol: str, direction: str) -> list[tuple[str, str]]:
    pass

def read_daily_xcal_data(base_dir: str, date: str, location: str, operator: str) -> pd.DataFrame:
    pass

def filter_xcal_logs(df_xcal_logs: pd.DataFrame, periods: list[tuple[str, str]]) -> pd.DataFrame:
    pass

def main():
    # deal with one location and one operator per time
    operator = 'starlink'
    location = 'alaska'
    dates = ['20240618']

    periods_of_tcp_dl = collect_periods_of_tput_measurements(base_dir='', protocol='tcp', direction='downlink')
    periods_of_tcp_ul = collect_periods_of_tput_measurements(base_dir='', protocol='tcp', direction='uplink')
    periods_of_udp_dl = collect_periods_of_tput_measurements(base_dir='', protocol='udp', direction='downlink')
    periods_of_udp_ul = collect_periods_of_tput_measurements(base_dir='', protocol='udp', direction='uplink')

    df_xcal_all_logs = pd.DataFrame()
    for date in dates:
        df_xcal_daily_data = read_daily_xcal_data(base_dir='', date=date, location=location, operator=operator)
        df_xcal_all_logs = pd.concat([df_xcal_all_logs, df_xcal_daily_data])

    all_tput_periods = periods_of_tcp_dl + periods_of_tcp_ul + periods_of_udp_dl + periods_of_udp_ul
    df_xcal_tput_logs = filter_xcal_logs(df_xcal_all_logs, periods=all_tput_periods)
    base_output_dir = 'datasets/alaska_starlink_trip/processed'
    df_xcal_tput_logs.to_csv(path.join(base_output_dir, f'{operator}_xcal_tput.csv'), index=False)


if __name__ == "__main__":
    main()