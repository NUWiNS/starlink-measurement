import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from separate_dataset import main as separate_dataset_main

from parse_nuttcp_data_to_csv import main as parse_nuttcp_data_to_csv_main
from parse_iperf_data_to_csv import main as parse_iperf_data_to_csv_main
from parse_weather_area_data_to_csv import main as parse_weather_area_data_to_csv_main
from append_weather_area_to_tput_dataset import main as append_weather_area_to_tput_dataset_main
from parse_ping_result_to_csv import main as parse_ping_result_to_csv_main
from parse_traceroute_data_to_csv import main as parse_traceroute_data_to_csv_main
from parse_nslookup_data_to_csv import main as parse_nslookup_data_to_csv_main
from parse_xcal_tput_to_csv import main as parse_xcal_tput_to_csv_main
from calibrate_weather_area_data import main as calibrate_weather_area_data_main
from append_tech_to_latency_dataset import main as append_tech_to_latency_dataset_main
from append_weather_area_to_latency_dataset import main as append_weather_area_to_latency_dataset_main

from plot_cdf_throughput import main as plot_cdf_throughput_main
from plot_rtt_from_csv import main as plot_rtt_from_csv_main
from plot_traceroute import main as plot_traceroute_main

def parsing():
    parse_nuttcp_data_to_csv_main()
    parse_iperf_data_to_csv_main()
    # parse_ping_result_to_csv_main()
    # parse_traceroute_data_to_csv_main()
    # parse_nslookup_data_to_csv_main()
    # parse_xcal_tput_to_csv_main()
    # append_tech_to_latency_dataset_main()
    
    # parse_weather_area_data_to_csv_main()
    # calibrate_weather_area_data_main()
    append_weather_area_to_tput_dataset_main()
    # append_weather_area_to_latency_dataset_main()
    pass

def plotting():
    plot_cdf_throughput_main()
    plot_rtt_from_csv_main()
    plot_traceroute_main()


def main():
    # separate_dataset_main()

    parsing()

    # plotting()


if __name__ == '__main__':
    main()
