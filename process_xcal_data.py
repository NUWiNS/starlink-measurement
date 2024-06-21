import datetime
import pandas as pd
import os
import csv
import pickle
import matplotlib.pyplot as plt
import numpy as np

def convertLoggedTimeToHumanReadable(timestamp_str):
    try:
        timestamp_ms = int(timestamp_str)
        timestamp_s = timestamp_ms / 1000.0
        human_readable_time = datetime.datetime.fromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return human_readable_time
    except ValueError:
        return "Invalid timestamp"

def get_throughput_info(directory, df):
    df['TIME_STAMP'] = pd.to_datetime(df['TIME_STAMP'])
    throughput_files_info = {}
    total_cases = 0
    usable_cases = 0

    for root, _, files in os.walk(directory):
        for file in files:
            if ('uplink' in file or 'downlink' in file) and file.endswith('.out'):
                total_cases += 1
                file_path = os.path.join(root, file)
                start_time, end_time = None, None

                with open(file_path, 'r') as f:
                    lines = f.readlines()
                    for line in lines:
                        if line.startswith('Start time:'):
                            start_time = convertLoggedTimeToHumanReadable(line.split(':')[1].strip())
                        if line.startswith('End time:'):
                            end_time = convertLoggedTimeToHumanReadable(line.split(':')[1].strip())

                if start_time and end_time:
                    start_time_dt = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')
                    end_time_dt = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S.%f')
                    sub_df = df[(df['TIME_STAMP'] >= start_time_dt) & (df['TIME_STAMP'] <= end_time_dt)]
                    if len(sub_df) == 0:
                        # Print message if no traces found
                        print(f"No XCAL traces found during {start_time} and {end_time} in file {file_path}")
                    if len(sub_df) > 0:
                        throughput_files_info[file_path] = sub_df
                        usable_cases += 1
    
    return throughput_files_info, total_cases, usable_cases

def align_xcal_files(xcal_files, throughput_dirs, smart_throughput_cols):
    dictionary_pickle = "./throughput_dictionary_dict.pkl"
    if os.path.exists(dictionary_pickle):
        # Load the dictionary from the pickle file
        with open(dictionary_pickle, 'rb') as file:
            all_info = pickle.load(file)
        print("Dictionary loaded from pickle file")
        return all_info
    else:
        all_info = {}
        total_cases_all = 0
        usable_cases_all = 0
        for xcal_file, throughput_dir in zip(xcal_files, throughput_dirs):
            print(f"processing xcal_file {xcal_file}")
            df = pd.read_pickle(xcal_file)[smart_throughput_cols]
            key = (xcal_file, throughput_dir)
            throughput_files_info, total_cases, usable_cases = get_throughput_info(throughput_dir, df)
            total_cases_all += total_cases
            usable_cases_all += usable_cases

            if throughput_files_info:
                all_info[key] = throughput_files_info

        print(f"total cases: {total_cases_all}")
        print(f"usable cases: {usable_cases_all}")

        # Save the dictionary to the pickle file
        with open(dictionary_pickle, 'wb') as file:
            pickle.dump(all_info, file)
        print("Dictionary saved to pickle file.")

        return all_info

def extract_throughput_data(info):
    traffic_types = {
        'udp_uplink': [],
        'udp_downlink': [],
        'tcp_uplink': [],
        'tcp_downlink': []
    }

    for (xcal_file, throughput_dir), sub_dfs in info.items():
        for file_path, df in sub_dfs.items():
            if 'udp_uplink' in file_path:
                traffic_types['udp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'])
            elif 'udp_downlink' in file_path:
                traffic_types['udp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'])
            elif 'tcp_uplink' in file_path:
                traffic_types['tcp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'])
            elif 'tcp_downlink' in file_path:
                traffic_types['tcp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'])

    return traffic_types


def plot_throughput_cdf_from_xcal(info, title):
    traffic_types = {
        'att_udp_uplink': [],
        'att_udp_downlink': [],
        'att_tcp_uplink': [],
        'att_tcp_downlink': [],
        'verizon_udp_uplink': [],
        'verizon_udp_downlink': [],
        'verizon_tcp_uplink': [],
        'verizon_tcp_downlink': []
    }

    # Extract throughput data
    for (xcal_file, throughput_dir), sub_dfs in info.items():
        for file_path, df in sub_dfs.items():
            if 'att' in file_path:
                if 'udp_uplink' in file_path:
                    traffic_types['att_udp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'udp_downlink' in file_path:
                    traffic_types['att_udp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())
                elif 'tcp_uplink' in file_path:
                    traffic_types['att_tcp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'tcp_downlink' in file_path:
                    traffic_types['att_tcp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())
            elif 'verizon' in file_path:
                if 'udp_uplink' in file_path:
                    traffic_types['verizon_udp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'udp_downlink' in file_path:
                    traffic_types['verizon_udp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())
                elif 'tcp_uplink' in file_path:
                    traffic_types['verizon_tcp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'tcp_downlink' in file_path:
                    traffic_types['verizon_tcp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())

    def plot_cdf(traffic_types, ax, direction):
        for traffic_type, data_list in traffic_types.items():
            if direction in traffic_type and data_list:
                data = np.concatenate(data_list)
                sorted_data = np.sort(data)
                cdf = np.arange(len(sorted_data)) / float(len(sorted_data))
                min_throughput = sorted_data.min()
                max_throughput = sorted_data.max()
                median_throughput = np.median(sorted_data)
                label = (f"{traffic_type}\n"
                         f"Min: {min_throughput:.2f} Mbps\n"
                         f"Max: {max_throughput:.2f} Mbps\n"
                         f"Median: {median_throughput:.2f} Mbps")
                ax.plot(sorted_data, cdf, label=label)
        ax.set_xlabel('Throughput [Mbps]')
        ax.set_ylabel('CDF')
        ax.set_xlim(left=0)
        ax.legend()
        ax.grid(True)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

    plot_cdf(traffic_types, axes[0], 'uplink')
    axes[0].set_title(f'{title} - Uplink')

    plot_cdf(traffic_types, axes[1], 'downlink')
    axes[1].set_title(f'{title} - Downlink')

    plt.tight_layout()
    plt.savefig(f"xcal_throughput_cdf")

def analyze_outliers(info):
    outlier_sub_dfs = []

    for (xcal_file, throughput_dir), sub_dfs in info.items():
        for file_path, df in sub_dfs.items():
            if 'tcp_uplink' in file_path:
                high_throughput_df = df[df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'] > 300]
                if not high_throughput_df.empty:
                    outlier_sub_dfs.append((file_path, high_throughput_df))
                    print(f"Outliers found in file: {file_path}")
                    print(high_throughput_df)

    return outlier_sub_dfs

def plot_throughput_cdf_from_out_files(directory):
    traffic_types = {
        'att_udp_uplink': [],
        'att_udp_downlink': [],
        'att_tcp_uplink': [],
        'att_tcp_downlink': [],
        'verizon_udp_uplink': [],
        'verizon_udp_downlink': [],
        'verizon_tcp_uplink': [],
        'verizon_tcp_downlink': [],
    }

    # Extract throughput data from CSV files
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path)
            df['throughput_mbps'] = df['throughput_mbps'].dropna()

            if 'att' in filename:
                if 'udp_uplink' in filename:
                    traffic_types['att_udp_uplink'].append(df['throughput_mbps'])
                elif 'udp_downlink' in filename:
                    traffic_types['att_udp_downlink'].append(df['throughput_mbps'])
                elif 'tcp_uplink' in filename:
                    traffic_types['att_tcp_uplink'].append(df['throughput_mbps'])
                elif 'tcp_downlink' in filename:
                    traffic_types['att_tcp_downlink'].append(df['throughput_mbps'])
            elif 'verizon' in filename:
                if 'udp_uplink' in filename:
                    traffic_types['verizon_udp_uplink'].append(df['throughput_mbps'])
                elif 'udp_downlink' in filename:
                    traffic_types['verizon_udp_downlink'].append(df['throughput_mbps'])
                elif 'tcp_uplink' in filename:
                    traffic_types['verizon_tcp_uplink'].append(df['throughput_mbps'])
                elif 'tcp_downlink' in filename:
                    traffic_types['verizon_tcp_downlink'].append(df['throughput_mbps'])

    def plot_cdf(traffic_types, ax, direction):
        for traffic_type, data_list in traffic_types.items():
            if direction in traffic_type and data_list:
                data = np.concatenate(data_list)
                sorted_data = np.sort(data)
                cdf = np.arange(len(sorted_data)) / float(len(sorted_data))
                min_throughput = sorted_data.min()
                max_throughput = sorted_data.max()
                median_throughput = np.median(sorted_data)
                label = (f"{traffic_type}\n"
                         f"Min: {min_throughput:.2f} Mbps\n"
                         f"Max: {max_throughput:.2f} Mbps\n"
                         f"Median: {median_throughput:.2f} Mbps")
                ax.plot(sorted_data, cdf, label=label)
        ax.set_xlabel('Throughput [Mbps]')
        ax.set_ylabel('CDF')
        ax.set_xlim(left=0)
        ax.legend()
        ax.grid(True)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

    plot_cdf(traffic_types, axes[0], 'uplink')
    axes[0].set_title('CDF of Throughput - Uplink')

    plot_cdf(traffic_types, axes[1], 'downlink')
    axes[1].set_title('CDF of Throughput - Downlink')

    plt.tight_layout()
    plt.savefig(f"iperf_nuttcp_throughput_cdf.png")

def plot_combined_throughput_cdf(info):
    # Change this to your new throughput directory for the throughput from
    # *.out file
    directory = "starlink-measurement/datasets/maine_starlink_trip/throughput/"
    
    traffic_types_xcal = {
        'att_udp_uplink': [],
        'att_udp_downlink': [],
        'att_tcp_uplink': [],
        'att_tcp_downlink': [],
        'verizon_udp_uplink': [],
        'verizon_udp_downlink': [],
        'verizon_tcp_uplink': [],
        'verizon_tcp_downlink': []
    }

    traffic_types_out = {
        'att_udp_uplink': [],
        'att_udp_downlink': [],
        'att_tcp_uplink': [],
        'att_tcp_downlink': [],
        'verizon_udp_uplink': [],
        'verizon_udp_downlink': [],
        'verizon_tcp_uplink': [],
        'verizon_tcp_downlink': []
    }

    # Extract throughput data from xcal
    for (xcal_file, throughput_dir), sub_dfs in info.items():
        for file_path, df in sub_dfs.items():
            if 'att' in file_path:
                if 'udp_uplink' in file_path:
                    traffic_types_xcal['att_udp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'udp_downlink' in file_path:
                    traffic_types_xcal['att_udp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())
                elif 'tcp_uplink' in file_path:
                    traffic_types_xcal['att_tcp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'tcp_downlink' in file_path:
                    traffic_types_xcal['att_tcp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())
            elif 'verizon' in file_path:
                if 'udp_uplink' in file_path:
                    traffic_types_xcal['verizon_udp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'udp_downlink' in file_path:
                    traffic_types_xcal['verizon_udp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())
                elif 'tcp_uplink' in file_path:
                    traffic_types_xcal['verizon_tcp_uplink'].append(df['Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'].dropna())
                elif 'tcp_downlink' in file_path:
                    traffic_types_xcal['verizon_tcp_downlink'].append(df['Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'].dropna())

    # Extract throughput data from out files
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path)
            df['throughput_mbps'] = df['throughput_mbps'].dropna()

            if 'att' in filename:
                if 'udp_uplink' in filename:
                    traffic_types_out['att_udp_uplink'].append(df['throughput_mbps'])
                elif 'udp_downlink' in filename:
                    traffic_types_out['att_udp_downlink'].append(df['throughput_mbps'])
                elif 'tcp_uplink' in filename:
                    traffic_types_out['att_tcp_uplink'].append(df['throughput_mbps'])
                elif 'tcp_downlink' in filename:
                    traffic_types_out['att_tcp_downlink'].append(df['throughput_mbps'])
            elif 'verizon' in filename:
                if 'udp_uplink' in filename:
                    traffic_types_out['verizon_udp_uplink'].append(df['throughput_mbps'])
                elif 'udp_downlink' in filename:
                    traffic_types_out['verizon_udp_downlink'].append(df['throughput_mbps'])
                elif 'tcp_uplink' in filename:
                    traffic_types_out['verizon_tcp_uplink'].append(df['throughput_mbps'])
                elif 'tcp_downlink' in filename:
                    traffic_types_out['verizon_tcp_downlink'].append(df['throughput_mbps'])

    def plot_cdf(ax, data_xcal, data_out, title_suffix):
        if data_xcal:
            data_xcal = np.concatenate(data_xcal)
            sorted_data_xcal = np.sort(data_xcal)
            cdf_xcal = np.arange(len(sorted_data_xcal)) / float(len(sorted_data_xcal))
            min_throughput_xcal = sorted_data_xcal.min()
            max_throughput_xcal = sorted_data_xcal.max()
            median_throughput_xcal = np.median(sorted_data_xcal)
            label_xcal = (f"XCAL\n"
                          f"Min: {min_throughput_xcal:.2f} Mbps\n"
                          f"Max: {max_throughput_xcal:.2f} Mbps\n"
                          f"Median: {median_throughput_xcal:.2f} Mbps")
            ax.plot(sorted_data_xcal, cdf_xcal, label=label_xcal)
        
        if data_out:
            data_out = np.concatenate(data_out)
            sorted_data_out = np.sort(data_out)
            cdf_out = np.arange(len(sorted_data_out)) / float(len(sorted_data_out))
            min_throughput_out = sorted_data_out.min()
            max_throughput_out = sorted_data_out.max()
            median_throughput_out = np.median(sorted_data_out)
            label_out = (f"OUT\n"
                         f"Min: {min_throughput_out:.2f} Mbps\n"
                         f"Max: {max_throughput_out:.2f} Mbps\n"
                         f"Median: {median_throughput_out:.2f} Mbps")
            ax.plot(sorted_data_out, cdf_out, linestyle='--', label=label_out)
        
        ax.set_title(f'CDF of Throughput - {title_suffix}')
        ax.set_xlabel('Throughput [Mbps]')
        ax.set_ylabel('CDF')
        ax.set_xlim(left=0)
        ax.legend()
        ax.grid(True)

    fig, axes = plt.subplots(2, 4, figsize=(24, 12), sharey=True)

    plot_cdf(axes[0, 0], traffic_types_xcal['att_tcp_uplink'], traffic_types_out['att_tcp_uplink'], 'ATT TCP Uplink')
    plot_cdf(axes[0, 1], traffic_types_xcal['att_tcp_downlink'], traffic_types_out['att_tcp_downlink'], 'ATT TCP Downlink')
    plot_cdf(axes[0, 2], traffic_types_xcal['att_udp_uplink'], traffic_types_out['att_udp_uplink'], 'ATT UDP Uplink')
    plot_cdf(axes[0, 3], traffic_types_xcal['att_udp_downlink'], traffic_types_out['att_udp_downlink'], 'ATT UDP Downlink')
    
    plot_cdf(axes[1, 0], traffic_types_xcal['verizon_tcp_uplink'], traffic_types_out['verizon_tcp_uplink'], 'Verizon TCP Uplink')
    plot_cdf(axes[1, 1], traffic_types_xcal['verizon_tcp_downlink'], traffic_types_out['verizon_tcp_downlink'], 'Verizon TCP Downlink')
    plot_cdf(axes[1, 2], traffic_types_xcal['verizon_udp_uplink'], traffic_types_out['verizon_udp_uplink'], 'Verizon UDP Uplink')
    plot_cdf(axes[1, 3], traffic_types_xcal['verizon_udp_downlink'], traffic_types_out['verizon_udp_downlink'], 'Verizon UDP Downlink')

    plt.tight_layout()
    plt.savefig("combined_throughput_cdf.png")



# Modify from this for the new dataset

xcal_files = [
    # Tell Yufei or Imran to help preprocess the xcal csv files, load the  dfs,
    # and save the dfs as pkl files as follows
    "datasets/maine_starlink_trip/xcal/20240527_MAINE_VERIZON_100MS.pkl",
    "datasets/maine_starlink_trip/xcal/20240528_MAINE_VERIZON_100MS.pkl",
    "datasets/maine_starlink_trip/xcal/20240529_MAINE_VERIZON_100MS.pkl",
    "datasets/maine_starlink_trip/xcal/20240527_MAINE_ATT_100MS.pkl",
    "datasets/maine_starlink_trip/xcal/20240528_MAINE_ATT_100MS.pkl",
    "datasets/maine_starlink_trip/xcal/20240529_MAINE_ATT_100MS.pkl"
]

throughput_dirs = [
    # The throughput directories
    "datasets/maine_starlink_trip/raw/verizon/20240527/",
    "datasets/maine_starlink_trip/raw/verizon/20240528/",
    "datasets/maine_starlink_trip/raw/verizon/20240529/",
    "datasets/maine_starlink_trip/raw/att/20240527/",
    "datasets/maine_starlink_trip/raw/att/20240528/",
    "datasets/maine_starlink_trip/raw/att/20240529/"
]

smart_throughput_cols = [
    # Throughput information from the XCAL dataframe
    'TIME_STAMP',
    'Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]',
    'Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'
]

# Run this to get the aligned XCAL throughput
xcal_throughput_info = align_xcal_files(xcal_files, throughput_dirs, smart_throughput_cols)

# Assuming you already preprocess the .out file into csv and put them into 
# starlink-measurement/datasets/maine_starlink_trip/throughput/
# Plot and compare .out throughput and xcal throughput
plot_combined_throughput_cdf(xcal_throughput_info)
