import pandas as pd
import numpy as np 
import math 
import matplotlib.pyplot as plt

def extractNuttcpTput(filename):
    fh = open(filename, "r")
    data = fh.readlines()
    tput_list = []
    start_time = None
    end_time = None
    for d in data:
        d = d.strip()
        if "sec" in d and "bps" in d and "retrans" in d:
            split_list = d.split()
            idx = 0
            for elem in split_list:
                elem = elem.strip()
                elem = elem.lower()
                if "bps" in elem:
                    if elem == "kbps":
                        tput_list.append(float(split_list[idx - 1]) * 1000)
                    elif elem == "bps":
                        tput_list.append(float(split_list[idx - 1]) * 1000000)
                    elif elem == "gbps":
                        tput_list.append(float(split_list[idx - 1]) / 1000)
                    elif elem == "mbps":
                        tput_list.append(float(split_list[idx - 1]))                            
                idx+=1
        elif "Start time:" in d:
            start_time = int(d.strip().split(":")[-1].strip())
        elif "End time:" in d:
            end_time = int(d.strip().split(":")[-1].strip())
    
    if end_time == None:
        end_time = start_time + 120000
    return [start_time / 1000, tput_list, end_time / 1000]

def extractPing(filename):
    fh = open(filename, "r")
    data = fh.readlines()
    ping_list = []
    for d in data:  
        d = d.strip()
        if "bytes" in d and "icmp_seq" in d and "ttl" in d and "time=" in d:
            ping_list.append(float(d.split()[-2].split("=")[1]))
    
    return ping_list

def datetime_to_timestamp(datetime_str):
    from datetime import datetime
    date, time_all = datetime_str.split()
    temp_year = date.split("-")[0]
    temp_month = date.split("-")[1]
    temp_date = date.split("-")[2]
    datetime_string = temp_date + "." + temp_month + "." + temp_year + " " + time_all
    dt_obj = datetime.strptime(datetime_string, '%d.%m.%Y %H:%M:%S.%f')
    sec = dt_obj.timestamp() 
    return sec


def change_to_relative_ts(timestamp):
    global prev_ts
    relative_time = timestamp - prev_ts
    # prev_ts = timestamp
    return relative_time
downlink_path = "/Users/moinakghoshal/Desktop/fall2021/driving_trip_data/chicago_trip/test_2/app/downlink_184550474.out"
uplink_path = "/Users/moinakghoshal/Desktop/fall2021/driving_trip_data/chicago_trip/test_2/app/uplink_184754550.out"
ping_path = "/Users/moinakghoshal/Desktop/fall2021/driving_trip_data/chicago_trip/test_2/app/ping_184958666.out"

xcal_template = "/Users/moinakghoshal/Desktop/fall2021/driving_trip_data/chicago_trip/test_2/xcal/18573612771_20230507_184547_MANUAL_F886006009_template.csv"
xcal_template_df = pd.read_csv(xcal_template)
xcal_template_df.drop(xcal_template_df.tail(8).index,inplace=True)
xcal_template_df["TIME_STAMP"] = xcal_template_df["TIME_STAMP"].apply(datetime_to_timestamp)
xcal_packet = "/Users/moinakghoshal/Desktop/fall2021/driving_trip_data/chicago_trip/test_2/xcal/18573612771_20230507_184547_MANUAL_F886006009_packet.csv"
xcal_packet_df = pd.read_csv(xcal_packet)
xcal_packet_df.drop(xcal_packet_df.tail(8).index,inplace=True)
xcal_packet_df["Absolute Time"] = xcal_packet_df["Absolute Time"].apply(datetime_to_timestamp)

# downlink parse
start_time, tput_list, end_time = extractNuttcpTput(downlink_path)
downlink_xcal_template_df = xcal_template_df.loc[(xcal_template_df['TIME_STAMP'] >= start_time) & (xcal_template_df['TIME_STAMP'] <= end_time)]
downlink_xcal_packet_df = xcal_packet_df.loc[(xcal_packet_df['Absolute Time'] >= start_time) & (xcal_packet_df['Absolute Time'] <= end_time)]
# now find the start of dl trace
# df_filtered = downlink_xcal_packet_df[(downlink_xcal_packet_df['Summary'].str.contains('SYN')) & (downlink_xcal_packet_df['Destination Port'] == 5002)]
# # ideally there should be two packets like this
# if len(df_filtered) != 2:
#     print("WTF!")
# else:
#     last_row = df_filtered.iloc[-1]
#     first_traffic_ts = df_filtered['Absolute Time'].iloc[0]
# # sample df with CA occuring every 100 ms
# downlink_xcal_template_df = downlink_xcal_template_df[["TIME_STAMP", "Lat", "Lon", '5G KPI Total Info DL CA Type']]
# app_df = pd.DataFrame({"TIME_STAMP": [first_traffic_ts + i*0.01 for i in range(len(tput_list))], "APP_Throughput": tput_list})
# merged_df = pd.merge(app_df, downlink_xcal_template_df, on='TIME_STAMP', how='outer')
# print()

# uplink parse 
start_time, nuttcp_tput_list, end_time = extractNuttcpTput(uplink_path)
uplink_xcal_template_df = xcal_template_df.loc[(xcal_template_df['TIME_STAMP'] >= start_time) & (xcal_template_df['TIME_STAMP'] <= end_time)]
uplink_xcal_packet_df = xcal_packet_df.loc[(xcal_packet_df['Absolute Time'] >= start_time) & (xcal_packet_df['Absolute Time'] <= end_time)]
tcp_packets = uplink_xcal_packet_df[(uplink_xcal_packet_df['Protocol'] == 'TCP') & ((uplink_xcal_packet_df['Destination Port'] == 5002) | (uplink_xcal_packet_df['Source Port'] == 5002))]
tcp_acks = tcp_packets[(tcp_packets['Ack Num'] != 0) & (tcp_packets['Ack Num'] != 626465071)].reset_index(drop=True)
start_time_ack = tcp_acks["Absolute Time"].iloc[0]
start_ack_num = tcp_acks["Ack Num"].iloc[0]

first_ts = start_time_ack
bytes_acked_all = 0
tput_list = []
first_time_list = []
for index, row in tcp_acks.iloc[1:, :].iterrows():
    time_diff = row["Absolute Time"] - start_time_ack
    bytes_acked = row["Ack Num"] - start_ack_num
    tput_list.append((bytes_acked * 8 * 0.000001)/time_diff)
    first_time_list.append(row["Absolute Time"] - first_ts)
    start_ack_num = row["Ack Num"]
    start_time_ack = row["Absolute Time"]
first_tput_list = tput_list.copy()
 
prev_ts = tcp_acks["Absolute Time"].iloc[0]
bytes_acked_all = 0
tput_list = []
tcp_acks["Absolute Time"] = tcp_acks["Absolute Time"].apply(change_to_relative_ts)
start_time_ack = tcp_acks["Absolute Time"].iloc[0]
start_ack_num = tcp_acks["Ack Num"].iloc[0]

tcp_acks_mod = tcp_acks[["Absolute Time", "Ack Num"]].reset_index(drop=True)
# create empty dictionary
d = {}

# set the time interval length in seconds
interval_length = 0.01

# round up the maximum time to the nearest interval
max_time = math.ceil(tcp_acks_mod['Absolute Time'].max() / interval_length) * interval_length

# iterate over each time interval and extract the corresponding Ack Num values
for i in range(0, int(max_time / interval_length)):
    start_time = i * interval_length
    end_time = (i + 1) * interval_length
    interval_key = f"{start_time:.2f}-{end_time:.2f}"
    interval_values = tcp_acks_mod.loc[(tcp_acks_mod['Absolute Time'] >= start_time) & (tcp_acks_mod['Absolute Time'] < end_time), 'Ack Num'].tolist()
    d[interval_key] = interval_values

second_time_list = []
start_flag = 1
for time_range in d.keys():
    start, end = time_range.split("-")
    start = float(start)
    end = float(end)
    if start_flag == 1:
        start_flag = 0
        last_ack = d[time_range][-1]
        last_time = end
        continue
    if len(d[time_range]) == 0:
        tput_list.append(0)
        second_time_list.append(end)
        continue
    # elif len(d[time_range]) == 1:
    else:
        tput = ((d[time_range][-1] - last_ack) * 8 * 0.000001) / (end - last_time)
        if tput > 500:
            print()
        last_time = end
        last_ack = d[time_range][-1]

    tput_list.append(tput)
    second_time_list.append(end)
second_tput_list = tput_list.copy()

tput_list = []
third_time_list = []
start_flag = 1
for time_range in d.keys():
    start, end = time_range.split("-")
    start = float(start)
    end = float(end)
    if start_flag == 1:
        start_flag = 0
        last_ack = d[time_range][-1]
        last_time = end
        continue
    if len(d[time_range]) == 0:
        tput_list.append(0)
        third_time_list.append(end)
        continue
    elif len(d[time_range]) == 1: 
        tput = ((d[time_range][-1] - last_ack) * 8 * 0.000001) / (end - last_time)
        last_time = end
        last_ack = d[time_range][-1]
    else:
        tput = ((d[time_range][-1] - d[time_range][0]) * 8 * 0.000001) / 0.01
        last_time = end
        last_ack = d[time_range][-1]
    if tput > 500:
        print()
    tput_list.append(tput)
    third_time_list.append(end)
third_tput_list = tput_list.copy()


fig, ax = plt.subplots(3, 1, figsize=(7, 7))
# ax.plot(first_time_list, first_tput_list, label="first")
ax[0].plot(np.arange(0, len(nuttcp_tput_list)/100, 0.01), nuttcp_tput_list, label='nuttcp')
# empty list to hold averaged throughput values
averaged_throughput_list = []

# loop over the original list and calculate average every 100 values
for i in range(0, len(nuttcp_tput_list), 100):
    averaged_throughput = sum(nuttcp_tput_list[i:i+100])/100
    averaged_throughput_list.append(averaged_throughput)
ax[0].plot(np.arange(0, len(averaged_throughput_list), 1), averaged_throughput_list, label='nuttcp - 1s')
ax[1].plot(second_time_list, second_tput_list, label="current last ack - previous last ack")
ax[2].plot(third_time_list, third_tput_list, label="current last ack - current first ack")
ax[0].set_ylim(0, 1300)
ax[1].set_ylim(0, 1300)
ax[2].set_ylim(0, 1300)
ax[0].legend(loc='upper left')
ax[1].legend(loc='upper left')
ax[2].legend(loc='upper left')
print()


