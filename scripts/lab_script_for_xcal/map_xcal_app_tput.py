import pandas as pd
import numpy as np 
import math 
import matplotlib.pyplot as plt
import glob
import pickle
import os

def extractNuttcpTput(filename):
    fh = open(filename, "r")
    data = fh.readlines()
    tput_list = []
    start_time = None
    end_time = None
    rtt = 0.0
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
        elif "connect" in d:
            split_list = d.split()
            for elem in split_list:
                elem = elem.strip()
                if "RTT" in elem:
                    rtt = float(elem[4:])
    
    if end_time == None:
        end_time = start_time + 120000
    return [start_time / 1000, tput_list, end_time / 1000, rtt]


def extractPing(filename):
    fh = open(filename, "r")
    data = fh.readlines()
    ping_list = []
    start_time = None
    end_time = None
    for d in data:  
        d = d.strip()
        if "bytes" in d and "icmp_seq" in d and "ttl" in d and "time=" in d:
            if "dup" in d.lower():
                continue
            ping_list.append(float(d.split()[-2].split("=")[1]))
    
        elif "Start time:" in d:
            start_time = int(d.strip().split(":")[-1].strip())
        elif "End time:" in d:
            end_time = int(d.strip().split(":")[-1].strip())
    
    if end_time == None:
        end_time = start_time + 120000
    return [start_time / 1000, ping_list, end_time / 1000]

def datetime_to_timestamp(datetime_str):
    from datetime import datetime
    date, time_all = datetime_str.split()
    temp_year = date.split("-")[0]
    temp_month = date.split("-")[1]
    temp_date = date.split("-")[2]

    try:
        datetime_string = temp_date + "." + temp_month + "." + temp_year + " " + time_all
        dt_obj = datetime.strptime(datetime_string, '%d.%m.%Y %H:%M:%S.%f')
    except Exception as ex:
        time_all = time_all + '.000'
        datetime_string = temp_date + "." + temp_month + "." + temp_year + " " + time_all
        dt_obj = datetime.strptime(datetime_string, '%d.%m.%Y %H:%M:%S.%f')
    
    sec = dt_obj.timestamp() 
    return sec

def change_to_relative_ts(timestamp):
    global prev_ts
    relative_time = timestamp - prev_ts
    # prev_ts = timestamp
    return relative_time


#FLAGS
pickle_dump = True
downlink_parse = True
uplink_parse = True
xcal_read = False 

#APP DATA READ
app_directory_list = glob.glob(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\atnt\08_08_2023\app\*")
# print(app_directory_list)

#XCAL READ
if (xcal_read):
    xcal_data_frames = []
    xcal_directory_list = glob.glob(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\atnt\08_08_2023\xcal\*")
    for xcal_csv in xcal_directory_list:
        print(xcal_csv)
        xcal_df = pd.read_csv(xcal_csv, low_memory= False)
        xcal_df = xcal_df.drop(xcal_df.index[-8:])
        xcal_df["TIME_STAMP"] = xcal_df["TIME_STAMP"].apply(datetime_to_timestamp)
        xcal_data_frames.append(xcal_df)

    #XCAL Timestamp process
    xcal_template_df = pd.concat(xcal_data_frames, ignore_index=True)
    #xcal_template_df.to_pickle(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\global_main_verizon_2.pkl")


# xcal_template_df["TIME_STAMP"] = xcal_template_df["TIME_STAMP"].apply(datetime_to_timestamp)
# xcal_template_df.drop(xcal_template_df.tail(8).index,inplace=True)
if (xcal_read == False):
    xcal_template_df = pd.read_pickle (r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\atnt\08_08_2023\pkl\global_main_atnt_1.pkl")


#DL/UL Dataframe List
df_list_dl = []
df_list_ul = []

acceptable_count = 0
ik_count = 0

if 1:
    for app_directory in app_directory_list:
    
        # if ".csv" in app_directory:
        #     continue
        print(app_directory)

        downlink_path = glob.glob(app_directory + "//*downlink*")[0]
        uplink_path = glob.glob(app_directory + "//*uplink*")[0]
        # ping_path = glob.glob(app_directory + "//*ping*")[0]
        
        # downlink parse
        if (downlink_parse): 
            start_time, tput_list, end_time, rtt = extractNuttcpTput(downlink_path)

            print(downlink_path)
            if (xcal_template_df['TIME_STAMP'].iloc[-1] < start_time):
                print ("DL Not in the Range of time!!", xcal_template_df['TIME_STAMP'].iloc[-1], start_time)
                continue

            app_df = pd.DataFrame({"TIME_STAMP": [start_time + 1.5*rtt*0.001 + 0.01 + i*0.01 for i in range(len(tput_list))], "APP_Throughput": tput_list})
            
            if (len(app_df) > 1):
                end_time_new = float(app_df['TIME_STAMP'].iloc[-1])
        
            print("DL:", "end_time", "end_time_new", end_time, end_time_new)

            # print(downlink_path)
            # print(xcal_template_df.dtypes, xcal_template_df['TIME_STAMP'][:4])
            # app_df.to_csv(downlink_path + ".csv")
            downlink_xcal_template_df = xcal_template_df.loc[(xcal_template_df['TIME_STAMP'] >= start_time) & (xcal_template_df['TIME_STAMP'] <= end_time_new)]

            if (len(downlink_xcal_template_df) == 0):
                print("Lenght Found 0! DL: ", downlink_path, start_time, end_time, end_time_new)
                continue
            column_names = list(downlink_xcal_template_df.columns.values)

            merged_df = pd.merge(app_df, downlink_xcal_template_df, on='TIME_STAMP', how='outer')
            # downlink_xcal_template_df = downlink_xcal_template_df.dropna()
            # app_df = app_df.dropna()
            downlink_xcal_template_df = downlink_xcal_template_df.reset_index(drop= True)
            # df_list_dl.append(downlink_xcal_template_df)
            if (len(merged_df) > 10):
                print("DL Acceptable Length")
                df_list_dl.append(merged_df)
                acceptable_count = acceptable_count +1
                if (pickle_dump):
                    filehandler = open (r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\atnt\08_08_2023\pkl\atnt_dl_df_list_1.pkl","wb")
                    pickle.dump(df_list_dl,filehandler)
                    filehandler.close()
            ik_count = ik_count + 1
            

        # # uplink parse 
        if (uplink_parse):
            start_time, tput_list, end_time, rtt = extractNuttcpTput(uplink_path)
            print(uplink_path)
            if (xcal_template_df['TIME_STAMP'].iloc[-1] < start_time):
                print ("UL Not in the Range of time!!", xcal_template_df['TIME_STAMP'].iloc[-1], start_time)
                continue
            # sample df with CA occuring every 100 ms
            # uplink_xcal_template_df = uplink_xcal_template_df[["TIME_STAMP", "Lat", "Lon", "Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]"]]
            app_df = pd.DataFrame({"TIME_STAMP": [start_time + 1.5*rtt*0.001 + 0.01 + i*0.01 for i in range(len(tput_list))], "APP_Throughput": tput_list})
            if (len(app_df) > 1):
                end_time_new = float(app_df['TIME_STAMP'].iloc[-1])
            print("UL:", "end_time", "end_time_new", end_time, end_time_new)

            print(uplink_path)
            # app_df.to_csv(uplink_path + ".csv")

            uplink_xcal_template_df = xcal_template_df.loc[(xcal_template_df['TIME_STAMP'] >= start_time) & (xcal_template_df['TIME_STAMP'] <= end_time_new)]
            if (len(uplink_xcal_template_df) == 0):
                print("Lenght Found 0! UL: ", downlink_path, start_time, end_time, end_time_new)
            # column_names = list(uplink_xcal_template_df.columns.values)

            merged_df = pd.merge(app_df, uplink_xcal_template_df, on='TIME_STAMP', how='outer')

            # # uplink_xcal_template_df = uplink_xcal_template_df.dropna()
            # # app_df = app_df.dropna()
            uplink_xcal_template_df = uplink_xcal_template_df.reset_index(drop= True)

            if (len(merged_df) > 10):
                print("UL Acceptable Length")
                df_list_ul.append(merged_df)
            
                if (pickle_dump):
                    filehandler =  open(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\atnt\08_08_2023\pkl\atnt_ul_df_list_1.pkl","wb")
                    pickle.dump(df_list_ul,filehandler)
                    filehandler.close()

        # ping data
        if 0:
            start_time, ping_list, end_time = extractPing(ping_path)
            ping_xcal_template_df = xcal_template_df.loc[(xcal_template_df['TIME_STAMP'] >= start_time) & (xcal_template_df['TIME_STAMP'] <= end_time)]

            # sample df with CA occuring every 100 ms
            ping_xcal_template_df = ping_xcal_template_df[["TIME_STAMP", "Lat", "Lon", "Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]"]]
            app_df = pd.DataFrame({"TIME_STAMP": [start_time + i*0.2 for i in range(len(ping_list))], "PING": ping_list})
            merged_df = pd.merge(app_df, ping_xcal_template_df, on='TIME_STAMP', how='outer')
            ping_xcal_template_df = ping_xcal_template_df.dropna()
            app_df = app_df.dropna()
            fig, ax = plt.subplots()
            ax.plot(list(app_df["TIME_STAMP"]), list(app_df["PING"]), label="APP")
            ax.plot(list(ping_xcal_template_df["TIME_STAMP"]), list(ping_xcal_template_df["Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]"]), label="XCAL")
            ax.legend(loc="best")
            ax.set_title("Ping")
            ax.set_ylim(ymin=0, ymax=100)
            # plt.savefig(r"C:\Users\ubwin\Desktop\drive_trip_2.0\05_13_2023\app\tmobile\plots\ping_" + str(count) + ".pdf")
            # plt.close()
            print("***** Run == " + str(count))
            count+=1
        print("No. of Files Processed so far: ", acceptable_count)    
    print("Total No. of Files Processed: ", acceptable_count)