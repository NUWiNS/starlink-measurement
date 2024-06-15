import glob
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# from datetime import datetime
import datetime
from datetime import timezone, timedelta
import pickle
import plotly.express as px
import plotly.graph_objects as go
import us 
from timezonefinder import TimezoneFinder
obj = TimezoneFinder()
import geopy.distance
import time
import random
from collections import OrderedDict
import scipy.stats


earfcn_freq_dict = {'900' : 1960.00, '954' : 1965.40, '854' : 1955.40, '1001' : 1970.10,'1076' : 1977.60,'66611' : 2127.50, '804' : 1950.40,'1099' : 1979.90, '1100' : 1980.00, '1123' : 1982.30, '1125' : 1982.50, '1126' : 1982.60, '1148' : 1984.80, '1150' : 1985.00, '2000' : 2115.00, '2050' : 2120.00, '2125' : 2127.50, '2175' : 2132.50, '2200' : 2135.00, '2225' : 2137.50, '2300' : 2145.00, '2325' : 2147.50, '2460': 875.0,  '39750' : 2506.00, '39907' : 2521.70, '39948' : 2525.80, '40072' : 2538.20, '40384' : 2569.40, '40770' : 2608.00, '40810' : 2612.00, '41176' : 2648.60, '41238' : 2654.80, '41490' : 2680.00, '5035' : 731.50, '5090' : 737, '5095' : 737.50, '5110' : 739.00, '5330' : 763.00, '5780' : 739, '5815': 742.5, '66486': 2115.00, '66487' : 2115.10, '66536' : 2120.00, '66561' : 2122.5, '66586' : 2125, '66661' : 2132.50, '66686' : 2135, '66711' : 2137.50, '66736' : 2140.00, '66786' : 2145.00, '66811': 2147.5, '66836': 2150, '66886': 2150, '66911': 2150, '66961': 2150, '66986' : 2165.00, '67011': 2167.5, '675' : 1937.50, '676': 1937.6, '677': 1937.7, '68611': 619.5, '68636': 622, '68661': 624.5, '68686': 627, '68786': 637, '68836': 637, '68861': 637, '68886': 637, '68911': 649.5, '700' : 1940.00, '725': 1942.5, '750': 1942.5, '775': 1942.5, '801': 1950.1, '8115': 1937.5, '825': 1952.5, '8264': 1952.4, '8290': 1952.4, '8315': 1952.4, '8465': 1972.5, '850' : 1955.00, '851': 1955.1, '852': 1955.1, '8539': 1979.9, '8562': 1982.2, '8640': 1982.2, '8665': 1992.5, '875' : 1957.50, '876': 1957.6, '8763': 866.3, '877': 1957.7, '8950': 885, '901': 1960.1, '925' : 1962.50, '41305' : 2661.50, '66761' : 2142.50, '5230': 751.00, '2600': 889, '2560' : 885, '2450': 874, '1175': 1987.50}

arfcn_freq_dict = {'125400' : 627.000, '125900' : 629.500, '126400' : 632.000, '126490' :632.450, '126510' : 632.550, '126530' : 632.650, '126900' : 634.500, '506280' : 2531.400, '508296' : 2541.480, '509202' : 2546.010, '514056' : 2570.280, '520020' : 2600.100, '525204' :2626.020, '526002' : 2630.010, '526404' : 2632.020, '527202' : 2636.010, '528000' : 2640.000, '528696' : 2643.480, '529998' : 2649.990, '530700' : 2653.500}

def get_ho(df):
    
    cols = ["5G KPI PCell RF Frequency [MHz]", "5G KPI PCell RF Serving PCI", "LTE KPI PCell Serving EARFCN(DL)"]

    # df.loc[:,cols] = df.loc[:,cols].ffill()

    df_short_tput = df[df["Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]"].notna() | df["Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]"].notna()]
    

    df_short_ho = df[df['Event 5G-NR/LTE Events'].notna()]
    df_short_ho = df[df['Event 5G-NR/LTE Events'].str.contains("Handover Success") | df['Event 5G-NR/LTE Events'].str.contains("NR SCG Addition Success") | df['Event 5G-NR/LTE Events'].str.contains("NR SCG Modification Success")]
    df_merged = pd.concat([df_short_tput, df_short_ho])
    df_merged.loc[:,cols] = df_merged.loc[:,cols].ffill()
    df_merged = df_merged.sort_values(by=["TIME_STAMP"])
    df_merged.reset_index(inplace=True)
    # df_merged.to_clipboard()
    # df_short_tput.to_clipboard()
    if len(df_merged) == 0:
        print("0 Length")
    break_list = []
    event = -99
    start_flag = 0
                    
    for index, row in df_merged.iterrows():
        if start_flag == 0:
            # first entry
            # check if event is empty or not
            if pd.isnull(row['Event 5G-NR/LTE Events']):
                event = 0
                start_flag = 1
                start_index_count = 0 
                end_index_count = 0 
            else:
                #first entry is event
                event = 1
                start_flag = 1
        else:
            #row scan in progress 
            if event == 0 and pd.isnull(row['Event 5G-NR/LTE Events']):
            #keep increasing index count
                end_index_count+=1
            elif event == 0 and pd.notnull(row['Event 5G-NR/LTE Events']):
            # set event to 1 : new event started
                event = 1
            # add truncated df to break list
                break_list.append(df_merged[start_index_count:end_index_count+1])
            elif event == 1 and pd.notnull(row['Event 5G-NR/LTE Events']):
            # continue with event
                continue
            elif event == 1 and pd.isnull(row['Event 5G-NR/LTE Events']):
                # event stopped and throughput started
                # set event to 0
                # set start and end index count to current index + 1
                event = 0
                start_index_count = index
                end_index_count = index

    if event == 0:
    # add the last throughput value
    # if df_merged[start_index_count:end_index_count+1] != break_list[-1]:
        break_list.append(df_merged[start_index_count:end_index_count+1])
                            # now calculate technology - throughput
    ho = 0
    df_stat_list = []
    zero_speed_list = []
    count = 0
    len_count = []
    append_df = []
    for tput_df in break_list:
        count+= 1
        modified_tech = ""
                # check if 5G frequency or 5G PCI  is empty
        if len(list(tput_df["5G KPI PCell RF Frequency [MHz]"].dropna())) > 0 or len(list(tput_df["5G KPI PCell RF Serving PCI"].dropna())) > 0:
                # it is a 5G run 
                # get type of 5G 
                # max(set(freq_list), key=freq_list.count)
            freq_list = list(tput_df["5G KPI PCell RF Frequency [MHz]"].dropna())
                    # if len(freq_list) == 0:
                    #     continue
            ffreq = float(max(set(freq_list), key=freq_list.count))
            # tput_tech_dict = {"LTE" : [], "LTE-A" : [], "5G-low" : [], "5G-sub6" : [], "5G-mmWave 28 GHz" : [], "5G-mmWave 39 GHz" : []}
            if int(ffreq) < 1000:
                modified_tech = "5G-low"
            elif int(ffreq) > 1000 and int(ffreq) < 7000:
                modified_tech = "5G-sub6"
            elif int(ffreq) > 7000 and int(ffreq) < 35000:
                modified_tech = "5G-mmWave 28 GHz"
            elif int(ffreq) > 35000:
                modified_tech = "5G-mmWave 39 GHz"

        else:
                        # try:
                        # in all probability it is LTE
                        # what frequency ? 
            earfcn_list = list(tput_df["LTE KPI PCell Serving EARFCN(DL)"].dropna())
            if len(earfcn_list) == 0:
                modified_tech = ""
                continue
            lfreq = str(int(max(set(earfcn_list), key=earfcn_list.count)))
            if lfreq not in earfcn_freq_dict.keys():
                print("EARFCN not present in dict. Need to add." + str(lfreq))
                sys.exit(1)
            else:
                lfreq = earfcn_freq_dict[lfreq]
            if int(lfreq) < 1000:
                modified_tech = "LTE"
            elif int(lfreq) > 1000:
                modified_tech = "LTE-A"
        tput_df ['Tech'] = modified_tech
        ho = 1
        tput_df ['HO'] = ho
        tput_df.to_csv(rf"C:\Users\nuwin\OneDrive\Desktop\moinak\dl\test\{count}.csv")
        len_count.append(len(tput_df))
        tput_df = tput_df[['TIME_STAMP', 'Tech', 'HO']]
        append_df.append(tput_df)
    
    final_df = pd.concat(append_df)
    df = pd.merge(df, final_df, on = 'TIME_STAMP', how ='outer')
    # df.Tech = df.Tech.fillna(0)
    df.loc[:,"Tech"] =df.loc[:,"Tech"].ffill().bfill()
    df.HO = df.HO.fillna(0)
    return df

def get_ho_ul(df):

    cols = ["5G KPI PCell RF Frequency [MHz]", "5G KPI PCell RF Serving PCI", "LTE KPI PCell Serving EARFCN(DL)"]

    # df.loc[:,cols] = df.loc[:,cols].ffill()

    df_short_tput = df[df["Qualcomm 5G-NR MAC PUSCH Info PCell TB Size[Avg] [bytes]"].notna() | df["Qualcomm Lte/LteAdv MAC UL TB(Per TTI) PCell UL Grant [Bytes]"].notna()]

    df_short_ho = df[df['Event 5G-NR/LTE Events'].notna()]
    df_short_ho = df[df['Event 5G-NR/LTE Events'].str.contains("Handover Success") | df['Event 5G-NR/LTE Events'].str.contains("NR SCG Addition Success") | df['Event 5G-NR/LTE Events'].str.contains("NR SCG Modification Success")]

    df_merged = pd.concat([df_short_tput, df_short_ho])
    df_merged.loc[:,cols] = df_merged.loc[:,cols].ffill()
    df_merged = df_merged.sort_values(by=["TIME_STAMP"])
    df_merged.reset_index(inplace=True)
    # df_merged.to_clipboard()
    # df_short_tput.to_clipboard()
    if len(df_merged) == 0:
        print("0 Length")
    break_list = []
    event = -99
    start_flag = 0
                    
    for index, row in df_merged.iterrows():
        if start_flag == 0:
            # first entry
            # check if event is empty or not
            if pd.isnull(row['Event 5G-NR/LTE Events']):
                event = 0
                start_flag = 1
                start_index_count = 0 
                end_index_count = 0 
            else:
                #first entry is event
                event = 1
                start_flag = 1
        else:
            #row scan in progress 
            if event == 0 and pd.isnull(row['Event 5G-NR/LTE Events']):
            #keep increasing index count
                end_index_count+=1
            elif event == 0 and pd.notnull(row['Event 5G-NR/LTE Events']):
            # set event to 1 : new event started
                event = 1
            # add truncated df to break list
                break_list.append(df_merged[start_index_count:end_index_count+1])
            elif event == 1 and pd.notnull(row['Event 5G-NR/LTE Events']):
            # continue with event
                continue
            elif event == 1 and pd.isnull(row['Event 5G-NR/LTE Events']):
                # event stopped and throughput started
                # set event to 0
                # set start and end index count to current index + 1
                event = 0
                start_index_count = index
                end_index_count = index

    if event == 0:
    # add the last throughput value
    # if df_merged[start_index_count:end_index_count+1] != break_list[-1]:
        break_list.append(df_merged[start_index_count:end_index_count+1])
                            # now calculate technology - throughput
    ho = 0
    df_stat_list = []
    zero_speed_list = []
    count = 0
    len_count = []
    append_df = []
    for tput_df in break_list:
        count+= 1
        modified_tech = ""
                # check if 5G frequency or 5G PCI  is empty
        if len(list(tput_df["5G KPI PCell RF Frequency [MHz]"].dropna())) > 0 or len(list(tput_df["5G KPI PCell RF Serving PCI"].dropna())) > 0:
                # it is a 5G run 
                # get type of 5G 
                # max(set(freq_list), key=freq_list.count)
            freq_list = list(tput_df["5G KPI PCell RF Frequency [MHz]"].dropna())
                    # if len(freq_list) == 0:
                    #     continue
            ffreq = float(max(set(freq_list), key=freq_list.count))
            # tput_tech_dict = {"LTE" : [], "LTE-A" : [], "5G-low" : [], "5G-sub6" : [], "5G-mmWave 28 GHz" : [], "5G-mmWave 39 GHz" : []}
            if int(ffreq) < 1000:
                modified_tech = "5G-low"
            elif int(ffreq) > 1000 and int(ffreq) < 7000:
                modified_tech = "5G-sub6"
            elif int(ffreq) > 7000 and int(ffreq) < 35000:
                modified_tech = "5G-mmWave 28 GHz"
            elif int(ffreq) > 35000:
                modified_tech = "5G-mmWave 39 GHz"

        else:
                        # try:
                        # in all probability it is LTE
                        # what frequency ? 
            earfcn_list = list(tput_df["LTE KPI PCell Serving EARFCN(DL)"].dropna())
            if len(earfcn_list) == 0:
                modified_tech = ""
                continue
            lfreq = str(int(max(set(earfcn_list), key=earfcn_list.count)))
            if lfreq not in earfcn_freq_dict.keys():
                print("EARFCN not present in dict. Need to add." + str(lfreq))
                sys.exit(1)
            else:
                lfreq = earfcn_freq_dict[lfreq]
            if int(lfreq) < 1000:
                modified_tech = "LTE"
            elif int(lfreq) > 1000:
                modified_tech = "LTE-A"
        tput_df ['Tech'] = modified_tech
        ho = 1
        tput_df ['HO'] = ho
        tput_df.to_csv(rf"C:\Users\nuwin\OneDrive\Desktop\moinak\dl\test\{count}.csv")
        len_count.append(len(tput_df))
        tput_df = tput_df[['TIME_STAMP', 'Tech', 'HO']]
        append_df.append(tput_df)
    
    final_df = pd.concat(append_df)
    df = pd.merge(df, final_df, on = 'TIME_STAMP', how ='outer')
    # df.Tech = df.Tech.fillna(0)
    df.loc[:,"Tech"] =df.loc[:,"Tech"].ffill().bfill()
    df.HO = df.HO.fillna(0)
    return df

def get_speed(df):
    df_short_2 =  df[df['Lon'].notna()]
    df_short_tput = df_short_2[['TIME_STAMP', 'Lon', 'Lat']]

    lat_list = list(df_short_tput["Lat"])
    lon_list = list(df_short_tput["Lon"])
    ts_list = list(df_short_tput["TIME_STAMP"])
    prev_lat_lon = (lat_list[0], lon_list[0])
    prev_ts = ts_list[0]
    index_list = list(df_short_tput.index)

    speed_list = []
    for index, lat, lon, ts in zip(index_list[1:], lat_list[1:], lon_list[1:], ts_list[1:]):
            current_lat_lon = (lat, lon)
            distance_current = geopy.distance.geodesic(current_lat_lon, prev_lat_lon).miles
            # assuming we drove max of 150 miles/hour, we cannot have more than 0.021 miles in 0.5 sec
            # check if distance current > 0.03
            if distance_current > 0.003:
                # check timestamp diff
                ts_diff = ts - prev_ts
                # ideally each point should be 0.5 seconds apart
                # with some tolerance, each point should not be more than 5 seconds apart? 
                if ts_diff <= 5:
                    # if tolerance of 5 seconds is achieved, check if the distance complies with it or not
                    # if 0.021 miles in 0.5 sec is normal, what happens for ts_diff?
                    # is (0.021 * 2 * ts_diff) within range of distance_current?
                    if distance_current <= ((0.003 * 10 * ts_diff)):
                        # value can be accepted
                        speed = (distance_current / ts_diff) * 3600
                    else:
                        prev_lat_lon = (lat, lon)
                        prev_ts = ts
                        # print("Different trace?")
                        continue

                else:
                    prev_lat_lon = (lat, lon)
                    prev_ts = ts
                    # print("Different trace? - 2")
                    continue

            elif distance_current == 0:
                speed = 0
            else:
                speed = (distance_current / 0.1) * 3600
            if round(speed) > 150:
                import random
                speed = random.randint(110, 150)
            
            df_short_tput.at[index, 'Speed'] = speed
            # if speed > 0 and speed < 1:
            #     speed = 1

    df_short_tput['Speed'] = df_short_tput['Speed'].fillna(0)
    df_short_tput['Speed'] = df_short_tput['Speed'].replace(0, method='bfill')
    df_short_tput = df_short_tput[['TIME_STAMP', 'Speed']]
    df = pd.merge(df, df_short_tput, on = 'TIME_STAMP', how ='outer')
    return df


#load county density map
county_pop_density_df = pd.read_csv(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\uscities.csv")
county_pop_density_df = county_pop_density_df[["lat", "lng",  "county_name", "state_name", "density", ]]

global_columns = ["Throughput", "Speed", "CA", "5G_MCS", "4G_MCS", "5G_BLER", "4G_BLER", "5G_RSRP", "4G_RSRP", "5G_RSRQ", "4G_RSRQ", "Tech", "HO", "Operator", "Link", "APP_Throughput", "5G_TBS", "4G_TBS", "5G_RB", "4G_RB", "5G_PUSCH_POWER", "4G_PUSCH_POWER", "5G_PUCCH_POWER", "4G_PUCCH_POWER", "5G_ENDC_POWER", "5G_TOTAL_POWER", "4G_TOTAL_POWER" ]

column_rename_dict_dl = {"APP_Throughput": "Throughput" ,  
                      "5G KPI PCell Layer1 DL MCS (Avg)" :"5G_MCS", 
                      "LTE KPI PCell DL MCS0" : "4G_MCS", 
                      "5G KPI PCell Layer1 DL BLER [%]" : "5G_BLER", 
                      "LTE KPI PDSCH BLER[%]" : "4G_BLER", 
                      "5G KPI PCell RF Serving SS-RSRP [dBm]" : "5G_RSRP", 
                      "LTE KPI PCell Serving RSRP[dBm]" : "4G_RSRP", 
                      "5G KPI PCell RF Serving SS-RSRQ [dB]": "5G_RSRQ", 
                      "LTE KPI PCell Serving RSRQ[dBm]": "4G_RSRQ", 
                      "Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]": "5G_TBS", 
                      "Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]": "4G_TBS", 
                      "Qualcomm 5G-NR MAC PDSCH Info Total Info Total RB Num[Avg]" :"5G_RB", 
                      "LTE KPI PDSCH PRB Number(Avg)(Total)" : "4G_RB", 
                      "5G KPI PCell RF PUSCH Power [dBm]" : "5G_PUSCH_POWER", 
                      "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]" : "4G_PUSCH_POWER", 
                      "5G KPI PCell RF PUCCH Power [dBm]" : "5G_PUCCH_POWER", 
                      "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]" : "4G_PUCCH_POWER", 
                      "5G KPI PCell RF ENDC Tx Power [dBm]" : "5G_ENDC_POWER",
                      "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]" : "5G_TOTAL_POWER", 
                      "LTE KPI PCell Total Tx Power[dBm]" : "4G_TOTAL_POWER"}

column_rename_dict_ul = {"APP_Throughput": "Throughput" ,  
                      "5G KPI PCell Layer1 UL MCS (Avg)" :"5G_MCS", 
                      "LTE KPI PCell UL MCS" : "4G_MCS", 
                      "5G KPI PCell Layer1 UL BLER [%]" : "5G_BLER", 
                      "LTE KPI PUSCH BLER[%]" : "4G_BLER", 
                      "5G KPI PCell RF Serving SS-RSRP [dBm]" : "5G_RSRP", 
                      "LTE KPI PCell Serving RSRP[dBm]" : "4G_RSRP", 
                      "5G KPI PCell RF Serving SS-RSRQ [dB]": "5G_RSRQ", 
                      "LTE KPI PCell Serving RSRQ[dB]": "4G_RSRQ", 
                      "Qualcomm 5G-NR MAC PUSCH Info PCell TB Size[Avg] [bytes]": "5G_TBS", 
                      "Qualcomm Lte/LteAdv MAC UL TB(Per TTI) PCell UL Grant [Bytes]": "4G_TBS", 
                      "Qualcomm 5G-NR MAC PUSCH Info Total Info Total RB Num[Avg]" :"5G_RB", 
                      "LTE KPI PUSCH PRB Number(Avg)(Total)" : "4G_RB", 
                      "5G KPI PCell RF PUSCH Power [dBm]" : "5G_PUSCH_POWER", 
                      "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]" : "4G_PUSCH_POWER", 
                      "5G KPI PCell RF PUCCH Power [dBm]" : "5G_PUCCH_POWER", 
                      "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]" : "4G_PUCCH_POWER", 
                      "5G KPI PCell RF ENDC Tx Power [dBm]" : "5G_ENDC_POWER",
                      "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]" : "5G_TOTAL_POWER", 
                      "LTE KPI PCell Total Tx Power[dBm]" : "4G_TOTAL_POWER",
                      "5G KPI Total Info UL CA Type": "5G_UL_CA",
                      "LTE KPI UL CA Type": "LTE_UL_CA"}

                                   
                                   
if 1:
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="http")
    def figures_to_html(figs, filename="dashboard.html"):
        with open(filename, 'w') as dashboard:
            dashboard.write("<html><head></head><body>" + "\n")
            for fig in figs:
                inner_html = fig.to_html().split('<body>')[1].split('</body>')[0]
                dashboard.write(inner_html)
            dashboard.write("</body></html>" + "\n")
    #load county density map
    county_pop_density_df = pd.read_csv(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\uscities.csv")
    county_pop_density_df = county_pop_density_df[["lat", "lng",  "county_name", "state_name", "density", ]]
    dl_parse = True
    ul_parse = True
    count = 0
    if dl_parse:
        # tput , speed, tech, ca - prediction
        if 1:
            print("Processing Downlink!!")
            main_df_list = []
            data_point_count = 0 
            start = time.time()
            filehandler = open(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\verizon_dl_df_list_1.pkl","rb")
            end = time.time()
            print("Time to Load!:", end-start)
            global_tmobile_dl_df_list = pickle.load(filehandler)
            filehandler.close()
            count = 0
            for df_current in global_tmobile_dl_df_list:
                if (data_point_count == 0):
                    df_current.to_csv(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\why_dl.csv")
                    data_point_count+= 1
                count = count+1
                print("Count", count)

                try:
                    df_short = df_current[["TIME_STAMP", 
                                           "Lat", "Lon", 
                                           "APP_Throughput", 
                                           "Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]", 
                                           "Smart Phone Smart Throughput Mobile Network Rx Byte", 
                                           'Event 5G-NR/LTE Events', 
                                           "5G KPI PCell RF Frequency [MHz]", 
                                           "LTE KPI PCell Serving EARFCN(DL)", 
                                           'LTE KPI PCell Serving PCI', 
                                           "LTE KPI SCell[1] MAC DL Throughput[Mbps]", 
                                           "LTE KPI SCell[2] MAC DL Throughput[Mbps]", 
                                           "LTE KPI SCell[3] MAC DL Throughput[Mbps]", 
                                           "LTE KPI SCell[4] MAC DL Throughput[Mbps]", 
                                           "LTE KPI PDSCH PRB Number(Avg)(Total)",
                                           'LTE KPI PCell Serving RSRP[dBm]',
                                           "LTE KPI PCell Serving RSRQ[dB]",
                                           "LTE KPI PDSCH BLER[%]", 
                                           "LTE KPI PCell Serving BandWidth(DL)",
                                           "LTE KPI PCell DL MCS0",
                                           "LTE KPI PDSCH PRB Number(Avg)(Total)", 
                                           "LTE KPI PUSCH PRB Number(Avg)(Total)",  
                                           "LTE KPI PCell MAC DL Throughput[Mbps]",
                                           "LTE KPI PCell Total Tx Power[dBm]",
                                           "LTE KPI CA Type",
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]", 
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]",
                                           "Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]", 
                                           "Qualcomm Lte/LteAdv MAC UL TB(Per TTI) PCell UL Grant [Bytes]", 
                                           '5G KPI PCell RF Serving PCI', 
                                           "5G KPI PCell Layer2 MAC DL Throughput [Mbps]",
                                           "5G KPI Total Info DL CA Type", 
                                           "5G KPI SCell[1] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[2] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[3] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[4] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[5] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[6] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[7] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI PCell RF BandWidth", 
                                           "5G KPI PCell Layer1 DL MCS (Avg)", 
                                           "5G KPI PCell Layer1 DL BLER [%]", 
                                           '5G KPI PCell RF Serving SS-RSRP [dBm]', 
                                           "5G KPI PCell RF Serving SS-RSRQ [dB]", 
                                           "5G KPI PCell RF PUSCH Power [dBm]", 
                                           "5G KPI PCell RF ENDC Tx Power [dBm]",
                                           "5G KPI PCell RF PUCCH Power [dBm]",
                                           "Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]", 
                                           "Qualcomm 5G-NR MAC PUSCH Info PCell TB Size[Avg] [bytes]", 
                                           "Qualcomm 5G-NR MAC PDSCH Info Total Info Total RB Num[Avg]",  
                                           "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]",
                                           ]]
                except Exception as ex:
                    exception = str(ex)
                    # some fields missing - let us take a look
                    missing_field_list = ["LTE KPI PCell MAC DL Throughput[Mbps]", "LTE KPI SCell[1] MAC DL Throughput[Mbps]", "LTE KPI SCell[2] MAC DL Throughput[Mbps]", "LTE KPI SCell[3] MAC DL Throughput[Mbps]", "LTE KPI SCell[4] MAC DL Throughput[Mbps]", "5G KPI PCell Layer2 MAC DL Throughput [Mbps]", "5G KPI SCell[1] Layer2 MAC DL Throughput [Mbps]", "5G KPI SCell[2] Layer2 MAC DL Throughput [Mbps]", "5G KPI SCell[3] Layer2 MAC DL Throughput [Mbps]", "5G KPI SCell[4] Layer2 MAC DL Throughput [Mbps]", "5G KPI SCell[5] Layer2 MAC DL Throughput [Mbps]", "5G KPI SCell[6] Layer2 MAC DL Throughput [Mbps]", "5G KPI SCell[7] Layer2 MAC DL Throughput [Mbps]", "5G KPI PCell RF BandWidth", "LTE KPI PCell DL MCS0", "5G KPI PCell Layer1 DL MCS (Avg)", "LTE KPI PDSCH BLER[%]", "5G KPI PCell Layer1 DL BLER [%]", 'LTE KPI PCell Serving RSRP[dBm]', '5G KPI PCell RF Serving SS-RSRP [dBm]', "5G KPI PCell RF Serving SS-RSRQ [dB]", "LTE KPI PCell Serving RSRQ[dB]"]
                    for field in missing_field_list:
                        if field in exception:
                            # create dummy column with nan
                            df_current[field] = np.nan
                    df_short = df_current[["TIME_STAMP", 
                                           "Lat", "Lon", 
                                           "APP_Throughput", 
                                           "Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]", 
                                           "Smart Phone Smart Throughput Mobile Network Rx Byte", 
                                           'Event 5G-NR/LTE Events', "5G KPI PCell RF Frequency [MHz]", 
                                           "LTE KPI PCell Serving EARFCN(DL)", 
                                           'LTE KPI PCell Serving PCI', 
                                           "LTE KPI SCell[1] MAC DL Throughput[Mbps]", 
                                           "LTE KPI SCell[2] MAC DL Throughput[Mbps]", 
                                           "LTE KPI SCell[3] MAC DL Throughput[Mbps]", 
                                           "LTE KPI SCell[4] MAC DL Throughput[Mbps]", 
                                           "LTE KPI PDSCH PRB Number(Avg)(Total)",
                                           'LTE KPI PCell Serving RSRP[dBm]',
                                           "LTE KPI PCell Serving RSRQ[dB]",
                                           "LTE KPI PDSCH BLER[%]", 
                                           "LTE KPI PCell Serving BandWidth(DL)",
                                           "LTE KPI PCell DL MCS0",
                                           "LTE KPI PUSCH PRB Number(Avg)(Total)",  
                                           "LTE KPI PCell MAC DL Throughput[Mbps]",
                                           "LTE KPI PCell Total Tx Power[dBm]",
                                           "LTE KPI CA Type",
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]", 
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]",
                                           "Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]", 
                                           "Qualcomm Lte/LteAdv MAC UL TB(Per TTI) PCell UL Grant [Bytes]", 
                                           '5G KPI PCell RF Serving PCI', 
                                           "5G KPI PCell Layer2 MAC DL Throughput [Mbps]",
                                           "5G KPI Total Info DL CA Type", 
                                           "5G KPI SCell[1] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[2] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[3] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[4] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[5] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[6] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI SCell[7] Layer2 MAC DL Throughput [Mbps]", 
                                           "5G KPI PCell RF BandWidth", 
                                           "5G KPI PCell Layer1 DL MCS (Avg)", 
                                           "5G KPI PCell Layer1 DL BLER [%]", 
                                           '5G KPI PCell RF Serving SS-RSRP [dBm]', 
                                           "5G KPI PCell RF Serving SS-RSRQ [dB]", 
                                           "5G KPI PCell RF PUSCH Power [dBm]",
                                           "5G KPI PCell RF PUCCH Power [dBm]", 
                                           "5G KPI PCell RF ENDC Tx Power [dBm]",
                                           "Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]", 
                                           "Qualcomm 5G-NR MAC PUSCH Info PCell TB Size[Avg] [bytes]", 
                                           "Qualcomm 5G-NR MAC PDSCH Info Total Info Total RB Num[Avg]",  
                                           "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]"
                                           ]]
                df_check = df_current[df_current["Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]"].notna()]
                if (len(df_check) != 0):
                    df_short = get_ho(df_short)
                    df_short = get_speed(df_short)    
                # if (count == 1) :
                #     df_short.to_csv(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\test_dl_2_ik.csv")
                    df_short_dl = df_short[["TIME_STAMP", 
                                            "Lat", "Lon",
                                            "APP_Throughput",
                                            "LTE KPI PDSCH PRB Number(Avg)(Total)",
                                            'LTE KPI PCell Serving RSRP[dBm]',
                                            "LTE KPI PCell Serving RSRQ[dB]",
                                            "LTE KPI PDSCH BLER[%]", 
                                            "LTE KPI PCell DL MCS0", 
                                            "LTE KPI PCell Total Tx Power[dBm]",
                                            "LTE KPI CA Type",
                                            "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]", 
                                            "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]",
                                            "Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]",    
                                            "5G KPI PCell Layer1 DL MCS (Avg)", 
                                            "5G KPI PCell Layer1 DL BLER [%]", 
                                            '5G KPI PCell RF Serving SS-RSRP [dBm]', 
                                            "5G KPI PCell RF Serving SS-RSRQ [dB]", 
                                            "5G KPI PCell RF PUSCH Power [dBm]", 
                                            "5G KPI PCell RF PUCCH Power [dBm]",
                                            "5G KPI PCell RF ENDC Tx Power [dBm]",
                                            "5G KPI Total Info DL CA Type",
                                            "Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]",  
                                            "Qualcomm 5G-NR MAC PDSCH Info Total Info Total RB Num[Avg]",  
                                            "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]", 
                                            "Speed",
                                            "Tech",
                                            "HO",
                                            ]]
                else:
                    continue
               
                df_short_dl.rename(columns={"APP_Throughput": "Throughput" ,  
                      "5G KPI PCell Layer1 DL MCS (Avg)" :"5G_MCS", 
                      "LTE KPI PCell DL MCS0" : "4G_MCS", 
                      "5G KPI PCell Layer1 DL BLER [%]" : "5G_BLER", 
                      "LTE KPI PDSCH BLER[%]" : "4G_BLER", 
                      "5G KPI PCell RF Serving SS-RSRP [dBm]" : "5G_RSRP", 
                      "LTE KPI PCell Serving RSRP[dBm]" : "4G_RSRP", 
                      "5G KPI PCell RF Serving SS-RSRQ [dB]": "5G_RSRQ", 
                      "LTE KPI PCell Serving RSRQ[dB]": "4G_RSRQ", 
                      "5G KPI Total Info DL CA Type" : "5G_DL_CA",
                      "LTE KPI CA Type" : "LTE_DL_CA",
                      "Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]": "5G_TBS", 
                      "Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]": "4G_TBS", 
                      "Qualcomm 5G-NR MAC PDSCH Info Total Info Total RB Num[Avg]" :"5G_RB", 
                      "LTE KPI PDSCH PRB Number(Avg)(Total)" : "4G_RB", 
                      "5G KPI PCell RF PUSCH Power [dBm]" : "5G_PUSCH_POWER", 
                      "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]" : "4G_PUSCH_POWER", 
                      "5G KPI PCell RF PUCCH Power [dBm]" : "5G_PUCCH_POWER", 
                      "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]" : "4G_PUCCH_POWER", 
                      "5G KPI PCell RF ENDC Tx Power [dBm]" : "5G_ENDC_POWER",
                      "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]" : "5G_TOTAL_POWER", 
                      "LTE KPI PCell Total Tx Power[dBm]" : "4G_TOTAL_POWER"}, inplace = True)
                
                
                df_short_dl_final = df_short_dl.sort_values(by=['TIME_STAMP'])
                df_short_dl_final = df_short_dl_final.reset_index(drop = True)

                df_short_dl_final =df_short_dl_final[['TIME_STAMP', "Lat", "Lon", 'Throughput', '5G_TBS', '4G_TBS', '5G_MCS', '4G_MCS', '5G_RSRP', '4G_RSRP', '5G_RSRQ', '4G_RSRQ', '5G_RB',  '4G_RB', '5G_BLER', '4G_BLER', '5G_PUSCH_POWER', '4G_PUSCH_POWER', '5G_PUCCH_POWER', '4G_PUCCH_POWER', '5G_ENDC_POWER', '5G_TOTAL_POWER', '4G_TOTAL_POWER', "5G_DL_CA", "LTE_DL_CA", "Speed", "HO", "Tech" ]]


                cols = ['5G_TBS', '4G_TBS', '5G_MCS', '4G_MCS', '5G_RSRP', '4G_RSRP', '5G_RSRQ', '4G_RSRQ', '5G_RB',  '4G_RB', '5G_BLER', '4G_BLER', '5G_PUSCH_POWER', '4G_PUSCH_POWER', '5G_PUCCH_POWER', '4G_PUCCH_POWER', '5G_ENDC_POWER', '5G_TOTAL_POWER', '4G_TOTAL_POWER', "5G_DL_CA", "LTE_DL_CA", "Speed", "HO", "Tech"]
                df_short_dl_final.loc[:,cols] =df_short_dl_final.loc[:,cols].ffill().bfill()

                # df_short_dl_final = df_short_dl_final.dropna()
                df_short_dl_final = df_short_dl_final[df_short_dl_final['Throughput'].notna()]
                # df_short_dl_final = df_short_dl_final.iloc[: , 1:]
                main_df_list.append(df_short_dl_final)
                # if (count == 1) :
                #     df_short_dl_final.to_csv(rf"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\test_dl_{count}.csv")
                df_short_dl_final.to_csv(rf"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\check\test_dl_{count}.csv")
                df_current.to_csv(rf"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\check\why_{count}.csv")


            filehandler = open(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\verizon_dl_1.pkl", "wb")
            pickle.dump(main_df_list, filehandler)
            filehandler.close()

    if ul_parse:
        # tput , speed, tech, ca - prediction
        if 1:
            print("Processing Uplink!!")
            main_df_list_ul = []
            data_point_count = 0 
            filehandler = open(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\verizon_ul_df_list_1.pkl","rb")
            global_tmobile_ul_df_list = pickle.load(filehandler)
            filehandler.close()
            count = 0
            for df_current in global_tmobile_ul_df_list:
              
                df_current.to_csv(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\why_ul.csv")
                #data_point_count+= 1
                count = count + 1
                print (count)
                print("DF_CURRENT", len(df_current))

                try:
                    df_short = df_current[["TIME_STAMP", 
                                           "Lat", "Lon", 
                                           "APP_Throughput", 
                                           "Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]", 
                                           "Smart Phone Smart Throughput Mobile Network Tx Byte", 
                                           'Event 5G-NR/LTE Events',  
                                           "LTE KPI PCell Serving EARFCN(DL)", 
                                           'LTE KPI PCell Serving PCI', 
                                           "LTE KPI PCell Serving BandWidth(UL)",
                                           "LTE KPI PCell UL MCS", 
                                           "LTE KPI PUSCH BLER[%]", 
                                           'LTE KPI PCell Serving RSRP[dBm]', 
                                           "LTE KPI PCell Serving RSRQ[dB]" ,
                                           "LTE KPI PDSCH PRB Number(Avg)(Total)",
                                           "LTE KPI PUSCH PRB Number(Avg)(Total)",
                                           "LTE KPI PCell MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[1] MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[2] MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[3] MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[4] MAC UL Throughput[Mbps]", 
                                           "LTE KPI PCell Total Tx Power[dBm]",
                                           "LTE KPI UL CA Type",
                                           '5G KPI PCell RF Serving PCI', 
                                           "5G KPI PCell RF BandWidth",
                                           "5G KPI PCell RF Frequency [MHz]",
                                           "5G KPI PCell Layer1 UL MCS (Avg)",
                                           "5G KPI PCell Layer1 UL BLER [%]",
                                           '5G KPI PCell RF Serving SS-RSRP [dBm]', 
                                           "5G KPI PCell RF Serving SS-RSRQ [dB]", 
                                           "5G KPI PCell RF PUSCH Power [dBm]",
                                           "5G KPI PCell RF PUCCH Power [dBm]", 
                                           "5G KPI PCell RF ENDC Tx Power [dBm]",
                                           "5G KPI PCell Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[1] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[2] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[3] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[4] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[5] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[6] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[7] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI Total Info UL CA Type",
                                           "Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]", 
                                           "Qualcomm 5G-NR MAC PUSCH Info PCell TB Size[Avg] [bytes]",
                                           "Qualcomm 5G-NR MAC PUSCH Info Total Info Total RB Num[Avg]",
                                           "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]", 
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]",
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]",
                                           "Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]",
                                           "Qualcomm Lte/LteAdv MAC UL TB(Per TTI) PCell UL Grant [Bytes]"
                                           ]]
                except Exception as ex:
                    exception = str(ex)
                    # some fields missing - let us take a look
                    missing_field_list = ["5G KPI PCell RF Frequency [MHz]",  
                                          '5G KPI PCell RF Serving PCI', 
                                          "LTE KPI PCell MAC UL Throughput[Mbps]", 
                                          "LTE KPI SCell[1] MAC UL Throughput[Mbps]", 
                                          "LTE KPI SCell[2] MAC UL Throughput[Mbps]", 
                                          "LTE KPI SCell[3] MAC UL Throughput[Mbps]", 
                                          "LTE KPI SCell[4] MAC UL Throughput[Mbps]", 
                                          "5G KPI PCell Layer2 MAC UL Throughput [Mbps]", 
                                          "5G KPI SCell[1] Layer2 MAC UL Throughput [Mbps]", 
                                          "5G KPI SCell[2] Layer2 MAC UL Throughput [Mbps]", 
                                          "5G KPI SCell[3] Layer2 MAC UL Throughput [Mbps]", 
                                          "5G KPI SCell[4] Layer2 MAC UL Throughput [Mbps]", 
                                          "5G KPI SCell[5] Layer2 MAC UL Throughput [Mbps]", 
                                          "5G KPI SCell[6] Layer2 MAC UL Throughput [Mbps]", 
                                          "5G KPI SCell[7] Layer2 MAC UL Throughput [Mbps]", 
                                          "LTE KPI PCell UL MCS", 
                                          "5G KPI PCell Layer1 UL MCS (Avg)", 
                                          "LTE KPI PUSCH BLER[%]", 
                                          "5G KPI PCell Layer1 UL BLER [%]", 
                                          'LTE KPI PCell Serving RSRP[dBm]', 
                                          '5G KPI PCell RF Serving SS-RSRP [dBm]', 
                                          "5G KPI PCell RF Serving SS-RSRQ [dB]", 
                                          "LTE KPI PCell Serving RSRQ[dB]"]
                    for field in missing_field_list:
                        if field in exception:
                            # create dummy column with nan
                            df_current[field] = np.nan
                    # print("Continue " + str(global_count_continue))
                    # global_count_continue+=1
                    # continue
                    df_short = df_current[["TIME_STAMP", 
                                           "Lat", "Lon", 
                                           "APP_Throughput", 
                                           "Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]", 
                                           "Smart Phone Smart Throughput Mobile Network Tx Byte", 
                                           'Event 5G-NR/LTE Events',  
                                           "LTE KPI PCell Serving EARFCN(DL)", 
                                           'LTE KPI PCell Serving PCI', 
                                           "LTE KPI PCell Serving BandWidth(UL)",
                                           "LTE KPI PCell UL MCS", 
                                           "LTE KPI PUSCH BLER[%]", 
                                           'LTE KPI PCell Serving RSRP[dBm]', 
                                           "LTE KPI PCell Serving RSRQ[dB]" ,
                                           "LTE KPI PDSCH PRB Number(Avg)(Total)",
                                           "LTE KPI PUSCH PRB Number(Avg)(Total)",
                                           "LTE KPI PCell MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[1] MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[2] MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[3] MAC UL Throughput[Mbps]", 
                                           "LTE KPI SCell[4] MAC UL Throughput[Mbps]", 
                                           "LTE KPI PCell Total Tx Power[dBm]",
                                           "LTE KPI UL CA Type",
                                           '5G KPI PCell RF Serving PCI', 
                                           "5G KPI PCell RF BandWidth",
                                           "5G KPI PCell RF Frequency [MHz]",
                                           "5G KPI PCell Layer1 UL MCS (Avg)",
                                           "5G KPI PCell Layer1 UL BLER [%]",
                                           '5G KPI PCell RF Serving SS-RSRP [dBm]', 
                                           "5G KPI PCell RF Serving SS-RSRQ [dB]", 
                                           "5G KPI PCell RF PUSCH Power [dBm]",
                                           "5G KPI PCell RF PUCCH Power [dBm]", 
                                           "5G KPI PCell RF ENDC Tx Power [dBm]",
                                           "5G KPI Total Info UL CA Type",
                                           "5G KPI PCell Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[1] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[2] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[3] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[4] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[5] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[6] Layer2 MAC UL Throughput [Mbps]", 
                                           "5G KPI SCell[7] Layer2 MAC UL Throughput [Mbps]", 
                                           "Qualcomm 5G-NR MAC PDSCH Info PCell TB Size[Avg] [bytes]", 
                                           "Qualcomm 5G-NR MAC PUSCH Info PCell TB Size[Avg] [bytes]",
                                           "Qualcomm 5G-NR MAC PUSCH Info Total Info Total RB Num[Avg]",
                                           "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]", 
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]",
                                           "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]",
                                           "Qualcomm Lte/LteAdv MAC DL TB(Per TTI) PCell DL TBS [Bytes]",
                                           "Qualcomm Lte/LteAdv MAC UL TB(Per TTI) PCell UL Grant [Bytes]" 
                                           ]]
                df_check = df_current[df_current["Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]"].notna()]
                if (len(df_check) != 0):
                    df_short = get_ho_ul(df_short)
                    df_short = get_speed(df_short)

                    df_short_ul = df_short[[ "TIME_STAMP", 
                                                "Lat", "Lon",
                                                "APP_Throughput", 
                                                "LTE KPI PCell UL MCS", 
                                                "LTE KPI PUSCH BLER[%]", 
                                                'LTE KPI PCell Serving RSRP[dBm]', 
                                                "LTE KPI PCell Serving RSRQ[dB]" ,
                                                "LTE KPI PUSCH PRB Number(Avg)(Total)",
                                                "LTE KPI PCell Total Tx Power[dBm]",
                                                "LTE KPI UL CA Type",
                                                "5G KPI PCell Layer1 UL MCS (Avg)",
                                                "5G KPI PCell Layer1 UL BLER [%]",
                                                '5G KPI PCell RF Serving SS-RSRP [dBm]', 
                                                "5G KPI PCell RF Serving SS-RSRQ [dB]", 
                                                "5G KPI PCell RF PUSCH Power [dBm]",
                                                "5G KPI PCell RF PUCCH Power [dBm]", 
                                                "5G KPI PCell RF ENDC Tx Power [dBm]", 
                                                "5G KPI Total Info UL CA Type",
                                                "Qualcomm 5G-NR MAC PUSCH Info PCell TB Size[Avg] [bytes]",
                                                "Qualcomm 5G-NR MAC PUSCH Info Total Info Total RB Num[Avg]",
                                                "Qualcomm 5G-NR Tx Power Control Info PCell Total Total Tx Power [dBm]", 
                                                "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUSCH [dBm]",
                                                "Qualcomm Lte/LteAdv Power Info PCell Tx Power PUCCH [dBm]",
                                                "Qualcomm Lte/LteAdv MAC UL TB(Per TTI) PCell UL Grant [Bytes]",
                                                "Speed", "HO", "Tech"
                                                ]]
                else:
                    continue
                    
         
                df_short_ul.rename(columns=column_rename_dict_ul, inplace = True)
                
                df_short_ul_final = df_short_ul.sort_values(by=['TIME_STAMP'])
                df_short_ul_final =df_short_ul_final.reset_index(drop = True)
                
                df_short_ul_final =df_short_ul_final[['TIME_STAMP', "Lat", "Lon",'Throughput', '5G_TBS', '4G_TBS', '5G_MCS', '4G_MCS', '5G_RSRP', '4G_RSRP', '5G_RSRQ', '4G_RSRQ', '5G_RB',  '4G_RB', '5G_BLER', '4G_BLER', '5G_PUSCH_POWER', '4G_PUSCH_POWER', '5G_PUCCH_POWER', '4G_PUCCH_POWER', '5G_ENDC_POWER', '5G_TOTAL_POWER', '4G_TOTAL_POWER', "5G_UL_CA", "LTE_UL_CA", "Speed", "HO", "Tech" ]]
           

                cols = ['5G_TBS', '4G_TBS', '5G_MCS', '4G_MCS', '5G_RSRP', '4G_RSRP', '5G_RSRQ', '4G_RSRQ', '5G_RB',  '4G_RB', '5G_BLER', '4G_BLER', '5G_PUSCH_POWER', '4G_PUSCH_POWER', '5G_PUCCH_POWER', '4G_PUCCH_POWER', '5G_ENDC_POWER', '5G_TOTAL_POWER', '4G_TOTAL_POWER', "5G_UL_CA", "LTE_UL_CA","Speed", "HO", "Tech"]

                df_short_ul_final.loc[:,cols] =df_short_ul_final.loc[:,cols].ffill().bfill()
                # df_short_ul_final.to_csv (rf"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_07_2023\pkl\tput_prediction\test\{count}.csv")
                # df_short_ul_final = df_short_ul_final.dropna()
                df_short_ul_final = df_short_ul_final[df_short_ul_final['Throughput'].notna()]
                # df_short_ul_final = df_short_ul_final.iloc[: , 1:]

                main_df_list_ul.append(df_short_ul_final)
                if (count == 1) :
                    df_short_ul_final.to_csv (r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\verizon_ul_1.csv")



            filehandler = open(r"C:\Users\nuwin\OneDrive\Desktop\driving_trip_3.0\processed\Operator\verizon\08_08_2023\pkl\tput_prediction\verizon_ul_1.pkl", "wb")
            pickle.dump(main_df_list_ul, filehandler)
            filehandler.close()        