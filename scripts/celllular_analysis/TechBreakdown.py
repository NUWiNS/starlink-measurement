from os import path
import os
from typing import List, Tuple
import numpy as np
import pandas as pd

from scripts.constants import XcalField, XcallHandoverEvent
from scripts.utilities.distance_utils import DistanceUtils

class Segment:
    def __init__(
            self, 
            df: pd.DataFrame, 
            start_idx: int,
            end_idx: int,
            app_tput_protocol: str,
            app_tput_direction: str,
            time_field: str = XcalField.CUSTOM_UTC_TIME, 
            freq_field: str = XcalField.PCELL_FREQ_5G,
            network_type_field: str = XcalField.SMART_PHONE_SYSTEM_INFO_NETWORK_TYPE,
            tech_field: str = XcalField.TECH,
            band_field: str = XcalField.BAND,
            event_field: str = XcalField.EVENT_LTE,
            dl_tput_field: str = XcalField.SMART_TPUT_DL,
            ul_tput_field: str = XcalField.SMART_TPUT_UL,
            actual_tech_field: str = XcalField.ACTUAL_TECH,
            low_tput_threshold_mbps: float = 0.1,
            lon_field: str = XcalField.LON,
            lat_field: str = XcalField.LAT,
        ):
        self.df = df
        self.start_idx = start_idx
        self.end_idx = end_idx

        self.time_field = time_field
        self.freq_field = freq_field
        self.tech_field = tech_field
        self.actual_tech_field = actual_tech_field
        self.event_field = event_field
        self.band_field = band_field
        self.dl_tput_field = dl_tput_field
        self.ul_tput_field = ul_tput_field
        self.app_tput_protocol = app_tput_protocol
        self.app_tput_direction = app_tput_direction
        self.low_tput_threshold_mbps = low_tput_threshold_mbps
        self.lon_field = lon_field
        self.lat_field = lat_field
        # self.has_tput = self.get_dl_tput_count() > 0 or self.get_ul_tput_count() > 0
        # self.has_freq_5g = self.get_freq_5g_mhz() is not None
        self.duration_ms = self.get_duration_ms()
        self.has_handover = self.check_if_has_handover()
        self.has_no_service = self.check_if_no_service()

    def fill_tech(self):
        self.df[self.actual_tech_field] = self.get_tech()

    def get_cumulative_meters(self) -> float:

        lats = self.df[self.lat_field].values
        lons = self.df[self.lon_field].values
        return DistanceUtils.calculate_cumulative_meters(lons, lats)

    def get_range(self) -> str:
        return f"{self.start_idx}:{self.end_idx}"
    
    def get_freq_5g_mhz(self) -> float:
        if self.freq_field not in self.df.columns:
            return None
        freq_5g = self.get_field_with_max_occurence(self.freq_field)
        if freq_5g is None:
            return None
        return float(freq_5g)

    def get_tech(self, label: str = None) -> str:
        """
        determine the technology for this segment
        """
        all_techs = self.get_all_techs_from_xcal()

        if self.check_if_no_service():
            # Check if there is any non-zero DL throughput during no service period
            if self.app_tput_direction == 'downlink':
                tput_values = self.df[self.dl_tput_field].dropna()
                if any(float(tput) > self.low_tput_threshold_mbps for tput in tput_values):
                    print(f"Segment ({self.get_range()}) has no service but contains non-zero DL throughput (threshold: {self.low_tput_threshold_mbps}, max: {tput_values.max()})")
            elif self.app_tput_direction == 'uplink':
                tput_values = self.df[self.ul_tput_field].dropna()
                if any(float(tput) > self.low_tput_threshold_mbps for tput in tput_values):
                    print(f"Segment ({self.get_range()}) has no service but contains non-zero UL throughput (threshold: {self.low_tput_threshold_mbps}, max: {tput_values.max()})")
            return 'NO SERVICE'

        # 5G identification needs 5G frequency
        freq_5g_mhz = self.get_freq_5g_mhz()
        if freq_5g_mhz is not None:
            if freq_5g_mhz < 1000:  # Below 1 GHz
                return '5G-low'
            elif freq_5g_mhz < 6000:  # 1-6 GHz
                return '5G-mid'
            elif 27500 <= freq_5g_mhz <= 28350:  # 28 GHz band
                return '5G-mmWave (28GHz)'
            elif 37000 <= freq_5g_mhz <= 40000:  # 39 GHz band
                return '5G-mmWave (39GHz)'

        # if any('ca' in tech.lower() for tech in all_techs):
        #     return 'LTE-A'
        if any('lte' in tech.lower() for tech in all_techs):
            return 'LTE'
        
        if len(all_techs) == 0:
            print(f'[Warning] No tech found for segment ({self.get_range()})')
            # TODO: check
            if label is None:
                label = 'unknown'
            tput_df = self.df[self.df[self.dl_tput_field].notna() | self.df[self.ul_tput_field].notna()].copy()
            tmp_csv = f'no_tech_segment.{label}.csv'
            if path.exists(tmp_csv):
                # remove the file
                os.remove(tmp_csv)
            if not path.exists(tmp_csv):
                with open(tmp_csv, 'w') as f:
                    tput_df.to_csv(f, index=False)
            else:
                with open(tmp_csv, 'a') as f:
                    tput_df.to_csv(f, index=False, header=False)
            return 'Unknown'
        
        return 'Unknown'
    
    def get_all_techs_from_xcal(self) -> List[str]:
        return self.df[self.tech_field].dropna().unique().tolist()

    def check_if_only_one_or_zero_tech_exist(self) -> bool:
        unique_techs = self.df[self.tech_field].dropna().unique()
        num_of_techs = len(unique_techs)
        if num_of_techs == 0 or num_of_techs == 1:
            return True
        elif num_of_techs == 2 and any(tech.lower() == 'no service' for tech in unique_techs):
            # Check if one of the techs is "no service" (case insensitive)
            return True
        elif num_of_techs > 1 and \
                all(tech.lower().startswith('lte') for tech in unique_techs) and \
                all(tech.lower().startswith('5g') for tech in unique_techs):
            return True 
        else:
            raise ValueError(f"Segment ({self.get_range()}) has {num_of_techs} different techs: {unique_techs}")

    def get_band(self) -> str:
        raise NotImplementedError('get_band is not implemented')
    
    def get_tech_from_xcal(self) -> str:
        """
        Might be unreliable due to the 1s resolution of the data
        """
        return self.get_field_with_max_occurence(self.tech_field)
    
    def get_band_from_xcal(self) -> str:
        """
        Might be unreliable due to the 1s resolution of the data
        """
        return self.get_field_with_max_occurence(self.band_field)
    
    def get_dl_tput_count(self) -> int:
        return self.get_field_value_count(self.dl_tput_field)

    def get_ul_tput_count(self) -> int:
        return self.get_field_value_count(self.ul_tput_field)

    def check_if_multiple_freq(self) -> bool:
        return self.check_if_multiple_values_exist(self.freq_field)
    
    def check_if_multiple_values_exist(self, field: str) -> bool:
        """
        check if there are multiple values in the field
        """
        return self.df[field].nunique() > 1

    def get_dl_tput_df(self) -> pd.DataFrame:
        return self.df[self.df[self.dl_tput_field].notna()].copy()
    
    def get_ul_tput_df(self) -> pd.DataFrame:
        return self.df[self.df[self.ul_tput_field].notna()].copy()
    
    def check_if_no_service(self) -> bool:
        return self.df[self.df[self.tech_field].str.lower() == 'no service'].shape[0] > 0

    def check_if_has_handover(self) -> bool:
        return self.df[self.df[self.event_field].isin(XcallHandoverEvent.get_all_events())].shape[0] > 0

    def get_duration_ms(self) -> float:
        """
        get the duration of the segment in milliseconds
        """
        start_time = pd.to_datetime(self.df[self.time_field].iloc[0])
        end_time = pd.to_datetime(self.df[self.time_field].iloc[-1])
        duration = end_time - start_time
        return duration.total_seconds() * 1000 + duration.microseconds / 1000

    def get_field_with_max_occurence(self, field: str) -> str:
        """
        get the 5G frequency with the max occurence
        """
        if self.df[field].isna().all():
            return None
        return self.df[field].value_counts().idxmax()

    def get_field_value_count(self, field: str) -> int:
        """
        get the number of rows in the dataframe
        """
        if self.df[field].isna().all():
            return 0
        return self.df[field].value_counts().sum()

    def __lt__(self, other):
        """for min-heap sorting"""
        return self.start_idx < other.start_idx

class TechBreakdown:
    def __init__(
            self, 
            df: pd.DataFrame,
            app_tput_protocol: str,
            app_tput_direction: str,
            time_field: str = XcalField.CUSTOM_UTC_TIME,
            tech_field: str = XcalField.TECH,
            band_field: str = XcalField.BAND,
            dl_tput_field: str = XcalField.SMART_TPUT_DL,
            ul_tput_field: str = XcalField.SMART_TPUT_UL,
            event_field: str = XcalField.EVENT_LTE,
            freq_5g_field: str = XcalField.PCELL_FREQ_5G,
            network_type_field: str = XcalField.SMART_PHONE_SYSTEM_INFO_NETWORK_TYPE,
            earfcn_lte_field: str = XcalField.LTE_EARFCN_DL,
            label: str = None,
        ):
        self.df = df
        self.time_field = time_field
        self.tech_field = tech_field
        self.band_field = band_field
        self.dl_tput_field = dl_tput_field
        self.ul_tput_field = ul_tput_field
        self.event_field = event_field
        self.freq_5g_field = freq_5g_field
        self.app_tput_protocol = app_tput_protocol
        self.app_tput_direction = app_tput_direction
        self.label = label
        self.earfcn_lte_field = earfcn_lte_field
        self.network_type_field = network_type_field

    def process(self) -> List[Segment]:
        try:
            segments = self.partition_data_by_handover(self.df)
        except Exception as e:
            print(f"Error in partition_data_by_handover: {str(e)}")
            raise e
        try:
            segments = self.partition_data_by_no_service(segments)
        except Exception as e:
            print(f"Error in partition_data_by_no_service: {str(e)}")
            raise e
        return segments
    
    def create_segment(self, df: pd.DataFrame, start_idx: int, end_idx: int) -> Segment:
        return Segment(
                df=df,
                start_idx=start_idx,
                end_idx=end_idx,
                app_tput_protocol=self.app_tput_protocol,
                app_tput_direction=self.app_tput_direction,
                time_field=self.time_field,
                freq_field=self.freq_5g_field,
                tech_field=self.tech_field,
                band_field=self.band_field,
                dl_tput_field=self.dl_tput_field,
                ul_tput_field=self.ul_tput_field,
            )

    def partition_data_by_handover(self, df: pd.DataFrame) -> List[Segment]:
        """
        Splits the dataframe into multiple sub-dataframes, using handover events as splitting points.
        
        Args:
        df (pd.DataFrame): Input dataframe with 'Event 5G-NR/LTE Events' column
        
        Returns:
            list: List of dataframes, where each dataframe contains data between handover events
        """
        handover_events = XcallHandoverEvent.get_all_events()

        # Get indices where handover events occur
        split_indices = df[df[self.event_field].isin(handover_events)].index.tolist()
        split_indices.sort()
        
        # print(f"Found {len(split_indices)} handover events in the dataframe")
        # print(f"indices: {split_indices}")

        # Split dataframe into segments
        segments = []
        if len(df) == 0:
            return segments
        start_idx = df.index.tolist()[0]
        for split_idx in split_indices:
            start_idx = start_idx
            end_idx = split_idx
            try:
                segment_df = df.loc[start_idx:end_idx].copy()
            except Exception as e:
                print(f"Error in getting segment df: {str(e)}")
                raise e
            segment = self.create_segment(
                df=segment_df,
                start_idx=start_idx,
                end_idx=end_idx,
            )

            # threshold_ms = 500
            # if segment.duration_ms < threshold_ms:
            #     print(f"Segment ({start_idx}:{end_idx}) is less than threshold {threshold_ms} ms, duration: {segment.duration_ms} ms")
            segments.append(segment)
            start_idx = split_idx + 1

        last_idx = df.index.tolist()[-1]
        if start_idx < last_idx:
            end_idx = last_idx
            try:
                segment_df = df.loc[start_idx:end_idx].copy()
            except Exception as e:
                print(f"Error in getting segment df: {str(e)}")
                raise e
            segments.append(self.create_segment(
                df=segment_df, 
                start_idx=start_idx, 
                end_idx=end_idx,
            ))
        return segments

    def partition_data_by_no_service(self, segments: List[Segment]) -> List[Segment]:
        res = []
        for segment in segments:
            if not segment.check_if_no_service():
                res.append(segment)
                continue
            
            no_service_periods = self.find_no_service_periods(segment.df)
            first_idx = segment.start_idx
            
            # Process each period, including both no-service and has-service segments
            for start_idx, end_idx in no_service_periods:
                # Add has-service segment before the no-service period if it exists
                if first_idx < start_idx:
                    try:
                        service_df = segment.df.loc[first_idx:start_idx-1].copy()
                    except Exception as e:
                        print(f"Error in getting service df: {str(e)}")
                        raise e
                    service_segment = self.create_segment(
                        df=service_df,
                        start_idx=first_idx,
                        end_idx=start_idx-1
                    )
                    res.append(service_segment)
                
                # Add the no-service segment
                try:
                    no_service_df = segment.df.loc[start_idx:end_idx].copy()
                except Exception as e:
                    print(f"Error in getting no-service df: {str(e)}")
                    raise e
                no_service_segment = self.create_segment(
                    df=no_service_df,
                    start_idx=start_idx,
                    end_idx=end_idx
                )
                res.append(no_service_segment)
                
                first_idx = end_idx + 1
            
            # Add remaining has-service segment if it exists
            if first_idx <= segment.end_idx:
                try:
                    remaining_df = segment.df.loc[first_idx:segment.end_idx].copy()
                except Exception as e:
                    print(f"Error in getting remaining df: {str(e)}")
                    raise e
                remaining_segment = self.create_segment(
                    df=remaining_df,
                    start_idx=first_idx,
                    end_idx=segment.end_idx
                )
                res.append(remaining_segment)
                
        return res
    
    def find_first_non_zero_tput_idx(self, df: pd.DataFrame, start_idx: int, step: int) -> int:
        """
        Find the first index with non-zero throughput data starting from start_idx and moving in the direction specified by step.
        
        Args:
            df (pd.DataFrame): The dataframe to search in
            start_idx (int): The starting index for the search
            step (int): Direction of search (1 for downward/forward, -1 for upward/backward)
        
        Returns:
            int: Index of the last non-zero throughput value, or None if no non-zero throughput is found
        """
        tput_field = self.dl_tput_field if self.app_tput_direction == 'downlink' else self.ul_tput_field
        
        # Get actual DataFrame indices
        df_indices = df.index.tolist()
        start_idx_position = df_indices.index(start_idx)
        
        # Determine the range of indices to search based on step direction
        if step == 1:
            # Search downward to end of dataframe
            search_indices = df_indices[start_idx_position:]
        else:
            # Search upward to start of dataframe
            search_indices = df_indices[start_idx_position::-1]
        
        first_non_zero_idx = None
        for idx in search_indices:
            tput_value = df.loc[idx, tput_field]
            if pd.notna(tput_value) and float(tput_value) > 0:
                first_non_zero_idx = idx
                break
        return first_non_zero_idx
    
    def find_no_service_periods(self, df: pd.DataFrame) -> List[Tuple[int, int]]:
        initial_no_service_periods = self.find_consecutive_no_service_rows(df)
        res = []
        for start_idx, end_idx in initial_no_service_periods:
            # try to look up and down to see the last non-zero tput

            # look up from the start_idx
            up_first_non_zero_idx = self.find_first_non_zero_tput_idx(df, start_idx, -1)
            if up_first_non_zero_idx is None:
                # extend to the start of the dataframe
                start_idx = df.index.tolist()[0]
            else:
                start_idx = up_first_non_zero_idx + 1

            # look down from the end_idx
            down_first_non_zero_idx = self.find_first_non_zero_tput_idx(df, end_idx, 1)
            if down_first_non_zero_idx is None:
                # extend to the end of the dataframe
                end_idx = df.index.tolist()[-1]
            else:
                end_idx = down_first_non_zero_idx - 1
            res.append((start_idx, end_idx))
        return res
    
    def find_consecutive_no_service_rows(self, df: pd.DataFrame) -> List[Tuple[int, int]]:
      consecutive_periods = []
      start_idx = None
      last_idx_of_no_service = None
      for idx, row in df.iterrows():
          tech = row[XcalField.TECH]
          if pd.isna(tech):
              continue
          if tech.lower() == 'no service':
              if start_idx is None:
                  start_idx = idx
              last_idx_of_no_service = idx
          elif start_idx is not None:
              # Found the end of a period where condition was True
              consecutive_periods.append((start_idx, last_idx_of_no_service))
              start_idx = None
              last_idx_of_no_service = None
      
      # Handle case where condition extends to the end
      if start_idx is not None:
          consecutive_periods.append((start_idx, last_idx_of_no_service))
      
      return consecutive_periods

    def check_if_consecutive_segments(self, segments: List[Segment]):
        for i in range(len(segments) - 1):
            if segments[i].end_idx + 1 != segments[i + 1].start_idx:
                raise ValueError(f"Segments are not consecutive: {segments[i].get_range()} - {segments[i + 1].get_range()}")
    
    def reassemble_segments(self, segments: List[Segment]) -> pd.DataFrame:
        df = None
        segments.sort(key=lambda x: x.start_idx)
        for segment in segments:
            new_segment_df = segment.df
            try:
                new_segment_df[XcalField.ACTUAL_TECH] = segment.get_tech(self.label)
                new_segment_df[XcalField.SEGMENT_ID] = f'{segment.start_idx}:{segment.end_idx}'
            except Exception as e:
                print(f"Error in getting tech for segment {segment.get_range()}: {str(e)}")
                raise e
            if df is None:
                df = new_segment_df
            else:
                df = pd.concat([df, new_segment_df])
        return df





