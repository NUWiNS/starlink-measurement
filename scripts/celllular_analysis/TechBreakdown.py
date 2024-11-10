from typing import List, Tuple
import pandas as pd

from scripts.constants import XcalField, XcallHandoverEvent


class Segment:
    def __init__(
            self, 
            df: pd.DataFrame, 
            start_idx: int,
            end_idx: int,
            time_field: str, 
            freq_field: str,
            tech_field: str,
            band_field: str,
            dl_tput_field: str,
            ul_tput_field: str,
            app_tput_protocol_field: str,
            app_tput_direction_field: str,
        ):
        self.df = df
        self.start_idx = start_idx
        self.end_idx = end_idx

        self.time_field = time_field
        self.freq_field = freq_field
        self.tech_field = tech_field
        self.band_field = band_field
        self.dl_tput_field = dl_tput_field
        self.ul_tput_field = ul_tput_field
        self.app_tput_protocol_field = app_tput_protocol_field
        self.app_tput_direction_field = app_tput_direction_field

        # self.has_tput = self.get_dl_tput_count() > 0 or self.get_ul_tput_count() > 0
        # self.has_freq_5g = self.get_freq_5g_mhz() is not None
        # self.duration_ms = get_segment_duration_ms(df, time_field=time_field)

    def get_range(self) -> str:
        return f"{self.start_idx}:{self.end_idx}"
    
    def get_freq_5g_mhz(self) -> float:
        freq_5g = get_field_with_max_occurence(self.df, field=self.freq_field)
        if freq_5g is None:
            return None
        return float(freq_5g)
    
    def get_tech(self) -> str:
        """
        use the 5G frequency to determine the technology
        """
        freq_5g_mhz = self.get_freq_5g_mhz()
        if freq_5g_mhz is None:
            return 'LTE'
        
        if freq_5g_mhz < 1000:  # Below 1 GHz
            return '5G-low'
        elif freq_5g_mhz < 6000:  # 1-6 GHz
            return '5G-mid'
        elif 27500 <= freq_5g_mhz <= 28350:  # 28 GHz band
            return '5G-mmWave (28GHz)'
        elif 37000 <= freq_5g_mhz <= 40000:  # 39 GHz band
            return '5G-mmWave (39GHz)'
        return 'Unknown'
    
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
        return get_field_with_max_occurence(self.df, field=self.tech_field)
    
    def get_band_from_xcal(self) -> str:
        """
        Might be unreliable due to the 1s resolution of the data
        """
        return get_field_with_max_occurence(self.df, field=self.band_field)
    
    def get_dl_tput_count(self) -> int:
        return get_field_value_count(self.df, field=self.dl_tput_field)
    
    def get_ul_tput_count(self) -> int:
        return get_field_value_count(self.df, field=self.ul_tput_field)

    def check_if_multiple_freq(self) -> bool:
        return check_if_multiple_values_exist(self.df, field=self.freq_field)
    
    def get_dl_tput_df(self) -> pd.DataFrame:
        return self.df[self.df[self.dl_tput_field].notna()].copy()
    
    def get_ul_tput_df(self) -> pd.DataFrame:
        return self.df[self.df[self.ul_tput_field].notna()].copy()
    
    def check_if_no_service(self) -> bool:
        return self.df[self.df[self.tech_field].str.lower() == 'no service'].shape[0] > 0

    def __lt__(self, other):
        """for min-heap sorting"""
        return self.start_idx < other.start_idx

class TechBreakdown:
    def __init__(
            self, 
            df: pd.DataFrame,
            app_tput_protocol: str,
            app_tput_direction: str,
            time_field: str = XcalField.TIMESTAMP,
            tech_field: str = XcalField.TECH,
            band_field: str = XcalField.BAND,
            dl_tput_field: str = XcalField.SMART_TPUT_DL,
            ul_tput_field: str = XcalField.SMART_TPUT_UL,
            event_field: str = XcalField.EVENT_LTE,
            freq_5g_field: str = XcalField.PCELL_FREQ_5G,
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

    def process(self) -> List[Segment]:
        segments = self.partition_data_by_handover(self.df)
        segments = self.partition_data_by_no_service(segments)
        return segments
    
    def create_segment(self, df: pd.DataFrame, start_idx: int, end_idx: int) -> Segment:
        return Segment(
                df=df,
                start_idx=start_idx,
                end_idx=end_idx,
                time_field=self.time_field,
                freq_field=self.freq_5g_field,
                tech_field=self.tech_field,
                band_field=self.band_field,
                dl_tput_field=self.dl_tput_field,
                ul_tput_field=self.ul_tput_field,
                app_tput_protocol_field=self.app_tput_protocol,
                app_tput_direction_field=self.app_tput_direction,
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
        
        print(f"Found {len(split_indices)} handover events in the dataframe")
        print(f"indices: {split_indices}")

        # Split dataframe into segments
        segments = []
        start_idx = 0
        for split_idx in split_indices:
            start_idx = start_idx
            end_idx = split_idx
            segment_df = df.loc[start_idx:end_idx].copy()
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

        if start_idx < len(df):
            end_idx = len(df) - 1
            segment_df = df.loc[start_idx:end_idx].copy()
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
                    service_df = segment.df.loc[first_idx:start_idx-1].copy()
                    service_segment = self.create_segment(
                        df=service_df,
                        start_idx=first_idx,
                        end_idx=start_idx-1
                    )
                    res.append(service_segment)
                
                # Add the no-service segment
                no_service_df = segment.df.loc[start_idx:end_idx].copy()
                no_service_segment = self.create_segment(
                    df=no_service_df,
                    start_idx=start_idx,
                    end_idx=end_idx
                )
                res.append(no_service_segment)
                
                first_idx = end_idx + 1
            
            # Add remaining has-service segment if it exists
            if first_idx <= segment.end_idx:
                remaining_df = segment.df.loc[first_idx:segment.end_idx].copy()
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


  


def partition_data_by_no_service(segment: Segment) -> List[Segment]:
    consecutive_periods = find_consecutive_no_service_rows(segment.df)

    if len(consecutive_periods) == 0:
        return []

    segments = []
    for start_idx, end_idx in consecutive_periods:
        segment_df = segment.df.loc[start_idx:end_idx].copy()
        segment = Segment(df=segment_df, start_idx=start_idx, end_idx=end_idx)
        segments.append(segment)
    return segments
    

def label_tech(df: pd.DataFrame) -> pd.DataFrame:
    # use a min-heap to store the segments sorted by the start time
    segments = partition_data_by_handover(df)

    for idx, segment in enumerate(segments):
        # try:
        #     segment.check_if_only_one_or_zero_tech_exist()
        # except ValueError as e:
        #     logger.warn(e)
        if segment.check_if_no_service():
            sub_segments = partition_data_by_no_service(segment)
            if len(sub_segments) > 0:
                # replace the segment with the sub-segments
                replace_with_elements(segments, idx, sub_segments, inplace=True)
    
    for segment in segments:
        try:
            segment.get_tech()
        except ValueError as e:
            logger.warn(e)

    return segments

def filter_segments_with_tput(segments: List[Segment]) -> List[Segment]:
    return [segment for segment in segments if segment.has_tput]

def get_segment_duration_ms(segment: pd.DataFrame, time_field: str = 'time') -> float:
    """
    get the duration of the segment in milliseconds
    """
    start_time = pd.to_datetime(segment[time_field].iloc[0])
    end_time = pd.to_datetime(segment[time_field].iloc[-1])
    duration = end_time - start_time
    return duration.total_seconds() * 1000 + duration.microseconds / 1000

def get_field_with_max_occurence(df: pd.DataFrame, field: str) -> str:
    """
    get the 5G frequency with the max occurence
    """
    if df[field].isna().all():
        return None
    return df[field].value_counts().idxmax()

def check_if_multiple_values_exist(df: pd.DataFrame, field: str) -> bool:
    """
    check if there are multiple values in the field
    """
    return df[field].nunique() > 1

def get_field_value_count(df: pd.DataFrame, field: str) -> int:
    """
    get the number of rows in the dataframe
    """
    if df[field].isna().all():
        return 0
    return df[field].value_counts().sum()