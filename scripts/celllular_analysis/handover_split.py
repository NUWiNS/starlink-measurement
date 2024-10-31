import os
import pandas as pd
import sys
from typing import List
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.logging_utils import create_logger
from scripts.alaska_starlink_trip.configs import ROOT_DIR as AL_DATASET_DIR

current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, 'outputs')

logger = create_logger('handover_split', filename=os.path.join(output_dir, f'handover_process.log'))

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

        self.has_tput = self.get_dl_tput_count() > 0 or self.get_ul_tput_count() > 0
        self.has_freq_5g = self.get_freq_5g_mhz() is not None
        self.duration_ms = get_segment_duration_ms(df, time_field=time_field)

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


def partition_data_by_handover(df: pd.DataFrame) -> List[Segment]:
    """
    Splits the dataframe into multiple sub-dataframes, using handover events as splitting points.
    
    Args:
        df (pd.DataFrame): Input dataframe with 'Event 5G-NR/LTE Events' column
        
    Returns:
        list: List of dataframes, where each dataframe contains data between handover events
    """
    handover_events = [
        'Handover Success', 
        'NR To EUTRA Redirection Success', 
        'NR Interfreq Handover Success', 
        'ulInformationTransferMRDC', 
        'MCG DRB Success', 
        'NR SCG Addition Success', 
        'Mobility from NR to EUTRA Success', 
        'NR Intrafreq Handover Success', 
        'NR SCG Modification Success', 
        'scgFailureInformationNR', 
        'EUTRA To NR Redirection Success'
    ]

    # Get indices where handover events occur
    split_indices = df[df['Event 5G-NR/LTE Events'].isin(handover_events)].index.tolist()
    
    # Add start and end indices to create complete segments
    all_indices = [0] + split_indices + [len(df)]
    
    logger.info(f"Found {len(split_indices)} handover events in the dataframe")
    logger.info(f"indices: {split_indices}")

    # Split dataframe into segments
    segments = []
    for i in range(len(all_indices) - 1):
        start_idx = all_indices[i]
        end_idx = all_indices[i + 1]
        segment_df = df.iloc[start_idx:end_idx].copy()
        segment = Segment(
            df=segment_df,
            start_idx=start_idx,
            end_idx=end_idx,
            time_field='utc_dt',
            freq_field='5G KPI PCell RF Frequency [MHz]',
            tech_field='Event Technology',
            band_field='Event Technology(Band)',
            dl_tput_field='Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]',
            ul_tput_field='Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]',
        )

        threshold_ms = 500
        if segment.duration_ms < threshold_ms:
            logger.warning(f"Segment ({start_idx}:{end_idx}) is less than threshold {threshold_ms} ms, duration: {segment.duration_ms} ms")
        segments.append(segment)
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

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logger.info('--------------------------------')
    # Example usage
    df = pd.read_csv(os.path.join(AL_DATASET_DIR, 'xcal', 'att_xcal_raw_tput_logs.csv'))
    segments = partition_data_by_handover(df)
    for segment in segments:
        output_str = f"Segment ({segment.get_range()})"
        output_str += f", duration: {segment.duration_ms} ms"
        output_str += f", 5G freq: {segment.get_freq_5g_mhz()}"
        output_str += f", tech: {segment.get_tech()}"
        output_str += f", dl: {segment.get_dl_tput_count()}"
        output_str += f", ul: {segment.get_ul_tput_count()}"
        output_str += f", has tput: {segment.has_tput}"
        logger.info(output_str)

        if segment.check_if_multiple_freq():
            logger.warn(f"[WARN] Segment ({segment.get_range()}) has multiple 5G frequencies")
            sys.exit(1)

    logger.info('before filtering, there are {} segments'.format(len(segments)))
    segments_that_have_tput = filter_segments_with_tput(segments)
    logger.info('after filtering, there are {} segments that have tput'.format(len(segments_that_have_tput)))

    FIELD_NEW_TECH = 'tech'
    smart_tput_with_tech = []
    for segment in segments_that_have_tput:
        smart_dl_tput = segment.get_dl_tput_df()
        smart_ul_tput = segment.get_ul_tput_df()
        smart_dl_tput[FIELD_NEW_TECH] = segment.get_tech()
        smart_ul_tput[FIELD_NEW_TECH] = segment.get_tech()
        smart_tput_with_tech.append(smart_dl_tput)
        smart_tput_with_tech.append(smart_ul_tput)

    smart_tput_with_tech = pd.concat(smart_tput_with_tech)
    output_csv_path = os.path.join(output_dir, 'smart_tput_with_tech.csv')
    smart_tput_with_tech.to_csv(output_csv_path, index=False)
    logger.info(f"saved to {output_csv_path}")
    # merge_short_segments(segments)


if __name__ == "__main__":
    main()