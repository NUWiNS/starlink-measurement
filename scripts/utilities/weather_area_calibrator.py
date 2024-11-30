from datetime import datetime
import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import XcalField, CommonField


class TimedValueCalibrator:
    def __init__(self, df: pd.DataFrame):
        self.df = df.sort_values(by=CommonField.UTC_TS).reset_index(drop=True)
        if len(self.df) == 0:
            raise ValueError("df is empty")
    
    def add_period(
            self, 
            from_dt: datetime, 
            to_dt: datetime,
            value: str
        ):
        """Add a period with a specific value, preserving values before and after."""
        from_ts = from_dt.timestamp()
        from_idx = self.df[CommonField.UTC_TS].searchsorted(from_ts)
            
        exact_from_match = self.df.iloc[from_idx][CommonField.UTC_TS] == from_ts
        prev_value = None
        if exact_from_match:
            prev_value = self.df.iloc[from_idx]['value']
        else:
            if from_idx > 0:
                prev_value = self.df.iloc[from_idx - 1]['value']
            else:
                prev_value = self.df.iloc[0]['value']

        self.add_point(from_dt, value)
        self.add_point(to_dt, prev_value)

    def add_point(self, dt: datetime, value: str):
        ts = dt.timestamp()
        idx = self.df[CommonField.UTC_TS].searchsorted(ts)
        if idx >= len(self.df):
            # append a new row
            self.df = pd.concat([self.df, pd.DataFrame([{CommonField.LOCAL_DT: dt, CommonField.UTC_TS: ts, 'value': value}])]).reset_index(drop=True)
        else:
            exact_match = self.df.iloc[idx][CommonField.UTC_TS] == ts
            row = {CommonField.LOCAL_DT: dt, CommonField.UTC_TS: ts, 'value': value}
            if exact_match:
              # replace the existing row
                self.df.iloc[idx] = row
            else:
                # insert a new row  
                self.df = pd.concat([self.df.iloc[:idx], pd.DataFrame([row]), self.df.iloc[idx:]]).reset_index(drop=True)
        



class AreaCalibratedData:
    def __init__(
        self,
        start_seg_id: str,
        end_seg_id: str,
        value: str,
        start_idx: int | None = None,
        end_idx: int | None = None
    ):
        self.start_seg_id = start_seg_id
        self.end_seg_id = end_seg_id
        self.value = value
        self.start_idx = start_idx
        self.end_idx = end_idx

class AreaCalibratorWithXcal(TimedValueCalibrator):
    def __init__(self, df: pd.DataFrame, xcal_tput_df: pd.DataFrame):
        super().__init__(df)
        self.xcal_tput_df = xcal_tput_df
    
    def calibrate(self, data_list: list[AreaCalibratedData]):
        for data in data_list:
            from_dt, to_dt = self.get_dt_range_from_df(data)
            self.add_period(from_dt, to_dt, data.value)

    def index_overflow(self, seg_df: pd.DataFrame, idx: int):
        return idx < seg_df[XcalField.SRC_IDX].iloc[0] or idx >= seg_df[XcalField.SRC_IDX].iloc[-1]

    def get_dt_range_from_df(self, data: AreaCalibratedData):
        start_seg_df = self.xcal_tput_df[self.xcal_tput_df[XcalField.SEGMENT_ID] == data.start_seg_id]
        end_seg_df = self.xcal_tput_df[self.xcal_tput_df[XcalField.SEGMENT_ID] == data.end_seg_id]
        if len(start_seg_df) == 0 or len(end_seg_df) == 0:
            raise ValueError(f"start_seg_id {data.start_seg_id} or end_seg_id {data.end_seg_id} not found in xcal_tput_df")

        # If start_idx not provided, use first row's src_idx for this segment
        if data.start_idx is None:
            start_idx = start_seg_df.iloc[0][XcalField.SRC_IDX]
        else:
            if self.index_overflow(start_seg_df, data.start_idx):
                raise ValueError(f"start_idx {data.start_idx} is out of range")
            start_idx = data.start_idx
        
        # If end_idx not provided, use last row's src_idx for this segment
        if data.end_idx is None:
            end_idx = end_seg_df.iloc[-1][XcalField.SRC_IDX]
        else:
            if self.index_overflow(end_seg_df, data.end_idx):
                raise ValueError(f"end_idx {data.end_idx} is out of range")
            end_idx = data.end_idx
        
        # Get the timestamps from the rows matching the src_idx
        start_time = self.xcal_tput_df[self.xcal_tput_df[XcalField.SRC_IDX] == start_idx][XcalField.LOCAL_TIME].iloc[0]
        end_time = self.xcal_tput_df[self.xcal_tput_df[XcalField.SRC_IDX] == end_idx][XcalField.LOCAL_TIME].iloc[0]
        
        return datetime.fromisoformat(start_time), datetime.fromisoformat(end_time)