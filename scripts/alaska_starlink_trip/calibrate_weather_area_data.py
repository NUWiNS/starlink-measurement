from datetime import datetime
import os
import pandas as pd
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.utilities.weather_area_calibrator import AreaCalibratedData, AreaCalibratorWithXcal
from scripts.alaska_starlink_trip.configs import ROOT_DIR

xcal_dir = os.path.join(ROOT_DIR, 'xcal')
others_dir = os.path.join(ROOT_DIR, 'others')

def main():
    """
    Based on ATT's xcal data to do the area calibration.
    """
    area_csv = os.path.join(others_dir, 'area.csv')
    area_df = pd.read_csv(area_csv)
    att_xcal_tput_csv = os.path.join(xcal_dir, 'att_xcal_smart_tput.csv')
    att_xcal_tput_df = pd.read_csv(att_xcal_tput_csv)
    area_calibrator = AreaCalibratorWithXcal(area_df, att_xcal_tput_df)
    data_list = [
        AreaCalibratedData(
            start_seg_id='91:511', 
            start_idx=91,
            end_seg_id='518:888', 
            end_idx=888,
            value='urban'
        ),
        AreaCalibratedData(
            start_seg_id='1025:1332', 
            start_idx=1025,
            end_seg_id='1025:1332', 
            end_idx=1332,
            value='suburban'
        ),
        AreaCalibratedData(
            start_seg_id='10050:10731', 
            start_idx=10065,
            end_seg_id='12161:12579', 
            end_idx=12577,
            value='suburban'
        ),
        AreaCalibratedData(
            start_seg_id='13326:13336', 
            start_idx=13326,
            end_seg_id='15980:16390', 
            end_idx=16386,
            value='suburban'
        ),
        AreaCalibratedData(
            start_seg_id='16831:16881', 
            start_idx=16832,
            end_seg_id='19512:19786', 
            end_idx=19786,
            value='suburban'
        ),
    ]
    area_calibrator.calibrate(data_list)
    area_calibrator.df.to_csv(area_csv, index=False)
    print(f'Saved calibrated area data to {area_csv}')

if __name__ == '__main__':
    main()
