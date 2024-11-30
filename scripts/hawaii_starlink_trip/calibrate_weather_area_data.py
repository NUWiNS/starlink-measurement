from datetime import datetime
import os
import pandas as pd
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.utilities.weather_area_calibrator import AreaCalibratedData, AreaCalibratorWithXcal
from scripts.hawaii_starlink_trip.configs import ROOT_DIR

xcal_dir = os.path.join(ROOT_DIR, 'xcal')
others_dir = os.path.join(ROOT_DIR, 'others')

def main():
    area_csv = os.path.join(others_dir, 'area.csv')
    area_df = pd.read_csv(area_csv)
    att_xcal_tput_csv = os.path.join(xcal_dir, 'att_xcal_smart_tput.csv')
    att_xcal_tput_df = pd.read_csv(att_xcal_tput_csv)
    area_calibrator = AreaCalibratorWithXcal(area_df, att_xcal_tput_df)
    data_list = [
        AreaCalibratedData(start_seg_id='32972:33022', end_seg_id='34859:34995', value='urban'),
        AreaCalibratedData(
            start_seg_id='34996:35346', 
            start_idx=35005,
            end_seg_id='35347:35473', 
            end_idx=35424,
            value='rural'
        ),
        AreaCalibratedData(
            start_seg_id='78520:78633',
            start_idx=78522, 
            end_seg_id='80678:80744', 
            end_idx=80709,
            value='rural'
        ),
        AreaCalibratedData(
            start_seg_id='77819:77863', 
            start_idx=77833,
            end_seg_id='77980:78075', 
            end_idx=78073,
            value='rural'
        ),
        AreaCalibratedData(
            start_seg_id='12623:12678', 
            start_idx=12628,
            end_seg_id='12679:12715', 
            end_idx=12712,
            value='urban'
        ),
        AreaCalibratedData(
            start_seg_id='14320:14533',
            start_idx=14332,
            end_seg_id='14678:14759', 
            end_idx=14756,
            value='urban'
        ),
        AreaCalibratedData(
            start_seg_id='61769:62188', 
            start_idx=61769,
            end_seg_id='61769:62188', 
            end_idx=61850,
            value='suburban'
        ),
        AreaCalibratedData(
            start_seg_id='62195:62559', 
            start_idx=62300,
            end_seg_id='62195:62559', 
            end_idx=62450,
            value='suburban'
        ),
    ]
    area_calibrator.calibrate(data_list)
    area_calibrator.df.to_csv(area_csv, index=False)

if __name__ == '__main__':
    main()
