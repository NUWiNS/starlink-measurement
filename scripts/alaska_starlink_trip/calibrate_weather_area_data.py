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
    area_csv = os.path.join(others_dir, 'area.csv')
    area_df = pd.read_csv(area_csv)
    att_xcal_tput_csv = os.path.join(xcal_dir, 'att_xcal_smart_tput.csv')
    att_xcal_tput_df = pd.read_csv(att_xcal_tput_csv)
    area_calibrator = AreaCalibratorWithXcal(area_df, att_xcal_tput_df)
    data_list = [
        AreaCalibratedData(
            start_seg_id='34996:35346', 
            start_idx=35005,
            end_seg_id='35347:35473', 
            end_idx=35424,
            value='rural'
        ),
    ]
    area_calibrator.calibrate(data_list)
    area_calibrator.df.to_csv(area_csv.replace('.csv', '.calibrated.csv'), index=False)

if __name__ == '__main__':
    main()
