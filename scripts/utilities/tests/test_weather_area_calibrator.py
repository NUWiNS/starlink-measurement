import unittest
from datetime import datetime
import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.utilities.weather_area_calibrator import TimedValueCalibrator, AreaCalibratorWithXcal, AreaCalibratedData
from scripts.constants import CommonField

def ts(dt_str: str) -> float:
    return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S').timestamp()

class TestTimedValueCalibrator(unittest.TestCase):
    def setUp(self):
        # Create a sample DataFrame for testing
        self.test_df = pd.DataFrame({
            CommonField.UTC_TS: [ts('2024-01-01T00:00:00'), ts('2024-01-01T02:00:00')],
            CommonField.LOCAL_DT: [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 2, 0)
            ],
            'value': ['sunny', 'cloudy']
        })
        self.calibrator = TimedValueCalibrator(self.test_df)

    def test_add_period_middle(self):
        # Test adding a period between existing entries
        from_dt = datetime(2024, 1, 1, 1, 0)  # 01:00
        to_dt = datetime(2024, 1, 1, 1, 30)   # 01:30
        self.calibrator.add_period(from_dt, to_dt, 'rainy')

        expected_df = pd.DataFrame({
            CommonField.UTC_TS: [
                ts('2024-01-01T00:00:00'),
                ts('2024-01-01T01:00:00'),
                ts('2024-01-01T01:30:00'),
                ts('2024-01-01T02:00:00')
            ],
            'value': ['sunny', 'rainy', 'sunny', 'cloudy']
        })
        pd.testing.assert_frame_equal(
            self.calibrator.df[[CommonField.UTC_TS, 'value']],
            expected_df,
        )

    def test_add_period_end(self):
        # Test adding a period at the end
        from_dt = datetime(2024, 1, 1, 2, 0)  # 02:00
        to_dt = datetime(2024, 1, 1, 2, 30)   # 02:30
        self.calibrator.add_period(from_dt, to_dt, 'rainy') 

        expected_df = pd.DataFrame({
            CommonField.UTC_TS: [
                ts('2024-01-01T00:00:00'),
                ts('2024-01-01T02:00:00'),
                ts('2024-01-01T02:30:00')
            ],
            'value': ['sunny', 'rainy', 'cloudy']
        })
        pd.testing.assert_frame_equal(
            self.calibrator.df[[CommonField.UTC_TS, 'value']],
            expected_df,
        )   

    def test_add_period_start(self):
        # Test adding a period at the start
        from_dt = datetime(2024, 1, 1, 0, 0)  # 00:00
        to_dt = datetime(2024, 1, 1, 0, 30)   # 00:30
        self.calibrator.add_period(from_dt, to_dt, 'rainy')

        expected_df = pd.DataFrame({
            CommonField.UTC_TS: [
                ts('2024-01-01T00:00:00'),
                ts('2024-01-01T00:30:00'),
                ts('2024-01-01T02:00:00')
            ],
            'value': ['rainy', 'sunny', 'cloudy']
        })
        pd.testing.assert_frame_equal(
            self.calibrator.df[[CommonField.UTC_TS, 'value']],
            expected_df,
        )

if __name__ == '__main__':
    unittest.main() 