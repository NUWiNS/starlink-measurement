import unittest

import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.celllular_analysis.TechBreakdown import Segment
from scripts.constants import XcalField


class TestSegment(unittest.TestCase):
    def test_segment_get_tech(self):
        # if no 5G frequency, then tech is LTE or LTE-A
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'LTE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), 'LTE')

        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'LTE(2CA)', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), 'LTE-A')

        # if 5G frequency is below 1 GHz, then tech is 5G-low
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), '5G-low')

        # if 5G frequency is between 1 and 6 GHz, then tech is 5G-mid
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 3999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), '5G-mid')

        # if 5G frequency is between 28 and 39 GHz, then tech is 5G-mmWave (28GHz) or 5G-mmWave (39GHz)
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 28350, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), '5G-mmWave (28GHz)')

        # if 5G frequency is above 39 GHz, then tech is 5G-mmWave (39GHz)
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 40000, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), '5G-mmWave (39GHz)')

        # if multiple frequencies, then use the most common frequency
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 2, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 3, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 3999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 4, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 3999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 5, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 3999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=4, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), '5G-mid')

        # if only no-service is detected, then tech is NO_SERVICE
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'NO SERVICE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), 'NO SERVICE')

        # if no service but there is non-zero DL throughput, then raise error
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'NO SERVICE', XcalField.SMART_TPUT_DL: 1, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        with self.assertRaises(ValueError):
            segment.get_tech()

        # if no service but there is non-zero UL throughput, then raise error
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'NO SERVICE', XcalField.SMART_TPUT_UL: 1, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'uplink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='uplink')
        with self.assertRaises(ValueError):
            segment.get_tech()

        # Edge case: if no service but there is a throughput < 0.01, then tech is NO_SERVICE
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'NO SERVICE', XcalField.SMART_TPUT_DL: 0.001, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=0, app_tput_protocol='tcp', app_tput_direction='downlink')
        self.assertEqual(segment.get_tech(), 'NO SERVICE')

    def test_ffill_tech(self):
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'LTE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 2, XcalField.TECH: None, XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 3, XcalField.TECH: None, XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])
        segment = Segment(df, start_idx=0, end_idx=2, app_tput_protocol='tcp', app_tput_direction='downlink')
        segment.ffill_tech()
        # check segment.df[XcalField.TECH] is now 'LTE' for all rows
        self.assertEqual(segment.df[XcalField.ACTUAL_TECH].tolist(), ['LTE', 'LTE', 'LTE'])
        # while not changing the original tech field of df 
        self.assertEqual(segment.df[XcalField.TECH].tolist(), ['LTE', None, None])

if __name__ == '__main__':
    unittest.main()