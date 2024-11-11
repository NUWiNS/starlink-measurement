import unittest
import sys
import os
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.celllular_analysis.TechBreakdown import TechBreakdown
from scripts.constants import XcalField, XcallHandoverEvent


class TestTechBreakdown(unittest.TestCase):
    def test_breakdown_by_handover_events(self):
        # Only one handover event
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'LTE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 2, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 100, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 3, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 200, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 4, XcalField.TECH: None, XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: XcallHandoverEvent.HANDOVER_SUCCESS, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 5, XcalField.TECH: '5G', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 6, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 300, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
        ])

        tech_breakdown = TechBreakdown(df, app_tput_protocol='tcp', app_tput_direction='downlink')
        segments = tech_breakdown.process()
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0].start_idx, 0)
        self.assertEqual(segments[0].end_idx, 3)
        self.assertEqual(segments[1].start_idx, 4)
        self.assertEqual(segments[1].end_idx, 5)

    def test_split_by_no_service(self):
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 0, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 3, XcalField.TECH: 'NO SERVICE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 2, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 4, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },    
        ])
        tech_breakdown = TechBreakdown(df, app_tput_protocol='tcp', app_tput_direction='downlink')
        segments = tech_breakdown.process()
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0].start_idx, 0)
        self.assertEqual(segments[0].end_idx, 4)

    def test_tech_breakdown(self):
        df = pd.DataFrame([
            # The following should be LTE
            {XcalField.CUSTOM_UTC_TIME: 0, XcalField.TECH: 'LTE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0.1, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 2, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 3, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0.2, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            # The following should be NO_SERVICE
            {XcalField.CUSTOM_UTC_TIME: 4, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink'},
            {XcalField.CUSTOM_UTC_TIME: 5, XcalField.TECH: None, XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 6, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 7, XcalField.TECH: 'NO SERVICE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 8, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 9, XcalField.TECH: None, XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 10, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            # The following should be LTE-A
            {XcalField.CUSTOM_UTC_TIME: 11, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0.2, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 12, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 0, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 13, XcalField.TECH: 'LTE(2CA)', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            # handover
            {XcalField.CUSTOM_UTC_TIME: 14, XcalField.TECH: None, XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: XcallHandoverEvent.HANDOVER_SUCCESS, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            # The following should be 5G-low
            {XcalField.CUSTOM_UTC_TIME: 15, XcalField.TECH: '5G-NR_NSA', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: 999, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 16, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 100, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 17, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 200, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
            {XcalField.CUSTOM_UTC_TIME: 18, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 300, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None, XcalField.APP_TPUT_PROTOCOL: 'tcp', XcalField.APP_TPUT_DIRECTION: 'downlink' },
        ])
        tech_breakdown = TechBreakdown(df, app_tput_protocol='tcp', app_tput_direction='downlink')
        segments = tech_breakdown.process()

        self.assertEqual(len(segments), 4)

        # detect boundaries
        self.assertEqual(segments[0].start_idx, 0)
        self.assertEqual(segments[0].end_idx, 3)
        self.assertEqual(segments[1].start_idx, 4)
        self.assertEqual(segments[1].end_idx, 10)
        self.assertEqual(segments[2].start_idx, 11)
        self.assertEqual(segments[2].end_idx, 14)
        self.assertEqual(segments[3].start_idx, 15)
        self.assertEqual(segments[3].end_idx, 18)

        # detech actual tech
        self.assertEqual(segments[0].get_tech(), 'LTE')
        self.assertEqual(segments[1].get_tech(), 'NO SERVICE')
        self.assertEqual(segments[2].get_tech(), 'LTE-A')
        self.assertEqual(segments[3].get_tech(), '5G-low')

        # detect handover
        self.assertEqual(segments[2].has_handover, True)
        self.assertEqual(segments[1].has_handover, False)

        # iterate through segments, all idx should be consecutive
        tech_breakdown.check_if_consecutive_segments(segments)

        # reassemble segments
        df = tech_breakdown.reassemble_segments(segments)
        self.assertEqual(len(df), 19)
        # all tech field of df should contain non-na
        self.assertEqual(df[XcalField.ACTUAL_TECH].notna().all(), True)

    # def test_no_service_with_csv(self):
    #     df = pd.read_csv(os.path.join(os.path.dirname(__file__), './new_segment_df.csv'))
    #     tech_breakdown = TechBreakdown(df, app_tput_protocol='tcp', app_tput_direction='upl')
    #     segments = tech_breakdown.process()
    #     reassembled_df = tech_breakdown.reassemble_segments(segments)
    #     self.assertEqual(len(reassembled_df), 19)

if __name__ == '__main__':
    unittest.main()