import unittest
import sys
import os
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.celllular_analysis.TechBreakdown import TechBreakdown
from scripts.constants import XcalField, XcallHandoverEvent


class TestTechBreakdown(unittest.TestCase):

    def test_tech_breakdown(self):
        pass

    def test_breakdown_by_handover_events(self):
        # Only one handover event
        df = pd.DataFrame([
            {XcalField.CUSTOM_UTC_TIME: 1, XcalField.TECH: 'LTE', XcalField.SMART_TPUT_DL: None, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None},
            {XcalField.CUSTOM_UTC_TIME: 2, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 100, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None},
            {XcalField.CUSTOM_UTC_TIME: 3, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 100, XcalField.EVENT_LTE: None, XcalField.PCELL_FREQ_5G: None},
            {XcalField.CUSTOM_UTC_TIME: 4, XcalField.TECH: None, XcalField.SMART_TPUT_DL: 300, XcalField.EVENT_LTE: XcallHandoverEvent.HANDOVER_SUCCESS, XcalField.PCELL_FREQ_5G: None},
        ])

        tech_breakdown = TechBreakdown(df)
        segments = tech_breakdown.process()
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0].start_idx, 0)
        self.assertEqual(segments[0].end_idx, 3)

if __name__ == '__main__':
    unittest.main()