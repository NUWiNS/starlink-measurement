import re
import unittest
from bisect import bisect_right
from typing import List

import pytz
from datetime import datetime


def now(fmt='%Y%m%d%H%M%S'):
    if fmt:
        return datetime.now().strftime(fmt)
    return datetime.now()


def format_datetime_as_iso_8601(dt: datetime):
    """
    :param dt:
    :return:
    """
    return dt.isoformat()


def append_timezone(dt: datetime, timezone_str: str, is_dst: bool = True):
    timezone = pytz.timezone(timezone_str)
    dt_aware = timezone.localize(dt, is_dst=is_dst)  # is_dst=True for daylight saving time
    return dt_aware


class StartEndLogTimeProcessor:
    @staticmethod
    def get_start_end_time_from_log(content: str, timezone_str: str = 'auto') -> (datetime, datetime):
        """
        Get the start and end time from the log file
        :param log:
        :return:
        """
        if timezone_str == 'auto':
            timezone_str = 'UTC'
        start_pattern = r'Start time:\s*(\d+)'
        end_pattern = r'End time:\s*(\d+)'

        # Find all matches for start and end times
        start_times = re.findall(start_pattern, content)
        end_times = re.findall(end_pattern, content)

        # Convert timestamps to datetime objects and pair them
        time_pairs = []
        for start, end in zip(start_times, end_times):
            start_dt = datetime.fromtimestamp(int(start) / 1000.0, pytz.timezone(timezone_str))
            end_dt = datetime.fromtimestamp(int(end) / 1000.0, pytz.timezone(timezone_str))
            time_pairs.append((start_dt, end_dt))

        return time_pairs


class TimeIntervalQuery:
    def __init__(self, ts_traces: List[float]):
        self.ts_traces = ts_traces

    def query_interval_start_end_index(self, ts: float) -> (float, float):
        """
        Query the interval with start and end time that the timestamp ts belongs to
        :param ts:
        :return:
        """
        pos = bisect_right(self.ts_traces, ts)
        if pos == 0:
            return None, 0
        if pos == len(self.ts_traces):
            return len(self.ts_traces) - 1, None
        return pos - 1, pos


class Unittest(unittest.TestCase):
    def test_start_end_time_extraction(self):
        d1 = datetime(2021, 6, 21, 0, 0, 0)
        d2 = datetime(2021, 6, 21, 0, 0, 1)
        d3 = datetime(2021, 6, 21, 0, 0, 2)
        d4 = datetime(2021, 6, 21, 0, 0, 3)
        content = f"""
        Start time: {int(d1.timestamp() * 1000)} 
        End time: {int(d2.timestamp() * 1000)}
        Start time: {int(d3.timestamp() * 1000)}
        End time: {int(d4.timestamp() * 1000)}
        """
        time_pairs = StartEndLogTimeProcessor.get_start_end_time_from_log(content)
        self.assertEqual(len(time_pairs), 2)
        self.assertEqual(time_pairs[0][0], d1)
        self.assertEqual(time_pairs[0][1], d2)
        self.assertEqual(time_pairs[1][0], d3)
        self.assertEqual(time_pairs[1][1], d4)

    def test_time_interval_query(self):
        ts_traces = [100, 200, 300, 400, 500]
        query = TimeIntervalQuery(ts_traces)
        self.assertEqual(query.query_interval_start_end_index(50), (None, 0))
        self.assertEqual(query.query_interval_start_end_index(100), (0, 1))
        self.assertEqual(query.query_interval_start_end_index(250), (1, 2))
        self.assertEqual(query.query_interval_start_end_index(550), (4, None))
