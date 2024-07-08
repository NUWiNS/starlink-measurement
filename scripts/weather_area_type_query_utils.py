import re
import unittest
from datetime import datetime
from typing import List, Tuple, Dict

from scripts.time_utils import TimeIntervalQuery


def parse_weather_area_type_log_line(line: str) -> Dict | None:
    pattern = r"\[(.*)?\] (.*)?: (.*)"
    match = re.match(pattern, line)
    if not match:
        return None
    return {
        'ts': datetime.fromisoformat(match.group(1)),
        'type': match.group(2),
        'value': match.group(3)
    }


def parse_weather_area_type_log(log_path: str) -> Dict[str, List[Tuple[datetime, str]]]:
    """
    Parse weather & area type logs
    :param log_path:
    :return:
    """
    weather_data = []
    area_data = []
    with open(log_path, 'r') as f:
        for line in f:
            result = parse_weather_area_type_log_line(line)
            if not result:
                continue
            if result['type'] == 'weather':
                weather_data.append((result['ts'], result['value']))
            elif result['type'] == 'area':
                area_data.append((result['ts'], result['value']))
    return {
        'weather': weather_data,
        'area': area_data
    }


class TypeIntervalQueryUtil:
    def __init__(self, data: List[Tuple[datetime, str]]):
        """
        :param data: List of tuples containing time and weather type, e.g. [(dt, 'sunny')]
        """
        self.data = data
        self.ts_traces: List[float] = []
        self.interval_query: TimeIntervalQuery | None = None

    def build_interval_query(self):
        self.ts_traces = self.convert_datetime_list_to_timestamp_traces()
        self.interval_query = TimeIntervalQuery(self.ts_traces)

    def query(self, ts: datetime | float) -> str:
        """
        Query the weather type at the timestamp ts
        :param ts:
        :return:
        """
        ts = float(ts.timestamp()) if isinstance(ts, datetime) else ts
        if not self.interval_query:
            self.build_interval_query()
        start_i, end_i = self.interval_query.query_interval_start_end_index(ts)
        if start_i is None:
            return 'unknown'
        # Use the value of the left closest record
        return self.data[start_i][1]

    def convert_datetime_list_to_timestamp_traces(self):
        """
        Convert datetime to timestamp
        :return:
        """
        return [float(dt.timestamp()) for dt, _ in self.data]


class Unittest(unittest.TestCase):
    def test_parse_weather_area_type_log_line(self):
        line = "[2021-06-21T00:00:00] weather: sunny"
        result = parse_weather_area_type_log_line(line)
        self.assertEqual(result['ts'], datetime(2021, 6, 21, 0, 0, 0))
        self.assertEqual(result['type'], 'weather')
        self.assertEqual(result['value'], 'sunny')

        line = '[2021-06-21T00:00:01] area: urban'
        result = parse_weather_area_type_log_line(line)
        self.assertEqual(result['ts'], datetime(2021, 6, 21, 0, 0, 1))
        self.assertEqual(result['type'], 'area')
        self.assertEqual(result['value'], 'urban')
