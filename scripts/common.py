import enum
import os
from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import List, Callable, Any, Dict

from scripts.time_utils import StartEndLogTimeProcessor, format_datetime_as_iso_8601


def extract_operator_from_filename(file_path):
    """
    :param file_path: assume the format like /path/to/starlink/20240529/115627940/tcp_downlink_115630977.csv
    :return: the operator name, e.g. starlink
    """
    operator = file_path.split(os.sep)[-4]  # Adjust based on the exact structure of your file paths
    return operator

class TputBaseProcessor(ABC):
    # Assume data is collected for 2min with 500ms interval
    EXPECTED_NUM_OF_DATA_POINTS = 240
    INTERVAL_SEC = 0.5

    class Status(enum.Enum):
        EMPTY = 'EMPTY'
        NORMAL = 'NORMAL'
        TIMEOUT = 'TIMEOUT'
        INCOMPLETE = 'INCOMPLETE'

    def __init__(
            self,
            content: str,
            protocol: str,
            direction: str,
            file_path: str = None,
            timezone_str: str = None,
    ):
        self.content = content
        self.protocol = protocol
        self.direction = direction
        self.file_path = file_path
        self.data_points: List = []
        self.status = self.Status.EMPTY
        self.timezone_str = timezone_str
        self.start_time = None
        self.end_time = None

    def process(self):
        start_end_time_list = StartEndLogTimeProcessor.get_start_end_time_from_log(
            self.content,
            timezone_str=self.timezone_str
        )

        if start_end_time_list:
            # should only be one pair of start and end time
            self.start_time, self.end_time = start_end_time_list[0]

        extracted_result = self.parse_measurement_content(self.content)
        self.status = self.check_validity(extracted_result)

        self.data_points = extracted_result['data_points']
        self.postprocess_data_points()

    def parse_measurement_content(self, content: str):
        data_points = self.parse_data_points(content)
        summary = self.parse_measurement_summary(content)
        return {
            'data_points': data_points,
            'has_summary': summary['has_summary'],
            'avg_tput_mbps': summary['avg_tput_mbps'],
        }

    @abstractmethod
    def postprocess_data_points(self):
        pass

    @abstractmethod
    def parse_data_points(self, content: str):
        pass

    @abstractmethod
    def parse_measurement_summary(self, content: str):
        pass

    @staticmethod
    def check_validity(parsed_result: Dict):
        data_points = parsed_result['data_points']

        if parsed_result['has_summary']:
            return TputBaseProcessor.Status.NORMAL

        if data_points is None or len(data_points) == 0:
            return TputBaseProcessor.Status.EMPTY

        data_len = len(data_points)
        if data_len >= 240:
            return TputBaseProcessor.Status.TIMEOUT
        else:
            return TputBaseProcessor.Status.INCOMPLETE

    @staticmethod
    def pad_tput_data_points(
            raw_data: List,
            create_default_value: Callable[[str], Any],
            expected_len: int,
            start_time: datetime = None,
            interval_sec: float = 1,
    ):
        """
        Pad the throughput data points to the expected length
        """
        _raw_data = raw_data.copy()
        raw_data_len = len(_raw_data)
        if raw_data_len >= expected_len:
            return _raw_data
        if raw_data_len == 0 and start_time is None:
            raise ValueError('Please specify start time if the raw data is empty')

        new_time = start_time
        if start_time is None:
            # get the last time in the raw data + interval
            new_time = datetime.fromisoformat(_raw_data[-1].time) + timedelta(seconds=interval_sec)

        missing_count = expected_len - raw_data_len
        for i in range(0, missing_count):
            default_value = create_default_value(format_datetime_as_iso_8601(new_time))
            _raw_data.append(default_value)
            new_time += timedelta(seconds=interval_sec)
        return _raw_data

    def get_result(self) -> List[Dict[str, str]]:
        return list(map(lambda x: asdict(x), self.data_points))

    def get_status(self) -> str:
        return self.status.value