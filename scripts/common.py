import enum
from logging import Logger
import os
import unittest
from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import List, Callable, Any, Dict

import numpy
import numpy as np

from scripts.time_utils import StartEndLogTimeProcessor, format_datetime_as_iso_8601
from scripts.utils import safe_get
from scripts.logging_utils import SilentLogger

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
        EMPTY_BLOCKED = 'EMPTY_BLOCKED'
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
            logger: Logger = None,
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
        self.logger = logger or SilentLogger()

    def process(self):
        self.logger.info(f'[start processing] {self.file_path}')

        start_end_time_list = StartEndLogTimeProcessor.get_start_end_time_from_log(
            self.content,
            timezone_str=self.timezone_str
        )

        if start_end_time_list:
            # should only be one pair of start and end time
            self.start_time, self.end_time = start_end_time_list[0]

        try:
            estimated_data_points = self.estimate_data_points(self.start_time, self.end_time)
        except Exception as e:
            self.logger.error(f'Failed to estimate data points for file: {self.file_path}, check if start and end time are correctly extracted')
            raise e
            
        self.logger.info(f'-- [estimating data points] {estimated_data_points} (start_time: {self.start_time}, end_time: {self.end_time})')

        extracted_result = self.parse_measurement_content(self.content)
        self.status = self.check_validity(extracted_result)
        self.logger.info(f'-- [check validity] It is a {self.status} result')

        self.data_points = extracted_result['data_points']
        self.logger.info(f'-- [extracted data points] {len(self.data_points)}, diff from estiamte: {len(self.data_points) - estimated_data_points}, diff from expected: {len(self.data_points) - self.EXPECTED_NUM_OF_DATA_POINTS}')
        
        self.logger.info(f'-- [start postprocessing]')
        self.postprocess_data_points()
        self.logger.info(f'-- [end postprocessing]')
        self.logger.info(f'[end processing] {self.file_path}\n\n')

    def estimate_data_points(self, start_time: datetime, end_time: datetime):
        return int((end_time - start_time).total_seconds() / self.INTERVAL_SEC)

    def parse_measurement_content(self, content: str):
        data_points = self.parse_data_points(content)
        summary = self.parse_measurement_summary(content)
        if not summary['avg_tput_mbps'] or summary['avg_tput_mbps'] == -1:
            if isinstance(data_points, list) and len(data_points) > 0:
                # calculate the average throughput
                summary['avg_tput_mbps'] = np.mean(
                    list(map(lambda x: float(getattr(x, 'throughput_mbps')), data_points)))
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

        if data_points is None or len(data_points) == 0:
            return TputBaseProcessor.Status.EMPTY

        data_len = len(data_points)
        if all(map(lambda x: float(safe_get(x, 'throughput_mbps')) == 0, data_points)) and data_len < 5:
            return TputBaseProcessor.Status.EMPTY

        # consider extremely low throughput as empty, which helps udp blockage detection
        if parsed_result['avg_tput_mbps'] < 0.1 and data_len < 20:
            return TputBaseProcessor.Status.EMPTY

        if parsed_result['has_summary'] and data_len > 1:
            return TputBaseProcessor.Status.NORMAL

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


class Unittest(unittest.TestCase):

    def test_check_validity(self):
        # --- Normal cases: has summary and tput not all 0 ---
        input_data = {
            'data_points': [{'throughput_mbps': 100}] * 240,
            'has_summary': True,
            'avg_tput_mbps': 100
        }
        self.assertEqual(TputBaseProcessor.Status.NORMAL,
                         TputBaseProcessor.check_validity(input_data))

        input_data = {
            'data_points': [{'throughput_mbps': 100}] * 250,
            'has_summary': True,
            'avg_tput_mbps': 100
        }
        self.assertEqual(TputBaseProcessor.Status.NORMAL,
                         TputBaseProcessor.check_validity(input_data))

        # --- Empty cases ---
        # if there is only one data point with 0 throughput, it should be considered as empty
        input_data = {
            'data_points': [{'throughput_mbps': 0}],
            'has_summary': True,
            'avg_tput_mbps': -1
        }
        self.assertEqual(TputBaseProcessor.Status.EMPTY, TputBaseProcessor.check_validity(input_data))

        # if there are less than 5 data points and all 0 tput, it should be considered as empty
        input_data = {
            'data_points': [{'throughput_mbps': 0}] * 4,
            'has_summary': True,
            'avg_tput_mbps': 0
        }
        self.assertEqual(TputBaseProcessor.Status.EMPTY, TputBaseProcessor.check_validity(input_data))

        # if the avg tput is less than 0.1 Mbps, it should be considered as empty
        input_data = {
            'data_points': [{'throughput_mbps': 0.09}] * 10,
            'has_summary': True,
            'avg_tput_mbps': 0.09
        }
        self.assertEqual(TputBaseProcessor.Status.EMPTY, TputBaseProcessor.check_validity(input_data))

        # --- no summary cases ---
        input_data = {
            'data_points': [],
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(TputBaseProcessor.Status.EMPTY, TputBaseProcessor.check_validity(input_data))

        input_data = {
            'data_points': None,
            'has_summary': False,
            'avg_tput_mbps': -1
        }
        self.assertEqual(TputBaseProcessor.Status.EMPTY, TputBaseProcessor.check_validity(input_data))

        # --- Incomplete cases (no summary) ---
        input_data = {
            'data_points': [{'throughput_mbps': 100}] * 239,
            'has_summary': False,
            'avg_tput_mbps': 100
        }
        self.assertEqual(TputBaseProcessor.Status.INCOMPLETE,
                         TputBaseProcessor.check_validity(input_data))

        # --- Timeout cases ---
        input_data = {
            'data_points': [{'throughput_mbps': 100}] * 250,
            'has_summary': False,
            'avg_tput_mbps': 100
        }
        self.assertEqual(TputBaseProcessor.Status.TIMEOUT,
                         TputBaseProcessor.check_validity(input_data))
