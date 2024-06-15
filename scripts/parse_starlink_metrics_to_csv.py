import re
import unittest
import json
from typing import Dict


def parse_starlink_metric_logs(content: str):
    """
    :param content:
    :return: List[Tuple[time, rtt_ms]]
    """
    pattern = re.compile(r'req: (.*?) \| res: (.*?) \| data: (.*)')

    extracted_data = []

    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            dt, rtt, data = match.groups()
            metric = StarlinkMetric(data)
            extracted_data.append({

            })

    return extracted_data


def parse_metric_json(json_data: str):
    """
    :param json_data:
    :return: Dict
    """
    data = json.loads(json_data)
    return data


def flatten_json(nested_json, parent_key='', sep='.'):
    """
    Flatten a nested json object

    :param nested_json: The json object to flatten
    :param parent_key: The base key string for recursion
    :param sep: The separator between keys
    :return: A flattened json object
    """
    items = []

    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)


class StarlinkMetric:
    def __init__(self, json_data: str | Dict):
        if isinstance(json_data, str):
            self.data = json.loads(json_data)
        elif isinstance(json_data, dict):
            self.data = json_data
        else:
            raise ValueError("Invalid input type, must be str or dict.")

        self.flattened_data = flatten_json(self.data)

    def get(self, key):
        return self.flattened_data.get(key)

    def get_useful_metrics(self):
        return {
            'latency': self.get('latency'),
            'download_speed': self.get('download_speed'),
            'upload_speed': self.get('upload_speed')
        }


class Unittest(unittest.TestCase):

    def test_parse_starlink_metric_logs(self):
        content = "req: 2024-05-27T10:53:13.844772-04:00 | res: 2024-05-27T10:53:14.085348-04:00 | data: {}"
        result = parse_starlink_metric_logs(content)
        self.assertEqual(result, [('2024-05-27T10:53:13.844772-04:00', '2024-05-27T10:53:14.085348-04:00', '{}')])

    def test_get_starlink_metric(self):
        json_data = {
            "a": {
                "a1": {
                    "a2": "value"
                }
            },
            "b": None,
            "c": "value"
        }
        metric = StarlinkMetric(json_data)
        self.assertEqual(metric.get('a.a1.a2'), "value")
        self.assertEqual(metric.get('b'), None)
        self.assertEqual(metric.get('c'), "value")
