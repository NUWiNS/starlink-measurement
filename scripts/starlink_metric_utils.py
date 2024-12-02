import re
import unittest
import json
from datetime import datetime
from typing import Dict
import sys
import os

import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))        

from scripts.time_utils import format_datetime_as_iso_8601
from scripts.utils import find_files


def find_starlink_metric_files(base_dir):
    return find_files(base_dir, prefix="dish_status", suffix=".out")


def parse_starlink_metric_time(time_str: str):
    """
    :param time_str: example: 2024-05-27T10:53:14.085348-04:00
    :return:
    """
    _datetime = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    return _datetime


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
            req_time, res_time, data = match.groups()
            req_time = parse_starlink_metric_time(req_time)
            res_time = parse_starlink_metric_time(res_time)
            metric = StarlinkMetric(data)
            extracted_data.append({
                "req_time": format_datetime_as_iso_8601(req_time),
                "res_time": format_datetime_as_iso_8601(res_time),
                **metric.get_useful_metrics()
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
    Flatten a nested json object and create a key structure map

    :param nested_json: The json object to flatten
    :param parent_key: The base key string for recursion
    :param sep: The separator between keys
    :return: (flattened_dict, key_map) where key_map allows prefix-based key lookup
    """
    items = []
    key_map = {}

    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, dict):
            flattened_sub, sub_map = flatten_json(v, new_key, sep=sep)
            items.extend(flattened_sub.items())
            # Add all sub-keys to the key map
            key_map.update(sub_map)
            # Add this key's immediate children to the map
            key_map[new_key] = list(v.keys())
        else:
            items.append((new_key, v))
            # Leaf nodes have no children
            key_map[new_key] = []

    flattened_dict = dict(items)
    
    # Add the root level keys
    if not parent_key:
        key_map['__root__'] = list(nested_json.keys())

    return flattened_dict, key_map


class StarlinkMetric:
    def __init__(self, json_data: str | Dict):
        if isinstance(json_data, str):
            self.data = json.loads(json_data)
        elif isinstance(json_data, dict):
            self.data = json_data
        else:
            raise ValueError("Invalid input type, must be str or dict.")

        if 'dishGetStatus' in self.data:
            self.flattened_data, self.key_map = flatten_json(self.data['dishGetStatus'])
        else:
            self.flattened_data, self.key_map = {}, {}

    def get(self, key):
        return self.flattened_data.get(key)

    def get_immediate_subkeys(self, prefix: str | None =''):
        """
        Get immediate child keys for a given prefix
        
        :param prefix: The prefix to get children for (dot notation)
        :return: List of immediate child keys
        """
        if not prefix or prefix == '__root__':
            return self.key_map['__root__']
        if prefix.startswith('__root__.'):
            prefix = prefix.replace('__root__.', '')
        return self.key_map.get(prefix, [])

    def get_useful_metrics(self):
        # Get all alerts that are True
        alerts = self.get_immediate_subkeys('alerts')
        active_alerts = [event.lower() for event in alerts if self.get(f'alerts.{event}') is True]
        alert_str = ','.join(active_alerts) if active_alerts else ''

        return {
            'latency_ms': self.get('popPingLatencyMs'),
            'tput_dl_bps': self.get('downlinkThroughputBps'),
            'tput_ul_bps': self.get('uplinkThroughputBps'),
            'snr_above_noise_floor': self.get('isSnrAboveNoiseFloor'),
            'snr_persistently_low': self.get('isSnrPersistentlyLow'),
            'outage_cause': self.get('outage.cause'),
            'outage_start_time_ns': self.get('outage.startTimestampNs'),
            'outage.duration_ns': self.get('outage.durationNs'),
            'outage.did_switch': self.get('outage.didSwitch'),
            'obstruction_flag': self.get('obstructionStats.currentlyObstructed'),
            'obstruction_fraction': self.get('obstructionStats.fractionObstructed'),
            'obstruction_valid_s': self.get('obstructionStats.validS'),
            'obstruction_avg_prolonged_s': self.get('obstructionStats.avgProlongedObstructionDurationS'),
            'obstruction_avg_interval_s': self.get('obstructionStats.avgProlongedObstructionIntervalS'),
            'obstruction_time_s': self.get('obstructionStats.timeObstructed'),
            'obstruction_patches_valid': self.get('obstructionStats.patchesValid'),
            'alerts': alert_str,
        }


class Unittest(unittest.TestCase):
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
        metric, key_map = flatten_json(json_data)
        self.assertEqual(metric.get('a.a1.a2'), "value")
        self.assertEqual(metric.get('b'), None)
        self.assertEqual(metric.get('c'), "value")
        self.assertEqual(key_map['__root__'], ['a', 'b', 'c'])
        self.assertEqual(key_map['a'], ['a1'])
        self.assertEqual(key_map['a.a1'], ['a2'])

    def test_parse_starlink_metric_logs(self):
        content = 'req: 2024-05-27T10:53:13.844772-04:00 | res: 2024-05-27T10:53:14.085348-04:00 | data: {"id":"0","status":null,"apiVersion":"17","dishGetStatus":{"deviceInfo":{"id":"ut01000000-00000000-0085fd5f","hardwareVersion":"rev4_prod1","softwareVersion":"d8c0f816-b72e-4a22-94d5-bb69144fd759.uterm_manifest.release","countryCode":"","utcOffsetS":0,"softwarePartitionsEqual":false,"isDev":false,"bootcount":51,"antiRollbackVersion":0,"isHitl":false,"manufacturedVersion":"","generationNumber":"1715651906","dishCohoused":false,"boardRev":0,"boot":null},"deviceState":{"uptimeS":"148"},"secondsToFirstNonemptySlot":-1,"popPingDropRate":1,"obstructionStats":{"fractionObstructed":0,"validS":0,"currentlyObstructed":false,"avgProlongedObstructionDurationS":0,"avgProlongedObstructionIntervalS":"NaN","avgProlongedObstructionValid":false,"timeObstructed":0,"patchesValid":0},"alerts":{"motorsStuck":false,"thermalShutdown":false,"thermalThrottle":false,"unexpectedLocation":false,"mastNotNearVertical":false,"slowEthernetSpeeds":false,"roaming":false,"installPending":false,"isHeating":false,"powerSupplyThermalThrottle":false,"isPowerSaveIdle":false,"movingWhileNotMobile":false,"dbfTelemStale":false,"movingTooFastForPolicy":false,"lowMotorCurrent":false,"lowerSignalThanPredicted":false},"downlinkThroughputBps":0,"uplinkThroughputBps":12499.026,"popPingLatencyMs":-1,"stowRequested":false,"boresightAzimuthDeg":0,"boresightElevationDeg":0,"outage":{"cause":"BOOTING","startTimestampNs":"-1","durationNs":"0","didSwitch":false},"gpsStats":{"gpsValid":false,"gpsSats":2,"noSatsAfterTtff":false,"inhibitGps":false},"ethSpeedMbps":1000,"mobilityClass":"STATIONARY","isSnrAboveNoiseFloor":true,"readyStates":{"cady":false,"scp":true,"l1l2":true,"xphy":true,"aap":true,"rf":true},"classOfService":"UNKNOWN_USER_CLASS_OF_SERVICE","softwareUpdateState":"IDLE","isSnrPersistentlyLow":false,"hasActuators":"HAS_ACTUATORS_NO","disablementCode":"UNKNOWN_STATE","hasSignedCals":true,"softwareUpdateStats":{"softwareUpdateState":"IDLE","softwareUpdateProgress":0},"alignmentStats":{"hasActuators":"HAS_ACTUATORS_NO","actuatorState":"ACTUATOR_STATE_IDLE","tiltAngleDeg":1.3907721,"boresightAzimuthDeg":0,"boresightElevationDeg":0,"attitudeEstimationState":"FILTER_RESET","attitudeUncertaintyDeg":51.756836,"desiredBoresightAzimuthDeg":0,"desiredBoresightElevationDeg":0},"initializationDurationSeconds":{"attitudeInitialization":0,"burstDetected":42,"ekfConverged":0,"firstCplane":-1,"firstPopPing":-1,"gpsValid":0,"initialNetworkEntry":0,"networkSchedule":0,"rfReady":41,"stableConnection":0},"isCellDisabled":false,"swupdateRebootReady":false,"config":{"snowMeltMode":"ALWAYS_OFF","locationRequestMode":"NONE","levelDishMode":"TILT_LIKE_NORMAL","powerSaveStartMinutes":0,"powerSaveDurationMinutes":0,"powerSaveMode":false,"applySnowMeltMode":true,"applyLocationRequestMode":true,"applyLevelDishMode":true,"applyPowerSaveStartMinutes":true,"applyPowerSaveDurationMinutes":true,"applyPowerSaveMode":true}}}'
        result = parse_starlink_metric_logs(content)
        self.assertEqual([{
            "req_time": datetime.strptime("2024-05-27T10:53:13.844772-04:00", "%Y-%m-%dT%H:%M:%S.%f%z"),
            "res_time": datetime.strptime("2024-05-27T10:53:14.085348-04:00", "%Y-%m-%dT%H:%M:%S.%f%z"),
            'latency_ms': -1,
            'tput_dl_bps': 0,
            'tput_ul_bps': 12499.026,
            'snr_above_noise_floor': True,
            'snr_persistently_low': False,
            'outage_cause': 'BOOTING',
            'outage_start_time_ns': '-1',
            'outage.duration_ns': '0',
            'outage.did_switch': False,
            'obstruction_fraction': 0,
            'obstruction_valid_s': 0,
            'obstruction_flag': False,
            'obstruction_avg_prolonged_s': 0,
            'obstruction_avg_interval_s': 'NaN',
            'obstruction_time_s': 0,
            'obstruction_patches_valid': 0,
            'alerts': '',
        }], result)

    def test_key_structure_map(self):
        json_data = {
            "alerts": {
                "motorsStuck": False,
                "thermalShutdown": False
            },
            "obstructionStats": {
                "fractionObstructed": 0.1,
                "validS": 100
            }
        }
        
        flattened, key_map = flatten_json(json_data)
        
        # Test root level keys
        self.assertEqual(set(key_map['__root__']), {'alerts', 'obstructionStats'})
        
        # Test immediate children of 'alerts'
        self.assertEqual(set(key_map['alerts']), {'motorsStuck', 'thermalShutdown'})
        
        # Test full flattened keys
        expected_keys = {
            'alerts.motorsStuck',
            'alerts.thermalShutdown',
            'obstructionStats.fractionObstructed',
            'obstructionStats.validS'
        }
        self.assertEqual(set(flattened.keys()), expected_keys)

    def test_starlink_metric_key_access(self):
        json_data = {
            "dishGetStatus": {
                "alerts": {
                    "motorsStuck": False,
                    "thermalShutdown": False
                },
                "obstructionStats": {
                    "fractionObstructed": 0.1,
                    "validS": 100
                }
            }
        }
        
        metric = StarlinkMetric(json_data)
        # Test getting immediate children
        alert_children = metric.get_immediate_subkeys('alerts')
        self.assertEqual(set(alert_children), {'motorsStuck', 'thermalShutdown'})

        leaf_children = metric.get_immediate_subkeys('alerts.motorsStuck')
        self.assertEqual(leaf_children, [])


if __name__ == '__main__':
    unittest.main()
