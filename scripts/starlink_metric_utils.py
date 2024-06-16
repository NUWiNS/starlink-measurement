import re
import unittest
import json
from datetime import datetime
from typing import Dict

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
    # remove the timezone info
    return _datetime.replace(tzinfo=None)


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
            req_time = format_datetime_as_iso_8601(parse_starlink_metric_time(req_time))
            res_time = format_datetime_as_iso_8601(parse_starlink_metric_time(res_time))
            metric = StarlinkMetric(data)
            extracted_data.append({
                "req_time": req_time,
                "res_time": res_time,
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

        if 'dishGetStatus' in self.data:
            self.flattened_data = flatten_json(self.data['dishGetStatus'])
        else:
            self.flattened_data = {}

    def get(self, key):
        return self.flattened_data.get(key)

    def get_useful_metrics(self):
        return {
            'latency_ms': self.get('popPingLatencyMs'),
            'tput_dl_bps': self.get('downlinkThroughputBps'),
            'tput_ul_bps': self.get('uplinkThroughputBps')
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
        metric = flatten_json(json_data)
        self.assertEqual(metric.get('a.a1.a2'), "value")
        self.assertEqual(metric.get('b'), None)
        self.assertEqual(metric.get('c'), "value")

    def test_parse_starlink_metric_logs(self):
        content = 'req: 2024-05-27T10:53:13.844772-04:00 | res: 2024-05-27T10:53:14.085348-04:00 | data: {"id":"0","status":null,"apiVersion":"17","dishGetStatus":{"deviceInfo":{"id":"ut01000000-00000000-0085fd5f","hardwareVersion":"rev4_prod1","softwareVersion":"d8c0f816-b72e-4a22-94d5-bb69144fd759.uterm_manifest.release","countryCode":"","utcOffsetS":0,"softwarePartitionsEqual":false,"isDev":false,"bootcount":51,"antiRollbackVersion":0,"isHitl":false,"manufacturedVersion":"","generationNumber":"1715651906","dishCohoused":false,"boardRev":0,"boot":null},"deviceState":{"uptimeS":"148"},"secondsToFirstNonemptySlot":-1,"popPingDropRate":1,"obstructionStats":{"fractionObstructed":0,"validS":0,"currentlyObstructed":false,"avgProlongedObstructionDurationS":0,"avgProlongedObstructionIntervalS":"NaN","avgProlongedObstructionValid":false,"timeObstructed":0,"patchesValid":0},"alerts":{"motorsStuck":false,"thermalShutdown":false,"thermalThrottle":false,"unexpectedLocation":false,"mastNotNearVertical":false,"slowEthernetSpeeds":false,"roaming":false,"installPending":false,"isHeating":false,"powerSupplyThermalThrottle":false,"isPowerSaveIdle":false,"movingWhileNotMobile":false,"dbfTelemStale":false,"movingTooFastForPolicy":false,"lowMotorCurrent":false,"lowerSignalThanPredicted":false},"downlinkThroughputBps":0,"uplinkThroughputBps":12499.026,"popPingLatencyMs":-1,"stowRequested":false,"boresightAzimuthDeg":0,"boresightElevationDeg":0,"outage":{"cause":"BOOTING","startTimestampNs":"-1","durationNs":"0","didSwitch":false},"gpsStats":{"gpsValid":false,"gpsSats":2,"noSatsAfterTtff":false,"inhibitGps":false},"ethSpeedMbps":1000,"mobilityClass":"STATIONARY","isSnrAboveNoiseFloor":true,"readyStates":{"cady":false,"scp":true,"l1l2":true,"xphy":true,"aap":true,"rf":true},"classOfService":"UNKNOWN_USER_CLASS_OF_SERVICE","softwareUpdateState":"IDLE","isSnrPersistentlyLow":false,"hasActuators":"HAS_ACTUATORS_NO","disablementCode":"UNKNOWN_STATE","hasSignedCals":true,"softwareUpdateStats":{"softwareUpdateState":"IDLE","softwareUpdateProgress":0},"alignmentStats":{"hasActuators":"HAS_ACTUATORS_NO","actuatorState":"ACTUATOR_STATE_IDLE","tiltAngleDeg":1.3907721,"boresightAzimuthDeg":0,"boresightElevationDeg":0,"attitudeEstimationState":"FILTER_RESET","attitudeUncertaintyDeg":51.756836,"desiredBoresightAzimuthDeg":0,"desiredBoresightElevationDeg":0},"initializationDurationSeconds":{"attitudeInitialization":0,"burstDetected":42,"ekfConverged":0,"firstCplane":-1,"firstPopPing":-1,"gpsValid":0,"initialNetworkEntry":0,"networkSchedule":0,"rfReady":41,"stableConnection":0},"isCellDisabled":false,"swupdateRebootReady":false,"config":{"snowMeltMode":"ALWAYS_OFF","locationRequestMode":"NONE","levelDishMode":"TILT_LIKE_NORMAL","powerSaveStartMinutes":0,"powerSaveDurationMinutes":0,"powerSaveMode":false,"applySnowMeltMode":true,"applyLocationRequestMode":true,"applyLevelDishMode":true,"applyPowerSaveStartMinutes":true,"applyPowerSaveDurationMinutes":true,"applyPowerSaveMode":true}}}'
        result = parse_starlink_metric_logs(content)
        self.assertEqual([{
            "req_time": "2024-05-27T10:53:13.844772",
            "res_time": "2024-05-27T10:53:14.085348",
            'latency_ms': -1,
            'tput_dl_bps': 0,
            'tput_ul_bps': 12499.026
        }], result)
