import os
import pandas as pd
import sys
from typing import Dict, List, Tuple

from scripts.utilities.distance_utils import DistanceUtils

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.constants import XcalField, operator_color_map
from scripts.alaska_starlink_trip.configs import ROOT_DIR as AL_DATASET_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HI_DATASET_DIR


cellular_location_conf = {
    'alaska': {
        'label': 'AK',
        'root_dir': AL_DATASET_DIR,
        'operators': ['verizon', 'att'],
        'order': 1,
        'tcp_downlink': {
            'interval_x': 100,
            'max_xlim': 300,
        },
        'tcp_uplink': {
            'interval_x': 25,
            'max_xlim': 100,
        },
    },
    'hawaii': {
        'label': 'HI',
        'root_dir': HI_DATASET_DIR,
        'operators': ['verizon', 'att', 'tmobile'],
        'order': 2,
        'tcp_downlink': {
            'interval_x': 100,
            'max_xlim': 300,
        },
        'tcp_uplink': {
            'interval_x': 50,
            'max_xlim': 150,
        },
    }
}


location_conf = {
    'alaska': {
        'label': 'AK',
        'root_dir': AL_DATASET_DIR,
        'operators': ['starlink', 'verizon', 'att'],
        'tcp_downlink': {
            'interval_x': 100,
            'max_xlim': 300,
        },
        'tcp_uplink': {
            'interval_x': 50,
            'max_xlim': 100,
        },
        'order': 1,
        'color': 'b',
    },
    'hawaii': {
        'label': 'HI',
        'root_dir': HI_DATASET_DIR,
        'operators': ['starlink', 'verizon', 'att', 'tmobile'],
        'tcp_downlink': {
            'interval_x': 300,
            'max_xlim': 900,
        },
        'tcp_uplink': {
            'interval_x': 50,
            'max_xlim': 150,
        },
        'order': 2,
        'color': 'r',
    }
}

cellular_operator_conf = {
    'att': {
        'label': 'AT&T',
        'order': 1,
        'color': operator_color_map['att'],
        'linestyle': '-'
    },
    'verizon': {
        'label': 'Verizon',
        'order': 2,
        'color': operator_color_map['verizon'],
        'linestyle': 'dashed'
    },
    'tmobile': {
        'label': 'T-Mobile',
        'order': 3,
        'color': operator_color_map['tmobile'],
        'linestyle': 'dotted'
    },
}

operator_conf = {
    **cellular_operator_conf,
    'starlink': {
        'label': 'Starlink',
        'order': 4,
        'color': operator_color_map['starlink'],
        'linestyle': '-.',
    },
}

tech_conf = {
    'LTE': {
        'label': 'LTE',
        'color': '#326f21',
        'order': 1
    },
    'LTE-A': {
        'label': 'LTE-A',
        'color': '#86c84d',
        'order': 2
    },
    '5G-low': {
        'label': '5G Low',
        'color': '#ffd700',
        'order': 3
    },
    '5G-mid': {
        'label': '5G Mid',
        'color': '#ff9900',
        'order': 4
    },
    '5G-mmWave (28GHz)': {
        'label': '5G-mmWave (28GHz)',
        'color': '#ff4500',
        'order': 5
    },
    '5G-mmWave (39GHz)': {
        'label': '5G-mmWave (39GHz)',
        'color': '#ba281c',
        'order': 6
    },
    'NO SERVICE': {
        'label': 'No Service',
        'color': '#808080',
        'order': 7
    },
    'Unknown': {
        'label': 'Unknown',
        'color': '#000000',
        'order': 8
    },
}

# Colors for different technologies - from grey (NO SERVICE) to rainbow gradient (green->yellow->orange->red) for increasing tech
colors = ['#808080', '#326f21', '#86c84d', '#ffd700', '#ff9900', '#ff4500', '#ba281c']  # Grey, green, light green, yellow, amber, orange, red
tech_order = ['Unknown', 'NO SERVICE', 'LTE', 'LTE-A', '5G-low', '5G-mid', '5G-mmWave (28GHz)', '5G-mmWave (39GHz)']


def read_xcal_tput_data(root_dir: str, operator: str, protocol: str = None, direction: str = None):
    input_csv_path = os.path.join(root_dir, 'xcal/sizhe_new_data', f'{operator}_xcal_smart_tput.csv')
    df = pd.read_csv(input_csv_path)
    if protocol:
        df = df[df[XcalField.APP_TPUT_PROTOCOL] == protocol]
    if direction:
        df = df[df[XcalField.APP_TPUT_DIRECTION] == direction]
    return df

def aggregate_xcal_tput_data(
        root_dir: str, 
        operators: List[str], 
        protocol: str = None, 
        direction: str = None, 
    ):
    data = pd.DataFrame()
    for operator in operators:
        df = read_xcal_tput_data(
            root_dir=root_dir, 
            operator=operator, 
            protocol=protocol, 
            direction=direction, 
        )
        df['operator'] = operator
        data = pd.concat([data, df])
    return data

def aggregate_xcal_tput_data_by_location(
        locations: List[str], 
        location_conf: Dict[str, Dict],
        protocol: str = None, 
        direction: str = None, 
    ):
    data = pd.DataFrame()
    for location in locations:
        conf = location_conf[location]
        df = aggregate_xcal_tput_data(
            root_dir=conf['root_dir'], 
            operators=conf['operators'], 
            protocol=protocol, 
            direction=direction, 
        )
        df['location'] = location
        data = pd.concat([data, df])
    return data


def read_latency_data(root_dir: str, operator: str):
    latency_data_path = os.path.join(root_dir, 'ping/sizhe_new_data', f'{operator}_ping.csv')
    if not os.path.exists(latency_data_path):
        raise FileNotFoundError(f'Latency data file not found: {latency_data_path}')
    return pd.read_csv(latency_data_path)

def aggregate_operator_latency_data(root_dir: str, operators: List[str]):
    df = pd.DataFrame()
    for operator in operators:
        latency_data = read_latency_data(root_dir, operator)
        latency_data['operator'] = operator_conf[operator]['label']
        df = pd.concat([df, latency_data], ignore_index=True)
    return df

def aggregate_latency_data_by_location(locations: List[str], location_conf: Dict[str, Dict]):
    combined_data = pd.DataFrame()
    for location in locations:
        conf = location_conf[location]
        latency_data = aggregate_operator_latency_data(conf['root_dir'], conf['operators'])
        latency_data['location'] = location
        combined_data = pd.concat([combined_data, latency_data], ignore_index=True)
    return combined_data


def calculate_tech_coverage_in_miles(grouped_df: pd.DataFrame) -> Tuple[dict, float]:
    # Initialize mile fractions for each tech
    tech_distance_mile_map = {}
    for tech in tech_order:
        tech_distance_mile_map[tech] = 0

    # calculate cumulative distance for each segment
    for segment_id, segment_df in grouped_df:
        unique_techs = segment_df[XcalField.ACTUAL_TECH].unique()
        if len(unique_techs) > 1:
            raise ValueError(f"Segment ({segment_id}) should only have one tech: {unique_techs}")
        tech = unique_techs[0]

        tech_distance_miles = DistanceUtils.calculate_cumulative_miles(segment_df[XcalField.LON].tolist(), segment_df[XcalField.LAT].tolist())
        # add to total distance for this tech
        tech_distance_mile_map[tech] += tech_distance_miles

    total_distance_miles = sum(tech_distance_mile_map.values())

    return tech_distance_mile_map, total_distance_miles
