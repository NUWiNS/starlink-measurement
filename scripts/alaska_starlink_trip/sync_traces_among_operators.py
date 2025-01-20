import glob
import json
import os
import sys
from typing import Dict, List, Tuple

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.utilities.trace_sync import TimestampMatcher
from scripts.alaska_starlink_trip.configs import ROOT_DIR
from scripts.alaska_starlink_trip.separate_dataset import read_dataset, DatasetLabel
from scripts.common import TputBaseProcessor
from scripts.constants import CommonField

starlink_dir = os.path.join(ROOT_DIR, 'raw/starlink_merged')
tmp_dir = os.path.join(ROOT_DIR, 'tmp')
current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, 'outputs')
mptcp_dir = os.path.join(ROOT_DIR, 'mptcp')


def scan_files_in_dataset(dataset_list: List[str], file_pattern: str) -> List[str]:
    files = []
    for dir in dataset_list:
        found_files = glob.glob(os.path.join(dir, file_pattern))
        if not found_files:
            continue
        files.extend(found_files)
    return files

def extract_metadata_from_filename(filename: str) -> Dict[str, str]:
    metadata = {
        'trace_type': '',
        'time_ms': '',
        'status': '',
    }
    components = filename.split('.')
    if filename.startswith('ping'):
        metadata['trace_type'] = 'ping'
        metadata['time_ms'] = components[0].replace('ping_', '')
    elif filename.startswith('tcp_downlink'):
        metadata['trace_type'] = 'tcp_downlink'
        metadata['time_ms'] = components[0].replace('tcp_downlink_', '')
        metadata['status'] = components[1]
    elif filename.startswith('tcp_uplink'):
        metadata['trace_type'] = 'tcp_uplink'
        metadata['time_ms'] = components[0].replace('tcp_uplink_', '')
        metadata['status'] = components[1]
    else:
        raise ValueError(f'Unsupported filename type: {filename}')

    return metadata

def extract_metadata_from_full_path(full_path: str) -> Dict[str, str]:
    path_components = full_path.split('/')
    filename = path_components[-1]
    filename_metadata = extract_metadata_from_filename(filename)
    date = path_components[-3]
    operator = path_components[-4]
    if operator.endswith('_merged'):
        operator = operator.replace('_merged', '')

    time_s = filename_metadata['time_ms'][:-3]
    datetime = f'{date}_{time_s}'
    return {
        'full_path': full_path,
        'operator': operator,
        'trace_type': filename_metadata['trace_type'],
        'datetime': datetime,
        'status': filename_metadata['status'],
    }

def save_operator_trace_list(trace_list: List, output_filename: str):
    with open(output_filename, 'w') as f:
        json.dump(trace_list, f)

def read_operator_trace_list(operator: str, base_dir: str):
    output_filename = get_operator_trace_list_path(operator, base_dir)
    with open(output_filename, 'r') as f:
        return json.load(f)

def get_operator_trace_list_path(operator: str, base_dir: str):
    return os.path.join(base_dir, f'trace_list.{operator}.json')

def get_matched_operator_trace_list_path(base_dir: str, trace_type: str, operator_a: str, operator_b: str):
    return os.path.join(base_dir, f'matched_result.{operator_a}_{operator_b}_{trace_type}.json')

def get_path_of_map_datetime_to_fullpath(operator: str, trace_type: str, base_dir: str):
    return os.path.join(base_dir, f'map_datetime_to_fullpath.{operator}.{trace_type}.json')

def get_path_of_map_fullpath_to_trace_metadata(operator: str, trace_type: str, base_dir: str):
    return os.path.join(base_dir, f'map_fullpath_to_trace_metadata.{operator}.{trace_type}.json')

def generate_operator_trace_list(base_dir: str):
    for operator in ['starlink', 'att', 'verizon', 'tmobile']:
        operator_dataset = {}
        dataset_list = read_dataset(operator, DatasetLabel.NORMAL.value)

        for trace_type in ['tcp_downlink', 'tcp_uplink', 'ping']:
            operator_dataset[trace_type] = []
            csv_list = scan_files_in_dataset(dataset_list, f'{trace_type}*.csv')
            csv_list.sort()
            for csv_path in csv_list:
                metadata = extract_metadata_from_full_path(csv_path)
                operator_dataset[trace_type].append(metadata)

        output_filename = get_operator_trace_list_path(operator, base_dir)
        save_operator_trace_list(operator_dataset, output_filename)
        print(f'save operator {operator} trace to {output_filename}')

def generate_file_path_map(trace_list: List):
    file_path_map = {}
    for trace in trace_list:
        file_path_map[trace['full_path']] = trace
    return file_path_map

def generate_reverse_timestamp_map(trace_list: List):
    timestamp_map = {}
    for trace in trace_list:
        timestamp_map[trace['datetime']] = trace['full_path']
    return timestamp_map

def filter_trace_list(trace_list: List):
    """
    filter traces that are not EMPTY or EMPTY_BLOCKED
    """
    filtered_trace_list = []
    for trace in trace_list:
        if trace['status'] not in [TputBaseProcessor.Status.EMPTY, TputBaseProcessor.Status.EMPTY_BLOCKED]:
            filtered_trace_list.append(trace)
    return filtered_trace_list

def read_json(filename: str):
    with open(filename, 'r') as f:
        return json.load(f)

def save_json(json_obj, filename: str):
    with open(filename, 'w') as f:
        json.dump(json_obj, f)

def sort_pairs(pairs: List[Tuple[str, str]]):
    return list(sorted(pairs, key=lambda x: x[0]))

def match_traces_between_operators(base_dir: str, operator_a: str, operator_b: str):
    operator_a_dataset = read_operator_trace_list(operator_a, base_dir)
    operator_b_dataset = read_operator_trace_list(operator_b, base_dir)

    timestamp_matcher = TimestampMatcher(threshold_seconds=60 * 5)

    for trace_type in ['tcp_downlink', 'tcp_uplink', 'ping']:
        operator_a_trace_list = operator_a_dataset[trace_type]
        print(f'processing {trace_type} with operator {operator_a} and {operator_b}')
        valid_operator_a_trace_list = filter_trace_list(operator_a_trace_list)
        print(f'valid Operator {operator_a} traces: {len(valid_operator_a_trace_list)} / {len(operator_a_trace_list)}')

        operator_a_file_path_map = generate_file_path_map(valid_operator_a_trace_list)
        operator_a_datetime_map = generate_reverse_timestamp_map(valid_operator_a_trace_list)
        operator_a_datetime_list = list(sorted(operator_a_datetime_map.keys()))

        save_json(operator_a_file_path_map, get_path_of_map_fullpath_to_trace_metadata(operator_a, trace_type, base_dir))
        save_json(operator_a_datetime_map, get_path_of_map_datetime_to_fullpath(operator_a, trace_type, base_dir))

        operator_b_trace_list = operator_b_dataset[trace_type]
        valid_operator_b_trace_list = filter_trace_list(operator_b_trace_list)
        print(f'valid Operator {operator_b} traces: {len(valid_operator_b_trace_list)} / {len(operator_b_trace_list)}')

        operator_b_file_path_map = generate_file_path_map(valid_operator_b_trace_list)
        operator_b_datetime_map = generate_reverse_timestamp_map(valid_operator_b_trace_list)
        operator_b_datetime_list = list(sorted(operator_b_datetime_map.keys()))

        save_json(operator_b_file_path_map, get_path_of_map_fullpath_to_trace_metadata(operator_b, trace_type, base_dir))
        save_json(operator_b_datetime_map, get_path_of_map_datetime_to_fullpath(operator_b, trace_type, base_dir))

        matched_pairs, leftover_a, leftover_b = timestamp_matcher.match_datetimes(operator_a_datetime_list, operator_b_datetime_list)
        res = {
            'matched_pairs': sort_pairs(matched_pairs),
            'leftover_a': list(sorted(leftover_a)),
            'leftover_b': list(sorted(leftover_b)),
        }
        output_filename = get_matched_operator_trace_list_path(base_dir, trace_type, operator_a, operator_b)
        save_json(res, output_filename)

        print(f'save matching result to {output_filename}, matched: {len(matched_pairs)}, a_leftover: {len(leftover_a)}, b_leftover: {len(leftover_b)}')

def sync_trace_between_operators(base_dir: str, operator_a: str, operator_b: str, output_dir: str):
    for trace_type in ['tcp_downlink', 'tcp_uplink', 'ping']:
        data_field = CommonField.RTT_MS if trace_type == 'ping' else CommonField.TPUT_MBPS

        print(f'syncing traces between {operator_a} and {operator_b} for {trace_type}')
        matched_result = read_json(get_matched_operator_trace_list_path(base_dir, trace_type, operator_a, operator_b))
        matched_pairs = matched_result['matched_pairs']
        op_a_time_to_path_map = read_json(get_path_of_map_datetime_to_fullpath(operator=operator_a, trace_type=trace_type, base_dir=base_dir))
        op_b_time_to_path_map = read_json(get_path_of_map_datetime_to_fullpath(operator=operator_b, trace_type=trace_type, base_dir=base_dir))
        
        # Create lists to store all matched data
        all_matched_data = []
        
        for pair in matched_pairs:
            op_a_path = op_a_time_to_path_map[pair[0]]
            op_b_path = op_b_time_to_path_map[pair[1]]
            matched, leftover_a, leftover_b = generate_matched_tput_trace(op_a_path, op_b_path)
            print(f'synced result: matched: {len(matched)}, leftover_a: {len(leftover_a)}, leftover_b: {len(leftover_b)}')

            if not matched:
                print(f'no matched traces found between {op_a_path} and {op_b_path}')
                continue

            # Extract matched data into a format suitable for DataFrame
            for row_a, row_b in matched:
                matched_data = {
                    'A_time': row_a['time'],
                    f'A_{data_field}': row_a.get(data_field),
                    'B_time': row_b['time'],
                    f'B_{data_field}': row_b.get(data_field),
                }
                all_matched_data.append(matched_data)

        if all_matched_data:
            # Create DataFrame from all matched data
            fused_df = pd.DataFrame(all_matched_data)
            
            # Save to CSV
            output_path = os.path.join(output_dir, f"fused_trace.{trace_type}.{operator_a}_{operator_b}.csv")
            fused_df.to_csv(output_path, index=False)
            print(f"Saved fused trace to {output_path} with {len(fused_df)} rows")
        else:
            print(f"No matched data found for {trace_type} between {operator_a} and {operator_b}")

def generate_matched_tput_trace(csv_path_a: str, csv_path_b: str):
    """
    Fuse two throughput traces by matching rows with closest timestamps within the overlapping time period.
    
    Args:
        csv_path_a: Path to first CSV file
        csv_path_b: Path to second CSV file
        
    Returns:
        Tuple of (matched_pairs, unmatched_a, unmatched_b) where each element is a list of rows
    """
    # Read CSVs and ensure time column is parsed as datetime
    df_a = pd.read_csv(csv_path_a)
    df_b = pd.read_csv(csv_path_b)
    df_a['time'] = pd.to_datetime(df_a['time'])
    df_b['time'] = pd.to_datetime(df_b['time'])

    # Find overlapping time period
    start_time = max(df_a['time'].iloc[0], df_b['time'].iloc[0])
    end_time = min(df_a['time'].iloc[-1], df_b['time'].iloc[-1])

    # Filter rows within the overlapping period
    rows_a = df_a[(df_a['time'] >= start_time) & (df_a['time'] <= end_time)].to_dict('records')
    rows_b = df_b[(df_b['time'] >= start_time) & (df_b['time'] <= end_time)].to_dict('records')

    # Create TimestampMatcher with 1 second threshold
    matcher = TimestampMatcher(threshold_seconds=1)
    
    # Define key functions to extract timestamps from rows
    def get_timestamp(row):
        return int(row['time'].timestamp())

    # Match rows based on timestamps
    matched_pairs, unmatched_a, unmatched_b = matcher.greedy_match(
        list_a=rows_a,
        list_b=rows_b,
        threshold=1.0,  # 1 second threshold
        key_fn_a=get_timestamp,
        key_fn_b=get_timestamp
    )

    print(f"Found {len(matched_pairs)} matched pairs")
    print(f"Unmatched from trace A: {len(unmatched_a)}")
    print(f"Unmatched from trace B: {len(unmatched_b)}")

    return matched_pairs, unmatched_a, unmatched_b

def main():
    for dir in [output_dir, mptcp_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir)

    generate_operator_trace_list(base_dir=tmp_dir)
    operator_a = 'starlink'
    for operator_b in ['verizon', 'att', 'tmobile']:
        match_traces_between_operators(
            operator_a=operator_a, 
            operator_b=operator_b, 
            base_dir=tmp_dir,
        )

    operator_a = 'starlink'
    for operator_b in ['verizon', 'att', 'tmobile']:
        sync_trace_between_operators(
            operator_a=operator_a, 
            operator_b=operator_b,
            base_dir=tmp_dir, 
            output_dir=mptcp_dir
        )

    #    pass
    # test_tput_path = '/datasets/alaska_starlink_trip/raw/tmobile_merged/20240626/174456699/tcp_downlink_175013017.INCOMPLETE.csv'
    # print(extract_metadata_from_full_path(test_tput_path))

    # test_ping_path = '/datasets/alaska_starlink_trip/raw/tmobile_merged/20240626/174456699/ping_175013017.csv'
    # print(extract_metadata_from_full_path(test_ping_path))

if __name__ == "__main__":
    main()