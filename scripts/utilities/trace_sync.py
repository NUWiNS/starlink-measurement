from datetime import datetime
import logging
from typing import List, Tuple, Set

import numpy as np
import pandas as pd
import glob
import json
import os
from typing import Dict, List, Tuple

import pandas as pd

from scripts.common import TputBaseProcessor
from scripts.constants import CommonField, XcalField

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

    # Threshold is 5 minutes
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


def append_time_diff_to_df(df: pd.DataFrame, id_pairs: List[Tuple[int, int]], time_pairs: List[Tuple[pd.Timestamp, pd.Timestamp]], operator_a: str, operator_b: str, based_operator: str):
    for time_pair, id_pair in zip(time_pairs, id_pairs):
        df = pd.concat([df, pd.DataFrame([{
            'A': operator_a,
            'A_time': time_pair[0],
            'A_idx': id_pair[0],
            'B': operator_b,
            'B_time': time_pair[1],
            'B_idx': id_pair[1],
            'time_diff_sec': (time_pair[1] - time_pair[0]).total_seconds(),
            'based': based_operator,
        }])], ignore_index=True)
    return df

def generate_matched_tput_trace(csv_path_a: str, csv_path_b: str, run_time_a: str, run_time_b: str):
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
    df_a['run_time'] = run_time_a
    df_b['time'] = pd.to_datetime(df_b['time'])
    df_b['run_time'] = run_time_b

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



def sync_trace_between_operators(base_dir: str, operator_a: str, operator_b: str, output_dir: str, logger: logging.Logger):
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
            op_a_run_time = pair[0]
            op_a_path = op_a_time_to_path_map[op_a_run_time]
            op_b_run_time = pair[1]
            op_b_path = op_b_time_to_path_map[op_b_run_time]
            matched, leftover_a, leftover_b = generate_matched_tput_trace(op_a_path, op_b_path, op_a_run_time, op_b_run_time)
            print(f'synced result: matched: {len(matched)}, leftover_a: {len(leftover_a)}, leftover_b: {len(leftover_b)}')

            if not matched:
                print(f'no matched traces found between {op_a_path} and {op_b_path}')
                continue

            # Extract matched data into a format suitable for DataFrame
            for row_a, row_b in matched:
                matched_data = {
                    'A': operator_a,
                    'A_run': op_a_run_time,
                    'A_time': row_a['time'],
                    f'A_{data_field}': row_a.get(data_field),
                    f'A_{XcalField.ACTUAL_TECH}': None,
                    'B': operator_b,
                    'B_run': op_b_run_time,
                    'B_time': row_b['time'],
                    f'B_{data_field}': row_b.get(data_field),
                    f'B_{XcalField.ACTUAL_TECH}': None,
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


def save_inter_operator_zero_tput_diff(operator_a: str, operator_b: str, base_dir: str, output_dir: str, logger: logging.Logger):
    for trace_type in ['tcp_downlink', 'tcp_uplink']:

        df = pd.DataFrame(columns=['A', 'A_time', 'A_idx', 'B', 'B_time', 'B_idx', 'time_diff_sec', 'based'])
        print(f'syncing traces between {operator_a} and {operator_b} for {trace_type}') 
        matched_result = read_json(get_matched_operator_trace_list_path(base_dir, trace_type, operator_a, operator_b))
        matched_file_pairs = matched_result['matched_pairs']
        op_a_time_to_path_map = read_json(get_path_of_map_datetime_to_fullpath(operator=operator_a, trace_type=trace_type, base_dir=base_dir))
        op_b_time_to_path_map = read_json(get_path_of_map_datetime_to_fullpath(operator=operator_b, trace_type=trace_type, base_dir=base_dir))

        matcher = MatchFutureNearestZeroTput(
            base_dir=base_dir,
            field_id=CommonField.SRC_IDX,
            field_data=CommonField.TPUT_MBPS,
            field_time=CommonField.TIME,
        )

        for file_pair in matched_file_pairs:
            logger.info(f'Processing file pair: {file_pair}')
            # Iterate one aligned measurement period with two operators
            op_a_path = op_a_time_to_path_map[file_pair[0]]
            op_b_path = op_b_time_to_path_map[file_pair[1]]
            op_a_df = pd.read_csv(op_a_path)
            op_a_df[CommonField.SRC_IDX] = op_a_df.index
            op_b_df = pd.read_csv(op_b_path)
            op_b_df[CommonField.SRC_IDX] = op_b_df.index
            res = matcher.match_zero_tput_duration_between_operators(op_a_df, op_b_df)
            
            op_a_based_id_pairs = res['op_a_based_id_pairs']
            op_a_based_time_pairs = res['op_a_based_time_pairs']
            op_b_based_id_pairs = res['op_b_based_id_pairs']
            op_b_based_time_pairs = res['op_b_based_time_pairs']

            if len(op_a_based_time_pairs) == 0:
                logger.info(f'No zero-tput pairs with {operator_a} as reference is found for file pair: {file_pair}')
            else:
                df = append_time_diff_to_df(
                    df=df, 
                    id_pairs=op_a_based_id_pairs, 
                    time_pairs=op_a_based_time_pairs, 
                    operator_a=operator_a, 
                    operator_b=operator_b, 
                    based_operator=operator_a
                )
                logger.info(f'Append op_a_based_time_pairs to df: {len(op_a_based_time_pairs)}')

            if len(op_b_based_time_pairs) == 0:
                logger.info(f'No zero-tput pairs with {operator_b} as reference is found for file pair: {file_pair}')
            else:
                df = append_time_diff_to_df(
                    df=df, 
                    id_pairs=op_b_based_id_pairs, 
                    time_pairs=op_b_based_time_pairs, 
                    operator_a=operator_a, 
                    operator_b=operator_b, 
                    based_operator=operator_b
                )
                logger.info(f'Append op_b_based_time_pairs to df: {len(op_b_based_time_pairs)}')

        output_csv_path = os.path.join(output_dir, f'zero_tput_diff.{operator_a}_{operator_b}.{trace_type}.csv')
        df.to_csv(output_csv_path, index=False)
        logger.info(f'saved zero tput diff (total len: {len(df)}) to {output_csv_path}')


def op_key(operator_a_or_b: str, key: str):
    return f'{operator_a_or_b.upper()}_{key}'

def append_actual_tech_to_df(
        df: pd.DataFrame, 
        ref_df: pd.DataFrame, 
        operator_a_or_b: str, 
        logger: logging.Logger,
        df_time_field: str = CommonField.TIME, 
        ref_time_field: str = CommonField.LOCAL_DT,
        diff_sec_threshold: int = 1,
    ):
    # Create the actual tech column
    logger.info(f'mapping {op_key(operator_a_or_b, XcalField.ACTUAL_TECH)}...')
    total_rows = len(df)
    missed_rows = 0

    # Convert ISO8601 datetime strings to timestamps for comparison
    ref_df[ref_time_field] = pd.to_datetime(ref_df[ref_time_field], format='ISO8601').apply(lambda x: x.timestamp())
    ref_df = ref_df.sort_values(by=ref_time_field)
    ref_timestamps = ref_df[ref_time_field].values
    
    # For each row in df, find closest timestamp in ref_df
    for idx, row in df.iterrows():
        # Convert target time to timestamp
        local_dt = row[op_key(operator_a_or_b, df_time_field)]
        target_time = pd.to_datetime(local_dt, format='ISO8601').timestamp()
        
        # Binary search for closest timestamp
        insert_idx = np.searchsorted(ref_timestamps, target_time)
        
        # Handle edge cases
        if insert_idx == 0:
            closest_idx = 0
        elif insert_idx == len(ref_timestamps):
            closest_idx = len(ref_timestamps) - 1
        else:
            # Compare distances to find closest
            prev_diff = abs(ref_timestamps[insert_idx - 1] - target_time)
            curr_diff = abs(ref_timestamps[insert_idx] - target_time)
            # All diffs should be less than 1 second
            closest_idx = insert_idx - 1 if prev_diff < curr_diff else insert_idx

        closest_row = ref_df.iloc[closest_idx]
        # Check if the diff is less than 1 second
        matched_time = closest_row[ref_time_field]
        if abs(matched_time - target_time) > diff_sec_threshold:
            # logger.info(f'[{idx}, {local_dt}]: row has diff that is greater than 3 seconds')
            missed_rows += 1
            continue

        # Assign the actual tech from the closest matching row
        actual_tech = ref_df.iloc[closest_idx][XcalField.ACTUAL_TECH]
        df.at[idx, op_key(operator_a_or_b, XcalField.ACTUAL_TECH)] = actual_tech
        # logger.info(f'[{idx}, {local_dt}]: row is matched with tech ({actual_tech})')
    
    logger.info(f'mapping {op_key(operator_a_or_b, XcalField.ACTUAL_TECH)} is done, missed rows: {missed_rows} / {total_rows} (percentage: {np.round((missed_rows / total_rows) * 100, 2)}%)')
    return df

class TimestampMatcher:
    def __init__(self, threshold_seconds: int = 600):
        self.threshold_seconds = threshold_seconds
    
    def convert_to_seconds(self, timestamp_str: str) -> int:
        """Convert timestamp string to seconds since midnight"""
        try:
            dt = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            return dt.hour * 3600 + dt.minute * 60 + dt.second
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format: {timestamp_str}. Expected format: YYYYMMDD_HHMMSS") from e

    def convert_to_timestamp(self, datetime_str: str) -> int:
        try:
            dt = datetime.strptime(datetime_str, "%Y%m%d_%H%M%S")
            return int(dt.timestamp())
        except ValueError as e:
            raise ValueError(f"Invalid datetime format: {datetime_str}. Expected format: YYYYMMDD_HHMMSS") from e

    def convert_to_datetime_str(self, timestamp: float) -> str:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y%m%d_%H%M%S")

    def greedy_match(self, list_a: List, list_b: List, 
                     threshold: float,
                     key_fn_a=lambda x: x,
                     key_fn_b=lambda x: x,
                     distance_fn=lambda a, b: abs(a - b)) -> Tuple[List[Tuple], List, List]:
        """
        Generic greedy matching function that matches elements from two lists based on a threshold.
        
        Args:
            list_a: First list of elements
            list_b: Second list of elements
            threshold: Maximum allowed distance between matched elements
            key_fn_a: Function to extract comparable value from elements in list_a
            key_fn_b: Function to extract comparable value from elements in list_b
            distance_fn: Function to compute distance between two comparable values
        
        Returns:
            Tuple containing:
            - List of matched pairs (element_a, element_b)
            - List of unmatched elements from list_a
            - List of unmatched elements from list_b
        """
        # Generate all valid pairs with their distances
        pairs = []
        for i, a in enumerate(list_a):
            a_val = key_fn_a(a)
            for j, b in enumerate(list_b):
                b_val = key_fn_b(b)
                dist = distance_fn(a_val, b_val)
                if dist <= threshold:
                    pairs.append((dist, a, b, i, j))
        
        # Sort pairs by distance
        pairs.sort(key=lambda x: x[0])
        
        # Match greedily while maintaining one-to-one constraint
        matched_pairs = []
        used_a = set()
        used_b = set()
        
        for _, a, b, a_idx, b_idx in pairs:
            if a_idx not in used_a and b_idx not in used_b:
                matched_pairs.append((a, b))
                used_a.add(a_idx)
                used_b.add(b_idx)
        
        # Find unmatched elements
        leftover_a = [a for i, a in enumerate(list_a) if i not in used_a]
        leftover_b = [b for i, b in enumerate(list_b) if i not in used_b]
        
        return matched_pairs, leftover_a, leftover_b

    def match_datetimes(self, list_a: List[str], list_b: List[str]) -> Tuple[List[Tuple[str, str]], List[str], List[str]]:
        """
        Match timestamps between two lists based on closest time difference within threshold.
        
        Args:
            list_a: First list of timestamps
            list_b: Second list of timestamps
        
        Returns:
            Tuple containing:
            - List of matched pairs (timestamp_a, timestamp_b)
            - List of unmatched timestamps from list_a
            - List of unmatched timestamps from list_b
        """
        return self.greedy_match(
            list_a=list_a,
            list_b=list_b,
            threshold=self.threshold_seconds,
            key_fn_a=self.convert_to_timestamp,
            key_fn_b=self.convert_to_timestamp
        )

class MatchFutureNearestZeroTput:
    def __init__(self, 
                 base_dir: str,
                 field_id: str,
                 field_data: str,
                 field_time: str,
                 ):
        self.base_dir = base_dir
        self.field_id = field_id
        self.field_data = field_data
        self.field_time = field_time

    def match_zero_tput_duration_between_operators(self, operator_a_df: str, operator_b_df: str):
        """
        Match zero tput between operators for each measurement period (~2min for dl or ul measurement)
        Take operator_a as the reference operator.
        """
        op_a_based_id_pairs, op_a_based_time_pairs = self.match_future_nearest_zero_tput(ref=operator_a_df, target=operator_b_df)
        op_b_based_id_pairs, op_b_based_time_pairs = self.match_future_nearest_zero_tput(ref=operator_b_df, target=operator_a_df)
        return {
            'op_a_based_id_pairs': op_a_based_id_pairs,
            'op_a_based_time_pairs': op_a_based_time_pairs,
            'op_b_based_id_pairs': op_b_based_id_pairs,
            'op_b_based_time_pairs': op_b_based_time_pairs,
        }
            

    def match_future_nearest_zero_tput(self, ref: pd.DataFrame, target: pd.DataFrame):
        """
        Match future nearest zero tput between two operators.
        
        Args:
            ref: Reference DataFrame with 'time' and 'tput_mbps' columns
            target: Target DataFrame with 'time' and 'tput_mbps' columns
            
        Returns:
            List of tuples containing matched zero throughput point IDs (ref_id, target_id)
            If no match is found for a reference point, the target_id will be None
        """
        # Convert time strings to datetime objects
        ref[self.field_time] = pd.to_datetime(ref[self.field_time])
        target[self.field_time] = pd.to_datetime(target[self.field_time])

        # Sort
        ref = ref.sort_values(by=self.field_time)
        target = target.sort_values(by=self.field_time)
        
        # Filter zero throughput points
        ref_zero_tput = ref[ref[self.field_data] == 0].copy()
        target_zero_tput = target[target[self.field_data] == 0].copy()
        
        matched_idx_pairs = []
        matched_time_pairs = []

        # If either dataframe has no zero throughput points, return empty list
        if ref_zero_tput.empty or target_zero_tput.empty:
            return matched_idx_pairs, matched_time_pairs
            
        # Iterate through reference zero throughput points
        for _, ref_row in ref_zero_tput.iterrows():
            ref_time = ref_row[self.field_time]
            ref_id = ref_row[self.field_id]
            max_time = max(ref[self.field_time].max(), target[self.field_time].max())
            
            # Find future zero throughput points in target
            future_zero_points = target_zero_tput[target_zero_tput[self.field_time] >= ref_time]
            
            if not future_zero_points.empty:
                # Get the nearest future point
                nearest_point = future_zero_points.iloc[0]
                matched_idx_pairs.append((ref_id, nearest_point[self.field_id]))
                matched_time_pairs.append((ref_time, nearest_point[self.field_time]))
            # else:
                # If no future point found, use None as target_id
                # matched_idx_pairs.append((ref_id, None))
                # matched_time_pairs.append((ref_time, max_time))

        return matched_idx_pairs, matched_time_pairs
