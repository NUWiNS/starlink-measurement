import os
import sys

import pandas as pd
import numpy as np


sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.time_utils import ensure_timezone
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger
from scripts.utilities.trace_sync import append_actual_tech_to_df, extract_metadata_from_full_path, get_operator_trace_list_path, match_traces_between_operators, op_key, save_inter_operator_zero_tput_diff, save_operator_trace_list, scan_files_in_dataset, sync_trace_between_operators
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE
from scripts.alaska_starlink_trip.separate_dataset import read_dataset, DatasetLabel

starlink_dir = os.path.join(ROOT_DIR, 'raw/starlink_merged')
tmp_dir = os.path.join(ROOT_DIR, 'tmp')
current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, 'outputs')
mptcp_dir = os.path.join(ROOT_DIR, 'mptcp')

logger = create_logger('sync_traces_among_operators', filename=os.path.join(output_dir, 'sync_traces_among_operators.log'))


def generate_operator_trace_list(base_dir: str):
    for operator in ['starlink', 'att', 'verizon']:
        operator_dataset = {}
        dataset_list = read_dataset(operator, DatasetLabel.NORMAL.value)
        # Consider BBR testing data as normal data as it was done in cities of Alaska
        bbr_dataset_list = read_dataset(operator, DatasetLabel.BBR_TESTING_DATA.value)
        dataset_list.extend(bbr_dataset_list)

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


ref_df_path_map = {
    'verizon': {
        'tcp_downlink': os.path.join(ROOT_DIR, 'xcal/sizhe_new_data', f'verizon_xcal_smart_tput.csv'),
        'tcp_uplink': os.path.join(ROOT_DIR, 'xcal/sizhe_new_data', f'verizon_xcal_smart_tput.csv'),
        'ping': os.path.join(ROOT_DIR, 'ping/sizhe_new_data', f'verizon_ping.csv'),
    },
    'att': {
        'tcp_downlink': os.path.join(ROOT_DIR, 'xcal/sizhe_new_data', f'att_xcal_smart_tput.csv'),
        'tcp_uplink': os.path.join(ROOT_DIR, 'xcal/sizhe_new_data', f'att_xcal_smart_tput.csv'),
        'ping': os.path.join(ROOT_DIR, 'ping/sizhe_new_data', f'att_ping.csv'),
    },
}

def main():
    for dir in [output_dir, mptcp_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir)

    # generate_operator_trace_list(base_dir=tmp_dir)

    operators = ['starlink', 'verizon', 'att']
    for i in range(len(operators) - 1):
        for j in range(i+1, len(operators)):
            operator_a = operators[i]
            operator_b = operators[j]

            # match_traces_between_operators(
            #     operator_a=operator_a, 
            #     operator_b=operator_b, 
            #     base_dir=tmp_dir,
            # )

            sync_trace_between_operators(
                operator_a=operator_a, 
                operator_b=operator_b,
                base_dir=tmp_dir, 
                output_dir=mptcp_dir,
                logger=logger
            )

            # append actual tech to the fused trace
            for trace_type in ['tcp_downlink', 'tcp_uplink']:
                df_path = os.path.join(mptcp_dir, f'fused_trace.{trace_type}.{operator_a}_{operator_b}.csv')
                df = pd.read_csv(df_path)
                        
                for op_idx, operator in enumerate([operator_a, operator_b]):
                    op_config = ref_df_path_map.get(operator)
                    if op_config is None:
                        continue
                    ref_df_path = op_config.get(trace_type)
                    if ref_df_path is None:
                        continue
                    ref_df = pd.read_csv(ref_df_path)
                    append_actual_tech_to_df(
                        df=df, 
                        ref_df=ref_df, 
                        operator_a_or_b='A' if op_idx == 0 else 'B', 
                        logger=logger,
                        df_time_field=CommonField.TIME,
                        ref_time_field=CommonField.LOCAL_DT,
                        diff_sec_threshold=1,
                    )
                df.to_csv(df_path, index=False)
                logger.info(f'Save rows with appended tech to {df_path}')

            for trace_type in ['ping']:
                df_path = os.path.join(mptcp_dir, f'fused_trace.{trace_type}.{operator_a}_{operator_b}.csv')
                df = pd.read_csv(df_path)
                df[op_key('A', CommonField.TIME)] = pd.to_datetime(df[op_key('A', CommonField.TIME)]).apply(lambda x: ensure_timezone(x, TIMEZONE))
                df[op_key('B', CommonField.TIME)] = pd.to_datetime(df[op_key('B', CommonField.TIME)]).apply(lambda x: ensure_timezone(x, TIMEZONE))

                for op_idx, operator in enumerate([operator_a, operator_b]):
                    op_config = ref_df_path_map.get(operator)
                    if op_config is None:
                        continue
                    ref_df_path = op_config.get(trace_type)
                    if ref_df_path is None:
                        continue
                    ref_df = pd.read_csv(ref_df_path)
                    append_actual_tech_to_df(
                        df=df, 
                        ref_df=ref_df, 
                        operator_a_or_b='A' if op_idx == 0 else 'B', 
                        logger=logger,
                        df_time_field=CommonField.TIME,
                        ref_time_field=CommonField.LOCAL_DT,
                        diff_sec_threshold=1,
                    )
                df.to_csv(df_path, index=False)
                logger.info(f'Save rows with appended tech to {df_path}')

            # save_inter_operator_zero_tput_diff(
            #     operator_a=operator_a, 
            #     operator_b=operator_b,
            #     base_dir=tmp_dir,
            #     output_dir=mptcp_dir,
            #     logger=logger
            # )
        

if __name__ == "__main__":
    main()