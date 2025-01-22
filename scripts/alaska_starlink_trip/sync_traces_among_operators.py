import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.logging_utils import create_logger
from scripts.utilities.trace_sync import extract_metadata_from_full_path, get_operator_trace_list_path, match_traces_between_operators, save_inter_operator_zero_tput_diff, save_operator_trace_list, scan_files_in_dataset, sync_trace_between_operators
from scripts.alaska_starlink_trip.configs import ROOT_DIR
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

def main():
    for dir in [output_dir, mptcp_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir)

    generate_operator_trace_list(base_dir=tmp_dir)

    operators = ['starlink', 'verizon', 'att']
    for i in range(len(operators) - 1):
        for j in range(i+1, len(operators)):
            operator_a = operators[i]
            operator_b = operators[j]

            match_traces_between_operators(
                operator_a=operator_a, 
                operator_b=operator_b, 
                base_dir=tmp_dir,
            )

            sync_trace_between_operators(
                operator_a=operator_a, 
                operator_b=operator_b,
                base_dir=tmp_dir, 
                output_dir=mptcp_dir,
                logger=logger
            )

            # save_inter_operator_zero_tput_diff(
            #     operator_a=operator_a, 
            #     operator_b=operator_b,
            #     base_dir=tmp_dir,
            #     output_dir=mptcp_dir,
            #     logger=logger
            # )
        

if __name__ == "__main__":
    main()