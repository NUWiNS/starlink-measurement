import json
import os
from typing import Dict, List
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.logging_utils import create_logger
from scripts.cell_leo_in_remote_us.cell_tcp_dl_with_areas.main import plot_tput_tech_breakdown_by_area_by_operator
from scripts.cell_leo_in_remote_us.common import cellular_location_conf, cellular_operator_conf, tech_conf

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('tcp_ul_with_areas', filename=os.path.join(current_dir, 'outputs', 'tcp_ul_with_areas.log'))


def main():
    output_dir = os.path.join(current_dir, 'outputs/sizhe_new_data')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    plot_tput_tech_breakdown_by_area_by_operator(
        # locations=['alaska', 'hawaii'],
        locations=['alaska'],
        protocol='tcp',
        direction='uplink',
        data_sample_threshold=480,
        output_dir=output_dir,
        location_conf=cellular_location_conf,
        operator_conf=cellular_operator_conf,
        tech_conf=tech_conf,
    )

if __name__ == '__main__':
    main()