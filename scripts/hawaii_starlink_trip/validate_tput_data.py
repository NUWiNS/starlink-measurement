import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.logging_utils import create_logger
from scripts.hawaii_starlink_trip.configs import ROOT_DIR
from scripts.validations import validate_data_points

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
validation_dir = os.path.join(ROOT_DIR, 'validation')
def main():
    nuttcp_log_file_path = os.path.join(validation_dir, 'nuttcp_data_validation.log')
    validate_data_points(
        nuttcp_log_file_path, 
        logger=create_logger('nuttcp_validation_result', filename=os.path.join(validation_dir, f'nuttcp_data_validation_result.log'), filemode='w')
    )

    iperf_log_file_path = os.path.join(validation_dir, 'iperf_data_validation.log')
    validate_data_points(
        iperf_log_file_path, 
        logger=create_logger('iperf_validation_result', filename=os.path.join(validation_dir, f'iperf_data_validation_result.log'), filemode='w')
    )




if __name__ == '__main__':
    main()