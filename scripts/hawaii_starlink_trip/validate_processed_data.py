import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.validations.validate_app_tput_time_mapping import AppTputTimeMappingForXcal
from scripts.logging_utils import create_logger
from scripts.hawaii_starlink_trip.configs import ROOT_DIR
from scripts.validations import validate_tput_data_points, validate_ping_data_points

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
validation_dir = os.path.join(ROOT_DIR, 'validation')
def main():
    # nuttcp_log_file_path = os.path.join(validation_dir, 'nuttcp_data_validation.log')
    # validate_tput_data_points(
    #     nuttcp_log_file_path, 
    #     logger=create_logger('nuttcp_validation_result', filename=os.path.join(validation_dir, f'nuttcp_data_validation_result.log'), filemode='w')
    # )

    # iperf_log_file_path = os.path.join(validation_dir, 'iperf_data_validation.log')
    # validate_tput_data_points(
    #     iperf_log_file_path, 
    #     logger=create_logger('iperf_validation_result', filename=os.path.join(validation_dir, f'iperf_data_validation_result.log'), filemode='w')
    # )

    # ping_log_file_path = os.path.join(validation_dir, 'ping_data_validation.log')
    # validate_ping_data_points(
    #     ping_log_file_path, 
    #     logger=create_logger('ping_validation_result', filename=os.path.join(validation_dir, f'ping_data_validation_result.log'), filemode='w')
    # )

    app_tput_time_mapping_for_xcal = AppTputTimeMappingForXcal(
        base_dir=os.path.join(ROOT_DIR, 'xcal'),
        output_dir=validation_dir
    )
    app_tput_time_mapping_for_xcal.validate_app_tput_time_mapping(operator='att')
    app_tput_time_mapping_for_xcal.validate_app_tput_time_mapping(operator='verizon')
    app_tput_time_mapping_for_xcal.validate_app_tput_time_mapping(operator='tmobile')




if __name__ == '__main__':
    main()