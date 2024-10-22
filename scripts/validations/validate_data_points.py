import os
import re
import logging

import pandas as pd
from scripts.logging_utils import PrintLogger
from scripts.nuttcp_utils import NuttcpBaseProcessor

def validate_tput_data_points(log_file_path, logger: logging.Logger = None):
    """
    Validate data points for the processing scripts of the dataset
    Examine the differences among:
    - estimated data points based on start and end time
    - extracted data points
    - data points after postprocessing

    Args:
    log_file_path (str): Path to the log file
    logger (logging.Logger): Logger to use, default is PrintLogger
    Returns:
    pandas.DataFrame: A DataFrame containing file path, individual group data, and summed metrics
    """
    if logger is None:
        logger = PrintLogger()

    with open(log_file_path, 'r') as file:
        log_content = file.read()

    # Split the log content into groups
    groups = log_content.split('\n\n')

    # Regular expressions to extract the required information
    filepath_pattern = r'\[start processing\] (.*)'
    estimated_pattern = r'\[estimating data points\] (\d+)'
    extracted_pattern = r'\[extracted data points\] (\d+)'
    auto_completion_pattern = r'\[end auto_complete_data_points\] after: (\d+)'

    results = []

    logger.info(f'[start processing] {len(groups)} groups')
    for group in groups:
        if not group.strip():  # Skip empty groups
            continue

        filepath_match = re.search(filepath_pattern, group)
        estimated_match = re.search(estimated_pattern, group)
        extracted_match = re.search(extracted_pattern, group)
        auto_completion_match = re.search(auto_completion_pattern, group)

        if filepath_match and estimated_match and extracted_match:
            group_data = {
                'file_path': filepath_match.group(1),
                'estimated': int(estimated_match.group(1)),
                'extracted': int(extracted_match.group(1)),
                'final': int(extracted_match.group(1)),
            }
            results.append(group_data)
        else:
            logger.error(f'Invalid log format in group: {group}')
        
        if auto_completion_match:
            group_data['final'] = int(auto_completion_match.group(1))

    df = pd.DataFrame(results)

    logger.info(f'[end processing] {len(groups)} groups\n\n')

    logger.info('--- [summary] ---')
    logger.info(f"Files processed: {df['file_path'].count()}")
    logger.info(f"Total expected data points: {df['file_path'].count() * NuttcpBaseProcessor.EXPECTED_NUM_OF_DATA_POINTS}")
    logger.info(f"Total estimated data points: {df['estimated'].sum()}")
    logger.info(f"Total extracted data points: {df['extracted'].sum()}")
    logger.info(f"Total final data points after auto-completion: {df['final'].sum()}")

    return df

def validate_ping_data_points(log_file_path, logger: logging.Logger | None = None):
    INTERVAL_SEC = 0.2
    DURATION_SEC = 30
    EXPECTED_NUM_OF_DATA_POINTS = int(DURATION_SEC / INTERVAL_SEC)

    if logger is None:
        logger = PrintLogger()

    with open(log_file_path, 'r') as file:
        log_content = file.read()

    # Split the log content into groups
    groups = log_content.split('\n\n')

    # Regular expressions to extract the required information
    filepath_pattern = r'\[start processing\] (.*)'
    estimated_pattern = r'\[estimating data points\] (\d+)'
    extracted_pattern = r'\[extracted data points\] (\d+)'

    results = []

    logger.info(f'[start processing] {len(groups)} groups')
    for group in groups:
        if not group.strip():  # Skip empty groups
            continue

        filepath_match = re.search(filepath_pattern, group)
        estimated_match = re.search(estimated_pattern, group)
        extracted_match = re.search(extracted_pattern, group)

        if filepath_match and estimated_match and extracted_match:
            group_data = {
                'file_path': filepath_match.group(1),
                'estimated': int(estimated_match.group(1)),
                'extracted': int(extracted_match.group(1)),
            }
            results.append(group_data)
        else:
            logger.error(f'Invalid log format in group: {group}')

    df = pd.DataFrame(results)

    logger.info(f'[end processing] {len(groups)} groups\n\n')

    logger.info('--- [summary] ---')
    logger.info(f"Files processed: {df['file_path'].count()}")
    logger.info(f"Total expected data points: {df['file_path'].count() * EXPECTED_NUM_OF_DATA_POINTS}")
    logger.info(f"Total estimated data points: {df['estimated'].sum()}")
    logger.info(f"Total extracted data points: {df['extracted'].sum()}")

    return df
