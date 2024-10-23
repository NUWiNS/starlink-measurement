import os
import re
import logging

import pandas as pd
from scripts.logging_utils import PrintLogger
from scripts.nuttcp_utils import NuttcpBaseProcessor
import plotly.figure_factory as ff

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


def validate_extracted_periods_of_tput_measurements(csv_file_path: str, output_file_path: str, timezone_str: str = 'UTC'):
    # Read the CSV file
    df = pd.read_csv(csv_file_path, parse_dates=['start_time', 'end_time'])
    
    # Convert timestamps to the specified timezone
    df['start_time'] = df['start_time'].dt.tz_convert(timezone_str)
    df['end_time'] = df['end_time'].dt.tz_convert(timezone_str)
    
    # Create a color map for different protocol directions
    color_map = {
        'tcp_downlink': 'rgb(0, 255, 0)',
        'tcp_uplink': 'rgb(0, 0, 255)',
        'udp_downlink': 'rgb(255, 0, 0)',
        'udp_uplink': 'rgb(255, 165, 0)'
    }
    
    # Prepare data for the Gantt chart
    df_plot = []
    for _, row in df.iterrows():
        df_plot.append(dict(Task=row['protocol_direction'], 
                            Start=row['start_time'], 
                            Finish=row['end_time'],
                            Resource=row['protocol_direction']))
    
    # Create the Gantt chart
    fig = ff.create_gantt(df_plot, colors=color_map, index_col='Resource', 
                          show_colorbar=True, group_tasks=True)
    
    # Update the layout
    fig.update_layout(
        title=f'Throughput Measurement Periods ({timezone_str})',
        xaxis_title='Time',
        yaxis_title='Protocol Direction',
        height=600,
        width=1200
    )
    
    # Save the plot as an HTML file
    fig.write_html(output_file_path)