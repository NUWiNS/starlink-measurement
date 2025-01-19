import json
import os
from typing import Dict, List
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.cell_leo_in_remote_us.common import cellular_location_conf, cellular_operator_conf, tech_conf
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('icmp_latency_with_areas', filename=os.path.join(current_dir, 'outputs', 'icmp_latency_with_areas.log'))



def read_latency_data(root_dir: str, operator: str, protocol: str = 'icmp'):
    if protocol == 'icmp':
        input_csv_path = os.path.join(root_dir, 'ping/sizhe_new_data', f'{operator}_ping.csv')
    else:
        raise ValueError(f'Unsupported protocol: {protocol}')

    df = pd.read_csv(input_csv_path)
    return df

def aggregate_latency_data_by_operator(
        root_dir: str,
        operators: List[str], 
        protocol: str = 'icmp',
    ):
    data = pd.DataFrame()
    for operator in operators:
        df = read_latency_data(
            root_dir=root_dir, 
            operator=operator, 
            protocol=protocol
        )
        df['operator'] = operator
        data = pd.concat([data, df])
    return data

def aggregate_latency_data_by_location(
        locations: List[str], 
        location_conf: Dict[str, Dict],
        protocol: str = 'icmp',
    ):
    """Aggregate latency data from multiple locations.
    
    Args:
        locations (List[str]): List of locations to process
        protocol (str): Protocol used for latency measurements
        
    Returns:
        pd.DataFrame: Combined DataFrame with latency data
    """
    data = pd.DataFrame()
    for location in locations:
        df = aggregate_latency_data_by_operator(
            root_dir=location_conf[location]['root_dir'],
            operators=location_conf[location]['operators'], 
            protocol=protocol
        )
        df['location'] = location
        data = pd.concat([data, df])
    return data

def plot_tech_breakdown_cdfs_in_a_row(
        df: pd.DataFrame,
        data_field: str,
        output_filepath: str,
        operators: List[str],
        operator_conf: Dict[str, Dict],
        location_conf: Dict[str, Dict],
        tech_conf: Dict[str, Dict],
        title: str = '',
        max_xlim: float | None = None,
        interval_x: float | None = None,
        data_sample_threshold: int = 240, # 1 rounds data (~2min)
    ):
    # Create figure with horizontal subplots (one per operator)
    n_operators = len(operators)
    
    left_margin = 0.12 if n_operators > 2 else 0.12
    y_label_pos = 0
    fig = plt.figure(figsize=(2.5 * 3, 3.5))  # Fixed size for 3 operators (2.5 * 3)
    gs = fig.add_gridspec(1, n_operators, left=left_margin, bottom=0.2, top=0.85)
    axes = [fig.add_subplot(gs[0, i]) for i in range(n_operators)]
    
    # Ensure axes is always an array even with single operator
    if n_operators == 1:
        axes = np.array([axes])
    
    # Adjust spacing between subplots
    if n_operators > 2:
        plt.subplots_adjust(wspace=0.2)
    else:
        plt.subplots_adjust(wspace=0.15)
    
    # Add title at the top middle of the figure if requested
    # fig.suptitle(title, y=0.98, size=16, weight='bold')
    
    # Set up common y-axis label for all subplots
    fig.text(y_label_pos, 0.5, 'CDF', 
             rotation=90,
             verticalalignment='center',
             size=14,
             weight='bold')
    
    # Set up common x-axis label for all subplots
    fig.text(0.5, 0.08, 'Round-Trip Time (ms)', 
             horizontalalignment='center',
             size=14,
             weight='bold')

    # Plot content for each operator
    for idx, operator in enumerate(operators):
        ax = axes[idx]
        # Filter data for this operator
        operator_df = df[df['operator'] == operator]

        # Plot each technology's CDF
        for tech, tech_cfg in tech_conf.items():
            # Filter data for this technology
            operator_tech_data = operator_df[operator_df[XcalField.ACTUAL_TECH] == tech][data_field]
            
            if len(operator_tech_data) < data_sample_threshold:
                logger.warn(f'{operator}-{tech} data sample is less than required threshold, skip plotting: {len(operator_tech_data)} < {data_sample_threshold}')
                continue
            
            # Sort data and compute CDF
            data_sorted = np.sort(operator_tech_data)
            cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
            
            # Plot CDF line with tech color
            ax.plot(data_sorted, cdf, 
                    color=tech_cfg['color'],
                    label=tech_cfg['label'])
        
        # Set operator name as subplot title
        ax.set_title(operator_conf[operator]['label'])
        
        # Basic axis setup
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='both')
        
        # Only show y ticks for the first subplot
        if idx > 0:
            ax.set_yticklabels([])
        
        if max_xlim is not None:
            ax.set_xlim(0, max_xlim)
            if interval_x:
                ax.set_xticks(np.arange(0, max_xlim + 1, interval_x))
        else:
            # Find global max x value
            actual_max_x_value = 0
            for operator in operators:
                operator_df = df[df['operator'] == operator]
                for tech, tech_cfg in tech_conf.items():
                    operator_tech_data = operator_df[operator_df[XcalField.ACTUAL_TECH] == tech][data_field]
                    if len(operator_tech_data) < data_sample_threshold:
                        continue
                    actual_max_x_value = max(actual_max_x_value, np.max(operator_tech_data))
            ax.set_xlim(0, actual_max_x_value)
            if interval_x:
                ax.set_xticks(np.arange(0, actual_max_x_value + 1, interval_x))
        
        ax.legend(loc='lower right')
    
    # Save the figure
    plt.savefig(output_filepath, dpi=600)
    logger.info(f'Saved plot to {output_filepath}')
    plt.close()


def plot_latency_tech_breakdown_by_area_by_operator(
        locations: List[str], 
        protocol: str,
        data_sample_threshold: int = 240, # 1 rounds data (~2min)
    ):
    latency_df = aggregate_latency_data_by_location(
        locations=locations,
        protocol=protocol,
        location_conf=cellular_location_conf,
    )

    data_field = 'rtt_ms'
    output_dir = os.path.join(current_dir, 'outputs', 'sizhe_new_data')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    ak_df = latency_df[latency_df[CommonField.LOCATION] == 'alaska']
    ak_urban_df = ak_df[(ak_df[CommonField.AREA_TYPE] == 'urban') | (ak_df[CommonField.AREA_TYPE] == 'suburban')]
    plot_tech_breakdown_cdfs_in_a_row(
        title='AK Urban',
        df=ak_urban_df,
        data_field=data_field,
        data_sample_threshold=data_sample_threshold,
        operators=['att', 'verizon'],
        operator_conf=cellular_operator_conf,
        location_conf=cellular_location_conf,
        tech_conf=tech_conf,
        max_xlim=200,
        output_filepath=os.path.join(
            output_dir, f'{protocol}_latency.ak_urban.pdf'),
    )

    ak_rural_df = ak_df[ak_df[CommonField.AREA_TYPE] == 'rural']
    plot_tech_breakdown_cdfs_in_a_row(
        title='AK Rural',
        df=ak_rural_df,
        data_field=data_field,
        data_sample_threshold=data_sample_threshold,
        operators=['att', 'verizon'],
        operator_conf=cellular_operator_conf,
        location_conf=cellular_location_conf,
        tech_conf=tech_conf,
        max_xlim=200,
        output_filepath=os.path.join(
            output_dir, f'{protocol}_latency.ak_rural.pdf'),
    )

    # hi_df = latency_df[latency_df[CommonField.LOCATION] == 'hawaii']
    # hi_urban_df = hi_df[(hi_df[CommonField.AREA_TYPE] == 'urban') | (hi_df[CommonField.AREA_TYPE] == 'suburban')]
    # plot_tech_breakdown_cdfs_in_a_row(
    #     title='HI Urban',
    #     df=hi_urban_df,
    #     data_field=data_field,
    #     data_sample_threshold=data_sample_threshold,
    #     operators=['att', 'verizon', 'tmobile'],
    #     operator_conf=cellular_operator_conf,
    #     location_conf=cellular_location_conf,
    #     tech_conf=tech_conf,
    #     max_xlim=200,
    #     output_filepath=os.path.join(
    #         current_dir, 'outputs', f'{protocol}_latency.hi_urban.pdf'),
    # )

    # hi_rural_df = hi_df[hi_df[CommonField.AREA_TYPE] == 'rural']
    # plot_tech_breakdown_cdfs_in_a_row(
    #     title='HI Rural',
    #     df=hi_rural_df,
    #     data_field=data_field,
    #     data_sample_threshold=data_sample_threshold,
    #     operators=['att', 'verizon', 'tmobile'],
    #     operator_conf=cellular_operator_conf,
    #     location_conf=cellular_location_conf,
    #     tech_conf=tech_conf,
    #     max_xlim=200,
    #     output_filepath=os.path.join(
    #         current_dir, 'outputs', f'{protocol}_latency.hi_rural.pdf'),
    # )


def main():
    if not os.path.exists(os.path.join(current_dir, 'outputs')):
        os.makedirs(os.path.join(current_dir, 'outputs'))

    plot_latency_tech_breakdown_by_area_by_operator(
        # locations=['alaska', 'hawaii'],
        locations=['alaska'],
        protocol='icmp',
        data_sample_threshold=480,
    )

if __name__ == '__main__':
    main()