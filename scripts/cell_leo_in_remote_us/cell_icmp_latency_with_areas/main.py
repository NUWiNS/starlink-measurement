import json
import os
from typing import Dict, List
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scripts.cell_leo_in_remote_us.common import location_conf, operator_conf, tech_conf
from scripts.constants import CommonField, XcalField
from scripts.logging_utils import create_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = create_logger('icmp_latency_with_areas', filename=os.path.join(current_dir, 'outputs', 'icmp_latency_with_areas.log'))



def read_latency_data(root_dir: str, operator: str, protocol: str = 'icmp'):
    if protocol == 'icmp':
        input_csv_path = os.path.join(root_dir, 'ping', f'{operator}_ping_with_tech.csv')
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

def save_stats_to_json(
        data: Dict[str, pd.DataFrame],
        metrics: List[tuple],
        filepath: str,
    ):
    """Save hierarchical statistics for each metric.
    
    Structure:
    metric_name:
        location:
            operator:
                area_type:
                    tech:
                        stats (min, max, median, percentiles, etc.)
    """
    stats = {}
    
    for metric_name, _, _, data_field in metrics:
        stats[metric_name] = {}
        df = data[metric_name]
        
        # First level: Location
        for location in df['location'].unique():
            stats[metric_name][location] = {}
            loc_df = df[df['location'] == location]
            
            # Second level: Operator
            for operator in loc_df['operator'].unique():
                stats[metric_name][location][operator] = {}
                op_df = loc_df[loc_df['operator'] == operator]

                if XcalField.SEGMENT_ID in op_df.columns:
                    grouped_df = op_df.groupby([XcalField.SEGMENT_ID])
                    tech_distance_mile_map, total_distance_miles = calculate_tech_coverage_in_miles(grouped_df)
                
                # Fourth level: Technology
                tech_breakdown = {}
                for tech in op_df[XcalField.ACTUAL_TECH].unique():
                    tech_df = op_df[op_df[XcalField.ACTUAL_TECH] == tech]
                    tech_data = tech_df[data_field]
                    operator_total = len(op_df)
                    if len(tech_data) == 0:
                        continue
                        
                    tech_breakdown[tech] = {
                        'min': float(np.min(tech_data)),
                        'max': float(np.max(tech_data)),
                        'median': float(np.median(tech_data)),
                        'mean': float(np.mean(tech_data)),
                        'percentile_5': float(np.percentile(tech_data, 5)),
                        'percentile_25': float(np.percentile(tech_data, 25)),
                        'percentile_75': float(np.percentile(tech_data, 75)),
                        'percentile_95': float(np.percentile(tech_data, 95)),
                        'sample_count': len(tech_data),
                        'percentage_occurence': float(len(tech_data) / operator_total * 100 if operator_total > 0 else 0),
                    }
                    if XcalField.SEGMENT_ID in op_df.columns:
                        tech_breakdown[tech]['percentage_miles'] = float(tech_distance_mile_map[tech] / total_distance_miles * 100 if total_distance_miles > 0 else 0)
                
                stats[metric_name][location][operator] = {
                    'tech_breakdown': tech_breakdown,
                    'total_samples': operator_total
                }
    
    # Save to JSON file
    with open(filepath, 'w') as f:
        json.dump(stats, f, indent=4)

def plot_tech_breakdown_cdfs_in_a_row(
        df: pd.DataFrame,
        data_field: str,
        output_filepath: str,
        operators: List[str],
        operator_conf: Dict[str, Dict],
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
        
        ax.legend()
    
    # Save the figure
    plt.savefig(output_filepath, dpi=600)
    logger.info(f'Saved plot to {output_filepath}')
    plt.close()


def plot_latency_tech_breakdown_by_area_by_operator(
        locations: List[str], 
        protocol: str,
        data_sample_threshold: int = 240, # 1 rounds data (~2min)
    ):
    tput_df = aggregate_latency_data_by_location(
        locations=locations,
        protocol=protocol,
    )

    data_field = 'rtt_ms'

    ak_df = tput_df[tput_df[CommonField.LOCATION] == 'alaska']
    ak_urban_df = ak_df[(ak_df[CommonField.AREA_TYPE] == 'urban') | (ak_df[CommonField.AREA_TYPE] == 'suburban')]
    plot_tech_breakdown_cdfs_in_a_row(
        title='AK Urban',
        df=ak_urban_df,
        data_field=data_field,
        data_sample_threshold=data_sample_threshold,
        operators=['att', 'verizon'],
        operator_conf=operator_conf,
        tech_conf=tech_conf,
        max_xlim=200,
        output_filepath=os.path.join(
            current_dir, 'outputs', f'{protocol}_latency.ak_urban.png'),
    )

    ak_rural_df = ak_df[ak_df[CommonField.AREA_TYPE] == 'rural']
    plot_tech_breakdown_cdfs_in_a_row(
        title='AK Rural',
        df=ak_rural_df,
        data_field=data_field,
        data_sample_threshold=data_sample_threshold,
        operators=['att', 'verizon'],
        operator_conf=operator_conf,
        tech_conf=tech_conf,
        max_xlim=200,
        output_filepath=os.path.join(
            current_dir, 'outputs', f'{protocol}_latency.ak_rural.png'),
    )

    hi_df = tput_df[tput_df[CommonField.LOCATION] == 'hawaii']
    hi_urban_df = hi_df[(hi_df[CommonField.AREA_TYPE] == 'urban') | (hi_df[CommonField.AREA_TYPE] == 'suburban')]
    plot_tech_breakdown_cdfs_in_a_row(
        title='HI Urban',
        df=hi_urban_df,
        data_field=data_field,
        data_sample_threshold=data_sample_threshold,
        operators=['att', 'verizon', 'tmobile'],
        operator_conf=operator_conf,
        tech_conf=tech_conf,
        max_xlim=200,
        output_filepath=os.path.join(
            current_dir, 'outputs', f'{protocol}_latency.hi_urban.png'),
    )

    hi_rural_df = hi_df[hi_df[CommonField.AREA_TYPE] == 'rural']
    plot_tech_breakdown_cdfs_in_a_row(
        title='HI Rural',
        df=hi_rural_df,
        data_field=data_field,
        data_sample_threshold=data_sample_threshold,
        operators=['att', 'verizon', 'tmobile'],
        operator_conf=operator_conf,
        tech_conf=tech_conf,
        max_xlim=200,
        output_filepath=os.path.join(
            current_dir, 'outputs', f'{protocol}_latency.hi_rural.png'),
    )
    # save_stats_to_json(
    #     data=plot_data,
    #     metrics=metrics, 
    #     filepath=output_filepath.replace('.png', '.json'),
    # )


def main():
    if not os.path.exists(os.path.join(current_dir, 'outputs')):
        os.makedirs(os.path.join(current_dir, 'outputs'))

    plot_latency_tech_breakdown_by_area_by_operator(
        locations=['alaska', 'hawaii'],
        protocol='icmp',
        data_sample_threshold=480,
    )

if __name__ == '__main__':
    main()