import os
import sys
from typing import Dict, List

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.constants import operator_color_map

location_conf = {
    'alaska': {
        'label': 'AK',
        'root_dir': ALASKA_ROOT_DIR,
        'operators': ['starlink', 'verizon', 'att'],
        'order': 1
    },
    'hawaii': {
        'label': 'HI',
        'root_dir': HAWAII_ROOT_DIR,
        'operators': ['starlink', 'verizon', 'att', 'tmobile'],
        'order': 2
    }
}

operator_conf = {
    'att': {
        'label': 'AT&T',
        'order': 1,
        'color': operator_color_map['att'],
        'linestyle': '-'
    },
    'verizon': {
        'label': 'Verizon',
        'order': 2,
        'color': operator_color_map['verizon'],
        'linestyle': '--'
    },
    'tmobile': {
        'label': 'T-Mobile',
        'order': 3,
        'color': operator_color_map['tmobile'],
        'linestyle': ':'
    },
    'starlink': {
        'label': 'Starlink',
        'order': 4,
        'color': operator_color_map['starlink'],
        'linestyle': '-.',
    },
}

def read_tput_data(root_dir: str, operator: str, protocol: str, direction: str):
    tput_data_path = os.path.join(root_dir, 'throughput', f'{operator}_{protocol}_{direction}.csv')
    if not os.path.exists(tput_data_path):
        raise FileNotFoundError(f'Throughput data file not found: {tput_data_path}')
    return pd.read_csv(tput_data_path)

def aggregate_operator_tput_data(root_dir: str, operators: List[str], protocol: str, direction: str):
    df = pd.DataFrame()
    for operator in operators:
        tput_data = read_tput_data(root_dir, operator, protocol, direction)
        tput_data['operator'] = operator_conf[operator]['label']
        df = pd.concat([df, tput_data], ignore_index=True)
    return df

def plot_network_kpis_comparison(
        df: pd.DataFrame, 
        title: str,
        data_field: str,
        data_label: str,
        loc_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_filepath: str,
        show_location_label: bool = False,
        show_legend: bool = False,
        show_ylabel: bool = False,
    ):
    # Calculate number of locations for subplot layout
    n_locations = len(loc_conf)
    fig, axes = plt.subplots(n_locations, 1, figsize=(6, 3*n_locations))
    if n_locations == 1:
        axes = [axes]

    # Sort locations by order
    locations_sorted = sorted(df['location'].unique(), key=lambda x: loc_conf[x]['order'])

    # Create subplot for each location
    for idx, location in enumerate(locations_sorted):
        location_label = loc_conf[location]['label']
        sub_df = df[df['location'] == location]
        
        # Plot CDF for each operator
        for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
            operator_label = op_conf['label']
            if operator_label in sub_df['operator'].unique():
                operator_data = sub_df[sub_df['operator'] == operator_label][data_field]
                
                # Calculate CDF
                data_sorted = np.sort(operator_data)
                cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                
                # Plot CDF line
                axes[idx].plot(
                    data_sorted,
                    cdf,
                    label=operator_label,
                    color=op_conf['color'],
                    linewidth=2
                )
        
        # Customize each subplot
        if show_location_label:
            # Add location label to the left of the plot
            axes[idx].text(-0.15, 0.5, location_label, 
                          transform=axes[idx].transAxes,
                          rotation=0,
                          verticalalignment='center',
                          horizontalalignment='right',
                          fontsize=14,
                          fontweight='bold')
            
        axes[idx].grid(True, alpha=0.3)
        axes[idx].set_xlabel(data_label, fontsize=12)
        if show_ylabel:
            axes[idx].set_ylabel('CDF', fontsize=12)
        
        # Set y-axis ticks in 0.25 steps
        axes[idx].set_yticks(np.arange(0, 1.1, 0.25))
        axes[idx].tick_params(axis='both', labelsize=10)
        
        # Add legend for the first plot
        if show_legend and idx == 0:
            legend = axes[idx].legend(fontsize=12)
            for text in legend.get_texts():
                text.set_fontweight('bold')
    
    # Add overall title
    fig.suptitle(title, y=1.0, fontsize=16, fontweight='bold')
    
    # Adjust layout
    plt.subplots_adjust(hspace=0.3)
    if show_location_label:
        plt.subplots_adjust(left=0.15)
    plt.tight_layout()
    
    # Save plot
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    plt.close()

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, 'outputs')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    combined_data = pd.DataFrame()
    for location in ['alaska', 'hawaii']:
        conf = location_conf[location]
        tcp_dl_tput_data = aggregate_operator_tput_data(
            root_dir=conf['root_dir'], 
            operators=conf['operators'], 
            protocol='tcp', 
            direction='downlink'
        )
        tcp_dl_tput_data['location'] = location
        combined_data = pd.concat([combined_data, tcp_dl_tput_data], ignore_index=True)

    output_filepath = os.path.join(output_dir, f'all_locations_boxplot.tput_dl.png')
    plot_network_kpis_comparison(
        df=combined_data, 
        title='TCP DL',
        data_field='throughput_mbps',
        data_label='Throughput (Mbps)',
        loc_conf=location_conf,
        operator_conf=operator_conf,
        show_location_label=True,
        show_legend=True,
        show_ylabel=True,
        output_filepath=output_filepath
    )

if __name__ == '__main__':
    main()