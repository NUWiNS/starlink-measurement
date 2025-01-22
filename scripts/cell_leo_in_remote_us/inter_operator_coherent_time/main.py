import sys
import os
from typing import Dict

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.constants import CommonField
from scripts.logging_utils import create_logger
from scripts.cell_leo_in_remote_us.common import location_conf, operator_conf, cellular_location_conf, style_confs, threshold_confs

current_dir = os.path.abspath(os.path.dirname(__file__))
output_dir = os.path.join(current_dir, 'output')

logger = create_logger('coherent_time', filename=os.path.join(output_dir, 'coherent_time.log'))

class InterOperatorCoherentTime:
    def __init__(
            self, 
            base_dir: str,
            data_field: str,
            time_field: str,
            run_time_field: str,
        ):
        self.base_dir = base_dir
        self.data_field = data_field
        self.time_field = time_field
        self.run_time_field = run_time_field

    def read_mptcp_trace(self, operator_a: str, operator_b: str, trace_type: str):
        mptcp_trace_path = self.get_mptcp_trace_path(operator_a, operator_b, trace_type)
        df = pd.read_csv(mptcp_trace_path)
        return df
    
    def get_mptcp_trace_path(self, operator_a: str, operator_b: str, trace_type: str):
        return os.path.join(self.base_dir, f'mptcp_trace.{trace_type}.{operator_a}_{operator_b}.csv')
    
    def save_mptcp_trace(self, df: pd.DataFrame, operator_a: str, operator_b: str, trace_type: str):
        mptcp_trace_path = self.get_mptcp_trace_path(operator_a, operator_b, trace_type)
        df.to_csv(mptcp_trace_path, index=False)

    def attach_diff_data_to_df(self, df: pd.DataFrame):
        df[f'diff_{self.data_field}'] = df[f'A_{self.data_field}'] - df[f'B_{self.data_field}']
        return df


    def calculate_coherent_time_above_threshold_of_two_operators(
            self, 
            mptcp_df: pd.DataFrame,
            threshold: float,
        ):
        # Use Operator A as the reference
        df_grouped_by_run_time = mptcp_df.groupby(f'A_{self.run_time_field}')
        res = []
        for _, df_group in df_grouped_by_run_time:
            coherent_groups = self.get_coherent_time_above_threshold_of_two_operators(df_group, threshold)
            coherent_durations = self.get_coherent_time_durations(coherent_groups)
            res.extend(coherent_durations)
            # print(f'run_time: {run_time}, coherent_durations: {coherent_durations}')
        return res


    def get_coherent_time_above_threshold_of_two_operators(self, df: pd.DataFrame, threshold: float):
        df[self.time_field] = pd.to_datetime(df[f'A_{self.time_field}'])

        # Create masks for both positive and negative threshold conditions
        pos_mask = df[f'diff_{self.data_field}'] >= threshold
        neg_mask = df[f'diff_{self.data_field}'] <= -threshold
        
        # Initialize lists to store groups of coherent points
        coherent_groups = []
        
        # Helper function to process consecutive True values in a mask
        def process_consecutive_groups(mask):
            current_group = []
            for idx, val in enumerate(mask):
                if val:
                    current_group.append(idx)
                elif current_group:
                    if len(current_group) > 0:
                        coherent_groups.append(df.iloc[current_group])
                    current_group = []
            # Handle the last group if it exists
            if current_group:
                coherent_groups.append(df.iloc[current_group])
        
        # Process both positive and negative threshold groups
        process_consecutive_groups(pos_mask)
        process_consecutive_groups(neg_mask)
        
        return coherent_groups

    def get_coherent_time_durations(self, coherent_groups: list) -> list:
        """
        Calculate the duration of each coherent group using timestamp differences.
        Returns a list of durations in seconds.
        """
        durations = []
        for group in coherent_groups:
            if len(group) > 0:
                duration_sec = (group[self.time_field].max() - group[self.time_field].min()).total_seconds()
                durations.append(duration_sec)
        return durations

def plot_coherent_time_cdf_for_one_threshold_and_multiple_operators(
    coherent_time_duration_map: Dict,
    title: str,
    output_filename: str,
):
    fig, ax = plt.subplots(figsize=(10, 6))

    for operator_pair, durations in coherent_time_duration_map.items():
        if len(durations) == 0:
            continue
        style_conf = style_confs[operator_pair]
        sorted_durations = np.sort(durations)
        cumulative_prob = np.arange(1, len(sorted_durations) + 1) / len(sorted_durations)
        ax.plot(sorted_durations, cumulative_prob, 
                color=style_conf['color'], 
                label=f'{style_conf["label"]} (n={len(durations)})',
                linewidth=2)
        
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xlabel('Coherent Time Duration (seconds)')
    plt.ylabel('CDF')
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()


def plot_coherent_time_distribution(
    coherent_time_duration_map: Dict,
    threshold_confs: Dict,
    trace_type: str,
    output_filename: str,
):
    """
    Plot CDF of coherent time durations for different thresholds.
    Args:
        coherent_time_duration_map: Dictionary mapping threshold values to lists of coherent time durations
        threshold_conf: Configuration for thresholds including colors and labels
        trace_type: Type of trace (tcp_downlink or tcp_uplink)
        output_filename: Where to save the plot
    """
    plt.figure(figsize=(10, 6))
    
    for threshold, durations in coherent_time_duration_map.items():
        if len(durations) == 0:
            continue
            
        # Sort durations for CDF
        sorted_durations = np.sort(durations)
        # Calculate cumulative probabilities
        cumulative_prob = np.arange(1, len(sorted_durations) + 1) / len(sorted_durations)
        
        # Plot CDF with configuration from threshold_conf
        conf = threshold_confs[trace_type][threshold]
        plt.plot(sorted_durations, cumulative_prob, 
                color=conf['color'], 
                label=f'{conf["label"]} (n={len(durations)})',
                linewidth=2)
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xlabel('Coherent Time Duration (seconds)')
    plt.ylabel('CDF')
    plt.title(f'CDF of Coherent Time Durations - {trace_type}')
    plt.legend()
    
    # Use log scale for x-axis if durations span multiple orders of magnitude
    # if any(d > 100 for durations in coherent_time_duration_map.values() for d in durations):
    #     plt.xscale('log')
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()

def plot_diff_data_cdf(
    mptcp_df: pd.DataFrame,
    data_field: str,
    title: str,
    output_filename: str,
):
    field = f'diff_{data_field}'
    
    plt.figure(figsize=(10, 6))
    
    # Get the data and sort for CDF
    diff_data = mptcp_df[field].values
    sorted_data = np.sort(diff_data)
    
    # Calculate cumulative probabilities
    cumulative_prob = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    
    # Plot CDF
    plt.plot(sorted_data, cumulative_prob, 
            color='blue',
            label=f'n={len(diff_data)}',
            linewidth=2)
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xlabel(f'Difference in {data_field}')
    plt.ylabel('CDF')
    plt.title(title)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()

def plot_diff_data_distribution(
    mptcp_df: pd.DataFrame,
    data_field: str,
    title: str,
    output_filename: str,
):
    """
    Plot the distribution of difference data with a Gaussian fit.
    """
    field = f'diff_{data_field}'
    diff_data = mptcp_df[field].values
    
    plt.figure(figsize=(10, 6))
    
    # Calculate weights to convert to percentages (each value will be 100/n)
    weights = np.ones_like(diff_data) * (100.0 / len(diff_data))
    
    # Plot histogram with percentage
    counts, bins, _ = plt.hist(diff_data, bins=50, weights=weights, alpha=0.6, 
                              color='skyblue', label='Actual Distribution')
    
    # Fit a normal distribution to the data
    mu, sigma = np.mean(diff_data), np.std(diff_data)
    
    # Generate points for the Gaussian curve
    x = np.linspace(min(bins), max(bins), 100)
    gaussian = 100 * (1/(sigma * np.sqrt(2 * np.pi)) * np.exp(-(x - mu)**2 / (2 * sigma**2)))
    
    # Plot the Gaussian fit
    plt.plot(x, gaussian, 'r-', linewidth=2, 
            label=f'Gaussian Fit\nμ={mu:.2f}, σ={sigma:.2f}')
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xlabel(f'Difference in {data_field}')
    plt.ylabel('Percentage of Total Data (%)')  # Updated ylabel
    plt.title(f'{title}\n(n={len(diff_data)})')
    plt.legend()
    
    # Add text box with statistics and total percentage verification
    total_percentage = np.sum(counts)  # Should be approximately 100%
    stats_text = (f'Statistics:\nMedian: {np.median(diff_data):.2f}\n'
                 f'Mean: {mu:.2f}\nStd Dev: {sigma:.2f}\n'
                 f'Min: {np.min(diff_data):.2f}\nMax: {np.max(diff_data):.2f}\n'
                 f'Total %: {total_percentage:.1f}')
    
    plt.text(0.95, 0.8, stats_text,
             transform=plt.gca().transAxes,
             verticalalignment='top',
             horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    trace_type_conf = {
        'tcp_downlink': {
            'values': [0, 10, 50],
        },
        'tcp_uplink': {
            'values': [0, 5, 10],
        }
    }

    # Preprocess data for all operator pairs
    for location in ['alaska', 'hawaii']:
        loc_conf = location_conf[location]
        mptcp_dir = os.path.join(loc_conf['root_dir'], 'mptcp')
        
        for trace_type in ['tcp_downlink', 'tcp_uplink']:
            trace_conf = trace_type_conf[trace_type]
            coherent_time_duration_map = {}
            for threshold in trace_conf['values']:
                coherent_time_duration_map[threshold] = []
                operators = loc_conf['operators']
                for i in range(len(operators) - 1):
                    for j in range(i+1, len(operators)):
                        operator_a = operators[i]
                        operator_b = operators[j]

                        coherent_time_tool = InterOperatorCoherentTime(
                            base_dir=mptcp_dir,
                            data_field=CommonField.TPUT_MBPS,
                            time_field=CommonField.TIME,
                            run_time_field='run_time',
                        )
                        mptcp_df = coherent_time_tool.read_mptcp_trace(operator_a, operator_b, trace_type)
                        logger.info(f'read mptcp trace from {operator_a} and {operator_b} for {trace_type}, {len(mptcp_df)} rows')
                        mptcp_df = coherent_time_tool.attach_diff_data_to_df(mptcp_df)

                        # Plot CDF of difference data
                        diff_data_cdf_filename = os.path.join(output_dir, f'diff_tput_cdf.{location}.{trace_type}.{operator_a}_{operator_b}.pdf')
                        plot_diff_data_cdf(
                            mptcp_df=mptcp_df,
                            data_field=CommonField.TPUT_MBPS,
                            title=f'{location} {trace_type} {operator_a}-{operator_b}',
                            output_filename=diff_data_cdf_filename,
                        )
                        
                        # Plot distribution of difference data
                        diff_data_dist_filename = os.path.join(output_dir, f'diff_tput_dist.{location}.{trace_type}.{operator_a}_{operator_b}.pdf')
                        plot_diff_data_distribution(
                            mptcp_df=mptcp_df,
                            data_field=CommonField.TPUT_MBPS,
                            title=f'{location} {trace_type} {operator_a}-{operator_b}',
                            output_filename=diff_data_dist_filename,
                        )

                        coherent_time_tool.save_mptcp_trace(mptcp_df, operator_a, operator_b, trace_type)
                        logger.info(f'saved diff tput distribution to {diff_data_dist_filename}')

    
    # Plot coherent time distribution for Starlink - Cellular pairs
    for location in ['alaska', 'hawaii']:
        loc_conf = location_conf[location]
        cellular_operators = cellular_location_conf[location]['operators']
        mptcp_dir = os.path.join(loc_conf['root_dir'], 'mptcp')
        
        for trace_type in ['tcp_downlink', 'tcp_uplink']:
            trace_conf = trace_type_conf[trace_type]
            coherent_time_duration_map = {}

            for threshold in trace_conf['values']:
                coherent_time_duration_map[threshold] = []
                coherent_time_map_keyed_by_operator_pair = {}

                for cellular_operator in cellular_operators:
                    operator_a = 'starlink'
                    operator_b = cellular_operator

                    logger.info(f'calculating coherent durations above threshold: {threshold} Mbps for {operator_a} - {operator_b}')

                    coherent_time_tool = InterOperatorCoherentTime(
                        base_dir=mptcp_dir,
                        data_field=CommonField.TPUT_MBPS,
                        time_field=CommonField.TIME,
                        run_time_field='run_time',
                    )
                    mptcp_df = coherent_time_tool.read_mptcp_trace(operator_a, operator_b, trace_type)
                    durations = coherent_time_tool.calculate_coherent_time_above_threshold_of_two_operators(
                        mptcp_df=mptcp_df,
                        threshold=threshold,
                    )
                    coherent_time_duration_map[threshold].extend(durations)
                    logger.info(f'completed calculating coherent time durations above threshold: {threshold} Mbps')

                    coherent_time_map_keyed_by_operator_pair[f'{operator_a}_{operator_b}'] = durations

                plot_coherent_time_cdf_for_one_threshold_and_multiple_operators(
                    coherent_time_duration_map=coherent_time_map_keyed_by_operator_pair,
                    title=f'{trace_type} {operator_a}-{operator_b} threshold: {threshold} Mbps',
                    output_filename=os.path.join(output_dir, f'coherent_time.{location}.{trace_type}.{operator_a}_{operator_b}.threshold_{threshold}mbps.pdf'),
                )

            output_filename = os.path.join(output_dir, f'coherent_time.{location}.{trace_type}.pdf')
            plot_coherent_time_distribution(
                coherent_time_duration_map=coherent_time_duration_map,
                threshold_confs=threshold_confs,
                trace_type=trace_type,
                output_filename=output_filename
            )
            logger.info(f'Saved coherent time distribution to {output_filename}')

    

if __name__ == '__main__':
    main()
