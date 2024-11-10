import os
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import sys
from typing import List, Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.celllular_analysis.TechBreakdown import TechBreakdown
from scripts.utilities.list_utils import replace_with_elements
from scripts.constants import XcalField, XcallHandoverEvent
from scripts.logging_utils import create_logger
from scripts.alaska_starlink_trip.configs import ROOT_DIR as AL_DATASET_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HI_DATASET_DIR

current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, 'outputs')

logger = create_logger('handover_split', filename=os.path.join(output_dir, f'handover_process.log'))


def plot_tech_by_protocol_direction(df: pd.DataFrame, operator: str, output_dir: str):
    # Plot a stack plot of the tput for each tech grouped by protocol + direction
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Colors for different technologies
    colors = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c', '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5']
    tech_order = ['LTE', 'LTE-A', '5G-low', '5G-mid', '5G-mmWave (28GHz)', '5G-mmWave (39GHz)']
    
    # Create groups for protocol + direction combinations
    groups = []
    fractions = []
    
    for protocol in ['tcp', 'udp']:
        for direction in ['downlink', 'uplink']:
            # Filter data for this protocol and direction
            mask = (df[XcalField.APP_TPUT_PROTOCOL] == protocol) & \
                  (df[XcalField.APP_TPUT_DIRECTION] == direction)
            data = df[mask]
            
            # Get throughput field based on direction
            tput_field = XcalField.TPUT_DL if direction == 'downlink' else XcalField.TPUT_UL
            
            # Calculate fractions
            tech_totals = data.groupby(XcalField.ACTUAL_TECH)[tput_field].sum()
            total = tech_totals.sum()
            if total > 0:  # Avoid division by zero
                tech_fractions = tech_totals / total
                tech_fractions = tech_fractions.reindex(tech_order)
                fractions.append(tech_fractions)
                groups.append(f"{protocol}\n{direction}")
    
    # Plot stacked bars
    x = range(len(groups))
    bottom = np.zeros(len(groups))
    
    for i, tech in enumerate(tech_order):
        values = [f[tech] if tech in f else 0 for f in fractions]
        ax.bar(x, values, bottom=bottom, label=tech, color=colors[i])
        bottom += values
    
    ax.set_title(f'Technology Distribution by Protocol and Direction ({operator})')
    ax.set_xlabel('Protocol + Direction')
    ax.set_ylabel('Fraction of Total Throughput')
    ax.set_xticks(x)
    ax.set_xticklabels(groups)
    ax.legend(title='Technology', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, axis='y')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{operator}_tech_distribution.png'), bbox_inches='tight')
    plt.close()
    logger.info(f"Saved technology distribution plot to {os.path.join(output_dir, f'{operator}_tech_distribution.png')}")


def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    operators = ['att', 'tmobile', 'verizon']
    for operator in operators:

        logger.info('--------------------------------')
        # Example usage
        input_csv_path = os.path.join(AL_DATASET_DIR, 'xcal', f'{operator}_xcal_smart_tput.csv')
        df = pd.read_csv(input_csv_path)

        plot_tech_by_protocol_direction(df, operator, output_dir)

        # segments = partition_data_by_handover(df)
        # for segment in segments:
        #     output_str = f"Segment ({segment.get_range()})"
        #     output_str += f", duration: {segment.duration_ms} ms"
        #     output_str += f", 5G freq: {segment.get_freq_5g_mhz()}"
        #     output_str += f", tech: {segment.get_tech()}"
        #     output_str += f", dl: {segment.get_dl_tput_count()}"
        #     output_str += f", ul: {segment.get_ul_tput_count()}"
        #     output_str += f", has tput: {segment.has_tput}"
        #     logger.info(output_str)

        #     if segment.check_if_multiple_freq():
        #         logger.warn(f"[WARN] Segment ({segment.get_range()}) has multiple 5G frequencies")
        #         # sys.exit(1)

        #     if segment.has_no_service:
        #         logger.warn(f"[WARN] Segment ({segment.get_range()}) has data points during NO SERVICE")
        #         # sys.exit(1)

        # logger.info('before filtering, there are {} segments'.format(len(segments)))
        # segments_that_have_tput = filter_segments_with_tput(segments)
        # logger.info('after filtering, there are {} segments that have tput'.format(len(segments_that_have_tput)))

        # smart_tput_with_tech = []
        # for segment in segments_that_have_tput:
        #     smart_dl_tput = segment.get_dl_tput_df()
        #     smart_ul_tput = segment.get_ul_tput_df()
        #     smart_dl_tput[XcalField.TECH] = segment.get_tech()
        #     smart_ul_tput[XcalField.TECH] = segment.get_tech()
        #     smart_tput_with_tech.append(smart_dl_tput)
        #     smart_tput_with_tech.append(smart_ul_tput)

        # smart_tput_with_tech = pd.concat(smart_tput_with_tech)

        # output_csv_path = os.path.join(output_dir, f'hawaii_{operator}_smart_tput_with_tech.csv')
        # # if os.path.exists(output_csv_path):
        # #     smart_tput_with_tech = pd.read_csv(output_csv_path)

        # smart_tput_with_tech.to_csv(output_csv_path, index=False)
        # logger.info(f"saved to {output_csv_path}")

        # plot_tech_by_protocol_direction(smart_tput_with_tech)
    
    
    # tech_fractions = []
    # tech_order = []
    
    # # First pass to determine total counts across all operators
    # total_tech_counts = pd.Series(0)
    # for operator in operators:
    #     output_csv_path = os.path.join(output_dir, f'hawaii_{operator}_smart_tput_with_tech.csv')
    #     smart_tput_with_tech = pd.read_csv(output_csv_path)
    #     total_tech_counts = total_tech_counts.add(smart_tput_with_tech[XcalField.TECH].value_counts(), fill_value=0)
    
    # # Sort technologies by frequency in reverse order
    # tech_order = total_tech_counts.sort_values(ascending=False).index.tolist()
    
    # # Second pass to get fractions in correct order
    # for operator in operators:
    #     output_csv_path = os.path.join(output_dir, f'hawaii_{operator}_smart_tput_with_tech.csv')
    #     smart_tput_with_tech = pd.read_csv(output_csv_path)
        
    #     # Calculate fraction of each tech
    #     tech_counts = smart_tput_with_tech[XcalField.TECH].value_counts()
    #     total = tech_counts.sum()
    #     fractions = tech_counts / total
    #     # Reindex to ensure all operators have same columns in same order
    #     fractions = fractions.reindex(tech_order, fill_value=0)
    #     tech_fractions.append(fractions)
    
    # # Create stacked bar plot
    # tech_df = pd.DataFrame(tech_fractions, index=operators)
    # ax = tech_df.plot(kind='bar', stacked=True, figsize=(10,6))
    # plt.title('Technology Distribution by Operator')
    # plt.xlabel('Operator')
    # plt.ylabel('Fraction')
    # plt.legend(title='Technology', bbox_to_anchor=(1.05, 1), loc='upper left')
    # plt.tight_layout()
    # plt.savefig(os.path.join(output_dir, 'tech_distribution.png'))
    # plt.close()

if __name__ == "__main__":
    main()