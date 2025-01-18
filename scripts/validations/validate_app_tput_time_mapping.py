import os
import pandas as pd
import matplotlib.pyplot as plt
from scripts.constants import XcalField
import numpy as np

from scripts.logging_utils import create_logger


class AppTputTimeMappingForXcal:
    def __init__(self, 
      base_dir: str, 
      output_dir: str = '.',
      field_tput_dl: str = XcalField.TPUT_DL,
      field_tput_ul: str = XcalField.TPUT_UL,
      field_protocol: str = XcalField.APP_TPUT_PROTOCOL,
      field_direction: str = XcalField.APP_TPUT_DIRECTION,
    ):
        self.base_dir = base_dir
        self.output_dir = output_dir
        self.field_tput_dl = field_tput_dl
        self.field_tput_ul = field_tput_ul
        self.field_protocol = field_protocol
        self.field_direction = field_direction
        self.logger = create_logger('app_tput_time_mapping_for_xcal', filename=os.path.join(output_dir, f'app_tput_time_mapping_for_xcal.log'), filemode='w')

    def validate_app_tput_time_mapping(self, operator: str):
        xcal_smart_tput_csv_path = os.path.join(self.base_dir, f'{operator}_xcal_smart_tput.csv')
        # Read the CSV file
        df = pd.read_csv(xcal_smart_tput_csv_path)
        
        self.logger.info(f"Processing {operator} data with total {len(df)} rows")
        
        # Group by protocol and direction
        grouped = df.groupby([self.field_protocol, self.field_direction])
        
        # Create a figure for CDF plots
        plt.figure(figsize=(10, 6))
        
        # Prepare table headers
        table_rows = ["| Type | Total Rows | Negative Diff | Diff > 5Mbps |"]
        table_rows.append("|------|------------|--------------|-------------|")
        
        # Process each group
        for (protocol, direction), group in grouped:
            # Calculate diff based on direction
            if 'downlink' in direction.lower():
                diff = group[self.field_tput_dl] - group[self.field_tput_ul]
            else:  # uplink
                diff = group[self.field_tput_ul] - group[self.field_tput_dl]
            
            # Plot CDF
            x = np.sort(diff)
            y = np.arange(1, len(x) + 1) / len(x)
            plt.plot(x, y, label=f'{protocol}_{direction}')
            
            # Calculate statistics
            total_rows = len(group)
            negative_rows = group[diff < 0]
            negative_count = len(negative_rows)
            percentage = (negative_count / total_rows) * 100 if total_rows > 0 else 0
            
            huge_diff_rows = negative_rows[abs(diff[diff < 0]) > 5]
            huge_diff_count = len(huge_diff_rows)
            huge_diff_percentage = (huge_diff_count / total_rows) * 100 if total_rows > 0 else 0
            
            # Add row to table
            table_rows.append(
                f"| {protocol}_{direction} | {total_rows} | {negative_count} ({percentage:.2f}%) | {huge_diff_count} ({huge_diff_percentage:.2f}%) |"
            )
            
            if huge_diff_count > 0:
                self.logger.warning(f"\nDetailed negative differences > 5 Mbps for {protocol}_{direction}:")
                self.logger.warning(f"\n{huge_diff_rows[[self.field_tput_dl, self.field_tput_ul]]}")
        
        # Log the table
        self.logger.info("\nSummary Table:")
        self.logger.info('\n' + "\n".join(table_rows))
        
        # Customize plot
        plt.grid(True)
        plt.xlabel('Throughput Difference (Mbps)')
        plt.ylabel('CDF')
        plt.title(f'CDF of Throughput Differences - {operator}')
        plt.legend()
        
        # Save plot
        output_path = os.path.join(self.output_dir, f'{operator}_tput_diff_cdf.png')
        plt.savefig(output_path)
        plt.close()