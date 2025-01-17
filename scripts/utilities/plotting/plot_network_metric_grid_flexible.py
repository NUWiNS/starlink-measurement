from typing import Dict

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd


def plot_network_metric_grid_flexible(
        plot_data: Dict[str, pd.DataFrame],
        row_conf: Dict[str, Dict],  # Configuration for rows
        col_conf: Dict[str, Dict],  # Configuration for columns
        location_conf: Dict[str, Dict],
        operator_conf: Dict[str, Dict],
        output_filepath: str,
        title: str = None,
        max_xlim: float = None,
        x_step: float = None,
        percentile_filter: float = None,
    ):
    """Flexible function to plot grid of CDF plots with custom row and column configurations.
    """
    n_rows = len(row_conf)
    n_cols = len(col_conf)
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4.5*n_cols, 3*n_rows))
    if n_rows == 1 and n_cols == 1:
        axes = np.array([[axes]])
    elif n_rows == 1:
        axes = axes.reshape(1, -1)
    elif n_cols == 1:
        axes = axes.reshape(-1, 1)
    
    plt.subplots_adjust(wspace=0.2, hspace=0.2)
    
    rows_sorted = sorted(row_conf.keys(), key=lambda x: row_conf[x]['order'])
    cols_sorted = sorted(col_conf.keys(), key=lambda x: col_conf[x]['order'])
    
    # First pass: determine column-wise min and max values
    col_max_values = []
    col_min_values = []
    for col_id in cols_sorted:
        max_val = 0
        min_val = float('inf')
        col_filter_mask = col_conf[col_id]['filter_mask']
        x_field = col_conf[col_id]['x_field']
        if col_filter_mask:
            col_data = plot_data[col_filter_mask]
        else:
            col_data = plot_data
        
        for row_id in rows_sorted:
            row_filter_mask = row_conf[row_id]['filter_mask']
            if row_filter_mask:
                row_data = col_data[row_filter_mask]
            else:
                row_data = col_data
            
            operator_labels = list(map(lambda op: operator_conf[op]['label'], operator_conf.keys()))
            row_data = row_data[row_data['operator'].isin(operator_labels)]

            for op_label in operator_labels:
                op_data = row_data[row_data['operator'] == op_label][x_field]
                if percentile_filter:
                    max_val = max(max_val, np.percentile(op_data, percentile_filter))
                else:
                    max_val = max(max_val, np.max(op_data))
                min_val = min(min_val, np.min(op_data))
        
        col_max_values.append(max_val)
        col_min_values.append(min_val)
    
    # Create empty lines for legend
    first_ax = axes[0, 0]
    for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
        first_ax.plot([], [], 
                     label=op_conf['label'],
                     color=op_conf['color'],
                     linewidth=2)
    
    # Second pass: create plots
    for row_idx, row_id in enumerate(rows_sorted):
        for col_idx, col_id in enumerate(cols_sorted):
            ax = axes[row_idx, col_idx]
            col_filter_mask = col_conf[col_id]['filter_mask']
            x_field = col_conf[col_id]['x_field']
            if col_filter_mask:
                col_data = plot_data[col_filter_mask]
            else:
                col_data = plot_data

            operator_labels = list(map(lambda op: operator_conf[op]['label'], operator_conf.keys()))
            row_data = col_data[col_data['operator'].isin(operator_labels)]
            
            for _, op_conf in sorted(operator_conf.items(), key=lambda x: x[1]['order']):
                operator_label = op_conf['label']
                if operator_label in row_data['operator'].unique():
                    operator_data = row_data[row_data['operator'] == operator_label][x_field]
                    
                    if percentile_filter:
                        operator_data = operator_data[operator_data <= np.percentile(operator_data, percentile_filter)]
                    
                    data_sorted = np.sort(operator_data)
                    cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
                    
                    ax.plot(
                        data_sorted,
                        cdf,
                        color=op_conf['color'],
                        linewidth=2
                    )
            
            # Set x-axis limits and ticks
            x_min = col_min_values[col_idx] - 5
            if max_xlim:
                x_max = max_xlim
            else:
                x_max = col_max_values[col_idx]
            ax.set_xlim(x_min, x_max)

            if x_step:
                ax.set_xticks(np.arange(
                    round(x_min / x_step) * x_step, 
                    round(x_max / x_step) * x_step + 1, 
                    x_step
                ))
            
            ax.grid(True, alpha=0.3)
            ax.set_yticks(np.arange(0, 1.1, 0.25))
            ax.tick_params(axis='both', labelsize=10)
            
            if col_idx == 0:
                ax.text(-0.2, 0.5, row_conf[row_id]['label'],
                       transform=ax.transAxes,
                       rotation=0,
                       verticalalignment='center',
                       horizontalalignment='right',
                       fontsize=14,
                       fontweight='bold')
                ax.set_ylabel('CDF', fontsize=10)
            
            if row_idx == 0:
                ax.set_title(col_conf[col_id]['label'], fontsize=12, fontweight='bold')
            
            if row_idx == 0 and col_idx == 0:
                legend = ax.legend(fontsize=10, 
                                 loc='best',
                                 framealpha=0.9,
                                 edgecolor='black')
                for text in legend.get_texts():
                    text.set_fontweight('bold')
            
            if row_idx == n_rows - 1:
                ax.set_xlabel(col_conf[col_id]['x_label'], fontsize=10)
    
    if title:
        fig.suptitle(title, y=1.02, fontsize=14, fontweight='bold')
    
    plt.savefig(output_filepath, bbox_inches='tight', dpi=300)
    print(f'Saved plot to {output_filepath}')
    plt.close()