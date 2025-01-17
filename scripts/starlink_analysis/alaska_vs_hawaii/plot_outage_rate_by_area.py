import os
import sys
from typing import Dict, List, Tuple

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from scripts.constants import CommonField
from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.maine_starlink_trip.configs import ROOT_DIR as MAINE_ROOT_DIR

current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, "outputs")

def get_starlink_metrics(root_dir: str):
    metric_csv = os.path.join(
        root_dir, "starlink", "starlink_metric.app_tput_filtered.csv"
    )
    df = pd.read_csv(metric_csv)
    df[CommonField.LOCAL_DT] = pd.to_datetime(df[CommonField.LOCAL_DT])
    return df


location_conf = {
    "alaska": {
        "label": "Alaska",
        "order": 1,
    },
    "hawaii": {
        "label": "Hawaii",
        "order": 2,
    },
    'maine': {
        "label": "Maine",
        "order": 3,
    }
}

area_conf = {
    "urban": {
        "label": "Urban",
        "color": "blue",
        "order": 1,
    },
    "rural": {
        "label": "Rural",
        "color": "green",
        "order": 2,
    },
}


def get_total_duration_s(df: pd.DataFrame):
    # Drop rows where SEGMENT_ID or LOCAL_DT is NA
    clean_df = df.dropna(subset=[CommonField.SEGMENT_ID])
    grouped_df = clean_df.groupby([CommonField.SEGMENT_ID])
    total_duration_s = 0
    for _, segment_df in grouped_df:
        first_utc_ts = segment_df[CommonField.LOCAL_DT].min()
        last_utc_ts = segment_df[CommonField.LOCAL_DT].max()
        segment_duration_s = (last_utc_ts - first_utc_ts).total_seconds()
        total_duration_s += segment_duration_s
    return total_duration_s


def get_outage_duration_s(df: pd.DataFrame):
    # Drop rows where outage_start_time_ns or outage.duration_ns is NA
    clean_df = df.dropna(subset=["outage_start_time_ns", "outage.duration_ns"])
    grouped_df = clean_df.groupby(["outage_start_time_ns"])
    total_outage_duration_s = 0
    for _, outage_df in grouped_df:
        max_duration = outage_df["outage.duration_ns"].max()
        max_duration_sec = max_duration / 1e9
        total_outage_duration_s += max_duration_sec
    return total_outage_duration_s


def calculate_outage_fraction(df: pd.DataFrame):
    total_duration_s = get_total_duration_s(df)
    total_outage_duration_s = get_outage_duration_s(df)
    return total_outage_duration_s / total_duration_s


def plot_outage_rate_by_area(
    plot_data: Dict,
    location_conf: Dict,
    area_conf: Dict,
    output_dir: str,
):
    # Set up the plot
    fig, ax = plt.subplots(figsize=(4, 3))
    
    # Calculate bar positions
    n_locations = len(plot_data)
    n_areas = len(area_conf)
    width = 0.3  # Width of bars
    
    # Calculate outage rates and prepare plotting data
    x_positions = np.arange(n_locations)
    outage_rates = {
        location: {} for location in plot_data.keys()
    }
    
    for location in plot_data.keys():
        area_data = plot_data[location]
        for area in area_data.keys():
            loc_area_df = area_data[area]
            outage_fraction = calculate_outage_fraction(loc_area_df)
            outage_rates[location][area] = round(outage_fraction * 100, 2)
    
    # Track whether we've added labels to legend
    labels_used = set()
    
    for i, location in enumerate(sorted(outage_rates.keys(), key=lambda x: location_conf[x]["order"])):
        area_rates = outage_rates[location]
        for j, area in enumerate(sorted(area_rates.keys(), key=lambda x: area_conf[x]["order"])):
            outage_rate = area_rates[area]
            # Calculate offset for each bar within the location group
            offset = (j - (n_areas - 1) / 2) * width
            # Only add label to legend if we haven't used it yet
            label = area_conf[area]["label"] if area not in labels_used else None
            ax.bar(
                x_positions[i] + offset,  # Add offset to position bars side by side
                outage_rate,
                width,
                label=label,
                color=area_conf[area]["color"]
            )
            labels_used.add(area)
    
    # Customize the plot
    ax.set_ylabel("Outage Rate (%)")
    ax.set_title("Starlink Outage Rate")
    ax.set_ylim(0, 15)
    # Set x-axis ticks and labels
    ax.set_xticks(
        x_positions,
        [location_conf[loc]["label"] for loc in sorted(plot_data.keys(), key=lambda x: location_conf[x]["order"])]
    )
    
    # Add legend
    ax.legend()
    
    # Adjust layout and save
    plt.tight_layout()
    output_path = os.path.join(output_dir, "outage_rate_by_area.png")
    fig.savefig(output_path)
    print(f"Saved outage rate by area plot to {output_path}")
    plt.close()

def get_duration_map_of_outage_causes(df: pd.DataFrame) -> Dict[str, float]:
    # Drop rows where outage cause or duration is NA
    clean_df = df.dropna(subset=["outage_cause", "outage_start_time_ns", "outage.duration_ns"])
    
    # Group by outage cause and calculate total duration for each cause
    cause_groups = clean_df.groupby(["outage_cause", "outage_start_time_ns"])
    
    cause_duration_map = {}
    for (cause, _), group_df in cause_groups:
        duration_s = group_df["outage.duration_ns"].max() / 1e9
        cause_duration_map[cause] = cause_duration_map.get(cause, 0) + duration_s
    
    return cause_duration_map

def plot_outage_reason_distribution_by_area_with_white_list(
    plot_data: Dict,
    location_conf: Dict,
    area_conf: Dict,
    output_dir: str,
):
    # Set up the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Define whitelist of causes
    whitelist_causes = ["NO_SCHEDULE", "OBSTRUCTED"]
    
    # Create color map for whitelisted causes
    cause_colors = {
        "NO_SCHEDULE": 'brown',
        "OBSTRUCTED": 'gray',
    }
    
    # Track whether we've added labels to legend
    labels_used = set()
    
    # Create x labels and positions
    x_labels = []
    x_positions = []
    pos = 0
    
    for location in sorted(plot_data.keys(), key=lambda x: location_conf[x]["order"]):
        area_data = plot_data[location]
        for area in sorted(area_data.keys(), key=lambda x: area_conf[x]["order"]):
            x_labels.append(f"{location_conf[location]['label']} {area_conf[area]['label']}")
            x_positions.append(pos)
            pos += 1
    
    # Plot stacked bars
    bar_width = 0.6
    bar_idx = 0
    for location in sorted(plot_data.keys(), key=lambda x: location_conf[x]["order"]):
        area_data = plot_data[location]
        for area in sorted(area_data.keys(), key=lambda x: area_conf[x]["order"]):
            df = area_data[area]
            cause_duration_map = get_duration_map_of_outage_causes(df)
            
            # Filter for whitelisted causes and calculate total duration
            filtered_duration_map = {
                cause: duration 
                for cause, duration in cause_duration_map.items() 
                if cause in whitelist_causes
            }
            total_duration_s = sum(filtered_duration_map.values())
            
            if total_duration_s > 0:  # Only plot if there are whitelisted causes
                bottom = 0
                for cause in whitelist_causes:
                    if cause in filtered_duration_map:
                        duration_s = filtered_duration_map[cause]
                        percentage = duration_s / total_duration_s * 100

                        # Only add label to legend if we haven't used it yet
                        label = cause if cause not in labels_used else None
                        ax.bar(
                            x_positions[bar_idx],
                            percentage,
                            bar_width,
                            bottom=bottom,
                            label=label,
                            color=cause_colors[cause]
                        )
                        labels_used.add(cause)
                        bottom += percentage
            bar_idx += 1
    
    # Customize the plot
    ax.set_ylabel("Percentage of Outage Causes (%)")
    ax.set_title("Distribution of Starlink Outage Causes")
    
    # Set x-axis ticks and labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels)
    
    # Add legend
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Adjust layout and save
    plt.tight_layout()
    output_path = os.path.join(output_dir, "outage_cause_distribution_filtered.png")
    fig.savefig(output_path, bbox_inches='tight')
    plt.close()

def filter_data_by_area(df: pd.DataFrame, area_type: str):
    if area_type == 'urban':
        mask = (df[CommonField.AREA_TYPE] == 'urban') | (df[CommonField.AREA_TYPE] == 'suburban')
    elif area_type == 'rural':
        mask = df[CommonField.AREA_TYPE] == 'rural'
    else:
        raise ValueError(f'Unsupported area type: {area_type}')
    return df[mask]

def save_outage_rate_stats(
    plot_data: Dict,
    location_conf: Dict,
    area_conf: Dict,
    output_dir: str,
):
    stats = {}
    for location in sorted(plot_data.keys(), key=lambda x: location_conf[x]["order"]):
        stats[location] = {}
        area_data = plot_data[location]
        for area in sorted(area_data.keys(), key=lambda x: area_conf[x]["order"]):
            df = area_data[area]
            total_duration = get_total_duration_s(df)
            outage_duration = get_outage_duration_s(df)
            outage_percentage = (outage_duration / total_duration * 100) if total_duration > 0 else 0
            
            stats[location][area] = {
                "total_duration_s": total_duration,
                "outage_duration_s": outage_duration,
                "outage_percentage": round(outage_percentage, 2)
            }
    
    output_path = os.path.join(output_dir, "outage_rate_by_area.stats.json")
    with open(output_path, 'w') as f:
        json.dump(stats, f, indent=2)

def save_outage_distribution_stats_with_white_list(
    plot_data: Dict,
    location_conf: Dict,
    area_conf: Dict,
    output_dir: str,
):
    whitelist_causes = ["NO_SCHEDULE", "OBSTRUCTED"]
    stats = {}
    
    for location in sorted(plot_data.keys(), key=lambda x: location_conf[x]["order"]):
        stats[location] = {}
        area_data = plot_data[location]
        for area in sorted(area_data.keys(), key=lambda x: area_conf[x]["order"]):
            df = area_data[area]
            cause_duration_map = get_duration_map_of_outage_causes(df)
            
            # Filter for whitelisted causes
            filtered_duration_map = {
                cause: duration 
                for cause, duration in cause_duration_map.items() 
                if cause in whitelist_causes
            }
            total_duration = sum(filtered_duration_map.values())
            
            stats[location][area] = {
                "total_selected_outage_duration_s": total_duration,
                "causes": {}
            }
            
            # Calculate percentages for whitelisted causes
            for cause in whitelist_causes:
                duration = filtered_duration_map.get(cause, 0)
                percentage = (duration / total_duration * 100) if total_duration > 0 else 0
                stats[location][area]["causes"][cause] = {
                    "duration_s": duration,
                    "percentage": round(percentage, 2)
                }
    
    output_path = os.path.join(output_dir, "outage_cause_distribution_filtered.stats.json")
    with open(output_path, 'w') as f:
        json.dump(stats, f, indent=2)

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    alaska_df = get_starlink_metrics(ALASKA_ROOT_DIR)
    hawaii_df = get_starlink_metrics(HAWAII_ROOT_DIR)
    maine_df = get_starlink_metrics(MAINE_ROOT_DIR)


    plot_data = {
        "alaska": {
            "urban": filter_data_by_area(alaska_df, "urban"),
            "rural": filter_data_by_area(alaska_df, "rural"),
        },
        "hawaii": {
            "urban": filter_data_by_area(hawaii_df, "urban"),
            "rural": filter_data_by_area(hawaii_df, "rural"),
        },
        "maine": {
            "urban": filter_data_by_area(maine_df, "urban"),
            "rural": filter_data_by_area(maine_df, "rural"),
        },
    }

    # Plot and save outage rate stats
    plot_outage_rate_by_area(
        plot_data=plot_data,
        location_conf=location_conf,
        area_conf=area_conf,
        output_dir=output_dir,
    )
    save_outage_rate_stats(
        plot_data=plot_data,
        location_conf=location_conf,
        area_conf=area_conf,
        output_dir=output_dir,
    )

    # Plot and save filtered outage distribution stats
    plot_outage_reason_distribution_by_area_with_white_list(
        plot_data=plot_data,
        location_conf=location_conf,
        area_conf=area_conf,
        output_dir=output_dir,
    )
    save_outage_distribution_stats_with_white_list(
        plot_data=plot_data,
        location_conf=location_conf,
        area_conf=area_conf,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()
