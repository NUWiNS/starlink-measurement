import os
import sys
from typing import Dict, List, Tuple

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from scripts.constants import CommonField
from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR

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
    fig, ax = plt.subplots(figsize=(6, 4))
    
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
    plt.close()


def filter_data_by_area(df: pd.DataFrame, area_type: str):
    if area_type == 'urban':
        mask = (df[CommonField.AREA_TYPE] == 'urban') | (df[CommonField.AREA_TYPE] == 'suburban')
    elif area_type == 'rural':
        mask = df[CommonField.AREA_TYPE] == 'rural'
    else:
        raise ValueError(f'Unsupported area type: {area_type}')
    return df[mask]

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    alaska_df = get_starlink_metrics(ALASKA_ROOT_DIR)
    hawaii_df = get_starlink_metrics(HAWAII_ROOT_DIR)


    plot_data = {
        "alaska": {
            "urban": filter_data_by_area(alaska_df, "urban"),
            "rural": filter_data_by_area(alaska_df, "rural"),
        },
        "hawaii": {
            "urban": filter_data_by_area(hawaii_df, "urban"),
            "rural": filter_data_by_area(hawaii_df, "rural"),
        },
    }

    plot_outage_rate_by_area(
        plot_data=plot_data,
        location_conf=location_conf,
        area_conf=area_conf,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()
