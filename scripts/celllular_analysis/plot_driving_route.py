import glob
from typing import List
import pandas as pd
import folium
import os

import sys


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(CURRENT_DIR, '../..'))
OUTPUT_DIR = os.path.join(CURRENT_DIR, "outputs")

from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR
from scripts.constants import DATASET_DIR

def plot_driving_route(df: pd.DataFrame, center_coordinates: List[float], output_file_path: str):
    # Create a map centered on Alaska
    m = folium.Map(location=center_coordinates, zoom_start=6)

    # Create a PolyLine of the driving route
    route_coordinates = df[['latitude', 'longitude']].values.tolist()
    folium.PolyLine(route_coordinates, color="red", weight=2.5, opacity=1).add_to(m)

    # Add markers for start and end points
    start_point = route_coordinates[0]
    end_point = route_coordinates[-1]

    folium.Marker(start_point, popup="Start", icon=folium.Icon(color='green', icon='play')).add_to(m)
    folium.Marker(end_point, popup="End", icon=folium.Icon(color='red', icon='stop')).add_to(m)

    # Save the map
    m.save(output_file_path)

    print(f"Map saved as '{output_file_path}'")

def plot_route_in_alaska_with_all_xcal_data():
    alaska_xcal_files = glob.glob(os.path.join(DATASET_DIR, 'xcal', '*_ALASKA_*.xlsx'))
    if len(alaska_xcal_files) == 0:
        raise ValueError("No xcal files found in the dataset directory")
    # merge the Lat and Lon cols among all hawaii xcal files
    df_alaska_gps_data_in_alaska = pd.DataFrame({
        "latitude": [],
        "longitude": []
    })
    for alaska_xcal_file in alaska_xcal_files:
        df = pd.read_excel(alaska_xcal_file)
        df = df[["Lat", "Lon"]].dropna()
        df = df.drop_duplicates(subset=["Lat", "Lon"])  # Remove duplicate lat/lon pairs
        
        # Filter out coordinates not related to Alaska
        # alaska_lat_range = (60.5, 62.5)  # Approximate latitude range for Alaska
        # alaska_lon_range = (-150.5, -148.5)  # Approximate longitude range for Alaska
        # df = df[(df["Lat"].between(*alaska_lat_range)) & (df["Lon"].between(*alaska_lon_range))]
        
        df_alaska_gps_data_in_alaska = pd.concat([df_alaska_gps_data_in_alaska, pd.DataFrame({
            "latitude": df["Lat"],
            "longitude": df["Lon"]
        })], ignore_index=True)
        df_alaska_gps_data_in_alaska = df_alaska_gps_data_in_alaska.drop_duplicates(subset=["latitude", "longitude"])  # Remove duplicate lat/lon pairs after concatenation
        
    output_alaska_driving_route_file_path = os.path.join(OUTPUT_DIR, "alaska_driving_route_with_all_xcal_data.html")
    plot_driving_route(
        df_alaska_gps_data_in_alaska, 
        center_coordinates=[61.2181, -149.9003],  # Anchorage coordinates
        output_file_path=output_alaska_driving_route_file_path
    )

def plot_route_in_hawaii_with_all_xcal_data():
    hawaii_xcal_files = glob.glob(os.path.join(DATASET_DIR, 'xcal', '*_HAWAII_*.xlsx'))
    if len(hawaii_xcal_files) == 0:
        raise ValueError("No xcal files found in the dataset directory")
    # merge the Lat and Lon cols among all hawaii xcal files
    df_hawaii_gps_data_in_hawaii = pd.DataFrame({
        "latitude": [],
        "longitude": []
    })
    for hawaii_xcal_file in hawaii_xcal_files:
        df = pd.read_excel(hawaii_xcal_file)
        df = df[["Lat", "Lon"]].dropna()
        df = df.drop_duplicates(subset=["Lat", "Lon"])  # Remove duplicate lat/lon pairs
        
        # Filter out coordinates not related to Hawaii
        hawaii_lat_range = (18.5, 22.5)  # Approximate latitude range for Hawaii
        hawaii_lon_range = (-160.5, -154.5)  # Approximate longitude range for Hawaii
        df = df[(df["Lat"].between(*hawaii_lat_range)) & (df["Lon"].between(*hawaii_lon_range))]
        
        df_hawaii_gps_data_in_hawaii = pd.concat([df_hawaii_gps_data_in_hawaii, pd.DataFrame({
            "latitude": df["Lat"],
            "longitude": df["Lon"]
        })], ignore_index=True)
        df_hawaii_gps_data_in_hawaii = df_hawaii_gps_data_in_hawaii.drop_duplicates(subset=["latitude", "longitude"])  # Remove duplicate lat/lon pairs after concatenation
        
    output_hawaii_driving_route_file_path = os.path.join(OUTPUT_DIR, "hawaii_driving_route_with_all_xcal_data.html")
    plot_driving_route(
        df_hawaii_gps_data_in_hawaii, 
        center_coordinates=[20.7943, -156.3319],  # Maui coordinates
        output_file_path=output_hawaii_driving_route_file_path
    )



def plot_route_in_alaska_within_measurements():
    df_att_xcal_data_in_alaska = pd.read_csv(os.path.join(ALASKA_ROOT_DIR, "xcal", "att_smart_tput.csv"))
    df_att_gps_data_in_alaska = pd.DataFrame({
        "latitude": df_att_xcal_data_in_alaska["Lat"],
        "longitude": df_att_xcal_data_in_alaska["Lon"]
    })
    output_alaska_driving_route_file_path = os.path.join(OUTPUT_DIR, "alaska_driving_route.html")
    plot_driving_route(
        df_att_gps_data_in_alaska, 
        center_coordinates=[61.2181, -149.9003],  # Anchorage coordinates
        output_file_path=output_alaska_driving_route_file_path
    )

def plot_route_in_hawaii_within_measurements():
    df_att_xcal_data_in_hawaii = pd.read_csv(os.path.join(HAWAII_ROOT_DIR, "xcal", "att_smart_tput.csv"))
    df_att_gps_data_in_hawaii = pd.DataFrame({
        "latitude": df_att_xcal_data_in_hawaii["Lat"],
        "longitude": df_att_xcal_data_in_hawaii["Lon"]
    })
    output_hawaii_driving_route_file_path = os.path.join(OUTPUT_DIR, "hawaii_driving_route.html")
    plot_driving_route(
        df_att_gps_data_in_hawaii, 
        center_coordinates=[20.7943, -156.3319],  # Maui coordinates
        output_file_path=output_hawaii_driving_route_file_path
    )

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    plot_route_in_alaska_within_measurements()
    plot_route_in_hawaii_within_measurements()

    # Just for cross-validation
    plot_route_in_alaska_with_all_xcal_data()
    plot_route_in_hawaii_with_all_xcal_data()

# Example usage
if __name__ == "__main__":
   main()

