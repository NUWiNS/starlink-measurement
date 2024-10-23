import pandas as pd
import folium
import os

import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(CURRENT_DIR, '../..'))
OUTPUT_DIR = os.path.join(CURRENT_DIR, "outputs")

from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR


def plot_driving_route(df: pd.DataFrame, output_file_path: str):
    # Create a map centered on Alaska
    alaska_center = [61.2181, -149.9003]  # Anchorage coordinates
    m = folium.Map(location=alaska_center, zoom_start=6)

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

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    att_xcal_data_in_alaska = os.path.join(ALASKA_ROOT_DIR, "xcal", "att_smart_tput.csv")
    df = pd.read_csv(att_xcal_data_in_alaska)
    df_att_gps_data_in_alaska = pd.DataFrame({
        "latitude": df["Lat"],
        "longitude": df["Lon"]
    })
    output_alaska_driving_route_file_path = os.path.join(OUTPUT_DIR, "alaska_driving_route.html")
    plot_driving_route(df_att_gps_data_in_alaska, output_file_path=output_alaska_driving_route_file_path)
# Example usage
if __name__ == "__main__":
   main()

