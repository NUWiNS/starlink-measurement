import glob
from typing import List
import pandas as pd
import folium
import os

import sys


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(CURRENT_DIR, '../..'))
OUTPUT_DIR = os.path.join(CURRENT_DIR, "outputs")

from scripts.alaska_starlink_trip.configs import ROOT_DIR as ALASKA_ROOT_DIR, TIMEZONE as ALASKA_TIMEZONE
from scripts.hawaii_starlink_trip.configs import ROOT_DIR as HAWAII_ROOT_DIR, TIMEZONE as HAWAII_TIMEZONE
from scripts.constants import DATASET_DIR, XcalField
from scripts.celllular_analysis.configs import tech_config_map

COORD_ANCHORAGE = [61.2181, -149.9003]
COORD_MAUI = [20.7943, -156.3319]

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

def plot_driving_route_with_tech(df: pd.DataFrame, center_coordinates: List[float], output_file_path: str, timezone: str):
    # Create a map centered on the given coordinates
    m = folium.Map(location=center_coordinates, zoom_start=6)

    # sort by time
    df = df.sort_values(by=XcalField.CUSTOM_UTC_TIME)
    # Group the dataframe by segment_id
    grouped_df = df.groupby(XcalField.SEGMENT_ID)
    for segment_id, segment_data in grouped_df:
        # For each technology in the segment
        grouped_by_tech = segment_data.groupby(XcalField.ACTUAL_TECH)
        for tech, tech_segment in grouped_by_tech:
            # Get coordinates for this tech segment
            route_coordinates = tech_segment[[XcalField.LAT, XcalField.LON]].values.tolist()
            
            # Only create line if we have at least 2 points
            if len(route_coordinates) >= 2:
                # Get color from tech_config_map, default to NO SERVICE color if tech not found
                color = tech_config_map.get(tech, tech_config_map['NO SERVICE'])['color']
                run_id = segment_data[XcalField.RUN_ID].iloc[0]
                local_time = pd.to_datetime(run_id).tz_localize('UTC').tz_convert(timezone).strftime('%Y-%m-%d %H:%M:%S %Z')
                # Create a PolyLine for this tech segment
                folium.PolyLine(
                    route_coordinates,
                    color=color,
                    weight=2.5,
                    opacity=1,
                    popup=f"[{local_time}] Technology: {tech}"
                ).add_to(m)

    # Add markers for overall start and end points
    start_point = df.iloc[0][[XcalField.LAT, XcalField.LON]].tolist()
    end_point = df.iloc[-1][[XcalField.LAT, XcalField.LON]].tolist()

    folium.Marker(
        start_point,
        popup="Start",
        icon=folium.Icon(color='green', icon='play')
    ).add_to(m)
    
    folium.Marker(
        end_point,
        popup="End",
        icon=folium.Icon(color='red', icon='stop')
    ).add_to(m)

    # Add a legend using the tech_config_map
    legend_html = '''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px;">
    <h4>Technologies</h4>
    '''
    for tech, config in tech_config_map.items():
        legend_html += f'<p><span style="color:{config["color"]};">▬</span> {config["label"]}</p>'
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))

    # Save the map
    m.save(output_file_path)
    print(f"Map saved as '{output_file_path}'")

def plot_route_in_alaska_with_all_xcal_data(operator: str):
    alaska_xcal_files = glob.glob(os.path.join(DATASET_DIR, 'xcal', '*_ALASKA_*.xlsx'))
    if len(alaska_xcal_files) == 0:
        raise ValueError("No xcal files found in the dataset directory")
    # merge the Lat and Lon cols among all hawaii xcal files
    df_alaska_gps_data_in_alaska = pd.DataFrame({
        "latitude": [],
        "longitude": [],
        'timestamp': []
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
            "latitude": df[XcalField.LAT],
            "longitude": df[XcalField.LON],
            'timestamp': df[XcalField.TIMESTAMP],
        })], ignore_index=True)
        df_alaska_gps_data_in_alaska = df_alaska_gps_data_in_alaska.drop_duplicates(subset=["latitude", "longitude"])  # Remove duplicate lat/lon pairs after concatenation
        # sort by time
        df_alaska_gps_data_in_alaska = df_alaska_gps_data_in_alaska.sort_values(by='timestamp')
        
    output_alaska_driving_route_file_path = os.path.join(OUTPUT_DIR, f"{operator}_alaska_driving_route_with_all_xcal_data.html")
    plot_driving_route(
        df_alaska_gps_data_in_alaska, 
        center_coordinates=COORD_ANCHORAGE,  # Anchorage coordinates
        output_file_path=output_alaska_driving_route_file_path
    )

def plot_route_in_hawaii_with_all_xcal_data():
    hawaii_xcal_files = glob.glob(os.path.join(DATASET_DIR, 'xcal', '*_HAWAII_*.xlsx'))
    if len(hawaii_xcal_files) == 0:
        raise ValueError("No xcal files found in the dataset directory")
    # merge the Lat and Lon cols among all hawaii xcal files
    df_hawaii_gps_data_in_hawaii = pd.DataFrame({
        "latitude": [],
        "longitude": [],
        'timestamp': []
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
            "latitude": df[XcalField.LAT],
            "longitude": df[XcalField.LON],
            'timestamp': df[XcalField.TIMESTAMP],
        })], ignore_index=True)
        df_hawaii_gps_data_in_hawaii = df_hawaii_gps_data_in_hawaii.drop_duplicates(subset=["latitude", "longitude"])  # Remove duplicate lat/lon pairs after concatenation
        # sort by time
        df_hawaii_gps_data_in_hawaii = df_hawaii_gps_data_in_hawaii.sort_values(by='timestamp')
        
    output_hawaii_driving_route_file_path = os.path.join(OUTPUT_DIR, "hawaii_driving_route_with_all_xcal_data.html")
    plot_driving_route(
        df_hawaii_gps_data_in_hawaii, 
        center_coordinates=COORD_MAUI,  # Maui coordinates
        output_file_path=output_hawaii_driving_route_file_path
    )



def plot_route_in_alaska_within_measurements(operator: str):
    df_xcal_data_in_alaska = pd.read_csv(os.path.join(ALASKA_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv"))
    df_gps_data_in_alaska = pd.DataFrame({
        "latitude": df_xcal_data_in_alaska[XcalField.LAT],
        "longitude": df_xcal_data_in_alaska[XcalField.LON],
        'timestamp': df_xcal_data_in_alaska[XcalField.CUSTOM_UTC_TIME],
    })
    # sort by time
    df_gps_data_in_alaska = df_gps_data_in_alaska.sort_values(by='timestamp')
    output_alaska_driving_route_file_path = os.path.join(OUTPUT_DIR, f"{operator}_alaska_driving_route.html")
    plot_driving_route(
        df_gps_data_in_alaska, 
        center_coordinates=COORD_ANCHORAGE,  # Anchorage coordinates
        output_file_path=output_alaska_driving_route_file_path
    )

def plot_route_in_hawaii_within_measurements(operator: str):
    df_xcal_data_in_hawaii = pd.read_csv(os.path.join(HAWAII_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv"))
    df_gps_data_in_hawaii = pd.DataFrame({
        "latitude": df_xcal_data_in_hawaii[XcalField.LAT],
        "longitude": df_xcal_data_in_hawaii[XcalField.LON],
        'timestamp': df_xcal_data_in_hawaii[XcalField.CUSTOM_UTC_TIME],
    })
    # sort by time
    df_gps_data_in_hawaii = df_gps_data_in_hawaii.sort_values(by='timestamp')
    output_hawaii_driving_route_file_path = os.path.join(OUTPUT_DIR, f"{operator}_hawaii_driving_route.html")
    plot_driving_route(
        df_gps_data_in_hawaii, 
        center_coordinates=COORD_MAUI,  # Maui coordinates
        output_file_path=output_hawaii_driving_route_file_path
    )

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


    # dfs_alaska = {}
    # for operator in ["att", "verizon"]:
    #     df_xcal_data_in_alaska = pd.read_csv(os.path.join(ALASKA_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv"))
    #     dfs_alaska[operator] = df_xcal_data_in_alaska
    
    # for operator, df_xcal_data_in_alaska in dfs_alaska.items():
    #     output_file_path = os.path.join(OUTPUT_DIR, f"{operator}_alaska_driving_route_with_tech.html")
    #     plot_driving_route_with_tech(
    #         df_xcal_data_in_alaska, 
    #         center_coordinates=COORD_ANCHORAGE, 
    #         timezone=ALASKA_TIMEZONE, 
    #         output_file_path=output_file_path
    #     )

    dfs_hawaii = {}
    for operator in ["att", "verizon", "tmobile"]:
        df_xcal_data_in_hawaii = pd.read_csv(os.path.join(HAWAII_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv"))
        dfs_hawaii[operator] = df_xcal_data_in_hawaii
    for operator, df_xcal_data_in_hawaii in dfs_hawaii.items():
        output_file_path = os.path.join(OUTPUT_DIR, f"{operator}_hawaii_driving_route_with_tech.html")
        plot_driving_route_with_tech(
            df_xcal_data_in_hawaii, 
            center_coordinates=COORD_MAUI, 
            timezone=HAWAII_TIMEZONE, 
            output_file_path=output_file_path
        )

    # for operator in ["att", "verizon"]:
    #     plot_route_in_alaska_within_measurements(operator)
    
    # for operator in ["att", "verizon", 'tmobile']:
    #     plot_route_in_hawaii_within_measurements(operator)
    

    # Just for cross-validation
    # plot_route_in_alaska_with_all_xcal_data()
    # plot_route_in_hawaii_with_all_xcal_data()

# Example usage
if __name__ == "__main__":
   main()

