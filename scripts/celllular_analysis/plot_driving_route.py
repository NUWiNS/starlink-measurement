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

    # Sort data by time and group by run_id
    df = df.sort_values(by='timestamp')
    grouped_df = df.groupby('run_id')
    
    # Create a different colored line for each run
    for run_id, run_data in grouped_df:
        # Get coordinates for this run
        route_coordinates = run_data[['latitude', 'longitude']].values.tolist()
        
        # Only create line if we have at least 2 points
        if len(route_coordinates) >= 2:
            # Create a PolyLine for this run
            folium.PolyLine(
                route_coordinates,
                color="red",
                weight=2.5,
                opacity=1,
                popup=f"Run {run_id}"
            ).add_to(m)

    # Add markers only for overall start and end points
    overall_start = df.iloc[0][['latitude', 'longitude']].tolist()
    overall_end = df.iloc[-1][['latitude', 'longitude']].tolist()

    folium.Marker(
        overall_start, 
        popup="Start", 
        icon=folium.Icon(color='green', icon='play')
    ).add_to(m)
    
    folium.Marker(
        overall_end, 
        popup="End", 
        icon=folium.Icon(color='red', icon='stop')
    ).add_to(m)

    # Save the map
    m.save(output_file_path)

    print(f"Map saved as '{output_file_path}'")

def plot_driving_route_with_tech(
        df: pd.DataFrame, 
        center_coordinates: List[float], 
        output_file_path: str, 
        timezone: str = 'UTC',
        operator: str = None, 
    ):
    # Create a map centered on the given coordinates with grayscale tiles
    # Using CartoDB's light tiles which provide a light, neutral background
    grayscale_tiles = 'https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png'
    attr = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
    
    m = folium.Map(
        location=center_coordinates,
        zoom_start=6,
        tiles=grayscale_tiles,
        attr=attr,
        prefer_canvas=True
    )

    # Alternative tile providers if needed:
    # Stamen Toner Light:
    # tiles='https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}.png'
    # attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>'

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
    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px;">
    <h4>Technologies ({operator})</h4>
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
        'timestamp': [],
        'run_id': []
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
            'run_id': df[XcalField.RUN_ID],
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
        'run_id': df_xcal_data_in_alaska[XcalField.RUN_ID],
    })
    # sort by time
    df_gps_data_in_alaska = df_gps_data_in_alaska.sort_values(by='timestamp')
    output_alaska_driving_route_file_path = os.path.join(OUTPUT_DIR, f"alaska_driving_route.{operator}.html")
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
        'run_id': df_xcal_data_in_hawaii[XcalField.RUN_ID],
    })
    # sort by time
    df_gps_data_in_hawaii = df_gps_data_in_hawaii.sort_values(by='timestamp')
    output_hawaii_driving_route_file_path = os.path.join(OUTPUT_DIR, f"hawaii_driving_route.{operator}.html")
    plot_driving_route(
        df_gps_data_in_hawaii, 
        center_coordinates=COORD_MAUI,  # Maui coordinates
        output_file_path=output_hawaii_driving_route_file_path
    )

def plot_route_with_area_type(df: pd.DataFrame, area_field: str, operator: str, output_file_path: str):
    # Create a map centered on the mean coordinates with grayscale tiles
    center_coordinates = [df[XcalField.LAT].mean(), df[XcalField.LON].mean()]
    grayscale_tiles = 'https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png'
    attr = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
    
    m = folium.Map(
        location=center_coordinates,
        zoom_start=6,
        tiles=grayscale_tiles,
        attr=attr,
        prefer_canvas=True
    )

    # Define area type colors
    area_config = {
        'urban': {'color': 'red', 'label': 'Urban'},      # Red
        'suburban': {'color': 'blue', 'label': 'Suburban'}, # Blue
        'rural': {'color': 'green', 'label': 'Rural'}       # Green
    }

    # sort by time
    df = df.sort_values(by=XcalField.CUSTOM_UTC_TIME)
    # Group the dataframe by segment_id
    grouped_df = df.groupby(XcalField.SEGMENT_ID)
    
    for segment_id, segment_data in grouped_df:
        # Reset index to make it easier to work with consecutive rows
        area_segments = []

        segment_data = segment_data.reset_index(drop=True)
        # Initialize variables for tracking area segments
        current_area = None
        segment_start_idx = 0
        # Iterate through the rows to find consecutive segments
        for idx in range(len(segment_data)):
            area_type = segment_data.iloc[idx][area_field]

            if current_area == area_type and idx != len(segment_data) - 1:
                continue

            if current_area is not None:
                # If it's the last row and same area type, include it
                end_idx = idx if current_area != area_type else idx + 1
                area_segment = segment_data.iloc[segment_start_idx:end_idx]
                area_segments.append(area_segment)
            
            # Start new segment
            current_area = area_type
            segment_start_idx = idx

        # for each area segment, plot a polyline
        for area_segment in area_segments:
            # Get coordinates for this area segment
            route_coordinates = area_segment[[XcalField.LAT, XcalField.LON]].values.tolist()
            
            # Only create line if we have at least 2 points
            if len(route_coordinates) >= 2:
                # Get color from area_config
                area_type = area_segment[area_field].iloc[0]
                color = area_config[area_type]['color']
                if color == 'red':
                    color = 'rgba(255, 0, 0, 0.5)'
                elif color == 'blue':
                    color = 'rgba(0, 0, 255, 0.5)'
                elif color == 'green':
                    color = 'rgba(0, 255, 0, 0.5)'

                # For tracking: A segment will be further split into segments with different area types
                area_segment_id = f'{area_segment[XcalField.SRC_IDX].iloc[0]}:{area_segment[XcalField.SRC_IDX].iloc[-1]}'
                # Create a PolyLine for this area segment
                folium.PolyLine(
                    route_coordinates,
                    color=color,
                    weight=5,
                    opacity=1,
                    popup=f"{area_segment_id} - {area_config[area_type]['label']}"
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

    # Add a legend
    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px;">
    <h4>Area Types ({operator})</h4>
    '''
    for area_type, config in area_config.items():
        legend_html += f'<p><span style="color:{config["color"]};">▬</span> {config["label"]}</p>'
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))

    # Create output directory if it doesn't exist
    output_dir = os.path.join(OUTPUT_DIR, 'area_types')
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the map
    m.save(output_file_path)
    print(f"Map saved as '{output_file_path}'")

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


    # dfs_alaska = {}
    # for operator in ["att", "verizon"]:
    #     df_xcal_data_in_alaska = pd.read_csv(os.path.join(ALASKA_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv"))
    #     dfs_alaska[operator] = df_xcal_data_in_alaska
    
    # for operator, df_xcal_data_in_alaska in dfs_alaska.items():
        # output_file_path = os.path.join(OUTPUT_DIR, 'alaska', f"{operator}_driving_route_with_tech.html")
        # plot_driving_route_with_tech(
        #     df_xcal_data_in_alaska, 
        #     center_coordinates=COORD_ANCHORAGE, 
        #     timezone=ALASKA_TIMEZONE, 
        #     operator=operator,
        #     output_file_path=output_file_path
        # )

        # Use our manually classified area types
        # output_file_path = os.path.join(OUTPUT_DIR, 'alaska', f"{operator}_driving_route_with_area_type.html")
        # plot_route_with_area_type(
        #     df_xcal_data_in_alaska, 
        #     area_field=XcalField.AREA,
        #     operator=operator,
        #     output_file_path=output_file_path
        # )

        # # Use the area_geojson field
        # output_file_path = os.path.join(OUTPUT_DIR, 'alaska', f"{operator}_driving_route_with_area_geojson.html")
        # plot_route_with_area_type(
        #     df_xcal_data_in_alaska, 
        #     area_field=XcalField.AREA_GEOJSON,
        #     operator=operator,
        #     output_file_path=output_file_path
        # )

    dfs_hawaii = {}
    # for operator in ["att", "verizon", "tmobile"]:
    for operator in ["att"]:
        df_xcal_data_in_hawaii = pd.read_csv(os.path.join(HAWAII_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv"))
        dfs_hawaii[operator] = df_xcal_data_in_hawaii

    for operator, df_xcal_data_in_hawaii in dfs_hawaii.items():
        # output_file_path = os.path.join(OUTPUT_DIR, 'hawaii', f"{operator}_driving_route_with_tech.html")
        # plot_driving_route_with_tech(
        #     df_xcal_data_in_hawaii, 
        #     center_coordinates=COORD_MAUI, 
        #     timezone=HAWAII_TIMEZONE, 
        #     operator=operator,
        #     output_file_path=output_file_path
        # )

        # Use our manually classified area types
        output_file_path = os.path.join(OUTPUT_DIR, 'hawaii', f"{operator}_driving_route_with_area_type.calibrated.html")
        plot_route_with_area_type(
            df_xcal_data_in_hawaii, 
            area_field=XcalField.AREA,
            operator=operator,
            output_file_path=output_file_path
        )

    #     # Use the area_geojson field
    #     output_file_path = os.path.join(OUTPUT_DIR, 'hawaii', f"{operator}_driving_route_with_area_geojson.html")
    #     plot_route_with_area_type(
    #         df_xcal_data_in_hawaii, 
    #         area_field=XcalField.AREA_GEOJSON,
    #         operator=operator,
    #         output_file_path=output_file_path
    #     )

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

