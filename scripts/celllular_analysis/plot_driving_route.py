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

ALASKA_LANDMARKS = [
    # South to North (following the road system)
    (60.1042, -149.4422, "Exit Glacier", "rural"),
    (60.4827, -149.8483, "Seward", "suburban"),
    (60.5366, -149.5469, "Moose Pass", "rural"),
    (60.6419, -149.3266, "Portage", "rural"),
    (60.8317, -149.0131, "Whittier", "rural"),
    (60.9433, -149.1717, "Girdwood", "rural"),
    (61.1043, -149.8397, "Potter Marsh", "suburban"),
    (61.1508, -149.9002, "Anchorage International Airport", "urban"),
    (61.1958, -149.9333, "Midtown Anchorage", "urban"),
    (61.2181, -149.9003, "Anchorage Downtown", "urban"),
    (61.2156, -149.8269, "Joint Base Elmendorf-Richardson", "urban"),
    (61.3707, -149.5347, "Eagle River", "suburban"),
    (61.4806, -149.2125, "Chugiak", "suburban"),
    (61.5147, -149.1394, "Palmer", "suburban"),
    (61.5814, -149.4394, "Big Lake", "rural"),
    (61.6152, -149.3565, "Wasilla", "suburban"),
    (61.7320, -148.9108, "Sutton", "rural"),
    (62.1097, -145.5547, "Glennallen", "rural"),
    (62.3569, -145.7483, "Gulkana", "rural"),
    (62.7076, -144.4517, "Chistochina", "rural"),
    (63.3367, -143.0207, "Tok", "rural"),
    (63.8857, -145.6522, "Delta Junction", "rural"),
    (64.5611, -147.1029, "Eielson AFB", "suburban"),
    (64.7459, -147.3538, "North Pole", "suburban"),
    (64.8401, -147.7200, "Fairbanks", "urban"),
    (64.8588, -147.8159, "University of Alaska Fairbanks", "urban"),
    (64.9029, -147.6898, "Fort Wainwright", "urban")
]

MAUI_LANDMARKS = [
    # Clockwise around the island
    (20.8893, -156.4729, "Lahaina", "urban"),
    (20.8911, -156.5070, "Kaanapali", "suburban"),
    (20.9155, -156.6947, "Kapalua", "suburban"),
    (20.8983, -156.6700, "Kapalua Airport", "rural"),
    (21.0114, -156.6207, "Honolua Bay", "rural"),
    (20.9169, -156.2464, "Haiku", "rural"),
    (20.9280, -156.3366, "Paia", "suburban"),
    (20.8987, -156.3069, "Spreckelsville", "suburban"),
    (20.8911, -156.4346, "Wailuku", "urban"),
    (20.7967, -156.3319, "Kahului", "urban"),
    (20.7050, -156.2990, "Haleakala Airport", "rural"),
    (20.7204, -156.1551, "Haleakala National Park", "rural"),
    (20.6307, -156.3398, "Keokea", "rural"),
    (20.7544, -156.4561, "Kihei", "suburban"),
    (20.6899, -156.4422, "Wailea", "suburban"),
    (20.6328, -156.4445, "Makena", "suburban"),
    (20.7083, -156.3769, "Pukalani", "suburban"),
    (20.8871, -156.3345, "Kuau", "suburban")
]

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
        output_file_path=output_alaska_driving_route_file_path,
        landmarks=ALASKA_LANDMARKS  # Add landmarks
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
        output_file_path=output_hawaii_driving_route_file_path,
        landmarks=MAUI_LANDMARKS  # Add landmarks
    )

def plot_route_with_area_type(
    df: pd.DataFrame, 
    area_field: str, 
    operator: str, 
    output_file_path: str,
    landmarks: List[tuple] = None
):
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
        # For each area type in the segment
        grouped_by_area = segment_data.groupby(area_field)
        for area_type, area_segment in grouped_by_area:
            # Get coordinates for this area segment
            route_coordinates = area_segment[[XcalField.LAT, XcalField.LON]].values.tolist()
            
            # Only create line if we have at least 2 points
            if len(route_coordinates) >= 2:
                # Get color from area_config
                color = area_config[area_type]['color']
                if color == 'green':
                    color = 'rgba(0,128,0,0.5)'
                elif color == 'blue':
                    color = 'rgba(0,0,255,0.5)'
                elif color == 'red':
                    color = 'rgba(255,0,0,0.8)'
                # Create a PolyLine for this area segment
                folium.PolyLine(
                    route_coordinates,
                    color=color,
                    weight=5,
                    opacity=1,
                    popup=f"{segment_id} - {area_config[area_type]['label']}"
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

    # Add landmarks if provided
    if landmarks:
        for lat, lon, label, area_type in landmarks:
            # Get the color from area_config
            color = area_config[area_type]['color'].lstrip('#')  # Remove # from hex color
            folium.Marker(
                [lat, lon],
                popup=f"{label} ({area_config[area_type]['label']})",
                icon=folium.Icon(color=color, icon='info-sign'),
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


    dfs_alaska = {}
    for operator in ["att", "verizon"]:
        df_xcal_data_in_alaska = pd.read_csv(os.path.join(ALASKA_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv"))
        dfs_alaska[operator] = df_xcal_data_in_alaska
    
    for operator, df_xcal_data_in_alaska in dfs_alaska.items():
        # output_file_path = os.path.join(OUTPUT_DIR, 'alaska', f"{operator}_driving_route_with_tech.html")
        # plot_driving_route_with_tech(
        #     df_xcal_data_in_alaska, 
        #     center_coordinates=COORD_ANCHORAGE, 
        #     timezone=ALASKA_TIMEZONE, 
        #     operator=operator,
        #     output_file_path=output_file_path
        # )

        # Use our manually classified area types
        output_file_path = os.path.join(OUTPUT_DIR, 'alaska', f"{operator}_driving_route_with_area_type.html")
        plot_route_with_area_type(
            df_xcal_data_in_alaska, 
            area_field=XcalField.AREA,
            operator=operator,
            output_file_path=output_file_path,
            landmarks=ALASKA_LANDMARKS  # Add landmarks
        )

        # Use the area_geojson field
        # output_file_path = os.path.join(OUTPUT_DIR, 'alaska', f"{operator}_driving_route_with_area_geojson.html")
        # plot_route_with_area_type(
        #     df_xcal_data_in_alaska, 
        #     area_field=XcalField.AREA_GEOJSON,
        #     operator=operator,
        #     output_file_path=output_file_path
        # )

    dfs_hawaii = {}
    for operator in ["att", "verizon", "tmobile"]:
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
        output_file_path = os.path.join(OUTPUT_DIR, 'hawaii', f"{operator}_driving_route_with_area_type.html")
        plot_route_with_area_type(
            df_xcal_data_in_hawaii, 
            area_field=XcalField.AREA,
            operator=operator,
            output_file_path=output_file_path,
            landmarks=MAUI_LANDMARKS  # Add landmarks
        )

        # Use the area_geojson field
        # output_file_path = os.path.join(OUTPUT_DIR, 'hawaii', f"{operator}_driving_route_with_area_geojson.html")
        # plot_route_with_area_type(
        #     df_xcal_data_in_hawaii, 
        #     area_field=XcalField.AREA_GEOJSON,
        #     operator=operator,
        #     output_file_path=output_file_path
        # )

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

