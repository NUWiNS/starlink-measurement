import glob
from typing import List, Tuple
import pandas as pd
import folium
import os

import sys


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(CURRENT_DIR, "../.."))
OUTPUT_DIR = os.path.join(CURRENT_DIR, "outputs")

from scripts.alaska_starlink_trip.configs import (
    ROOT_DIR as ALASKA_ROOT_DIR,
    TIMEZONE as ALASKA_TIMEZONE,
)
from scripts.hawaii_starlink_trip.configs import (
    ROOT_DIR as HAWAII_ROOT_DIR,
    TIMEZONE as HAWAII_TIMEZONE,
)
from scripts.constants import DATASET_DIR, CommonField, XcalField
from scripts.celllular_analysis.configs import tech_config_map
from scripts.alaska_starlink_trip.configs import unknown_area_coords as unknown_area_coords_alaska
from scripts.hawaii_starlink_trip.configs import unknown_area_coords as unknown_area_coords_hawaii

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
    (64.9029, -147.6898, "Fort Wainwright", "urban"),
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
    (20.8871, -156.3345, "Kuau", "suburban"),
]

unknown_spot_that_might_be_5g = {
    "alaska": unknown_area_coords_alaska,
    "hawaii": unknown_area_coords_hawaii,
}


def plot_driving_route(
    df: pd.DataFrame, center_coordinates: List[float], output_file_path: str
):
    # Create a map centered on Alaska
    m = folium.Map(location=center_coordinates, zoom_start=6)

    # Sort data by time and group by run_id
    df = df.sort_values(by="timestamp")
    grouped_df = df.groupby("run_id")

    # Create a different colored line for each run
    for run_id, run_data in grouped_df:
        # Get coordinates for this run
        route_coordinates = run_data[["latitude", "longitude"]].values.tolist()

        # Only create line if we have at least 2 points
        if len(route_coordinates) >= 2:
            # Create a PolyLine for this run
            folium.PolyLine(
                route_coordinates,
                color="red",
                weight=2.5,
                opacity=1,
                popup=f"Run {run_id}",
            ).add_to(m)

    # Add markers only for overall start and end points
    overall_start = df.iloc[0][["latitude", "longitude"]].tolist()
    overall_end = df.iloc[-1][["latitude", "longitude"]].tolist()

    folium.Marker(
        overall_start, popup="Start", icon=folium.Icon(color="green", icon="play")
    ).add_to(m)

    folium.Marker(
        overall_end, popup="End", icon=folium.Icon(color="red", icon="stop")
    ).add_to(m)

    # Save the map
    m.save(output_file_path)

    print(f"Map saved as '{output_file_path}'")


def plot_driving_route_with_tech(
    df: pd.DataFrame,
    center_coordinates: List[float],
    output_file_path: str,
    timezone: str = "UTC",
    operator: str = None,
    hidden_area_coords: List[dict] = None,
):
    # Create a map centered on the given coordinates with grayscale tiles
    # Using CartoDB's light tiles which provide a light, neutral background
    grayscale_tiles = (
        "https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"
    )
    attr = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

    m = folium.Map(
        location=center_coordinates,
        zoom_start=6,
        tiles=grayscale_tiles,
        attr=attr,
        prefer_canvas=True,
    )

    # Alternative tile providers if needed:
    # Stamen Toner Light:
    # tiles='https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}.png'
    # attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>'

    # sort by time
    df = df.sort_values(by=CommonField.LOCAL_DT)
    # Group the dataframe by segment_id
    grouped_df = df.groupby(XcalField.SEGMENT_ID)
    for segment_id, segment_data in grouped_df:
        # For each technology in the segment
        grouped_by_tech = segment_data.groupby(XcalField.ACTUAL_TECH)
        for tech, tech_segment in grouped_by_tech:
            # FIXME: only show Unknown tech for now
            # if tech != "Unknown":
            #     continue

            # Get coordinates for this tech segment
            route_coordinates = tech_segment[
                [XcalField.LAT, XcalField.LON]
            ].values.tolist()

            # if hidden_area_coords is not None:
            #     before_masking_coords_num = len(route_coordinates)
            #     filtered_coords = []
            #     for coord in route_coordinates:
            #         lat = float(coord[0])
            #         lon = float(coord[1])
            #         should_hide = False
            #         for hidden_area_coord in hidden_area_coords:
            #             lat_range = hidden_area_coord['lat_range']
            #             lon_range = hidden_area_coord['lon_range']
            #             if lat_range[0] <= lat <= lat_range[1] and lon_range[0] <= lon <= lon_range[1]:
            #                 should_hide = True
            #                 break
            #         if not should_hide:
            #             filtered_coords.append(coord)
            #     route_coordinates = filtered_coords
            #     after_masking_coords_num = len(route_coordinates)
            #     if after_masking_coords_num < before_masking_coords_num:  
            #         print(
            #             f"After masking, coords num: {after_masking_coords_num}, diff: {before_masking_coords_num - after_masking_coords_num}",
            #         )

            # Only create line if we have at least 2 points
            if len(route_coordinates) >= 2:
                # Get color from tech_config_map, default to NO SERVICE color if tech not found
                color = tech_config_map.get(tech, tech_config_map["Unknown"])["color"]
                # run_id = segment_data[XcalField.RUN_ID].iloc[0]
                # local_time = (
                #     pd.to_datetime(run_id)
                #     .tz_localize("UTC")
                #     .tz_convert(timezone)
                #     .strftime("%Y-%m-%d %H:%M:%S %Z")
                # )
                start_coord = route_coordinates[0]
                end_coord = route_coordinates[-1]
                # Create a PolyLine for this tech segment
                folium.PolyLine(
                    route_coordinates,
                    color=color,
                    weight=5,
                    opacity=1,
                    # popup=f"[{local_time}] Technology: {tech}",
                    # popup=f"[{local_time}] start: {start_coord}, end: {end_coord}",
                    popup=f"[{segment_id}] start: {start_coord}, end: {end_coord}",
                ).add_to(m)

    # Plot hidden areas as boxes
    if hidden_area_coords is not None:
        for hidden_area_coord in hidden_area_coords:
            # Skip if we don't have at least 2 points to form a line
            if len(hidden_area_coord['corners']) < 2:
                continue

            coords = hidden_area_coord['corners'] + [hidden_area_coord['corners'][0]]
            folium.PolyLine(
                coords,
                color='red',
                weight=2,
                opacity=0.8,
                popup=f"Tech: {hidden_area_coord['tech']}",
            ).add_to(m)

    # Add markers for overall start and end points
    start_point = df.iloc[0][[XcalField.LAT, XcalField.LON]].tolist()
    end_point = df.iloc[-1][[XcalField.LAT, XcalField.LON]].tolist()

    folium.Marker(
        start_point, popup="Start", icon=folium.Icon(color="green", icon="play")
    ).add_to(m)

    folium.Marker(
        end_point, popup="End", icon=folium.Icon(color="red", icon="stop")
    ).add_to(m)

    # Add a legend using the tech_config_map
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px;">
    # <h4>Technologies ({operator})</h4>
    """
    for tech, config in tech_config_map.items():
        legend_html += (
            f'<p><span style="color:{config["color"]};">▬</span> {config["label"]}</p>'
        )
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    # Save the map
    m.save(output_file_path)
    print(f"Map saved as '{output_file_path}'")


def plot_route_in_alaska_with_all_xcal_data(operator: str):
    alaska_xcal_files = glob.glob(os.path.join(DATASET_DIR, "xcal", "*_ALASKA_*.xlsx"))
    if len(alaska_xcal_files) == 0:
        raise ValueError("No xcal files found in the dataset directory")
    # merge the Lat and Lon cols among all hawaii xcal files
    df_alaska_gps_data_in_alaska = pd.DataFrame(
        {"latitude": [], "longitude": [], "timestamp": []}
    )
    for alaska_xcal_file in alaska_xcal_files:
        df = pd.read_excel(alaska_xcal_file)
        df = df[["Lat", "Lon"]].dropna()
        df = df.drop_duplicates(subset=["Lat", "Lon"])  # Remove duplicate lat/lon pairs

        # Filter out coordinates not related to Alaska
        # alaska_lat_range = (60.5, 62.5)  # Approximate latitude range for Alaska
        # alaska_lon_range = (-150.5, -148.5)  # Approximate longitude range for Alaska
        # df = df[(df["Lat"].between(*alaska_lat_range)) & (df["Lon"].between(*alaska_lon_range))]

        df_alaska_gps_data_in_alaska = pd.concat(
            [
                df_alaska_gps_data_in_alaska,
                pd.DataFrame(
                    {
                        "latitude": df[XcalField.LAT],
                        "longitude": df[XcalField.LON],
                        "timestamp": df[XcalField.TIMESTAMP],
                    }
                ),
            ],
            ignore_index=True,
        )
        df_alaska_gps_data_in_alaska = df_alaska_gps_data_in_alaska.drop_duplicates(
            subset=["latitude", "longitude"]
        )  # Remove duplicate lat/lon pairs after concatenation
        # sort by time
        df_alaska_gps_data_in_alaska = df_alaska_gps_data_in_alaska.sort_values(
            by="timestamp"
        )

    output_alaska_driving_route_file_path = os.path.join(
        OUTPUT_DIR, f"{operator}_alaska_driving_route_with_all_xcal_data.html"
    )
    plot_driving_route(
        df_alaska_gps_data_in_alaska,
        center_coordinates=COORD_ANCHORAGE,  # Anchorage coordinates
        output_file_path=output_alaska_driving_route_file_path,
    )


def plot_route_in_hawaii_with_all_xcal_data():
    hawaii_xcal_files = glob.glob(os.path.join(DATASET_DIR, "xcal", "*_HAWAII_*.xlsx"))
    if len(hawaii_xcal_files) == 0:
        raise ValueError("No xcal files found in the dataset directory")
    # merge the Lat and Lon cols among all hawaii xcal files
    df_hawaii_gps_data_in_hawaii = pd.DataFrame(
        {"latitude": [], "longitude": [], "timestamp": [], "run_id": []}
    )
    for hawaii_xcal_file in hawaii_xcal_files:
        df = pd.read_excel(hawaii_xcal_file)
        df = df[["Lat", "Lon"]].dropna()
        df = df.drop_duplicates(subset=["Lat", "Lon"])  # Remove duplicate lat/lon pairs

        # Filter out coordinates not related to Hawaii
        hawaii_lat_range = (18.5, 22.5)  # Approximate latitude range for Hawaii
        hawaii_lon_range = (-160.5, -154.5)  # Approximate longitude range for Hawaii
        df = df[
            (df["Lat"].between(*hawaii_lat_range))
            & (df["Lon"].between(*hawaii_lon_range))
        ]

        df_hawaii_gps_data_in_hawaii = pd.concat(
            [
                df_hawaii_gps_data_in_hawaii,
                pd.DataFrame(
                    {
                        "latitude": df[XcalField.LAT],
                        "longitude": df[XcalField.LON],
                        "timestamp": df[XcalField.TIMESTAMP],
                        "run_id": df[XcalField.RUN_ID],
                    }
                ),
            ],
            ignore_index=True,
        )
        df_hawaii_gps_data_in_hawaii = df_hawaii_gps_data_in_hawaii.drop_duplicates(
            subset=["latitude", "longitude"]
        )  # Remove duplicate lat/lon pairs after concatenation
        # sort by time
        df_hawaii_gps_data_in_hawaii = df_hawaii_gps_data_in_hawaii.sort_values(
            by="timestamp"
        )

    output_hawaii_driving_route_file_path = os.path.join(
        OUTPUT_DIR, "hawaii_driving_route_with_all_xcal_data.html"
    )
    plot_driving_route(
        df_hawaii_gps_data_in_hawaii,
        center_coordinates=COORD_MAUI,  # Maui coordinates
        output_file_path=output_hawaii_driving_route_file_path,
    )


def plot_route_in_alaska_within_measurements(operator: str):
    df_xcal_data_in_alaska = pd.read_csv(
        os.path.join(ALASKA_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv")
    )
    df_gps_data_in_alaska = pd.DataFrame(
        {
            "latitude": df_xcal_data_in_alaska[XcalField.LAT],
            "longitude": df_xcal_data_in_alaska[XcalField.LON],
            "timestamp": df_xcal_data_in_alaska[XcalField.CUSTOM_UTC_TIME],
            "run_id": df_xcal_data_in_alaska[XcalField.RUN_ID],
        }
    )
    # sort by time
    df_gps_data_in_alaska = df_gps_data_in_alaska.sort_values(by="timestamp")
    output_alaska_driving_route_file_path = os.path.join(
        OUTPUT_DIR, f"alaska_driving_route.{operator}.html"
    )
    plot_driving_route(
        df_gps_data_in_alaska,
        center_coordinates=COORD_ANCHORAGE,  # Anchorage coordinates
        output_file_path=output_alaska_driving_route_file_path,
        landmarks=ALASKA_LANDMARKS,  # Add landmarks
    )


def plot_route_in_hawaii_within_measurements(operator: str):
    df_xcal_data_in_hawaii = pd.read_csv(
        os.path.join(HAWAII_ROOT_DIR, "xcal", f"{operator}_xcal_smart_tput.csv")
    )
    df_gps_data_in_hawaii = pd.DataFrame(
        {
            "latitude": df_xcal_data_in_hawaii[XcalField.LAT],
            "longitude": df_xcal_data_in_hawaii[XcalField.LON],
            "timestamp": df_xcal_data_in_hawaii[XcalField.CUSTOM_UTC_TIME],
            "run_id": df_xcal_data_in_hawaii[XcalField.RUN_ID],
        }
    )
    # sort by time
    df_gps_data_in_hawaii = df_gps_data_in_hawaii.sort_values(by="timestamp")
    output_hawaii_driving_route_file_path = os.path.join(
        OUTPUT_DIR, f"hawaii_driving_route.{operator}.html"
    )
    plot_driving_route(
        df_gps_data_in_hawaii,
        center_coordinates=COORD_MAUI,  # Maui coordinates
        output_file_path=output_hawaii_driving_route_file_path,
        landmarks=MAUI_LANDMARKS,  # Add landmarks
    )


def plot_route_with_area_type(
    df: pd.DataFrame,
    area_field: str,
    operator: str,
    output_file_path: str,
    landmarks: List[tuple] = None,
):
    # Create a map centered on the mean coordinates with grayscale tiles
    center_coordinates = [df[XcalField.LAT].mean(), df[XcalField.LON].mean()]
    grayscale_tiles = (
        "https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"
    )
    attr = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

    m = folium.Map(
        location=center_coordinates,
        zoom_start=6,
        tiles=grayscale_tiles,
        attr=attr,
        prefer_canvas=True,
    )

    # Define area type colors
    area_config = {
        "urban": {"color": "red", "label": "Urban"},  # Red
        "suburban": {"color": "blue", "label": "Suburban"},  # Blue
        "rural": {"color": "green", "label": "Rural"},  # Green
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
            route_coordinates = area_segment[
                [XcalField.LAT, XcalField.LON]
            ].values.tolist()

            # Only create line if we have at least 2 points
            if len(route_coordinates) >= 2:
                # Get color from area_config
                area_type = area_segment[area_field].iloc[0]
                color = area_config[area_type]["color"]
                if color == "red":
                    color = "rgba(255, 0, 0, 0.5)"
                elif color == "blue":
                    color = "rgba(0, 0, 255, 0.5)"
                elif color == "green":
                    color = "rgba(0, 255, 0, 0.5)"

                # For tracking: A segment will be further split into segments with different area types
                area_segment_id = f"{area_segment[XcalField.SRC_IDX].iloc[0]}:{area_segment[XcalField.SRC_IDX].iloc[-1]}"
                if color == "green":
                    color = "rgba(0,128,0,0.5)"
                elif color == "blue":
                    color = "rgba(0,0,255,0.5)"
                elif color == "red":
                    color = "rgba(255,0,0,0.8)"
                # Create a PolyLine for this area segment
                folium.PolyLine(
                    route_coordinates,
                    color=color,
                    weight=5,
                    opacity=1,
                    popup=f"{area_config[area_type]['label']}\nSEG_ID: {segment_id}\nAREA_SEG_ID: {area_segment_id}",
                ).add_to(m)

    # Add markers for overall start and end points
    start_point = df.iloc[0][[XcalField.LAT, XcalField.LON]].tolist()
    end_point = df.iloc[-1][[XcalField.LAT, XcalField.LON]].tolist()

    folium.Marker(
        start_point, popup="Start", icon=folium.Icon(color="green", icon="play")
    ).add_to(m)

    folium.Marker(
        end_point, popup="End", icon=folium.Icon(color="red", icon="stop")
    ).add_to(m)

    # Add landmarks if provided
    if landmarks:
        for lat, lon, label, area_type in landmarks:
            # Get the color from area_config
            color = area_config[area_type]["color"].lstrip(
                "#"
            )  # Remove # from hex color
            folium.Marker(
                [lat, lon],
                popup=f"{label} ({area_config[area_type]['label']})",
                icon=folium.Icon(color=color, icon="info-sign"),
            ).add_to(m)

    # Add a legend
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px;">
    <h4>Area Types ({operator})</h4>
    """
    for area_type, config in area_config.items():
        legend_html += (
            f'<p><span style="color:{config["color"]};">▬</span> {config["label"]}</p>'
        )
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    # Create output directory if it doesn't exist
    output_dir = os.path.join(OUTPUT_DIR, "area_types")
    os.makedirs(output_dir, exist_ok=True)

    # Save the map
    m.save(output_file_path)
    print(f"Map saved as '{output_file_path}'")


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        os.makedirs(os.path.join(OUTPUT_DIR, "alaska"))
        os.makedirs(os.path.join(OUTPUT_DIR, "hawaii"))

    dfs_alaska = {}
    for operator in ["att", "verizon"]:
        df_xcal_data_in_alaska = pd.read_csv(
            os.path.join(
                ALASKA_ROOT_DIR,
                "xcal/sizhe_new_data",
                f"{operator}_xcal_smart_tput.csv",
            )
        )
        df_xcal_data_in_alaska['type'] = df_xcal_data_in_alaska[XcalField.APP_TPUT_PROTOCOL] + '_' + df_xcal_data_in_alaska[XcalField.APP_TPUT_DIRECTION]
        df_rtt = pd.read_csv(
            os.path.join(
                ALASKA_ROOT_DIR,
                "ping/sizhe_new_data",
                f"{operator}_ping.csv",
            )
        )
        df_rtt['type'] = 'rtt'

        tput_sub_df = df_xcal_data_in_alaska[[CommonField.LOCAL_DT, CommonField.AREA_TYPE, XcalField.SEGMENT_ID, XcalField.ACTUAL_TECH, XcalField.LON, XcalField.LAT, 'type']]
        rtt_sub_df = df_rtt[[CommonField.LOCAL_DT, CommonField.AREA_TYPE, XcalField.SEGMENT_ID, XcalField.ACTUAL_TECH, XcalField.LON, XcalField.LAT, 'type']]
        df = pd.concat(
                [tput_sub_df, rtt_sub_df],
                ignore_index=True
            )
 
        dfs_alaska[operator] = df

    for operator, df_xcal_data_in_alaska in dfs_alaska.items():
        output_file_path = os.path.join(
            OUTPUT_DIR, "alaska", f"{operator}_driving_route_with_tech.html"
        )
        hidden_area_coords = unknown_spot_that_might_be_5g["alaska"].get(operator, None)
        plot_driving_route_with_tech(
            df_xcal_data_in_alaska,
            center_coordinates=COORD_ANCHORAGE,
            timezone=ALASKA_TIMEZONE,
            operator=operator,
            hidden_area_coords=hidden_area_coords,
            output_file_path=output_file_path,
        )

        # Use our manually classified area types
        # output_file_path = os.path.join(OUTPUT_DIR, f"{operator}_driving_route_with_area_type.alaska.html")
        # plot_route_with_area_type(
        #     df_xcal_data_in_alaska,
        #     area_field=XcalField.AREA,
        #     operator=operator,
        #     output_file_path=output_file_path,
        #     landmarks=ALASKA_LANDMARKS  # Add landmarks
        # )

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
        df_xcal_data_in_hawaii = pd.read_csv(
            os.path.join(
                HAWAII_ROOT_DIR,
                "xcal/sizhe_new_data",
                f"{operator}_xcal_smart_tput.csv",
            )
        )
        df_xcal_data_in_hawaii['type'] = df_xcal_data_in_hawaii[XcalField.APP_TPUT_PROTOCOL] + '_' + df_xcal_data_in_hawaii[XcalField.APP_TPUT_DIRECTION]
        df_rtt = pd.read_csv(
            os.path.join(
                HAWAII_ROOT_DIR,
                "ping/sizhe_new_data",
                f"{operator}_ping.csv",
            )
        )
        df_rtt['type'] = 'rtt'

        tput_sub_df = df_xcal_data_in_hawaii[[CommonField.LOCAL_DT, CommonField.AREA_TYPE, XcalField.SEGMENT_ID, XcalField.ACTUAL_TECH, XcalField.LON, XcalField.LAT, 'type']]
        rtt_sub_df = df_rtt[[CommonField.LOCAL_DT, CommonField.AREA_TYPE, XcalField.SEGMENT_ID, XcalField.ACTUAL_TECH, XcalField.LON, XcalField.LAT, 'type']]
        df = pd.concat(
                [tput_sub_df, rtt_sub_df],
                ignore_index=True
            )
 
        dfs_alaska[operator] = df

    for operator, df_xcal_data_in_hawaii in dfs_hawaii.items():
        output_file_path = os.path.join(
            OUTPUT_DIR, "hawaii", f"{operator}_driving_route_with_tech.html"
        )
        hidden_area_coords = unknown_spot_that_might_be_5g["hawaii"].get(operator, None)
        plot_driving_route_with_tech(
            df_xcal_data_in_hawaii,
            center_coordinates=COORD_MAUI,
            timezone=HAWAII_TIMEZONE,
            operator=operator,
            hidden_area_coords=hidden_area_coords,
            output_file_path=output_file_path,
        )

        # Use our manually classified area types
        # output_file_path = os.path.join(OUTPUT_DIR, f"{operator}_driving_route_with_area_type.hawaii.html")
        # plot_route_with_area_type(
        #     df_xcal_data_in_hawaii,
        #     area_field=XcalField.AREA,
        #     operator=operator,
        #     output_file_path=output_file_path,
        #     landmarks=MAUI_LANDMARKS  # Add landmarks
        # )

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
