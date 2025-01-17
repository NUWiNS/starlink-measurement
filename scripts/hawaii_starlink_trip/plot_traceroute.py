import logging
import os
import sys
import folium
import pandas as pd
import json
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.utilities.geo_ip_utils import IGeoIPQuery, JsonGeoIPQuery
from scripts.utilities.geo_ip_utils import MaxMindGeoIPQuery
from scripts.logging_utils import create_logger
from scripts.constants import DATASET_DIR, OUTPUT_DIR

base_dir = os.path.join(DATASET_DIR, "hawaii_starlink_trip/traceroute")
output_dir = os.path.join(OUTPUT_DIR, "hawaii_starlink_trip/plots")

logger = create_logger(
    'traceroute_plotting',
    filename=os.path.join(output_dir, 'plot_traceroute.log'),
    filemode='w',
    formatter=logging.Formatter(),
    console_output=True
)


def print_ips_by_hops():
    df = pd.read_csv(os.path.join(base_dir, 'starlink_traceroute.csv'))
    # find groups that have less than 3 hops
    df_groups = df.groupby('start_time')
    for start_time, group_df in df_groups:
        if group_df['hop_number'].max() < 3:
            logger.info(f'[CAUTION] group {start_time} has less than 3 hops')

    for hop in range(1, 8):
        hop_df = df[df['hop_number'] == hop]
        # exclude exceptional cases
        hop_df = hop_df[hop_df['exception'].isna()]

        unique_ip = hop_df["ip"].unique()
        results = []
        for ip in unique_ip:
            if pd.isna(ip):
                ip_df = hop_df[hop_df['ip'].isna()].copy()
            else:
                ip_df = hop_df[hop_df['ip'] == ip].copy()
            # get freq of this ip
            ip_freq = ip_df.shape[0]
            mean_rtt = pd.to_numeric(ip_df['rtt_ms'], errors='coerce').dropna()
            results.append(f'{ip} ({ip_freq}) - {mean_rtt.mean():.2f} ms')
        logger.info(f'hop {hop} count ({len(hop_df["ip"])}), unique: {results}')

def is_private_ip(ip):
    """Check if an IP address is private"""
    private_patterns = [
        '192.168.',
        '10.',
        '172.16.',
        '172.17.',
        '172.18.',
        '172.19.',
        '172.20.',
        '172.21.',
        '172.22.',
        '172.23.',
        '172.24.',
        '172.25.',
        '172.26.',
        '172.27.',
        '172.28.',
        '172.29.',
        '172.30.',
        '172.31.',
        '100.64.',  # Carrier-grade NAT
        '169.254.'  # Link-local
    ]
    return any(ip.startswith(pattern) for pattern in private_patterns)

def get_color_gradient(hop_number, max_hops):
    """Generate color with increasing intensity based on hop number"""
    # Use a colormap from matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    
    # Create colormap
    cmap = plt.cm.Blues
    # Normalize hop number to [0,1] range for color mapping
    norm = hop_number / max_hops
    # Get RGBA values and convert to hex
    rgba = cmap(norm)
    hex_color = mcolors.rgb2hex(rgba)
    return hex_color

def add_colorbar_to_map(m, max_hops):
    """Add a colorbar legend to the map"""
    # Create list of colors for the gradient
    colors = [get_color_gradient(i, max_hops) for i in range(max_hops + 1)]
    
    # Create linear color map
    colormap = folium.LinearColormap(
        colors=colors,
        vmin=0,
        vmax=max_hops,
        caption='Hop Number'  # Label for the colorbar
    )
    
    # Add to map
    colormap.add_to(m)

def plot_hop_points_on_map(geo_ip_query: IGeoIPQuery):
    """Plot hop points on map using provided GeoIP query interface
    
    Args:
        geo_ip_query: Implementation of IGeoIPQuery interface for IP geolocation
    """
    # Read data
    df = pd.read_csv(os.path.join(base_dir, 'starlink_traceroute.csv'))
    
    # Statistics tracking
    total_ips = set()
    private_ips = set()
    public_unmapped_ips = set()
    public_mapped_ips = set()
    ip_info_map = {}  # Store IP to location mapping

    grayscale_tiles = 'https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png'
    attr = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
    
    # Create base map centered around Hawaii
    m = folium.Map(location=[21.3069, -157.8583], zoom_start=4, tiles=grayscale_tiles, attr=attr)
    
    # Create feature groups
    all_points = folium.FeatureGroup(name="All Points")
    base_group = folium.FeatureGroup(name="Base Layer")
    path_groups = {}
    
    # Find global max hop number for consistent color scaling
    global_max_hops = df['hop_number'].max()
    
    # Create a dictionary to store unique IP locations and their hop numbers
    ip_locations = {}  # {ip: {'lat': lat, 'lon': lon, 'hops': set(), 'data': location_info}}
    
    # First pass: collect all unique IPs and their hop numbers
    for _, row in df.iterrows():
        if pd.isna(row['ip']):
            continue
            
        total_ips.add(row['ip'])
        ip = row['ip']
        
        if is_private_ip(ip):
            private_ips.add(ip)
            continue
            
        # Get and store location info for this IP
        location = geo_ip_query.get_location(ip)
        if location is None:
            public_unmapped_ips.add(ip)
            continue
            
        public_mapped_ips.add(ip)
        
        # Store the location info in our map
        if ip not in ip_info_map:
            ip_info_map[ip] = {
                'status': 'success',
                'country': location['country'],
                'city': location['city'],
                'lat': location['latitude'],
                'lon': location['longitude'],
                'timezone': location['timezone'],
                'accuracy_radius': location['accuracy_radius']
            }
        
        # Store location info for plotting
        if ip not in ip_locations:
            ip_locations[ip] = {
                'lat': location['latitude'],
                'lon': location['longitude'],
                'hops': set(),
                'data': location
            }
        ip_locations[ip]['hops'].add(row['hop_number'])
    
    # Create markers for unique IP locations
    for ip, location in ip_locations.items():
        avg_hop = sum(location['hops']) / len(location['hops'])
        color = get_color_gradient(avg_hop, global_max_hops)
        
        hop_list = sorted(location['hops'])
        popup_text = f"""
        IP: {ip}<br>
        Hops: {', '.join(map(str, hop_list))}<br>
        Location: {location['data']['city']}, {location['data']['country']}<br>
        Timezone: {location['data']['timezone']}
        """
        
        # Add marker to all_points group
        folium.CircleMarker(
            location=[location['lat'], location['lon']],
            radius=8,
            color=color,
            fill=True,
            popup=popup_text,
            weight=2
        ).add_to(all_points)
        
        # Add hop numbers as text to base_group
        folium.Popup(f"{min(hop_list)}-{max(hop_list)}", permanent=True).add_to(
            folium.CircleMarker(
                location=[location['lat'], location['lon']],
                radius=1,
                color='transparent',
                fill=False
            ).add_to(base_group)
        )
    
    # Add groups to map
    base_group.add_to(m)
    all_points.add_to(m)
    
    # Second pass: create separate groups for each traceroute path
    for start_time, group in df.groupby('start_time'):
        display_time = pd.to_datetime(start_time).strftime('%Y-%m-%d %H:%M:%S')
        group_name = f"Traceroute {display_time}"
        path_group = folium.FeatureGroup(name=group_name)
        
        path_coords = []
        path_metadata = []
        
        # Add markers for this traceroute
        for _, row in group.iterrows():
            if pd.isna(row['ip']) or row['ip'] not in ip_locations:
                continue
                
            location = ip_locations[row['ip']]
            path_coords.append([location['lat'], location['lon']])
            
            # Add marker specific to this traceroute
            folium.CircleMarker(
                location=[location['lat'], location['lon']],
                radius=8,
                color=get_color_gradient(row['hop_number'], global_max_hops),
                fill=True,
                popup=f"""
                Hop: {row['hop_number']}<br>
                IP: {row['ip']}<br>
                RTT: {row['rtt_ms']:.2f} ms<br>
                Location: {location['data']['city']}, {location['data']['country']}<br>
                Timezone: {location['data']['timezone']}
                """,
                weight=2
            ).add_to(path_group)
            
            path_metadata.append({
                'hop': row['hop_number'],
                'color': get_color_gradient(row['hop_number'], global_max_hops)
            })
        
        # Draw lines for this traceroute
        if len(path_coords) >= 2:
            for i in range(len(path_coords) - 1):
                points = [path_coords[i], path_coords[i + 1]]
                start_hop = path_metadata[i]['hop']
                end_hop = path_metadata[i + 1]['hop']
                
                avg_hop = (start_hop + end_hop) / 2
                line_color = get_color_gradient(avg_hop, global_max_hops)
                
                folium.PolyLine(
                    locations=points,
                    weight=3,
                    color=line_color,
                    opacity=0.8,
                    popup=f'Hop {start_hop} → {end_hop}'
                ).add_to(path_group)
        
        path_group.add_to(m)
    
    # Add layer control and colorbar
    folium.LayerControl(collapsed=False).add_to(m)
    add_colorbar_to_map(m, global_max_hops)
    
    # Log statistics
    logger.info(f"Total unique IPs found in traceroute: {len(total_ips)}")
    logger.info(f"Private IPs: {len(private_ips)}")
    if private_ips:
        logger.info("Private IPs found:")
        for ip in sorted(private_ips):
            logger.info(f"  - {ip}")
            
    logger.info(f"\nPublic IPs:")
    logger.info(f"Successfully mapped: {len(public_mapped_ips)}")
    logger.info(f"Not found in mapping: {len(public_unmapped_ips)}")
    if public_unmapped_ips:
        logger.info("Unmapped public IPs:")
        for ip in sorted(public_unmapped_ips):
            logger.info(f"  - {ip}")
    
    # Save both the map and the IP info
    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    
    # Save the map
    map_output_path = os.path.join(output_dir, f'traceroute_map_{timestamp}.html')
    m.save(map_output_path)
    logger.info(f'\nSaved traceroute map to {map_output_path}')
    
    # Save the IP info map
    ip_info_output_path = os.path.join(output_dir, f'traceroute_map_{timestamp}.ip.json')
    with open(ip_info_output_path, 'w') as f:
        json.dump(ip_info_map, f, indent=4)
    logger.info(f'Saved IP info map to {ip_info_output_path}')
    
    # Save unmapped IPs for future reference
    if public_unmapped_ips:
        unmapped_ips_path = os.path.join(output_dir, f'unmapped_ips_{timestamp}.txt')
        with open(unmapped_ips_path, 'w') as f:
            for ip in sorted(public_unmapped_ips):
                f.write(f"{ip}\n")
        logger.info(f'Saved list of unmapped IPs to {unmapped_ips_path}')
    
    # Print summary statistics
    logger.info("\nSummary Statistics:")
    logger.info(f"Total unique IPs: {len(total_ips)}")
    logger.info(f"Private IPs: {len(private_ips)}")
    logger.info(f"Public mapped IPs: {len(public_mapped_ips)}")
    logger.info(f"Public unmapped IPs: {len(public_unmapped_ips)}")
    
    return {
        'map_path': map_output_path,
        'ip_info_path': ip_info_output_path,
        'total_ips': len(total_ips),
        'mapped_ips': len(public_mapped_ips),
        'unmapped_ips': len(public_unmapped_ips),
        'private_ips': len(private_ips)
    }

def main():
    print_ips_by_hops()
    # mmdb_path = os.path.join(DATASET_DIR, 'others', 'GeoLite2-City_20241129/GeoLite2-City.mmdb')
    # with MaxMindGeoIPQuery(mmdb_path) as geo_ip:
    # json_path = os.path.join(base_dir, 'ip_info_map.json')
    # with JsonGeoIPQuery(json_path) as geo_ip:
    #     result = plot_hop_points_on_map(geo_ip)
        
    #     logger.info("\nOutput Files:")
    #     logger.info(f"Map: {result['map_path']}")
    #     logger.info(f"IP Info: {result['ip_info_path']}")
    #     logger.info(f"\nMapping Coverage: {result['mapped_ips']}/{result['total_ips']} IPs " +
    #                f"({result['mapped_ips']/result['total_ips']*100:.1f}%)")


if __name__ == '__main__':
    main()
