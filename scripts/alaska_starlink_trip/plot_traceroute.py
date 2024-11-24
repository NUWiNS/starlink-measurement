import logging
import os
import sys
import json
import folium
from folium import plugins
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.logging_utils import create_logger
from scripts.constants import OUTPUT_DIR
from scripts.alaska_starlink_trip.configs import ROOT_DIR

base_dir = os.path.join(ROOT_DIR, "traceroute")
output_dir = os.path.join(OUTPUT_DIR, "alaska_starlink_trip/plots")

logger = create_logger(
    'traceroute_plotting',
    filename=os.path.join(output_dir, 'plot_traceroute.log'),
    filemode='w',
    formatter=logging.Formatter(),
    console_output=True
)

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

def print_unique_ip_by_hops():
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

def plot_hop_points_on_map():
    # Read data
    df = pd.read_csv(os.path.join(base_dir, 'starlink_traceroute.csv'))
    with open(os.path.join(base_dir, 'ip_info_map.json'), 'r') as f:
        ip_info = json.load(f)
    
    # Statistics tracking
    total_ips = set()
    private_ips = set()
    public_unmapped_ips = set()
    public_mapped_ips = set()

    grayscale_tiles = 'https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png'
    attr = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
    
    # Create base map centered around Alaska
    m = folium.Map(location=[64.2008, -149.4937], zoom_start=4, tiles=grayscale_tiles, attr=attr)
    
    # Create a marker cluster group
    marker_cluster = plugins.MarkerCluster(
        options={
            'spiderfyOnMaxZoom': True,
            'disableClusteringAtZoom': 8,
            'spiderLegPolylineOptions': {'weight': 1.5, 'color': '#222', 'opacity': 0.5},
            'maxClusterRadius': 30
        }
    ).add_to(m)
    
    # Find global max hop number for consistent color scaling
    global_max_hops = df['hop_number'].max()
    
    # Create a dictionary to store unique IP locations and their hop numbers
    ip_locations = {}  # {ip: {'lat': lat, 'lon': lon, 'hops': set(), 'data': ip_data}}
    
    # First pass: collect all unique IPs and their hop numbers
    for _, row in df.iterrows():
        if pd.isna(row['ip']):
            continue
            
        total_ips.add(row['ip'])
        
        if is_private_ip(row['ip']):
            private_ips.add(row['ip'])
            continue
            
        if row['ip'] not in ip_info:
            public_unmapped_ips.add(row['ip'])
            continue
            
        ip_data = ip_info[row['ip']]
        if ip_data.get('status') == 'success':
            public_mapped_ips.add(row['ip'])
            
            if row['ip'] not in ip_locations:
                ip_locations[row['ip']] = {
                    'lat': ip_data['lat'],
                    'lon': ip_data['lon'],
                    'hops': set(),
                    'data': ip_data
                }
            ip_locations[row['ip']]['hops'].add(row['hop_number'])
    
    # Create markers for unique IP locations
    for ip, location in ip_locations.items():
        # Get color based on average hop number
        avg_hop = sum(location['hops']) / len(location['hops'])
        color = get_color_gradient(avg_hop, global_max_hops)
        
        # Create popup text showing all hop numbers this IP appears in
        hop_list = sorted(location['hops'])
        popup_text = f"""
        IP: {ip}<br>
        Hops: {', '.join(map(str, hop_list))}<br>
        Location: {location['data']['city']}, {location['data']['regionName']}<br>
        ISP: {location['data']['isp']}
        """
        
        # Add marker
        folium.CircleMarker(
            location=[location['lat'], location['lon']],
            radius=8,
            color=color,
            fill=True,
            popup=popup_text,
            weight=2
        ).add_to(marker_cluster)
        
        # Add hop numbers as text
        folium.Popup(f"{min(hop_list)}-{max(hop_list)}", permanent=True).add_to(
            folium.CircleMarker(
                location=[location['lat'], location['lon']],
                radius=1,
                color='transparent',
                fill=False
            ).add_to(m)
        )
    
    # Second pass: draw lines between points for each traceroute
    for start_time, group in df.groupby('start_time'):
        path_coords = []
        path_metadata = []
        
        for _, row in group.iterrows():
            if pd.isna(row['ip']) or row['ip'] not in ip_locations:
                continue
                
            location = ip_locations[row['ip']]
            path_coords.append([location['lat'], location['lon']])
            path_metadata.append({
                'hop': row['hop_number'],
                'color': get_color_gradient(row['hop_number'], global_max_hops)
            })
        
        # Draw lines
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
                ).add_to(m)
    
    # Add colorbar legend
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
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Save map
    output_path = os.path.join(output_dir, 'traceroute_map.html')
    m.save(output_path)
    logger.info(f'\nSaved traceroute map to {output_path}')

def main():
    # print_unique_ip_by_hops()
    plot_hop_points_on_map()

if __name__ == '__main__':
    main()
