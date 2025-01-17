import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))  

from scripts.utilities.distance_utils import DistanceUtils

def print_distance_between_ip(from_ip, from_gps, to_ip, to_gps):
  distance_meters = DistanceUtils.haversine_distance(
    lat1=from_gps[0],
    lon1=from_gps[1],
    lat2=to_gps[0],
    lon2=to_gps[1],
  )
  distance_km = DistanceUtils.meter_to_km(distance_meters)
  distance_miles = DistanceUtils.meter_to_mile(distance_meters)
  print(f'Distance between {from_ip} ({from_gps[2]}) and {to_ip} ({to_gps[2]}): {distance_km:.2f} km ({distance_miles:.2f} miles)')
  speed_of_light_m_per_s = 299792458
  time_of_flight_ms = distance_meters / speed_of_light_m_per_s * 1000
  print(f'Time of flight: {time_of_flight_ms:.2f} ms')
  print(f'Round trip time: {time_of_flight_ms * 2:.2f} ms')

def main():
  oregon_ip = '50.112.93.113'
  oregon_gps = (45.8399, -119.7006, 'Oregon')

  alaska_pop_ip = '206.224.65.150'
  alaska_pop_gps = (47.6043, -122.3298, 'Seattle, WA')

  hawaii_pop_ip = '206.224.66.2'
  # hawaii_pop_gps = (33.9192, -118.4166, 'El Segundo, CA')
  hawaii_pop_gps = (33.9540, -118.4016, 'Los Angeles, CA')

  maui_ip = 'unknown'
  maui_gps = (20.7955, -156.3313, 'Maui, HI')

  anchorage_ip = 'unknown'
  anchorage_gps = (61.2181, -149.9003, 'Anchorage, AK')

  fairbanks_ip = 'unknown'
  fairbanks_gps = (64.8378, -147.7164, 'Fairbanks, AK')
  
  # Hawaii
  print_distance_between_ip(
    from_ip=maui_ip,
    from_gps=maui_gps,
    to_ip=hawaii_pop_ip,
    to_gps=hawaii_pop_gps
  )
  print_distance_between_ip(
    from_ip=hawaii_pop_ip,
    from_gps=hawaii_pop_gps,
    to_ip=oregon_ip,
    to_gps=oregon_gps
  )

  print('\n\n')

  
  # Alaska
  print_distance_between_ip(
    from_ip=anchorage_ip,
    from_gps=anchorage_gps,
    to_ip=alaska_pop_ip,
    to_gps=alaska_pop_gps
  )
  print_distance_between_ip(
    from_ip=fairbanks_ip,
    from_gps=fairbanks_gps,
    to_ip=alaska_pop_ip,
    to_gps=alaska_pop_gps
  )
  print_distance_between_ip(
    from_ip=alaska_pop_ip,
    from_gps=alaska_pop_gps,
    to_ip=oregon_ip,
    to_gps=oregon_gps
  )

if __name__ == '__main__':
  main()
