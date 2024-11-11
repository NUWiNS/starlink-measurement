import numpy as np
from typing import List
class DistanceUtils:
    @staticmethod
    def meter_to_mile(meters: float) -> float:
        return meters * 0.000621371
    
    @staticmethod
    def mile_to_meter(miles: float) -> float:
        return miles * 1609.34
        
    @staticmethod
    def calculate_cumulative_meters(lons: List[float], lats: List[float]) -> float:
        if len(lons) == 0 or len(lats) == 0:
            return 0
        # Constants for distance calculation
        total_distance_meters = 0
        for i in range(1, len(lats)):
            distance_meters = DistanceUtils.haversine_distance(lons[i-1], lats[i-1], lons[i], lats[i])
            total_distance_meters += distance_meters

        return total_distance_meters

    @staticmethod
    def calculate_cumulative_miles(lons: List[float], lats: List[float]) -> float:
        meters = DistanceUtils.calculate_cumulative_meters(lons, lats)
        return DistanceUtils.meter_to_mile(meters)

    @staticmethod
    def haversine_distance(lon1, lat1, lon2, lat2):
        """
        Calculate the distance in meters between two points on the Earth's surface using the Haversine formula.
        """
        R = 6371000  # Earth's radius in meters

        # detect precision based on decimal places in first valid lat/lon pair
        lat_str = str(lat1)
        lon_str = str(lon1)
        lat_decimals = len(lat_str.split('.')[-1]) if '.' in lat_str else 0
        lon_decimals = len(lon_str.split('.')[-1]) if '.' in lon_str else 0
        precision = max(lat_decimals, lon_decimals)

        # Convert to radians
        lat1 = np.radians(lat1)
        lon1 = np.radians(lon1)
        lat2 = np.radians(lat2)
        lon2 = np.radians(lon2)

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
        distance = R * c

        # Round based on precision of input coordinates
        # Higher precision (more decimals) means less rounding
        if precision >= 6:  # Very high precision (~0.1m)
            distance = round(distance, 1)
        elif precision >= 5:  # High precision (~1m)
            distance = round(distance)
        else:  # Lower precision
            distance = round(distance, -1)  # Round to nearest 10m

        return distance