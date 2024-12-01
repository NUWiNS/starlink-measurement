import geoip2.database
from typing import Optional, Dict, Tuple
import os


class GeoIpUtils:
    def __init__(self, db_path: str = None):
        """Initialize GeoIP utility with path to MaxMind GeoLite2 database.
        
        Args:
            db_path: Path to GeoLite2-City.mmdb file. If None, will look for 
                    GEOLITE2_DB_PATH environment variable.
        """
        if db_path is None:
            db_path = os.getenv('GEOLITE2_DB_PATH')
            if db_path is None:
                raise ValueError("Database path not provided and GEOLITE2_DB_PATH env var not set")
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"GeoLite2 database not found at: {db_path}")
            
        self.reader = geoip2.database.Reader(db_path)

    def get_location(self, ip_address: str) -> Optional[Dict]:
        """Get location information for an IP address.
        
        Args:
            ip_address: IPv4 or IPv6 address string
            
        Returns:
            Dictionary containing location info or None if lookup fails:
            {
                'country': str,
                'city': str,
                'latitude': float,
                'longitude': float,
                'accuracy_radius': int,
                'timezone': str
            }
        """
        try:
            response = self.reader.city(ip_address)
            return {
                'country': response.country.name,
                'city': response.city.name,
                'latitude': response.location.latitude,
                'longitude': response.location.longitude,
                'accuracy_radius': response.location.accuracy_radius,
                'timezone': response.location.time_zone
            }
        except Exception:
            return None

    def get_coordinates(self, ip_address: str) -> Optional[Tuple[float, float]]:
        """Get latitude and longitude for an IP address.
        
        Args:
            ip_address: IPv4 or IPv6 address string
            
        Returns:
            Tuple of (latitude, longitude) or None if lookup fails
        """
        location = self.get_location(ip_address)
        if location:
            return location['latitude'], location['longitude']
        return None

    def get_country(self, ip_address: str) -> Optional[str]:
        """Get country name for an IP address.
        
        Args:
            ip_address: IPv4 or IPv6 address string
            
        Returns:
            Country name string or None if lookup fails
        """
        location = self.get_location(ip_address)
        if location:
            return location['country']
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reader.close()

    def close(self):
        """Close the database reader."""
        self.reader.close()
