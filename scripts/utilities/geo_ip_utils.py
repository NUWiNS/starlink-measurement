from abc import ABC, abstractmethod
from typing import Optional, Dict, Tuple, Any
import os
import json
import requests
from urllib.parse import urljoin


class IGeoIPQuery(ABC):
    """Interface for GeoIP queries"""
    
    @abstractmethod
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
        pass

    @abstractmethod
    def get_coordinates(self, ip_address: str) -> Optional[Tuple[float, float]]:
        """Get latitude and longitude for an IP address.
        
        Args:
            ip_address: IPv4 or IPv6 address string
            
        Returns:
            Tuple of (latitude, longitude) or None if lookup fails
        """
        pass

    @abstractmethod
    def get_country(self, ip_address: str) -> Optional[str]:
        """Get country name for an IP address.
        
        Args:
            ip_address: IPv4 or IPv6 address string
            
        Returns:
            Country name string or None if lookup fails
        """
        pass


class MaxMindGeoIPQuery(IGeoIPQuery):
    """GeoIP implementation using MaxMind's GeoLite2 database"""
    
    def __init__(self, db_path: str):
        """Initialize GeoIP utility with path to MaxMind GeoLite2 database.
        
        Args:
            db_path: Path to GeoLite2-City.mmdb file
        """
        import geoip2.database
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"GeoLite2 database not found at: {db_path}")
            
        self.reader = geoip2.database.Reader(db_path)

    def get_location(self, ip_address: str) -> Optional[Dict]:
        try:
            response = self.reader.city(ip_address)
            isp = self.reader.isp(ip_address)
            asn = self.reader.asn(ip_address)
            res = {
                'country': response.country.name,
                'city': response.city.name,
                'latitude': response.location.latitude,
                'longitude': response.location.longitude,
                'accuracy_radius': response.location.accuracy_radius,
                'timezone': response.location.time_zone,
                'asn': asn.autonomous_system_number,
                'isp': isp.isp
            }
            return res
        except Exception as e:
            print(f"Error getting location for {ip_address}: {e}")
            return None

    def get_coordinates(self, ip_address: str) -> Optional[Tuple[float, float]]:
        location = self.get_location(ip_address)
        if location:
            return location['latitude'], location['longitude']
        return None

    def get_country(self, ip_address: str) -> Optional[str]:
        location = self.get_location(ip_address)
        if location:
            return location['country']
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the database reader."""
        self.reader.close()


class JsonGeoIPQuery(IGeoIPQuery):
    """GeoIP implementation using a JSON mapping file"""
    
    def __init__(self, json_path: str):
        """Initialize GeoIP utility with path to JSON mapping file.
        
        Args:
            json_path: Path to JSON file containing IP to location mappings
        """
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"JSON mapping file not found at: {json_path}")
            
        with open(json_path, 'r') as f:
            self.ip_map = json.load(f)

    def get_location(self, ip_address: str) -> Optional[Dict]:
        if ip_address not in self.ip_map:
            return None
            
        ip_info = self.ip_map[ip_address]
        if ip_info.get('status') != 'success':
            return None
            
        return {
            'country': ip_info.get('country'),
            'city': ip_info.get('city'),
            'latitude': ip_info.get('lat'),
            'longitude': ip_info.get('lon'),
            'accuracy_radius': None,  # Not provided in JSON format
            'timezone': ip_info.get('timezone')
        }

    def get_coordinates(self, ip_address: str) -> Optional[Tuple[float, float]]:
        if ip_address not in self.ip_map:
            return None
            
        ip_info = self.ip_map[ip_address]
        if ip_info.get('status') != 'success':
            return None
            
        return ip_info.get('lat'), ip_info.get('lon')

    def get_country(self, ip_address: str) -> Optional[str]:
        if ip_address not in self.ip_map:
            return None
            
        ip_info = self.ip_map[ip_address]
        if ip_info.get('status') != 'success':
            return None
            
        return ip_info.get('country')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # No cleanup needed for JSON implementation


class IPAPIQuery(IGeoIPQuery):
    """GeoIP implementation using ip-api.com service"""
    
    def __init__(self):
        """Initialize IP-API query service"""
        self.base_url = "http://ip-api.com/json/"
        
    def _query_api(self, ip_address: str) -> Optional[Dict[str, Any]]:
        """Make API request and return raw response"""
        try:
            response = requests.get(urljoin(self.base_url, ip_address))
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "success":
                return None
                
            return data
        except (requests.RequestException, ValueError) as e:
            print(f"Error querying IP-API for {ip_address}: {e}")
            return None

    def get_location(self, ip_address: str) -> Optional[Dict]:
        data = self._query_api(ip_address)
        if not data:
            return None
            
        return {
            'country': data.get('country'),
            'city': data.get('city'),
            'latitude': data.get('lat'),
            'longitude': data.get('lon'),
            'accuracy_radius': None,  # IP-API doesn't provide this
            'timezone': data.get('timezone'),
            'asn': data.get('as'),
            'isp': data.get('isp')
        }

    def get_coordinates(self, ip_address: str) -> Optional[Tuple[float, float]]:
        data = self._query_api(ip_address)
        if not data:
            return None
        
        lat = data.get('lat')
        lon = data.get('lon')
        if lat is not None and lon is not None:
            return (lat, lon)
        return None

    def get_country(self, ip_address: str) -> Optional[str]:
        data = self._query_api(ip_address)
        if not data:
            return None
        
        return data.get('country')


class IPStackQuery(IGeoIPQuery):
    """GeoIP implementation using ipstack.com service"""
    
    def __init__(self, api_key: str):
        """Initialize IPStack query service
        
        Args:
            api_key: Your IPStack API key
        """
        if not api_key:
            raise ValueError("IPStack API key is required")
            
        self.base_url = "http://api.ipstack.com/"
        self.api_key = api_key
        self.bulk_url = urljoin(self.base_url, "bulk")
        self.MAX_BULK_IPS = 50  # IPStack limit
        
    def _query_api(self, ip_address: str) -> Optional[Dict[str, Any]]:
        """Make API request and return raw response"""
        try:
            params = {'access_key': self.api_key}
            response = requests.get(
                urljoin(self.base_url, ip_address),
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get('success') is False:
                print(f"IPStack error: {data.get('error', {}).get('info')}")
                return None
                
            return data
        except (requests.RequestException, ValueError) as e:
            print(f"Error querying IPStack for {ip_address}: {e}")
            return None

    def get_location(self, ip_address: str) -> Optional[Dict]:
        data = self._query_api(ip_address)
        if not data:
            return None
            
        return {
            'country': data.get('country_name'),
            'city': data.get('city'),
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            'accuracy_radius': None,  # IPStack doesn't provide this
            'timezone': data.get('time_zone', {}).get('id'),
            'asn': None,  # Only available in premium plans
            'isp': None   # Only available in premium plans
        }

    def get_coordinates(self, ip_address: str) -> Optional[Tuple[float, float]]:
        data = self._query_api(ip_address)
        if not data:
            return None
        
        lat = data.get('latitude')
        lon = data.get('longitude')
        if lat is not None and lon is not None:
            return (lat, lon)
        return None

    def get_country(self, ip_address: str) -> Optional[str]:
        data = self._query_api(ip_address)
        if not data:
            return None
        
        return data.get('country_name')

    def bulk_lookup(self, ip_addresses: list[str]) -> Optional[list[Dict]]:
        """Perform bulk IP lookup (up to 50 IPs per request).
        
        Args:
            ip_addresses: List of IPv4 or IPv6 addresses to look up
                        Maximum 50 IPs per request
            
        Returns:
            List of location dictionaries, one per IP address, or None if lookup fails.
            Each dictionary contains:
            {
                'ip': str,
                'country': str,
                'city': str,
                'latitude': float,
                'longitude': float,
                'timezone': str,
                'asn': Optional[int],  # Premium only
                'isp': Optional[str]   # Premium only
            }
            
        Raises:
            ValueError: If more than 50 IPs are provided
        """
        if len(ip_addresses) > self.MAX_BULK_IPS:
            raise ValueError(f"Maximum {self.MAX_BULK_IPS} IPs allowed per bulk request")
            
        try:
            # Join IPs with commas for the bulk endpoint
            ip_list = ','.join(ip_addresses)
            
            params = {
                'access_key': self.api_key,
                # Optional parameters can be added here:
                # 'hostname': 1,  # Enable hostname lookups
                # 'security': 1,  # Enable security data (Professional Plus plan)
                # 'language': 'en',  # Response language
                # 'fields': 'ip,country_name,city,latitude,longitude,timezone'  # Limit fields
            }
            
            response = requests.get(
                urljoin(self.base_url, ip_list),  # Use standard endpoint with comma-separated IPs
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and data.get('success') is False:
                error_info = data.get('error', {}).get('info', 'Unknown error')
                print(f"IPStack bulk lookup error: {error_info}")
                return None
                
            # For bulk requests, response will be a list
            if not isinstance(data, list):
                data = [data]  # Handle single IP response
                
            results = []
            for item in data:
                results.append({
                    'ip': item.get('ip'),
                    'country': item.get('country_name'),
                    'city': item.get('city'),
                    'latitude': item.get('latitude'),
                    'longitude': item.get('longitude'),
                    'timezone': item.get('time_zone', {}).get('id'),
                    'asn': None,  # Premium plan only
                    'isp': None   # Premium plan only
                })
                
            return results
            
        except (requests.RequestException, ValueError) as e:
            print(f"Error performing bulk lookup: {e}")
            return None


# For backward compatibility
GeoIpUtils = MaxMindGeoIPQuery
