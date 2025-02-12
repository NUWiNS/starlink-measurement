import logging
from typing import Optional
import geopandas as gpd
from overrides import override
from shapely.geometry import Point

from scripts.logging_utils import SilentLogger

class ZoneClassifier:
    def __init__(self, geojson_path: str, projected_crs: str, logger: logging.Logger = None):
        """Initialize the zone classifier with GeoJSON data."""
        self.gdf = self._load_zoning_data(geojson_path)
        # Cache the projected version
        self.gdf_projected = self.gdf.to_crs(projected_crs)
        self.projected_crs = projected_crs
        self.logger = logger if logger is not None else SilentLogger()
        
    def _load_zoning_data(self, geojson_path: str) -> gpd.GeoDataFrame:
        """Load Hawaii zoning GeoJSON data into a GeoDataFrame."""
        try:
            gdf = gpd.read_file(geojson_path)
            # Ensure the CRS is set to WGS84 (standard lat/lon)
            if gdf.crs is None:
                gdf.set_crs(epsg=4326, inplace=True)
            elif gdf.crs != 'EPSG:4326':
                gdf = gdf.to_crs(epsg=4326)
            return gdf
        except Exception as e:
            raise Exception(f"Failed to load GeoJSON file: {str(e)}")
    
    def get_zone_type(self, lat: float, lon: float) -> Optional[str]:
        raise NotImplementedError("Subclasses must implement get_zone_type")
    
    def classify_area_type(self, zone_type: Optional[str]) -> str:
        """Classify zone type into urban/suburban/rural categories."""
        raise NotImplementedError("Subclasses must implement classify_area_type")


class HawaiiZoneClassifier(ZoneClassifier):
    """Zone classifier specific to Hawaii zoning data."""
    @override
    def get_zone_type(self, lat: float, lon: float) -> Optional[str]:
        """Query the zone type for a given latitude/longitude coordinate."""
        point = Point(lon, lat)
        
        # Try intersects first (using unprojected coordinates)
        containing_zones = self.gdf[self.gdf.geometry.intersects(point)]
        
        self.logger.debug(f"Found {len(containing_zones)} zones for point ({lat}, {lon})")
        
        if len(containing_zones) == 0:
            # Project point to UTM for accurate distance calculation
            point_projected = gpd.GeoSeries([point], crs='EPSG:4326').to_crs(self.projected_crs).iloc[0]
            
            # Find nearest zone using projected coordinates
            distances = self.gdf_projected.geometry.distance(point_projected)
            nearest_idx = distances.idxmin()
            nearest_zone = self.gdf.iloc[nearest_idx]
            min_distance = distances[nearest_idx]
            
            self.logger.debug(f"Nearest zone: {nearest_zone['zone_class']} (distance: {min_distance:.2f}m)")
            
            # Use 1000m as threshold
            if min_distance < 1000:  # 1km threshold
                self.logger.debug("Using nearest zone (within threshold)")
                return nearest_zone['zone_class']
            
            self.logger.debug("Distance exceeds threshold (1000m), returning None")
            return None
        elif len(containing_zones) > 1:
            self.logger.debug(f"Multiple zones found: {[z['zone_class'] for z in containing_zones.itertuples()]}")
            return containing_zones.iloc[0]['zone_class']
        else:
            self.logger.debug(f"Single zone found: {containing_zones.iloc[0]['zone_class']}")
            return containing_zones.iloc[0]['zone_class']

    @override
    def classify_area_type(self, zone_type: Optional[str]) -> str:
        """Classify Hawaii zone type into urban/suburban/rural categories."""
        if zone_type is None:
            return 'rural'
            
        # Convert to lowercase for case-insensitive matching
        zone_type = str(zone_type).lower()
        self.logger.debug(f"Classifying zone type: {zone_type}")
        
        # Define classification rules based on Maui zoning codes
        urban_keywords = [
            'commercial', 'business', 'industrial', 'mixed use', 'downtown', 
            'urban', 'city', 'center', 'cbd', 'hotel', 'apartment', 
            'b-1', 'b-2', 'b-3', 'business - central',  # specific business zones
            'm-1', 'm-2', 'm-3',  # industrial zones
            'airport', 'research & technology',  # infrastructure
            'service business',  # SBR zones
            'urban reserve',  # UR zones
            'p-1', 'p-2'  # public/quasi-public (civic centers)
        ]
        
        suburban_keywords = [
            'residential', 'medium density', 'low density', 'suburban', 
            'neighborhood', 'community', 'r-', 'd-', 'duplex',
            'multi family', 'historic district',
            'country town', 'wct',  # small town centers
            'business - neighborhood',  # B-CT zones
        ]
        
        rural_keywords = [
            'agriculture', 'conservation', 'open space', 'park', 'golf course',
            'interim', 'rural', 'open', 'drainage', 'beach right-of-way',
            'road', 'unzoned'
        ]
        
        for keyword in urban_keywords:
            if keyword in zone_type:
                return 'urban'
                
        for keyword in suburban_keywords:
            if keyword in zone_type:
                return 'suburban'
                
        for keyword in rural_keywords:
            if keyword in zone_type:
                return 'rural'
                
        return 'rural'


class AlaskaZoneClassifier(ZoneClassifier):
    """Zone classifier specific to Alaska zoning data."""

    @override
    def get_zone_type(self, lat: float, lon: float) -> Optional[str]:
        """Query the zone type for a given latitude/longitude coordinate."""
        point = Point(lon, lat)
        
        # Try intersects first (using unprojected coordinates)
        containing_zones = self.gdf[self.gdf.geometry.intersects(point)]
        
        self.logger.debug(f"Found {len(containing_zones)} zones for point ({lat}, {lon})")
        
        if len(containing_zones) == 0:
            # Project point to UTM for accurate distance calculation
            point_projected = gpd.GeoSeries([point], crs='EPSG:4326').to_crs(self.projected_crs).iloc[0]
            
            # Find nearest zone using projected coordinates
            distances = self.gdf_projected.geometry.distance(point_projected)
            nearest_idx = distances.idxmin()
            nearest_zone = self.gdf.iloc[nearest_idx]
            min_distance = distances[nearest_idx]
            
            self.logger.debug(f"Nearest zone: {nearest_zone['DistrictType']} - {nearest_zone['DistrictName']} (distance: {min_distance:.2f}m)")
            
            # Use 1000m as threshold
            if min_distance < 1000:  # 1km threshold
                self.logger.debug("Using nearest zone (within threshold)")
                return f"{nearest_zone['DistrictType']} - {nearest_zone['DistrictName']}"
            
            self.logger.debug("Distance exceeds threshold (1000m), returning None")
            return None
        elif len(containing_zones) > 1:
            zone = containing_zones.iloc[0]
            self.logger.debug(f"Multiple zones found, using first: {zone['DistrictType']} - {zone['DistrictName']}")
            return f"{zone['DistrictType']} - {zone['DistrictName']}"
        else:
            zone = containing_zones.iloc[0]
            self.logger.debug(f"Single zone found: {zone['DistrictType']} - {zone['DistrictName']}")
            return f"{zone['DistrictType']} - {zone['DistrictName']}"
    
    @override
    def classify_area_type(self, zone_type: Optional[str]) -> str:
        """Classify Alaska zone type into urban/suburban/rural categories."""
        if zone_type is None:
            return 'rural'
            
        # Convert to lowercase for case-insensitive matching
        zone_type = str(zone_type).lower()
        self.logger.debug(f"Classifying zone type: {zone_type}")
        
        # Urban areas
        urban_keywords = [
            'central business',
            'general business',
            'community business',
            'marine commercial',
            'marine industrial',
            'heavy industrial',
            'light industrial',
            'townsite square commercial',
            'new townsite south commercial',
            'mixed-use',
            'residential mixed-use'
        ]
        
        # Suburban areas
        suburban_keywords = [
            'residential office',
            'mixed residential',
            'two-family residential',
            'single-family residential',
            'multifamily residential',
            'multiple-family residential',
            'medium-density',
            'planned community development',
            'local and neighborhood business',
            'alyeska highway mixed residential',
            'residential development',
            'public lands and institutions'
        ]
        
        # Rural areas
        rural_keywords = [
            'rural',
            'watershed',
            'parks',
            'parks and recreation',
            'transition',
            'low-density residential',
            'antenna farm',
            'turnagain arm',
            'girdwood open space',
            'alpine/slope',
            '1 acre',
            '2 acres',
            '4 acres',
            '20k'
        ]
        
        # First check DistrictType
        if 'commercial' in zone_type or 'industrial' in zone_type:
            return 'urban'
            
        # Then check specific keywords
        for keyword in urban_keywords:
            if keyword in zone_type:
                return 'urban'
                
        for keyword in suburban_keywords:
            if keyword in zone_type:
                return 'suburban'
                
        for keyword in rural_keywords:
            if keyword in zone_type:
                return 'rural'
        
        # Default classifications based on DistrictType
        if 'residential' in zone_type:
            # Default residential to suburban unless specifically marked as rural
            return 'suburban'
        elif 'other' in zone_type:
            # 'Other' category typically includes parks, open spaces, etc.
            return 'rural'
            
        return 'rural'
