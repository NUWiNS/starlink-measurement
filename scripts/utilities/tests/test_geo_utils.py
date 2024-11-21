import unittest
import os
import sys
from typing import Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from scripts.utilities.geo_utils import ZoneClassifier
from scripts.logging_utils import SilentLogger

class TestZoneClassifier(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are reused for all tests."""
        geojson_path = os.path.join('datasets', 'others', 'hawaii_geo_zoning.geojson')
        cls.classifier = ZoneClassifier(
            geojson_path=geojson_path, 
            projected_crs='EPSG:32604', # Hawaii Maui UTM zone 4N
            logger=SilentLogger()
        )
        
    def assert_zone_classification(
            self, 
            location: Tuple[float, float, str], 
            expected_area_type: str,
            expected_zone_contains: str = None
        ):
        """Helper method to test zone classification for a given location."""
        lat, lon, description = location
        zone_type = self.classifier.get_zone_type(lat, lon)
        area_type = self.classifier.classify_area_type(zone_type)
        
        # Assert area classification
        self.assertEqual(
            area_type, 
            expected_area_type, 
            f"Location '{description}' was classified as {area_type}, expected {expected_area_type}"
        )
        
        # If specified, check if zone_type contains expected string
        if expected_zone_contains:
            self.assertIsNotNone(
                zone_type,
                f"Location '{description}' returned None zone type, expected containing '{expected_zone_contains}'"
            )
            self.assertIn(
                expected_zone_contains.lower(),
                zone_type.lower(),
                f"Location '{description}' zone type '{zone_type}' doesn't contain '{expected_zone_contains}'"
            )

    def test_urban_areas(self):
        """Test classification of known urban areas."""
        urban_test_points = [
            (20.8892, -156.4707, "Kahului Airport/Commercial"),  # Should be near airport/commercial
            (20.7560, -156.4566, "Kihei Business District"),    # Business area
            (20.8772, -156.4572, "Kahului Residential"),    # Residential area
            (20.7836, -156.4642, "Kihei Residential"),        # Residential area
            (20.8852, -156.4534, "Wailuku Residential"),    # Residential area
        ]
        
        for point in urban_test_points:
            self.assert_zone_classification(point, "urban")

    def test_suburban_areas(self):
        """Test classification of known suburban areas."""
        suburban_test_points = [
            (20.8817, -156.6821, "Lahaina Downtown"),       # Historic downtown
        ]
        
        for point in suburban_test_points:
            self.assert_zone_classification(point, "suburban", "residential")

    def test_rural_areas(self):
        """Test classification of known rural areas."""
        rural_test_points = [
            (20.7984, -156.3319, "Hana"),          # Remote town
            (20.9155, -156.2461, "Keanae"),        # Remote agricultural
            (20.6318, -156.3740, "Kaupo"),         # Very remote
        ]
        
        for point in rural_test_points:
            self.assert_zone_classification(point, "rural")

    def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        edge_cases = [
            # Point far out in ocean
            (-156.5000, 20.8000, "Ocean Point"),
            # Point at exact zone boundary (you'd need to find actual boundary coordinates)
            # Point exactly between two zones
        ]
        
        for lon, lat, description in edge_cases:
            zone_type = self.classifier.get_zone_type(lat, lon)
            area_type = self.classifier.classify_area_type(zone_type)
            # For now, just ensure it returns a valid classification without error
            self.assertIn(area_type, ["urban", "suburban", "rural"])

    def test_invalid_inputs(self):
        """Test handling of invalid inputs."""
        with self.assertRaises(Exception):
            self.classifier.get_zone_type("invalid", "coordinates")
        
        # Test classification of None zone_type
        self.assertEqual(
            self.classifier.classify_area_type(None),
            "rural",
            "None zone_type should be classified as rural"
        )

if __name__ == '__main__':
    unittest.main() 