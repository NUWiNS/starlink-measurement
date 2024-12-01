import unittest
from unittest.mock import Mock, patch
import os
from scripts.utilities.geo_ip_utils import GeoIpUtils

# Set this path to your actual GeoLite2-City.mmdb file location for real database testing
current_dir = os.path.dirname(os.path.abspath(__file__))
GEOLITE2_DB_PATH = os.path.join(current_dir, "../../../datasets/others/GeoLite2-City_20241129/GeoLite2-City.mmdb")

# Set this to True to run tests with real database instead of mocks
USE_REAL_DB = False
# USE_REAL_DB = False


class TestGeoIpUtils(unittest.TestCase):
    def setUp(self):
        # Create a mock response object that mimics geoip2's response structure
        self.mock_response = Mock()
        self.mock_response.country.name = "United States"
        self.mock_response.city.name = "Mountain View"
        self.mock_response.location.latitude = 37.4056
        self.mock_response.location.longitude = -122.0775
        self.mock_response.location.accuracy_radius = 1000
        self.mock_response.location.time_zone = "America/Los_Angeles"

        if USE_REAL_DB:
            self.geo_ip = GeoIpUtils(GEOLITE2_DB_PATH)

    def tearDown(self):
        if USE_REAL_DB and hasattr(self, 'geo_ip'):
            self.geo_ip.close()

    @unittest.skipIf(USE_REAL_DB, "Using real database")
    @patch('geoip2.database.Reader')
    @patch('os.path.exists')
    def test_init_with_db_path(self, mock_exists, mock_reader):
        mock_exists.return_value = True
        db_path = "/path/to/GeoLite2-City.mmdb"
        geo_ip = GeoIpUtils(db_path)
        mock_exists.assert_called_once_with(db_path)
        mock_reader.assert_called_once_with(db_path)

    @unittest.skipIf(USE_REAL_DB, "Using real database")
    @patch('os.path.exists')
    def test_init_db_not_found(self, mock_exists):
        mock_exists.return_value = False
        db_path = "/nonexistent/path"
        with self.assertRaises(FileNotFoundError):
            GeoIpUtils(db_path)

    def test_get_location_success(self):
        if USE_REAL_DB:
            # Test with real Google DNS IP
            location = self.geo_ip.get_location("8.8.8.8")
            self.assertIsNotNone(location)
            self.assertIn('country', location)
            self.assertIn('city', location)
            self.assertIn('latitude', location)
            self.assertIn('longitude', location)
        else:
            with patch('geoip2.database.Reader') as mock_reader:
                mock_reader_instance = Mock()
                mock_reader_instance.city.return_value = self.mock_response
                mock_reader.return_value = mock_reader_instance
                with patch('os.path.exists', return_value=True):
                    with GeoIpUtils("/mock/path") as geo_ip:
                        location = geo_ip.get_location("8.8.8.8")
                        expected_location = {
                            'country': "United States",
                            'city': "Mountain View",
                            'latitude': 37.4056,
                            'longitude': -122.0775,
                            'accuracy_radius': 1000,
                            'timezone': "America/Los_Angeles"
                        }
                        self.assertEqual(location, expected_location)

    def test_get_location_failure(self):
        if USE_REAL_DB:
            location = self.geo_ip.get_location("invalid.ip")
            self.assertIsNone(location)
        else:
            with patch('geoip2.database.Reader') as mock_reader:
                mock_reader_instance = Mock()
                mock_reader_instance.city.side_effect = Exception("IP not found")
                mock_reader.return_value = mock_reader_instance
                with patch('os.path.exists', return_value=True):
                    with GeoIpUtils("/mock/path") as geo_ip:
                        location = geo_ip.get_location("invalid.ip")
                        self.assertIsNone(location)

    def test_get_coordinates(self):
        if USE_REAL_DB:
            coordinates = self.geo_ip.get_coordinates("8.8.8.8")
            self.assertIsNotNone(coordinates)
            self.assertEqual(len(coordinates), 2)
            self.assertIsInstance(coordinates[0], float)
            self.assertIsInstance(coordinates[1], float)
        else:
            with patch('geoip2.database.Reader') as mock_reader:
                mock_reader_instance = Mock()
                mock_reader_instance.city.return_value = self.mock_response
                mock_reader.return_value = mock_reader_instance
                with patch('os.path.exists', return_value=True):
                    with GeoIpUtils("/mock/path") as geo_ip:
                        coordinates = geo_ip.get_coordinates("8.8.8.8")
                        self.assertEqual(coordinates, (37.4056, -122.0775))

    def test_get_country(self):
        if USE_REAL_DB:
            country = self.geo_ip.get_country("8.8.8.8")
            self.assertIsNotNone(country)
            self.assertIsInstance(country, str)
        else:
            with patch('geoip2.database.Reader') as mock_reader:
                mock_reader_instance = Mock()
                mock_reader_instance.city.return_value = self.mock_response
                mock_reader.return_value = mock_reader_instance
                with patch('os.path.exists', return_value=True):
                    with GeoIpUtils("/mock/path") as geo_ip:
                        country = geo_ip.get_country("8.8.8.8")
                        self.assertEqual(country, "United States")


if __name__ == '__main__':
    unittest.main()
