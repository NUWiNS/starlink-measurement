import unittest
from typing import List

import requests
import json
import ipaddress


class IpQuery:
    MAX_IP_PER_REQUEST = 100

    @staticmethod
    def batch_ip_lookup(ip_list) -> List[dict]:
        """Perform a batch IP lookup using ip-api.com
        :param ip_list: List of IP addresses to lookup
        :return: List of dictionaries containing the results

        Example result:
        {
          "status": "success",
          "continent": "North America",
          "continentCode": "NA",
          "country": "United States",
          "countryCode": "US",
          "region": "VA",
          "regionName": "Virginia",
          "city": "Ashburn",
          "district": "",
          "zip": "20149",
          "lat": 39.03,
          "lon": -77.5,
          "timezone": "America/New_York",
          "offset": -14400,
          "currency": "USD",
          "isp": "Google LLC",
          "org": "Google Public DNS",
          "as": "AS15169 Google LLC",
          "asname": "GOOGLE",
          "mobile": false,
          "proxy": false,
          "hosting": true,
          "query": "8.8.8.8"
        }
        """
        url = "http://ip-api.com/batch?fields=66842623"

        if len(ip_list) > IpQuery.MAX_IP_PER_REQUEST:
            raise ValueError(f"Number of IP addresses exceeds the maximum limit of {IpQuery.MAX_IP_PER_REQUEST}")

        response = requests.post(url, json=ip_list)
        if hasattr(response, 'json'):
            return response.json()
        elif hasattr(response, 'text'):
            raise Exception(response.text)
        else:
            raise Exception('Unknown error')

    @staticmethod
    def filter_public_ips(ip_list):
        public_ips = []
        for ip in ip_list:
            try:
                ip_obj = ipaddress.ip_address(ip)
                if not ip_obj.is_private:
                    public_ips.append(ip)
            except ValueError:
                # If the IP address is invalid, we'll just skip it
                print(f"Invalid IP address: {ip}")
        return public_ips


class MyTestCase(unittest.TestCase):
    def test_batch_ip_query(self):
        # Example usage
        ip_addresses = [
            "8.8.8.8",
            "24.48.0.1",
            "208.67.222.222",
            "2001:4860:4860::8888"
        ]
        results = IpQuery.batch_ip_lookup(ip_addresses)

        # Print the results
        for result in results:
            print(json.dumps(result, indent=2))

        self.assertEqual(4, len(results))  # add assertion here

    def test_filter_public_ips(self):
        # Example usage
        ip_addresses = [
            # private IPs
            "192.168.1.1",
            "172.16.250.24",
            # public IPs
            '99.83.118.220',
        ]
        public_ips = IpQuery.filter_public_ips(ip_addresses)
        self.assertEqual(1, len(public_ips))
        self.assertEqual('99.83.118.220', public_ips[0])

if __name__ == '__main__':
    unittest.main()
