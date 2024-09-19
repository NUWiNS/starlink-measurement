import unittest
import requests
import json


class IpQuery:
    @staticmethod
    def batch_ip_lookup(ip_list):
        """Perform a batch IP lookup using ip-api.com
        :param ip_list: List of IP addresses to lookup
        :return: List of dictionaries containing the results

        Example result:
        {
            "status": "success",
            "country": "United States",
            "countryCode": "US",
            "region": "VA",
            "regionName": "Virginia",
            "city": "Ashburn",
            "zip": "20149",
            "lat": 39.03,
            "lon": -77.5,
            "timezone": "America/New_York",
            "isp": "Google LLC",
            "org": "Google Public DNS",
            "as": "AS15169 Google LLC",
            "query": "8.8.8.8"
        }
        """
        url = "http://ip-api.com/batch"

        # IP-API allows a maximum of 100 IP addresses per request for the free tier
        max_ips_per_request = 100

        results = []

        for i in range(0, len(ip_list), max_ips_per_request):
            batch = ip_list[i:i + max_ips_per_request]

            response = requests.post(url, json=batch)

            if response.status_code == 200:
                results.extend(response.json())
            else:
                if hasattr(response, 'json'):
                    results.extend(response.json())
                elif hasattr(response, 'text'):
                    results.append({"error": response.text})
                else:
                    results.append({"error": "Unknown error"})
        return results


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


if __name__ == '__main__':
    unittest.main()
