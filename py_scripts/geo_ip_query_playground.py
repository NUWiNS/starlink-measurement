import os
import json
from typing import List, Dict, Optional
import time

import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from scripts.utilities.geo_ip_utils import IPStackQuery

# Configuration
CONFIG = {
    'api_key': 'ee430158ea106f8ce1298fe1da23414e',  # Replace with your IPStack API key
    'input_file': '',  # Path to input file containing IP addresses
    'output_file': os.path.join(current_dir, './outputs/geo_ips.ipstack.json'),  # Path to save results
    'rate_limit_delay': 1,  # Delay between requests in seconds (for free plan rate limiting)
}

def load_ip_list(file_path: str) -> List[str]:
    """Load IP addresses from a file (one IP per line)"""
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def save_results(results: List[Dict], output_file: str):
    """Save lookup results to a JSON file"""
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

def process_ip_list(ip_list: List[str], api_key: str, rate_limit_delay: float = 1.0) -> List[Dict]:
    """Process IP list one by one using IPStack queries"""
    query = IPStackQuery(api_key)
    all_results = []
    
    total_ips = len(ip_list)
    for i, ip in enumerate(ip_list, 1):
        print(f"Processing IP {i}/{total_ips}: {ip}")
        
        location = query.get_location(ip)
        if location:
            # Add IP to the result since get_location doesn't include it
            result = {
                'ip': ip,
                **location
            }
            all_results.append(result)
        else:
            print(f"Failed to get location for IP: {ip}")
        
        # Respect rate limits for free plan
        if i < total_ips:  # Don't delay after the last request
            time.sleep(rate_limit_delay)
    
    return all_results

def main():
    try:
        # ip_list = load_ip_list(CONFIG['input_file'])
        ip_list = [
            "206.224.64.32", "206.224.64.200", "206.224.64.46", "206.224.64.20", "206.224.65.148", 
            "206.224.65.150", "206.224.65.146", "206.224.65.144", "206.224.64.34", "206.224.64.54", 
            "206.224.64.22", "206.224.64.202", "206.224.66.149", "206.224.66.157", "206.224.66.145", 
            "206.224.66.155", "206.224.66.150", "206.224.66.147", "206.224.66.143", "206.224.66.152"
        ]
        print(f"Loaded {len(ip_list)} IP addresses")
    except FileNotFoundError:
        print(f"Error: Input file '{CONFIG['input_file']}' not found")
        return
    except Exception as e:
        print(f"Error loading IP list: {e}")
        return
    
    # Process IPs
    try:
        results = process_ip_list(
            ip_list, 
            CONFIG['api_key'], 
            CONFIG['rate_limit_delay']
        )
        
        if results:
            # Save results
            save_results(results, CONFIG['output_file'])
            print(f"\nProcessed {len(results)} IPs successfully")
            print(f"Results saved to: {CONFIG['output_file']}")
        else:
            print("No results were obtained")
            
    except Exception as e:
        print(f"Error during processing: {e}")

if __name__ == "__main__":
    main()
