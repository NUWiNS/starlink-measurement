import os
import glob
import pandas as pd
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.utilities.geo_utils import ZoneClassifier
from scripts.constants import DATASET_DIR, XcalField
from scripts.hawaii_starlink_trip.configs import CRS_HAWAII_MAUI, ROOT_DIR
from scripts.logging_utils import create_logger

logger = create_logger('append_area')

def append_area_based_on_geojson_to_xcal_tput_traces(geojson_path: str, xcal_tput_dir: str):
    """Append area classification based on GeoJSON zoning data to throughput traces.
    
    Args:
        xcal_tput_dir: Directory containing the xcal throughput CSV files
    """
    # Load GeoJSON zoning data
    classifier = ZoneClassifier(
        geojson_path=geojson_path,
        projected_crs=CRS_HAWAII_MAUI
    )
  
    # Find all throughput CSV files
    all_tput_csv_files = glob.glob(os.path.join(xcal_tput_dir, '*_xcal_smart_tput.csv'))
    
    for csv_file in all_tput_csv_files:
        operator = os.path.basename(csv_file).split('_')[0]
        logger.info(f"Processing {operator} throughput data")
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        total_points = len(df)
        
        # Initialize area_geojson column
        df['area_geojson'] = None
        
        # Process each point
        for idx, row in df.iterrows():
            if idx % 1000 == 0:
                logger.info(f"Processed {idx}/{total_points} points")
                
            # Get zone type and classify area
            zone_type = classifier.get_zone_type(row[XcalField.LAT], row[XcalField.LON])
            area_type = classifier.classify_area_type(zone_type)
            df.at[idx, 'area_geojson'] = area_type
        
        # Save updated CSV
        df.to_csv(csv_file, index=False)
        
        # Log area type statistics
        area_counts = df['area_geojson'].value_counts()
        logger.info(f"\nArea classification statistics for {operator}:")
        for area_type, count in area_counts.items():
            percentage = (count / total_points) * 100
            logger.info(f"{area_type}: {count} points ({percentage:.2f}%)")
        
        logger.info(f"Finished processing {operator}\n")

        # release df memory
        del df

def main():
    xcal_tput_dir = os.path.join(ROOT_DIR, 'xcal')
    geojson_path = os.path.join(DATASET_DIR, 'others', 'hawaii_geo_zoning.geojson')
    append_area_based_on_geojson_to_xcal_tput_traces(geojson_path, xcal_tput_dir)

if __name__ == "__main__":
    main() 