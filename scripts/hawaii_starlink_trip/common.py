import pandas as pd
import logging
from scripts.constants import XcalField
from scripts.hawaii_starlink_trip.configs import unknown_area_coords

def patch_actual_tech(df: pd.DataFrame, operator: str, logger: logging.Logger):
    """Patch the actual tech column by special logic"""
    # try to patch tech from map calibration
    coords = unknown_area_coords.get(operator, None)
    if coords is not None:
        logger.info(f"-- Patching tech from map calibration for {operator}...")
        # Map all unknown data points to LTE if they are not matching below
        unknown_rows = df[df[XcalField.ACTUAL_TECH].str.lower() == 'unknown']
        for coord in coords:
            lat_range = coord['lat_range']
            lon_range = coord['lon_range']
            tech = coord['tech']
                
            # Find rows within the coordinate box
            matching_rows = unknown_rows[
                (unknown_rows[XcalField.LAT] >= lat_range[0]) & 
                (unknown_rows[XcalField.LAT] <= lat_range[1]) &
                (unknown_rows[XcalField.LON] >= lon_range[0]) & 
                (unknown_rows[XcalField.LON] <= lon_range[1])
            ]
            
            # Update tech for matching rows
            if len(matching_rows) > 0:
                segment_id_unique = matching_rows[XcalField.SEGMENT_ID].unique()
                all_rows_in_segment_with_unknown_tech = df[df[XcalField.SEGMENT_ID].isin(segment_id_unique)]
                df.loc[all_rows_in_segment_with_unknown_tech.index, XcalField.ACTUAL_TECH] = tech
                # logger.info(f"Patched {len(matching_rows)} rows in {coord['label']} to {tech}")
            
                # Remove matched rows from unknown_rows to avoid double processing
                unknown_rows = unknown_rows.drop(matching_rows.index)
            
        # Set remaining unknown rows to LTE
        if len(unknown_rows) > 0:
            segment_id_unique = unknown_rows[XcalField.SEGMENT_ID].unique()
            # If we update the tech column, we need to update the whole related segment
            all_rows_in_segment_with_unknown_tech = df[df[XcalField.SEGMENT_ID].isin(segment_id_unique)]
            # NOTE: different from Alaska, remaining unknown rows in Hawaii stay UNKNOWN!
            df.loc[all_rows_in_segment_with_unknown_tech.index, XcalField.ACTUAL_TECH] = 'Unknown'

            logger.info(f"---- Patched reall_rows_in_segment_with_unknown_techm {len(all_rows_in_segment_with_unknown_tech)} unknown rows to LTE")

    return df