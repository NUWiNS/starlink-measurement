import enum
import os

from scripts.constants import DATASET_DIR, OUTPUT_DIR as OUTPUT_DIR_ROOT

DATASET_NAME = "alaska_starlink_trip"
CRS_ALASKA_ANCHORAGE = "EPSG:3338"

ROOT_DIR = os.path.join(DATASET_DIR, DATASET_NAME)
OUTPUT_DIR = os.path.join(OUTPUT_DIR_ROOT, DATASET_NAME)
TIMEZONE = "US/Alaska"

unknown_area_coords = {
    "att": [
        {
            "label": "North_Box",
            "corners": [
                (64.86, -147.82),  # NW
                (64.86, -147.65),  # NE
                (64.82, -147.65),  # SE
                (64.82, -147.82),  # SW
            ],
            "lat_range": (64.82, 64.86),
            "lon_range": (-147.82, -147.65),
            'tech': '5G-low'
        },
        {
            "label": "North_Box",
            "corners": [
                (65.0, -149.3),  # NW
                (65.0, -144.6),  # NE
                (63.5, -144.6),  # SE
                (63.5, -149.3),  # SW
            ],
            "lat_range": (63.5, 65.0),
            "lon_range": (-149.3, -144.6),
            'tech': 'Unknown'
        },
        {
            "label": "West_Middle_box",
            "corners": [
                (63.3, -149.3),  # NW
                (63.3, -149.0),  # NE
                (63.2, -149.0),  # SE
                (63.2, -149.3),  # SW
            ],
            "lat_range": (63.2, 63.3),
            "lon_range": (-149.3, -149.0),
            'tech': 'Unknown'
        },
        {
            "label": "Middle_box",
            "corners": [
                (62.72, -144.73),  # NW
                (62.72, -144.13),  # NE
                (62.54, -144.13),  # SE
                (62.54, -144.73),  # SW
            ],
            "lat_range": (62.54, 62.72),
            "lon_range": (-144.73, -144.13),
            'tech': 'Unknown'
        },
        {
            "label": "Middle_Box",
            "corners": [
                (62.43, -145.3),  # NW
                (62.43, -144.8),  # NE
                (62.32, -144.8),  # SE
                (62.32, -145.3),  # SW
            ],
            "lat_range": (62.32, 62.43),
            "lon_range": (-145.3, -144.8),
            'tech': 'Unknown'
        },
        {
            "label": "West_Box",
            "corners": [
                (61.5, -149.9),  # NW
                (61.5, -149.38),  # NE
                (61.03, -149.38),  # SE
                (61.03, -149.9),  # SW
            ],
            "lat_range": (61.03, 61.5),
            "lon_range": (-149.9, -149.38),
            'tech': '5G-low'
        },
        {
            "label": "West_Box",
            "corners": [
                (61.0, -149.65),  # NW
                (61.0, -148.8),  # NE
                (60.8, -148.8),  # SE
                (60.8, -149.65),  # SW
            ],
            "lat_range": (60.8, 61.1),
            "lon_range": (-149.65, -148.8),
            'tech': 'Unknown'
        },
    ]
}
