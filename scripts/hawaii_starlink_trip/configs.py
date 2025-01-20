import enum
import os

from scripts.constants import DATASET_DIR, OUTPUT_DIR as OUTPUT_DIR_ROOT

DATASET_NAME = 'hawaii_starlink_trip'

ROOT_DIR = os.path.join(DATASET_DIR, DATASET_NAME)
OUTPUT_DIR = os.path.join(OUTPUT_DIR_ROOT, DATASET_NAME)

TIMEZONE = 'Pacific/Honolulu'
CRS_HAWAII_MAUI = 'EPSG:32604'

class DatasetLabel(enum.Enum):
    NORMAL = 'NORMAL'
    TEST_DATA = 'TEST_DATA'

unknown_area_coords = {
    "att": [
        {
            "label": "East_Box",
            "corners": [
                (20.82, -156.13),  # NW
                (20.82, -155.97),  # NE
                (20.65, -155.97),  # SE
                (20.65, -156.13),  # SW
            ],
            "lat_range": (20.65, 20.82),
            "lon_range": (-156.13, -155.97),
            'tech': 'LTE'
        },
    ],
    "verizon": [
        {
            "label": "East_Box",
            "corners": [
                (20.83, -156.14),  # NW
                (20.83, -155.97),  # NE
                (20.65, -155.97),  # SE
                (20.65, -156.14),  # SW
            ],
            "lat_range": (20.65, 20.83),
            "lon_range": (-156.14, -155.97),
            'tech': 'LTE'
        },
    ],
    "tmobile": [
        {
            "label": "East_Box",
            "corners": [
                (20.83, -156.14),  # NW
                (20.83, -155.97),  # NE
                (20.65, -155.97),  # SE
                (20.65, -156.14),  # SW
            ],
            "lat_range": (20.65, 20.83),
            "lon_range": (-156.14, -155.97),
            'tech': 'LTE'
        },
    ],
}
