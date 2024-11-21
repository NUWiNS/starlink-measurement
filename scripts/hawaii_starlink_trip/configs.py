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
