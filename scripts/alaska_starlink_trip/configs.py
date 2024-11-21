import enum
import os

from scripts.constants import DATASET_DIR, OUTPUT_DIR as OUTPUT_DIR_ROOT

DATASET_NAME = 'alaska_starlink_trip'
CRS_ALASKA_ANCHORAGE = 'EPSG:3338'

ROOT_DIR = os.path.join(DATASET_DIR, DATASET_NAME)
OUTPUT_DIR = os.path.join(OUTPUT_DIR_ROOT, DATASET_NAME)
TIMEZONE = 'US/Alaska'
