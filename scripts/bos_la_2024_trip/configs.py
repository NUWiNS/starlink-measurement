import enum
import os

from scripts.constants import DATASET_DIR

DATASET_NAME = 'bos_la_2024_trip'

ROOT_DIR = os.path.join(DATASET_DIR, DATASET_NAME)

TIMEZONE_US_WEST = 'America/Los_Angeles'
TIMEZONE_US_MOUNTAIN = 'America/Denver'
TIMEZONE_US_CENTRAL = 'America/Chicago' 
TIMEZONE_US_EAST = 'America/New_York'

class DatasetLabel(enum.Enum):
    NORMAL = 'NORMAL'
    TEST_DATA = 'TEST_DATA'
