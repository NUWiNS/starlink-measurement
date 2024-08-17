import enum
import os

from scripts.constants import DATASET_DIR

DATASET_NAME = 'hawaii_starlink_trip'

ROOT_DIR = os.path.join(DATASET_DIR, DATASET_NAME)

TIMEZONE = 'Pacific/Honolulu'

class DatasetLabel(enum.Enum):
    NORMAL = 'NORMAL'
    TEST_DATA = 'TEST_DATA'
