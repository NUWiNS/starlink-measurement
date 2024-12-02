import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.utilities.starlink_metric_utils import StarlinkMetricProcessor
from scripts.logging_utils import create_logger
from scripts.time_utils import now
from scripts.alaska_starlink_trip.configs import ROOT_DIR, TIMEZONE

def main():
    tmp_dir = os.path.join(ROOT_DIR, 'tmp')
    logger = create_logger(__name__, filename=os.path.join(tmp_dir, f'process_starlink_metric.{now()}.log'))
    processor = StarlinkMetricProcessor(
        root_dir=ROOT_DIR, 
        timezone=TIMEZONE, 
        logger=logger
    )
    processor.process()

if __name__ == '__main__':
    main()
