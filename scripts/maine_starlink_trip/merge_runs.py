import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.maine_starlink_trip.configs import ROOT_DIR
from scripts.logging_utils import create_logger
from scripts.utilities.RunMerger import RunMerger

DATA_DIR = os.path.join(ROOT_DIR, 'raw')

operators = ['starlink', 'att', 'verizon', 'tmobile']
logger = create_logger('merge_runs', filename=os.path.join(ROOT_DIR, 'merge_runs.log'))


def main():
    for operator in operators:
        logger.info('-----------------------------------')
        logger.info('Merging runs for operator: ' + operator)
        merger = RunMerger()
        if os.path.exists(os.path.join(DATA_DIR, f'{operator}_merged')):
            logger.error(f'Merged folder already exists for {operator}. Skipping...')
            continue
        tcp_folders, udp_folders = merger.classify_folders_by_protocol(dir=os.path.join(DATA_DIR, operator))
        matched, leftover_tcp, leftover_udp = merger.match_folders(tcp_folders,
                                                                   udp_folders)
        if len(leftover_tcp) > 0:
            logger.error(f'Leftover TCP folders: {merger.fmt_list(leftover_tcp)}')
        if len(leftover_udp) > 0:
            logger.error(f'Leftover UDP folders: {merger.fmt_list(leftover_udp)}')
        logger.info('Merging runs completed. Merged: ' + str(len(matched)))
        merger.save_merged_folders(operator, matched)
        logger.info('Merged folders saved to output directory')
        logger.info('-----------------------------------')


if __name__ == '__main__':
    main()
