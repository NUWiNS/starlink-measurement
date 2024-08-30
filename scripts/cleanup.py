import glob
import os

from scripts.constants import DATASET_DIR


def main():
    pattern = os.path.join(DATASET_DIR, 'alaska_starlink_trip/raw/**/tcp_uplink_*.csv')
    files = glob.glob(pattern, recursive=True)
    print(f"Found {len(files)} files")
    answer = input("Do you want to remove these files? (y/n): ")
    if answer.lower() != 'y':
        print("Abort")
        return

    # remove the files
    for file in files:
        os.remove(file)


if __name__ == '__main__':
    main()
