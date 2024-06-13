import os
import glob
import pandas as pd

base_directory = os.path.join(os.getcwd(), "../outputs/maine_starlink_trip/")


def read_csv_files(file_pattern):
    files = glob.glob(file_pattern, recursive=True)
    data_frames = []

    empty_file_count = 0
    total_file_count = 0

    for file in files:
        total_file_count += 1
        operator = extract_operator_from_filename(file)
        df = pd.read_csv(file)
        if df.empty:
            empty_file_count += 1
            print("--- Empty file detected: ", file)
            continue

        df['operator'] = operator
        data_frames.append(df)

    print(f"Total files processed: {total_file_count}")
    print(f"Empty files detected and skipped: {empty_file_count}")
    return pd.concat(data_frames, ignore_index=True)


def extract_operator_from_filename(file_path):
    """
    :param file_path: assume the format like /path/to/starlink/20240529/115627940/tcp_downlink_115630977.csv
    :return: the operator name, e.g. starlink
    """
    operator = file_path.split(os.sep)[-4]  # Adjust based on the exact structure of your file paths
    return operator


def get_data_frame_from_all_csv(protocol, direction):
    filename_pattern = os.path.join(base_directory, f'**/{protocol}_{direction}_*.csv')
    data_frame = read_csv_files(filename_pattern)
    return data_frame


def save_throughput_metric_to_csv(data_frame, protocol, direction, output_dir='.'):
    """
    :param data_frame:
    :param protocol: tcp | udp
    :param direction:  uplink | downlink
    :param output_dir:
    :return:
    """
    prefix = f"{protocol}_{direction}"
    csv_filepath = os.path.join(output_dir, f'{prefix}_all.csv')
    data_frame.to_csv(csv_filepath, index=False)
    print(f'save all the {prefix} data to csv file: {csv_filepath}')


def main():
    dataset_dir = os.path.join(base_directory, 'datasets')
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir, exist_ok=True)

    # print('-----------------------------------')
    # all_tcp_downlink_df = get_data_frame_from_all_csv('tcp', 'downlink')
    # save_throughput_metric_to_csv(data_frame=all_tcp_downlink_df, protocol='tcp', direction='downlink', output_dir=dataset_dir)

    # print('-----------------------------------')

    # all_tcp_uplink_df = get_data_frame_from_all_csv('tcp', 'uplink')
    # save_throughput_metric_to_csv(data_frame=all_tcp_uplink_df, protocol='tcp', direction='uplink', output_dir=dataset_dir)

    # print('-----------------------------------')
    # all_udp_uplink_df = get_data_frame_from_all_csv('udp', 'uplink')
    # save_throughput_metric_to_csv(data_frame=all_udp_uplink_df, protocol='udp', direction='uplink', output_dir=dataset_dir)


if __name__ == '__main__':
    main()
