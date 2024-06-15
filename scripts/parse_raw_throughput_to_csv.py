import pytz
from datetime import datetime

import os
import re


def find_files(base_dir, prefix, suffix):
    target_files = []

    # Walk through the directory structure
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.startswith(prefix) and file.endswith(suffix):
                target_files.append(os.path.join(root, file))
    return target_files


def format_datetime_as_iso_8601(dt: datetime):
    """
    Format the time in the EDT timezone
    :param dt:
    :return:
    """
    return dt.isoformat()


def append_timezone(dt: datetime, timezone_str: str, is_dst: bool = True):
    timezone = pytz.timezone(timezone_str)
    dt_aware = timezone.localize(dt, is_dst=is_dst)  # is_dst=True for daylight saving time
    return dt_aware


def append_edt_timezone(dt: datetime, is_dst: bool = True):
    return append_timezone(dt, "US/Eastern", is_dst)


def parse_nuttcp_timestamp(timestamp):
    # Parse the timestamp in the format of "2024-05-27 15:00:00.000000"
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")


def format_nuttcp_timestamp(dt_str: str):
    dt = parse_nuttcp_timestamp(dt_str)
    dt_edt = append_edt_timezone(dt)
    return format_datetime_as_iso_8601(dt_edt)


def parse_nuttcp_tcp_result(content):
    # Regular expression to match the target line
    pattern = re.compile(
        r"\[(.*?)\]\s+.*?=\s+([\d.]+)\s+Mbps\s+(\d+)\s+retrans\s+(\d+)\s+KB-cwnd"
    )

    extracted_data = []

    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            dt, throughput, retrans, cwnd = match.groups()
            dt_isoformat = format_nuttcp_timestamp(dt)
            row = [dt_isoformat, throughput, retrans, cwnd]
            extracted_data.append(','.join(row))
    return extracted_data


def parse_nuttcp_udp_result(content):
    pattern = re.compile(r'\[(.*?)\]\s+.*=\s+([\d.]+) Mbps\s+([-\d]+) /\s+(\d+) ~drop/pkt\s+([-\d.]+) ~%loss')

    extracted_data = []
    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            dt, throughput, pkt_drop, pkt_total, loss = match.groups()
            dt_isoformat = format_nuttcp_timestamp(dt)
            row = [dt_isoformat, throughput, pkt_drop, pkt_total, loss]
            extracted_data.append(','.join(row))
    return extracted_data


def parse_iperf_udp_result(content: str):
    # Regular expression to match the target line
    pattern = re.compile(
        r"\[(.*?)\]\s+\[\s*\d+\]\s+[\d.]+-[\d.]+\s+sec\s+[\d.]+\s+MBytes\s+([\d.]+)\s+Mbits/sec\s+[\d.]+\s+ms\s+(\d+)/(\d+)\s+\((\d+)%\)"
    )

    extracted_data = []

    for line in content.splitlines():
        match = pattern.search(line)
        if match:
            timestamp, throughput, pkt_drop, pkt_total, loss = match.groups()
            dt_isoformat = format_nuttcp_timestamp(timestamp)
            row = [dt_isoformat, throughput, pkt_drop, pkt_total, loss]
            extracted_data.append(','.join(row))
    return extracted_data


def save_to_csv(data, output_file):
    header = ['time', 'throughput_mbps', 'retrans', 'cwnd_kb']
    with open(output_file, 'w') as f:
        f.write(','.join(header) + '\n')
        for line in data:
            f.write(line + '\n')


def save_udp_data_to_csv(data, output_file):
    header = ['time', 'throughput_mbps', 'pkt_drop', 'pkt_total', 'loss']
    with open(output_file, 'w') as f:
        f.write(','.join(header) + '\n')
        for line in data:
            f.write(line + '\n')


def find_tcp_downlink_file(base_dir):
    return find_files(base_dir, prefix="tcp_downlink", suffix=".out")


def find_tcp_uplink_file(base_dir):
    return find_files(base_dir, prefix="tcp_uplink", suffix=".out")


def find_udp_downlink_file(base_dir):
    return find_files(base_dir, prefix="udp_downlink", suffix=".out")


def find_udp_uplink_file(base_dir):
    return find_files(base_dir, prefix="udp_uplink", suffix=".out")


def find_ping_file(base_dir):
    return find_files(base_dir, prefix="ping", suffix=".out")


def find_traceroute_file(base_dir):
    return find_files(base_dir, prefix="traceroute", suffix=".out")


def find_nslookup_file(base_dir):
    return find_files(base_dir, prefix="nslookup", suffix=".out")


def count_subfolders(base_dir):
    return len(os.listdir(base_dir))


print("Save the extracted iperf data of UDP DL to csv files")


def main():
    base_directory = os.path.join(os.getcwd(), "../outputs/maine_starlink_trip/")

    udp_downlink_files = find_udp_downlink_file(base_directory)
    excluded_files = []

    # Example to read and print the content of the found files
    for file in udp_downlink_files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                extracted_data = parse_iperf_udp_result(content)
                if not extracted_data:
                    excluded_files.append(file)
                    continue
                csv_file_path = file.replace('.out', '.csv')
                save_udp_data_to_csv(extracted_data, csv_file_path)
                print(f"Extracted data is saved to {csv_file_path}")
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print('Total files:', len(udp_downlink_files))
    print('Excluded files:', len(excluded_files))


if __name__ == '__main__':
    main()