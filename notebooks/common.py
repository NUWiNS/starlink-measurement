import os


def extract_operator_from_filename(file_path):
    """
    :param file_path: assume the format like /path/to/starlink/20240529/115627940/tcp_downlink_115630977.csv
    :return: the operator name, e.g. starlink
    """
    operator = file_path.split(os.sep)[-4]  # Adjust based on the exact structure of your file paths
    return operator