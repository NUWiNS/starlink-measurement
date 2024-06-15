import os


def find_files(base_dir, prefix, suffix):
    target_files = []

    # Walk through the directory structure
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.startswith(prefix) and file.endswith(suffix):
                target_files.append(os.path.join(root, file))
    return target_files

def count_subfolders(base_dir):
    return len(os.listdir(base_dir))