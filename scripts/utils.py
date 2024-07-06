import os


def find_files(base_dir, prefix, suffix):
    target_files = []

    # Walk through the directory structure
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            should_append = False
            if prefix and suffix:
                if file.startswith(prefix) and file.endswith(suffix):
                    should_append = True
            elif prefix:
                if file.startswith(prefix):
                    should_append = True
            elif suffix:
                if file.endswith(suffix):
                    should_append = True
            if should_append:
                target_files.append(os.path.join(root, file))
    return target_files


def count_subfolders(base_dir):
    return len(os.listdir(base_dir))
