import os


def base_path(name_or_path):
    base_dir = os.path.dirname(
        os.path.realpath(__file__)
    )
    return os.path.join(base_dir, name_or_path)
