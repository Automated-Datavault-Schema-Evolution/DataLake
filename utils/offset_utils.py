import os
import pickle

from logger import log


def read_last_offset(path):
    if not os.path.exists(path):
        return None

    try:
        with open(path, 'rb') as f:
            offset = pickle.load(f)
        log.debug(f"Read offset from {path}: {offset}")
        return offset
    except Exception as e:
        log.warning(f"Failed to read offset from {path}: {e}")
        return None


def write_last_offset(path, offset):
    try:
        with open(path, 'wb') as f:
            pickle.dump(offset, f)
        log.debug(f"Saved offset to {path}: {offset}")
    except Exception as e:
        log.warning(f"Failed to save offset to {path}: {e}")
