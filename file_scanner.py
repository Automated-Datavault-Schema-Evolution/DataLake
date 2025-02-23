# file_scanner.py
import glob
import os

import pandas as pd
from logger import log


def scan_filesystem(directory, pattern="*.csv"):
    """
    Scan a directory for CSV files and extract basic metadata.
    """
    log.debug(f"Scanning directory '{directory}' for files matching pattern '{pattern}'")
    csv_files = glob.glob(os.path.join(directory, pattern))
    metadata = {}
    for file in csv_files:
        try:
            df_sample = pd.read_csv(file, nrows=10)
            metadata[file] = {
                "columns": list(df_sample.columns),
                "dtypes": df_sample.dtypes.astype(str).to_dict()
            }
            log.info(f"Scanned file: {file} with columns: {metadata[file]['columns']}")
        except Exception as e:
            log.error(f"Error reading {file}: {e}")
    log.debug(f"Completed scanning. Found {len(csv_files)} files.")
    return csv_files, metadata


if __name__ == "__main__":
    files, meta = scan_filesystem("./data")
    log.info(f"Found files: {files}")
    log.info(f"Metadata: {meta}")
