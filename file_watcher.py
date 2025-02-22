# file_watcher.py (modified)
import os
import time
from logger import log
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka_streamer import stream_delta
from data_lake import create_data_lake_entry

class CSVEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            log.info(f"Detected change in {event.src_path}")
            if os.path.exists(event.src_path):
                # Stream data to Kafka
                stream_delta(event.src_path)
                # Also process the file into the data lake (i.e. load into PostgreSQL or save as Parquet)
                create_data_lake_entry(event.src_path)
            else:
                log.error(f"File not found: {event.src_path}")

def start_file_watcher(directory):
    if not os.path.exists(directory):
        log.error(f"Directory does not exist: {directory}")
        exit(1)
    event_handler = CSVEventHandler()
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()
    log.info(f"Started file watcher on directory: {directory}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log.info("File watcher stopped.")
    observer.join()

if __name__ == "__main__":
    from config import DATA_DIRECTORY
    start_file_watcher(DATA_DIRECTORY)
