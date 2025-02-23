import argparse
import threading

from logger import log

from config import DATA_DIRECTORY, PROCESSING_MODE
from data_lake import initial_setup_data_lake


def run_file_watcher():
    from file_watcher import start_file_watcher
    log.info("Starting file watcher...")
    start_file_watcher(DATA_DIRECTORY)


def run_kafka_consumer():
    from kafka_consumer import process_kafka_stream
    log.info("Starting Kafka consumer (PySpark streaming)...")
    process_kafka_stream()


def main():
    parser = argparse.ArgumentParser(description="Data Lake and Streaming Application")
    parser.add_argument("--mode", choices=["stream", "batch"], default=PROCESSING_MODE,
                        help="Processing mode: 'stream' for real-time or 'batch' for periodic processing")
    args = parser.parse_args()

    log.info("Running initial setup for the data lake (bulk ingestion)...")
    initial_setup_data_lake()

    if args.mode == "stream":
        watcher_thread = threading.Thread(target=run_file_watcher, daemon=True)
        consumer_thread = threading.Thread(target=run_kafka_consumer, daemon=True)

        watcher_thread.start()
        consumer_thread.start()

        log.info("Both file watcher and Kafka consumer are running.")
        watcher_thread.join()
        consumer_thread.join()
    else:
        from file_scanner import scan_filesystem
        from data_lake import create_data_lake_entry
        csv_files, _ = scan_filesystem(DATA_DIRECTORY)
        for file in csv_files:
            create_data_lake_entry(file)


if __name__ == "__main__":
    main()
