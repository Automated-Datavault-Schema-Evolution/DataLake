import atexit
import signal
import sys
import threading
from logger import log
from core.runner import run
from lake_grpc_service import serve as serve_lake_grpc
from utils.spark_work_autoscaler import cleanup_workers

def main():
    grpc_stop_event = threading.Event()
    grpc_thread = threading.Thread(target=serve_lake_grpc, args=(grpc_stop_event,), daemon=True, name='lake-grpc-server')
    grpc_thread.start()
    log.info('Lake gRPC server thread started.')

    def _shutdown_handler(signum=None, frame=None):
        log.info('Application stopping, signaling Lake gRPC server and cleaning up Spark workers')
        try:
            grpc_stop_event.set()
        except Exception:
            pass
        try:
            if grpc_thread.is_alive():
                grpc_thread.join(timeout=5)
        except Exception:
            pass
        cleanup_workers()
        if signum is not None:
            sys.exit(0)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _shutdown_handler)
    atexit.register(cleanup_workers)
    run()
__all__ = ['main']
