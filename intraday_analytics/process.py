import os
import sys
import shutil
import tempfile
from contextlib import contextmanager
import logging

@contextmanager
def managed_execution(lock_file_path="/tmp/intraday_analytics.lock"):
    """
    A context manager to handle process locking, temporary directory creation,
    and graceful shutdown on KeyboardInterrupt.
    """
    # --- Lock file acquisition ---
    try:
        lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode())
        os.close(lock_fd)
        logging.info(f"🔒 Acquired lock file: {lock_file_path}")
    except FileExistsError:
        logging.warning(f"Lock file {lock_file_path} exists. Checking if process is running...")
        try:
            with open(lock_file_path, "r") as f:
                pid = int(f.read())
            # Check if the process is running (on Unix-like systems)
            os.kill(pid, 0)
        except (ValueError, OSError):
            # The process does not exist, so the lock file is stale
            logging.warning("🔓 Stale lock file found. Removing it and starting.")
            os.remove(lock_file_path)
            # Try to acquire the lock again
            lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(lock_fd, str(os.getpid()).encode())
            os.close(lock_fd)
            logging.info(f"🔒 Acquired lock file: {lock_file_path}")
        else:
            logging.error(f"🏃 Process {pid} is still running. Exiting.")
            sys.exit(1)

    processes = []
    temp_dir = tempfile.mkdtemp(prefix="intraday_analytics_")
    logging.info(f"📂 Created temporary directory: {temp_dir}")
    
    try:
        # Yield the list for collecting processes and the temp directory path
        yield processes, temp_dir
        
        # Wait for all child processes to complete
        logging.info("⏳ Waiting for all processes to complete...")
        for p in processes:
            p.join()
        
        logging.info("✅ All processes completed.")

    except KeyboardInterrupt:
        logging.warning("🚦 Terminating child processes due to KeyboardInterrupt...")
        for p in processes:
            if p.is_alive():
                p.terminate()
            p.join()
        logging.info("🚦 Child processes terminated.")
    finally:
        # --- Cleanup ---
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            logging.info(f"🔑 Removed lock file: {lock_file_path}")
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logging.info(f"🗑️ Removed temporary directory: {temp_dir}")
        logging.info("🎉 Cleanup complete.")
