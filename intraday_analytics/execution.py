import cloudpickle
import sys
import os
import traceback
import subprocess
import tempfile
import random
import time
from multiprocessing import Process
import logging

from .batching import SymbolBatcherStreaming, PipelineDispatcher
from .utils import preload


def _create_runner_script(tmp_dir):
    """
    Creates a temporary Python script that acts as the bootstrap runner
    for the child process.
    """
    runner_script_content = """
import cloudpickle
import sys
import os
import traceback
import logging

def run_pickled_func():
    # File paths are passed as command-line arguments
    func_path = sys.argv[1]
    result_path = sys.argv[2]

    try:
        # 1. Load the function and args from the pickle file
        with open(func_path, 'rb') as f:
            func, args, kwargs = cloudpickle.load(f)

        # 2. Execute the function
        result = func(*args, **kwargs)

        # 3. Pickle the result (and a success status)
        with open(result_path, 'wb') as f:
            cloudpickle.dump(('result', result), f)

    except Exception as e:
        # 4. Pickle the exception (and a failure status)
        logging.error("Error in remote process", exc_info=True)
        exc_info = sys.exc_info()
        with open(result_path, 'wb') as f:
            cloudpickle.dump(('exception', exc_info), f)
        # Exit with a non-zero code to signal failure
        sys.exit(1)

    sys.exit(0)

if __name__ == '__main__':
    run_pickled_func()
"""
    runner_path = os.path.join(tmp_dir, "runner.py")
    with open(runner_path, "w") as f:
        f.write(runner_script_content)
    return runner_path


class RemoteExecutionError(Exception):
    """
    Custom exception raised when a function executed in a remote process fails.

    This exception wraps the original exception and traceback from the remote
    process, providing more context for debugging.
    """

    def __init__(self, message, original_exception=None, traceback_text=None):
        super().__init__(message)
        self.original_exception = original_exception
        self.traceback_text = traceback_text


def remote_process_executor_wrapper(func):
    """
    A decorator that executes a function in a separate, isolated subprocess.

    This wrapper serializes the function and its arguments using `cloudpickle`,
    runs it in a new Python process, and then deserializes the result. This
    approach is useful for running code that might have memory leaks or other
    side effects, as the subprocess is completely torn down after execution.
    Communication is handled via temporary files to avoid issues with
    multiprocessing queues.
    """

    def wrapper(*args, **kwargs):

        # 1. Create a temporary directory for communication files
        random.seed(str((os.getpid(), time.time())))
        with tempfile.TemporaryDirectory() as tmp_dir:

            # Define file paths for the function and the result
            func_pkl_path = os.path.join(tmp_dir, "func.pkl")
            result_pkl_path = os.path.join(tmp_dir, "result.pkl")

            # Create the bootstrap script once
            runner_path = _create_runner_script(tmp_dir)

            # 2. Serialize the function and arguments to a file
            payload = (func, args, kwargs)
            with open(func_pkl_path, "wb") as f:
                cloudpickle.dump(payload, f)

            # 3. Launch the subprocess
            command = [
                sys.executable,  # Path to the current Python interpreter
                runner_path,  # The bootstrap script
                func_pkl_path,  # Argument 1: path to the function pickle
                result_pkl_path,  # Argument 2: path for the result pickle
            ]

            # Run the command and wait for it to complete
            # Capture stdout/stderr for debugging
            process = subprocess.run(
                command,
                capture_output=True,
                text=True
            )

            # 4. Check exit code and retrieve result
            if process.returncode != 0:
                # If the script failed (non-zero exit code)
                logging.error(f"Remote process script failed with exit code {process.returncode}")
                logging.error(f"STDOUT: {process.stdout}")
                logging.error(f"STDERR: {process.stderr}")

                # Try to load the exception details if available
                if os.path.exists(result_pkl_path):
                    with open(result_pkl_path, "rb") as f:
                        status, exc_info = cloudpickle.load(f)

                        if status == "exception":
                            # Reconstruct and raise the remote exception
                            exc_type, exc_value, exc_traceback = exc_info
                            traceback_text = "".join(
                                traceback.format_exception(*exc_info)
                            )

                            raise RemoteExecutionError(
                                f"Remote process failed (Exit Code {process.returncode}). "
                                f"Exception: {exc_type.__name__}: {exc_value}",
                                original_exception=exc_value,
                                traceback_text=traceback_text,
                            )

                # Fallback for unexpected crash (e.g., OOM, Segmentation Fault)
                raise RemoteExecutionError(
                    f"Remote process crashed unexpectedly (Exit Code {process.returncode}). "
                    f"STDERR:\n{process.stderr}"
                )

            # 5. Load and return the successful result
            with open(result_pkl_path, "rb") as f:
                status, result = cloudpickle.load(f)

                if status == "result":
                    return result

                # Should not happen if returncode is 0, but safe check
                raise RemoteExecutionError(
                    "Remote process succeeded but returned an unexpected status."
                )

    return wrapper


from .tables import ALL_TABLES

class ProcessInterval(Process):
    """
    A multiprocessing.Process subclass for preparing data for the analytics pipeline.

    This class encapsulates the logic for loading, batching, and processing data
    in a separate process for a given time interval.
    """
    def __init__(self, sd, ed, config, get_pipeline, get_universe):
        super().__init__()
        self.sd = sd
        self.ed = ed
        self.config = config
        self.get_pipeline = get_pipeline
        self.get_universe = get_universe

    def run(self):
        # Configure logging for the new process
        logging.basicConfig(
            level=self.config.get("LOGGING_LEVEL", "INFO").upper(),
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )
        
        ref = self.get_universe(self.sd)
        nanoseconds = int(self.config["TIME_BUCKET_SECONDS"] * 1e9)
        pipe = self.get_pipeline(self.config["L2_LEVELS"], ref=ref, date=self.sd)

        MODE = self.config["PREPARE_DATA_MODE"]
        TEMP_DIR = self.config["TEMP_DIR"]

        if MODE == "naive":
            logging.info("🚚 Creating batch files...")
            
            tables_to_load_names = self.config.get("TABLES_TO_LOAD", ["trades", "l2", "l3"])
            table_definitions = [ALL_TABLES[name] for name in tables_to_load_names]
            loaded_tables = preload(self.sd, self.ed, ref, nanoseconds, table_definitions)
            
            def write_per_listing(df, listing_id):
                out_path = os.path.join(TEMP_DIR, f"out-listing-{listing_id}.parquet")
                df.write_parquet(out_path)

            writer = write_per_listing
            
            sbs_input = {}
            for table in table_definitions:
                sbs_input[table.name] = loaded_tables[table.name].sort(["ListingId", table.timestamp_col])
            
            sbs = SymbolBatcherStreaming(
                sbs_input,
                max_rows_per_table=self.config["MAX_ROWS_PER_TABLE"],
            )
            pid = PipelineDispatcher(pipe, writer, self.config)

            def process_batch(b):
                import resource
                # Limit memory to 16gb per batch
                # resource.setrlimit(resource.RLIMIT_AS, (16 * 1024**3, 16 * 1024**3))
                pid.run_batch(b)

            logging.info("Writing batches to disk...")
            for i, batch in enumerate(sbs.stream_batches()):
                logging.info(f"  - Writing batch {i} (l2 length: {len(batch.get('l2', []))})")
                for table_name, data in batch.items():
                    data.write_parquet(
                        os.path.join(TEMP_DIR, f"batch-{table_name}-{i}.parquet")
                    )
