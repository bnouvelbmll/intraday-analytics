import cloudpickle
import sys
import os
import traceback
import subprocess
import tempfile
import random
import time
from multiprocessing import Process


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
                # capture_output=True,
                # text=True
            )

            # 4. Check exit code and retrieve result
            if process.returncode != 0:
                # If the script failed (non-zero exit code)
                print("Script failed")

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


class PrepareDataProcess(Process):
    """
    A multiprocessing.Process subclass for preparing data for the analytics pipeline.

    This class encapsulates the logic for loading, batching, and processing data
    in a separate process. It is designed to be used in a larger data preparation
    workflow.
    """
    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs

    def run(self):
        # print(list(self.kwargs.keys()))
        globals().update(self.kwargs)
        ref = get_universe(sd)
        nanoseconds = int(CONFIG["TIME_BUCKET_SECONDS"] * 1e9)
        pipe = get_pipeline(CONFIG["L2_LEVELS"], ref=ref, date=sd)

        MODE = CONFIG["PREPARE_DATA_MODE"]
        TEMP_DIR = CONFIG["TEMP_DIR"]

        if MODE == "naive":
            lprint("CREATING BATCH FILES...")
            lprint("LAZY LOADING ALL DATA...")
            trades, l2, l3 = preload(sd, ed, ref, nanoseconds)

            def write_per_listing(df, listing_id):
                out_path = os.path.join(TEMP_DIR, f"out-listing-{listing_id}.parquet")
                df.write_parquet(out_path)

            writer = write_per_listing
            sbs = SymbolBatcherStreaming(
                {
                    "trades": trades.sort(["ListingId", "TradeTimestamp"]),
                    "l2": l2.sort(["ListingId", "EventTimestamp"]),
                    "l3": l3.sort(["ListingId", "EventTimestamp"]),
                    "marketstate": marketstate.sort(["ListingId", "EventTimestamp"]),
                },
                max_rows_per_table=CONFIG["MAX_ROWS_PER_TABLE"],
            )
            pid = PipelineDispatcher(pipe, writer, CONFIG)

            import sys

            def process_batch(b):
                sys.stderr.write(".")
                import resource

                # Limit memory to 16gb per batch
                # resource.setrlimit(resource.RLIMIT_AS, (16 * 1024**3, 16 * 1024**3))
                pid.run_batch(b)

            lprint("DUMPING BATCHES")
            for i, batch in enumerate(sbs.stream_batches()):
                sys.stderr.write(".")
                print(i, len(batch["l2"]))
                batch["l3"].write_parquet(
                    os.path.join(TEMP_DIR, f"batch-l3-{i}.parquet")
                )
                batch["l2"].write_parquet(
                    os.path.join(TEMP_DIR, f"batch-l2-{i}.parquet")
                )
                batch["trades"].write_parquet(
                    os.path.join(TEMP_DIR, f"batch-trades-{i}.parquet")
                )
