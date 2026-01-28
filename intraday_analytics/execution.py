def process_batch_indirect(batch_id: int, config: dict, get_pipeline_fn, get_universe_fn):
    """
    Function to be executed by a process pool worker, launching the
    CreateMetricsProcess directly.
    """
    # Set memory limit for the worker process
    memory_limit_gb = config.get("MEMORY_PER_WORKER", 20)
    try:
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit_gb * 1024**3, memory_limit_gb * 1024**3))
    except ValueError as e:
        logging.warning(f"Could not set memory limit for worker: {e}. This might happen on systems with lower total memory.")


    i = batch_id

    # Define output and get writer
    output_file = os.path.join(config["TEMP_DIR"], f"batch-metrics-{i}.parquet")
    bwriter = BatchWriter(output_file)
    writer = bwriter.write

    # Setup the pipeline
    # Pass config and get_universe_fn to get_pipeline_fn
    pipe = get_pipeline_fn(N=config["L2_LEVELS"], config=config)

    # Load the batch data
    batch = {}
    tables_to_load_names = config.get("TABLES_TO_LOAD", ["trades", "l2", "l3"])
    for table_name in tables_to_load_names:
        batch_file_path = os.path.join(config["TEMP_DIR"], f"batch-{table_name}-{i}.parquet")
        if os.path.exists(batch_file_path):
            batch[table_name] = pl.scan_parquet(batch_file_path).set_sorted("ListingId", ALL_TABLES[table_name].timestamp_col)
        else:
            logging.warning(f"Batch file {batch_file_path} not found for batch {i}. Skipping table {table_name}.")
            batch[table_name] = pl.DataFrame() # Provide an empty DataFrame to avoid errors

    # Run the pipeline
    pid = PipelineDispatcher(pipe, writer, config)
    pid.run_batch(batch)

    # Clean up input batch files after processing
    if config.get("CLEAN_UP_BATCH_FILES", True):
        files_to_remove = [
            os.path.join(config["TEMP_DIR"], f"batch-{table_name}-{i}.parquet")
            for table_name in tables_to_load_names
        ]
        for file_path in files_to_remove:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except OSError as e:
                logging.error(f"Error removing file {file_path}: {e}")
    logging.info(f"Metrics computed and files cleaned for batch {i}.")

    return batch_id

# Apply the remote_process_executor_wrapper if SEPARATE_METRIC_PROCESS is enabled
# This needs to be done outside the function definition
# The actual application of the decorator will happen in the main execution logic
# where the config is available.

def compute_metrics(config: dict, get_pipeline_fn, get_universe_fn):
    # Discover all batch files to process
    logging.info("COMPUTING METRICS")
    
    # Use glob to find any batch file to determine batch IDs
    # Assuming all tables for a given batch_id are present if one is.
    # We'll use trades as a reference, but any table would work.
    batch_files_pattern = os.path.join(config["TEMP_DIR"], "batch-trades-*.parquet")
    trade_files = glob.glob(batch_files_pattern)

    # Extract batch IDs (e.g., from "/tmp/batch-trades-5.parquet" -> "5")
    batch_ids = []
    for f in trade_files:
        try:
            # Assumes the format 'batch-TABLE_NAME-ID.parquet'
            match = re.search(r"batch-\w+-(\d+)\.parquet$", f)
            if match:
                batch_ids.append(int(match.group(1)))
        except Exception as e:
            logging.error(f"Error extracting batch ID from {f}: {e}")
    
    batch_ids = sorted(list(set(batch_ids))) # Ensure unique and sorted IDs

    if not batch_ids:
        logging.warning("No batch files found to process metrics.")
        return

    logging.info(f"Found {len(batch_ids)} batches to process: {batch_ids}")

    # Determine the executor based on SEPARATE_METRIC_PROCESS config
    if config.get("SEPARATE_METRIC_PROCESS", False):
        logging.info("Using ProcessPoolExecutor for metric computation.")
        # Wrap process_batch_indirect with the remote_process_executor_wrapper
        # for each call if it's not already wrapped.
        # For joblib.Parallel, we pass the function directly and it handles the process spawning.
        executor_fn = Parallel(n_jobs=config.get("NUM_WORKERS", -1), prefer="processes", max_tasks_per_child=1)
        processed_batches = executor_fn(
            delayed(process_batch_indirect)(batch_id, config, get_pipeline_fn, get_universe_fn)
            for batch_id in batch_ids
        )
    else:
        logging.info("Using ThreadPoolExecutor for metric computation (or direct call if NUM_WORKERS=1).")
        # If not using separate processes, we can use ThreadPoolExecutor or direct calls
        # depending on NUM_WORKERS.
        num_workers = config.get("NUM_WORKERS", 1)
        if num_workers == 1:
            processed_batches = [
                process_batch_indirect(batch_id, config, get_pipeline_fn, get_universe_fn)
                for batch_id in batch_ids
            ]
        else:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [
                    executor.submit(process_batch_indirect, batch_id, config, get_pipeline_fn, get_universe_fn)
                    for batch_id in batch_ids
                ]
                processed_batches = [f.result() for f in futures]

    logging.info(f"Successfully processed batches: {processed_batches}")
    logging.info("COMPUTING METRICS - DONE")