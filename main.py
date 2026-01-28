"""
This script runs the intraday analytics pipeline.

It processes high-frequency financial data for a specified date range and universe
of symbols. The pipeline is configurable through the `CONFIG` dictionary and can
be customized to compute various metrics.

The main steps of the pipeline are:
1.  **Data Preparation**: Data is loaded from the data lake, batched, and
    prepared for processing.
2.  **Metric Computation**: A series of analytics modules are run on the
    prepared data to compute metrics such as L2 order book features, trade
    analytics, and L3 event counts.
3.  **Output**: The results are written to Parquet files.

The script is designed to be run from the command line. It uses a locking
mechanism to prevent multiple instances from running simultaneously.
"""
import functools
import time
import bmll2
import bmll
import glob
import logging
import resource
import numpy as np
import polars as pl
import pandas as pd
import datetime as dt
import pyarrow.parquet as pq
import os
import re
import multiprocessing as mp
from typing import List, Dict, Optional, Callable
import cloudpickle
import multiprocessing
import subprocess
import tempfile
import sys
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import shutil
from pathlib import Path
from typing import Dict, List, Callable, Any, Optional
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from abc import ABC, abstractmethod
import random, time
import traceback
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from intraday_analytics import (
    AnalyticsPipeline,
    SymbolBatcherStreaming,
    PipelineDispatcher,
    preload,
    lprint,
)
from intraday_analytics.execution import PrepareDataProcess
from intraday_analytics.metrics.dense import DenseAnalytics
from intraday_analytics.metrics.l2 import L2AnalyticsLast, L2AnalyticsTW
from intraday_analytics.metrics.trade import TradeAnalytics
from intraday_analytics.metrics.l3 import L3Analytics
from intraday_analytics.metrics.execution import ExecutionAnalytics


# At 1min : 3 minutes per day.
# At 10 sec: 15 minutes per day ! 250gb matchine (20 GB memory)

CONFIG = {
    # --- Date & Scope ---
    "START_DATE": "2025-11-01",  # can we use calendar objects? (not really)
    "END_DATE": "2025-12-31",
    "PART_DAYS": 7,  # How many days per file (including saturday and sunday - the goal is to avoid over fragmentation)
    "DATASETNAME": "sample2d",  # Name of the dataset
    "UNIVERSE": {"Index": "bepacp"},
    # --- Analytics Parameters ---
    # "TIME_BUCKET_SECONDS": 2.5,
    "TIME_BUCKET_SECONDS": 60,  # Bertrand's default was 1 second;
    "L2_LEVELS": 10,  # Need to be less than 10 if relying on standard l2 analytics
    # --- Batching & Performance ---
    "BATCHING_STRATEGY": "heuristic",  # "heuristic" or "polars_scan" (how to group symbols -heuristics recommended)
    "NUM_WORKERS": -1,  # use -1 for all available CPUs
    "MAX_ROWS_PER_TABLE": {"trades": 500_000, "l2": 2_000_000, "l3": 10_000_000},
    # --- File Paths ---
    "HEURISTIC_SIZES_PATH": f"/tmp/symbol_sizes/latest.parquet",
    "TEMP_DIR": "/tmp/temp_ian3",  # may need to be updated if many similar notebook run at the same time
    "AREA": "user",
    "PREPARE_DATA_MODE": "s3symb",  # or naive
    "DEFAULT_FFILL": False,
    "DENSE_OUTPUT": True,
    "DENSE_OUTPUT_MODE": "adaptative",
    # Ben TODO: Make the below robust under summer/winter time shifts; limit dense output for lit continuous trading at the primary venue.
    "DENSE_OUTPUT_TIME_INTERVAL": [
        "08:00",
        "15:30",
    ],  # make this flexible with calendar objects? [limit ourselves to the lit market?]
    "MEMORY_PER_WORKER": 20,  # This will depend on resolution and universe (at 1 second this looks reasonable) (20gb fine up 10s)
    "METRIC_COMPUTATION": "parallel",  # Parallel or sequential
    "SEPARATE_METRIC_PROCESS": True,  # This may exceed resource limit on very large nodes
    "RUN_ONE_SYMBOL_AT_A_TIME": False,  # may impact required memory
    "PROFILE": False,
}

whitelist = [
    "AQXE",
    "BATE",
    "CHIX",
    "BOTC",
    "XMAD",
    "XETR",
    "XAMS",
    "XBRU",
    "XLIS",
    "XPAR",
    "XWAR",
    "XMIL",
    "XLON",
    "XOSL",
    "XSWX",
    "TRQX",
    "XHEL",
    "XSTO",
    "XCSE",
    "ONSE",
    "XDUB",
    "CEUX",
    "AQEU",
    "TREU",
    "TWEM",
    "SGMX",
    "SGMU",
    "XEQT",
    "XTAL",
    "XLIT",
    "XRIS",
    "TQEX",
    "XBUD",
    "XPRA",
    "XWBO",
    "XSEB",
    "AQSE",
    "BMTF",
    "ARTX",
    "XFRA",
    "XSTU",
    "XGAT",
    "XMUN",
    "XHAN",
    "XHAM",
    "XDUS",
    "XBER",
]


def _get_universe(date):
    """
    Retrieves the universe of symbols for a given date.

    This function loads a list of STOXX 600 symbols, filters them based on a
    whitelist of MICs, and enriches the data with reference information from
    the data lake.

    Args:
        date: The date for which to retrieve the universe.

    Returns:
        A pandas DataFrame containing the universe of symbols.
    """
    bmll2.get_file("support/ODDO/SXXP21July2025.csv")
    eumics = whitelist

    stoxx600bbg = (
        pd.read_csv("SXXP21July2025.csv")
        .rename(
            columns={
                "bbg_ticker": "BloombergTicker",
                "exchange": "Exchange",
                "isin": "ISIN",
                "currency": "CurrencyCode",
            }
        )
        .assign(
            BloombergTicker=lambda df: df["BloombergTicker"].str.strip(),
            CurrencyCode=lambda df: df["CurrencyCode"].str.strip(),
        )
    )

    ref = bmll2.get_market_data_range(
        markets=eumics,
        table_name="reference",
        start_date=date,
        end_date=date,
        df_engine="polars",
    )

    ref = (
        ref.filter(pl.col("ISIN").is_in(list(stoxx600bbg["ISIN"])))
        .filter(pl.col("IsPrimary"))
        .filter(pl.col("IsAlive"))
        .to_pandas()
    )

    return ref


def get_universe(date):
    """
    Retrieves the universe of symbols for a given date, with caching.

    This function is a cached version of `_get_universe`. It stores the universe
    data in a Parquet file to avoid re-computing it on subsequent calls for the
    same date.

    Args:
        date: The date for which to retrieve the universe.

    Returns:
        A Polars DataFrame containing the universe of symbols.
    """
    date = pd.Timestamp(date).date().isoformat()

    # Use a subdirectory in the temp dir for caching the universe
    cache_dir = os.path.join(CONFIG["TEMP_DIR"], "universe_cache")
    os.makedirs(cache_dir, exist_ok=True)
    universe_path = os.path.join(cache_dir, f"universe-{date}.parquet")

    if not os.path.exists(universe_path):
        u = _get_universe(date)
        u.to_parquet(universe_path)

    return pl.read_parquet(
        universe_path, storage_options={"region": "us-east-1"}
    )  # Ben


def get_pipeline(N, symbols=None, ref=None, date=None):
    """
    Constructs the analytics pipeline.

    This function creates an `AnalyticsPipeline` instance and adds the desired
    analytics modules to it. The modules are added in the order they should be
    executed.

    Args:
        N: The number of L2 order book levels to compute metrics for.
        symbols: An optional list of symbols to filter the universe by.
        ref: An optional reference DataFrame.
        date: The date for which the pipeline is being constructed.

    Returns:
        An `AnalyticsPipeline` instance.
    """
    assert date is not None
    modules = []

    if CONFIG["DENSE_OUTPUT"]:
        cref = ref if ref is not None else get_universe(date)
        if symbols is not None:
            cref = cref.filter(pl.col("ListingId").is_in(list(symbols)))
        modules += [DenseAnalytics(cref, CONFIG)]

    modules += [
        L2AnalyticsLast(N),
        L2AnalyticsTW(N, CONFIG),
        TradeAnalytics(),
        L3Analytics(),
        ExecutionAnalytics(),
    ]

    return AnalyticsPipeline(modules)


if __name__ == "__main__":
    os.environ["POLARS_MAX_THREADS"] = "48"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    lock_file_path = "/tmp/intraday_analytics.lock"
    temp_dir = None
    processes = []

    try:
        # Try to open the file in exclusive creation mode. This is atomic.
        lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode())
        os.close(lock_fd)
    except FileExistsError:
        print(f"Lock file {lock_file_path} exists. Checking if process is running...")
        try:
            with open(lock_file_path, "r") as f:
                pid = int(f.read())
            # Check if process is running (on Unix-like systems)
            os.kill(pid, 0)
        except (ValueError, OSError):
            # Stale lock file
            print("Stale lock file found. Removing it and starting.")
            os.remove(lock_file_path)
            # Second attempt to create lock file
            lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(lock_fd, str(os.getpid()).encode())
            os.close(lock_fd)
        else:
            print(f"Process {pid} is still running. Exiting.")
            sys.exit(1)

    try:
        temp_dir = tempfile.mkdtemp(prefix="intraday_analytics_")
        CONFIG["TEMP_DIR"] = temp_dir

        # storage path helper
        s3_user = (
            "s3://"
            + bmll2.storage_paths()[CONFIG["AREA"]]["bucket"]
            + "/"
            + bmll2.storage_paths()[CONFIG["AREA"]]["prefix"]
        )

        dates = pd.date_range(CONFIG["START_DATE"], CONFIG["END_DATE"], freq="D")

        for i in range(0, len(dates), CONFIG["PART_DAYS"]):
            chunk = dates[i : i + CONFIG["PART_DAYS"]]
            sd, ed = chunk[0], chunk[-1]

            lprint(f"STARTING {sd} -> {ed}")
            p = PrepareDataProcess(
                sd=sd,
                ed=ed,
                CONFIG=CONFIG,
                get_pipeline=get_pipeline,
                get_universe=get_universe,
                preload=preload,
                lprint=lprint,
                SymbolBatcherStreaming=SymbolBatcherStreaming,
                PipelineDispatcher=PipelineDispatcher,
            )
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        lprint("All processes completed.")

    except KeyboardInterrupt:
        print("\nTerminating child processes...")
        for p in processes:
            if p.is_alive():
                p.terminate()
            p.join()
        print("Child processes terminated.")
    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        print("Cleanup complete.")
