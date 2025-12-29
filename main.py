"""
Main entry point for the NHTSA data collection and processing script.

This script orchestrates the entire workflow:
1. Sets up the environment.
2. Identifies which test IDs need to be fetched (incremental collection).
3. Calls the network module to fetch and parse data from the NHTSA API for target IDs.
4. Calls the storage module to save the processed data to the filesystem, merging with existing data.
"""

import asyncio
import os
import sys
import warnings
from datetime import datetime
from typing import List

import config
from src.api.network import fetch_all_test_data
from src.utils.storage import get_existing_ids, save_by_year


def initialize_environment() -> None:
    """Initializes the environment, setting up policies and output directories."""
    if sys.platform == "win32":
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
    print(f"[*] Environment initialized. Output directory: '{config.OUTPUT_DIR}'")


async def main() -> None:
    """The main execution workflow for fetching and saving NHTSA test data."""
    start_time = datetime.now()
    print(f"=== NHTSA Direct Scanner Started at {start_time.strftime('%H:%M:%S')} ===")

    initialize_environment()

    # Define the full range of IDs to consider
    min_test_no: int = config.MIN_TEST_NO  # e.g., 6931
    # This range is from the new main.py, not config.py
    max_test_no: int = 20000

    # Get IDs of records already collected locally
    existing_ids = get_existing_ids(config.OUTPUT_DIR)

    # Determine which IDs still need to be fetched
    all_possible_ids = range(min_test_no, max_test_no + 1)
    target_ids: List[int] = [
        tid for tid in all_possible_ids if tid not in existing_ids
    ]

    skipped_count = len(all_possible_ids) - len(target_ids)

    print("\n[*] Starting Direct Scan (Incremental Mode)...")
    print(f"    - Total Range: {min_test_no} ~ {max_test_no}")
    print(f"    - Already Collected: {skipped_count} IDs (Skipping)")
    print(f"    - To Download: {len(target_ids)} IDs")

    if not target_ids:
        print("    - [Info] All data is up to date. No new records to fetch.")
        return

    # 1. Fetch and parse data from the API for the target IDs
    records = await fetch_all_test_data(target_ids)

    if not records:
        print("\n[!] No new records were found or processed. Check API connection or ID range.")
        return

    # 2. Save the collected records to files, merging with existing data
    save_by_year(records)

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n[Success] All tasks finished in {duration}.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
