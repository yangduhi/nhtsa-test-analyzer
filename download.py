"""
NHTSA Data File Downloader.

This script reads the metadata JSON files created by main.py and downloads the
associated data files, specifically TDMS (Test Data Management Streaming) for
instrument signals and PDF for reports.

It can be run independently after the metadata collection is complete.

Usage:
    python download.py
    python download.py --download-reports
"""

import argparse
import asyncio
import glob
import json
import os
import sys
from typing import Any, Coroutine, Dict, List

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm

import config

# --- Configuration ---
BASE_DOWNLOAD_DIR: str = "data/raw"
SIGNAL_DIR_NAME: str = "signals"
REPORT_DIR_NAME: str = "reports"


async def download_file(
    session: aiohttp.ClientSession, url: str, save_path: str, semaphore: asyncio.Semaphore
) -> bool:
    """Downloads a single file from a URL and saves it locally.

    Skips download if the file already exists.

    Args:
        session: The aiohttp client session.
        url: The URL of the file to download.
        save_path: The local path to save the file.
        semaphore: The semaphore to limit concurrent downloads.

    Returns:
        True if the file was downloaded successfully or already existed,
        False otherwise.
    """
    if not url or not isinstance(url, str):
        return False
    if os.path.exists(save_path):
        return True

    try:
        async with semaphore:
            async with session.get(url, timeout=300) as response:
                if response.status == 200:
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    async with aiofiles.open(save_path, mode="wb") as f:
                        await f.write(await response.read())
                    return True
                return False
    except Exception:
        return False


async def process_test_record(
    session: aiohttp.ClientSession,
    record: Dict[str, Any],
    download_reports: bool,
    semaphore: asyncio.Semaphore,
) -> Dict[str, int]:
    """Processes a single test record to download its associated files.

    Args:
        session: The aiohttp client session.
        record: A dictionary representing one test record.
        download_reports: A flag to indicate whether to download PDF reports.
        semaphore: The semaphore for concurrent downloads.

    Returns:
        A dictionary with counts of downloaded signals and reports.
    """
    test_no = record.get("test_no")
    year = record.get("model_year", "unknown_year")
    links = record.get("links", {})
    reports = record.get("reports", [])
    results = {"signals": 0, "reports": 0}

    # 1. Download measurement data (TDMS only)
    tdms_url = links.get("URL_TDMS")
    if tdms_url:
        filename = os.path.basename(tdms_url)
        save_path = os.path.join(
            BASE_DOWNLOAD_DIR, SIGNAL_DIR_NAME, str(year), str(test_no), filename
        )
        if await download_file(session, tdms_url, save_path, semaphore):
            results["signals"] += 1

    # 2. Download reports (PDF only, if enabled)
    if download_reports and reports:
        for rep in reports:
            pdf_url = rep.get("URL")
            if pdf_url and pdf_url.lower().endswith(".pdf"):
                filename = os.path.basename(pdf_url)
                save_path = os.path.join(
                    BASE_DOWNLOAD_DIR, REPORT_DIR_NAME, str(year), str(test_no), filename
                )
                if await download_file(session, pdf_url, save_path, semaphore):
                    results["reports"] += 1
    return results


def load_metadata_records() -> List[Dict[str, Any]]:
    """Loads all test records from the nhtsa_*.json files.

    Returns:
        A list of all records found.
    """
    json_files = glob.glob(os.path.join(config.OUTPUT_DIR, "nhtsa_*.json"))
    if not json_files:
        print(f"[!] No metadata JSON files found in '{config.OUTPUT_DIR}'.")
        return []

    all_records: List[Dict[str, Any]] = []
    for jf in json_files:
        with open(jf, "r", encoding="utf-8") as f:
            try:
                all_records.extend(json.load(f))
            except json.JSONDecodeError:
                print(f"[!] Warning: Could not decode JSON from {jf}.")
    return all_records


async def main(args: argparse.Namespace) -> None:
    """Main function to orchestrate the download process.

    Args:
        args: Command-line arguments from argparse.
    """
    all_records = load_metadata_records()
    if not all_records:
        return

    print(f"[*] Found {len(all_records)} total test records.")
    print("[*] Downloading Signals (Target: TDMS zip)...")
    if args.download_reports:
        print("[*] Downloading Reports (Target: PDF)...")

    # Semaphore for limiting concurrent downloads (files can be large)
    sem = asyncio.Semaphore(5)
    total_signals = 0
    total_reports = 0

    async with aiohttp.ClientSession() as session:
        tasks: List[Coroutine[Any, Any, Dict[str, int]]] = [
            process_test_record(session, rec, args.download_reports, sem)
            for rec in all_records
        ]
        for f in tqdm(
            asyncio.as_completed(tasks), total=len(tasks), desc="Downloading"
        ):
            res = await f
            total_signals += res["signals"]
            total_reports += res["reports"]

    print("\n[Done] Download Finished.")
    print(f"    - Successfully Downloaded TDMS Zips: {total_signals}")
    print(f"    - Successfully Downloaded Report PDFs: {total_reports}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="NHTSA File Downloader (TDMS & PDF Only)"
    )
    parser.add_argument(
        "--download-reports",
        action="store_true",
        help="Download PDF reports in addition to TDMS signal files.",
    )
    cmd_args = parser.parse_args()

    asyncio.run(main(cmd_args))
