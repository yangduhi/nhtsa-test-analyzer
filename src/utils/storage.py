"""
Module for saving and managing processed data in the filesystem.
"""

import glob
import json
import os
from typing import Any, Dict, List, Set

import config


def get_existing_ids(output_dir: str) -> Set[int]:
    """Scans the output directory for JSON files and returns a set of existing test IDs.

    This is used to prevent re-downloading and processing data that already
    exists locally.

    Args:
        output_dir: The directory where the nhtsa_*.json files are stored.

    Returns:
        A set of integers representing the test_no of all found records.
    """
    existing_ids: Set[int] = set()
    file_pattern = os.path.join(output_dir, "nhtsa_*.json")
    files = glob.glob(file_pattern)

    print("[*] Checking for existing local data...")
    for f_path in files:
        try:
            with open(f_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                for record in data:
                    t_no = record.get("test_no")
                    if t_no:
                        existing_ids.add(int(t_no))
        except (json.JSONDecodeError, ValueError, TypeError):
            print(f"    - Warning: Could not parse existing data in {f_path}.")
            pass  # Ignore corrupted or invalid files

    print(f"    - Found {len(existing_ids)} existing records locally.")
    return existing_ids


def save_by_year(records: List[Dict[str, Any]]) -> None:
    """Saves records into separate JSON files, merging with existing data.

    This function groups new records by year, reads the corresponding existing
    JSON file (if any), merges the new records, de-duplicates by 'test_no',
    and overwrites the file with the combined, sorted data.

    Args:
        records: A list of newly processed record dictionaries to save.
    """
    if not records:
        print("\n[*] No new records to save.")
        return

    print(f"\n[*] Saving Data by Year to '{config.OUTPUT_DIR}'...")

    # Group new records by year
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    unknown_list: List[Dict[str, Any]] = []
    for record in records:
        year = record.get("model_year")
        if year:
            try:
                year_int = int(year)
                if 2009 < year_int < 2030:
                    if year_int not in grouped:
                        grouped[year_int] = []
                    grouped[year_int].append(record)
                    continue
            except (ValueError, TypeError):
                pass
        unknown_list.append(record)

    # Process and save for each year group
    for year, items in grouped.items():
        filename = os.path.join(config.OUTPUT_DIR, f"nhtsa_{year}.json")
        _merge_and_save(filename, items)

    # Process and save for unknown year group
    if unknown_list:
        filename = os.path.join(config.OUTPUT_DIR, "nhtsa_unknown.json")
        _merge_and_save(filename, unknown_list)


def _merge_and_save(filename: str, new_items: List[Dict[str, Any]]) -> None:
    """Reads a JSON file, merges new items, de-duplicates, and saves back.

    Args:
        filename: The path to the JSON file.
        new_items: A list of new records to merge into the file.
    """
    existing_data: List[Dict[str, Any]] = []
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass  # Overwrite if file is corrupted

    # De-duplicate by test_no, giving precedence to new items
    record_map = {record["test_no"]: record for record in existing_data}
    for item in new_items:
        record_map[item["test_no"]] = item

    # Sort the final list by test number for consistency
    final_list = sorted(record_map.values(), key=lambda x: x["test_no"])

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(final_list, f, indent=4, ensure_ascii=False)
    print(f"    - Saved/Updated {len(final_list)} records to {os.path.basename(filename)}")