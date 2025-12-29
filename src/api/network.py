"""
Module for handling network requests to the NHTSA API.
"""

import asyncio
from typing import Any, Dict, List, Optional

import aiohttp
from tqdm.asyncio import tqdm

import config
from src.core.parser import parse_record

# Counter for problematic test_ids to limit debug output
_problematic_id_count = 0
_MAX_DEBUG_PROBLEM_IDS = 5


async def _fetch_json(
    session: aiohttp.ClientSession, url: str
) -> Optional[Dict[str, Any]]:
    """Fetches JSON data from a given URL asynchronously.

    Args:
        session: The aiohttp client session to use for the request.
        url: The URL to fetch the JSON data from.

    Returns:
        A dictionary containing the JSON data if the request is successful,
        otherwise None.
    """
    try:
        async with session.get(
            url, headers=config.API_HEADERS, timeout=config.TIMEOUT_SECONDS
        ) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"    - DEBUG_NETWORK: Failed to fetch {url}, status: {response.status}")
                return None
    except asyncio.TimeoutError:
        print(f"    - DEBUG_NETWORK: Timeout fetching {url}")
        return None
    except Exception as e:
        print(f"    - DEBUG_NETWORK: Exception fetching {url}: {e}")
        return None


async def _fetch_and_parse_id(
    session: aiohttp.ClientSession, test_id: int, sem: asyncio.Semaphore
) -> Optional[Dict[str, Any]]:
    """Fetches, processes, and refines data for a single test ID.

    Args:
        session: The aiohttp client session.
        test_id: The test number to fetch data for.
        sem: The semaphore to limit concurrent requests.

    Returns:
        A dictionary containing the refined data for the test, or None if
        the data is invalid or not found.
    """
    global _problematic_id_count
    base_url = "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1/vehicle-database-test-results/metadata"
    url = f"{base_url}/{test_id}"

    async with sem:
        raw_data = await _fetch_json(session, url)
        if not raw_data:
            if _problematic_id_count < _MAX_DEBUG_PROBLEM_IDS:
                print(f"    - DEBUG_NETWORK: No raw data for test_id {test_id}")
                _problematic_id_count += 1
            return None
        
        # Check if "results" wrapper exists and is not empty before proceeding
        results_wrapper = raw_data.get("results", [])
        if not results_wrapper:
            if _problematic_id_count < _MAX_DEBUG_PROBLEM_IDS:
                print(f"    - DEBUG_NETWORK: 'results' wrapper is empty for test_id {test_id}. Raw Data: {raw_data}")
                _problematic_id_count += 1
            return None

        # Check if "VEHICLE" list exists and is not empty inside the first result
        first_wrapper = results_wrapper[0]
        vehicle_list = first_wrapper.get("VEHICLE", [])
        if not vehicle_list:
            if _problematic_id_count < _MAX_DEBUG_PROBLEM_IDS:
                print(f"    - DEBUG_NETWORK: 'VEHICLE' list is empty for test_id {test_id}. Raw Data (first_wrapper): {first_wrapper}")
                _problematic_id_count += 1
            return None
        
        return parse_record(test_id, raw_data)


async def fetch_all_test_data(target_ids: List[int]) -> List[Dict[str, Any]]:
    """Fetches and processes data for a given list of test IDs.

    This function fetches and parses metadata for each ID in `target_ids`,
    using a semaphore to limit concurrent requests.

    Args:
        target_ids: A list of integer test IDs to fetch data for.

    Returns:
        A list of dictionaries, where each dictionary is a processed record.
    """
    if not target_ids:
        print("    - No new IDs to fetch.")
        return []

    print(f"[*] Starting Direct Scan for {len(target_ids)} IDs (Analytical Mode)...")
    print(f"    - Endpoint: .../metadata/{{id}}")
    print(f"    - Concurrency: {config.MAX_CONCURRENT_REQUESTS}")

    valid_records: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _fetch_and_parse_id(session, tid, sem)
            for tid in target_ids
        ]
        for f in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="    - Processing Records",
        ):
            result = await f
            if result:
                valid_records.append(result)

    print(f"    - [Done] Collected {len(valid_records)} analytical records.")
    return valid_records