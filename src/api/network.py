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
            elif response.status == 404:
                # 404는 데이터가 없는 경우이므로 조용히 처리
                return None
            else:
                print(
                    f"    - DEBUG_NETWORK: Failed to fetch {url}, status: {response.status}"
                )
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

        # 1. 데이터가 아예 없는 경우 (HTTP 404 or Error)
        if not raw_data:
            return None

        # 2. 결과 래퍼가 비어있는 경우
        results_wrapper = raw_data.get("results", [])
        if not results_wrapper:
            # 결과 자체가 비어있으면 데이터가 없는 것으로 간주
            return None

        first_wrapper = results_wrapper[0]

        # 3. 껍데기만 있는 경우 (Soft 404: TEST/VEHICLE 모두 None)
        # 서버에서 200 OK를 주지만 실제 데이터는 없는 결번 처리
        if first_wrapper.get("TEST") is None and first_wrapper.get("VEHICLE") is None:
            return None

        # 4. VEHICLE 정보가 없는 경우
        vehicle_list = first_wrapper.get("VEHICLE", [])
        if not vehicle_list:
            # 유효한 테스트 번호 같으나 차량 정보가 없는 경우에만 제한적으로 로깅
            if _problematic_id_count < _MAX_DEBUG_PROBLEM_IDS:
                print(
                    f"    - DEBUG_NETWORK: Valid Test ID {test_id} but no vehicle data."
                )
                _problematic_id_count += 1
            return None

        # 5. 파서(parser.py)를 통한 변환 및 필터링
        # 이 과정에서 links와 reports 정보가 포함됩니다.
        parsed_record = parse_record(test_id, raw_data)

        if not parsed_record:
            return None

        return parsed_record


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
        tasks = [_fetch_and_parse_id(session, tid, sem) for tid in target_ids]

        # tqdm 설정: 전체 진행 상황을 깔끔하게 보여줌
        for f in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="    - Processing Records",
            ncols=100,  # 프로그레스 바 너비 고정
            unit="rec",  # 단위 표시
        ):
            result = await f
            if result:
                valid_records.append(result)

    print(f"    - [Done] Collected {len(valid_records)} analytical records.")
    return valid_records
