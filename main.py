# -*- coding: utf-8 -*-
"""
NHTSA 충돌 테스트 메타데이터 수집을 위한 메인 실행 스크립트.

이 스크립트는 설정된 범위 내의 테스트 ID에 대한 메타데이터를 NHTSA API로부터
비동기적으로 수집하고, 데이터베이스에 저장하는 역할을 수행합니다.

주요 기능:
- 데이터베이스에 이미 존재하는 테스트 ID를 제외하고 수집 대상을 동적으로 선정.
- 대규모 요청을 작은 '청크(chunk)' 단위로 분할하여 안정적으로 처리.
- 각 청크 처리 후 즉시 데이터베이스에 저장하여, 중단 시에도 데이터 손실 최소화.
"""

import asyncio
import sys
import warnings
from datetime import datetime
from typing import List

from config import settings
from src.api.client import NHTSAClient
from src.core.models import NHTSARecord
from src.utils.storage import DatabaseHandler


def initialize_environment() -> None:
    """
    Windows 환경에서 asyncio 실행을 위한 이벤트 루프 정책을 설정합니다.

    Python 3.8 이상, Windows 환경에서 `SelectorEventLoop`가 기본값이 되면서
    발생할 수 있는 `RuntimeError`를 방지하기 위함입니다.
    """
    if sys.platform == "win32":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def main() -> None:
    """
    메타데이터 수집 프로세스의 메인 로직을 수행합니다.
    """
    start_time = datetime.now()
    print(
        f"=== NHTSA Metadata Scanner Started at {start_time.strftime('%H:%M:%S')} ==="
    )

    initialize_environment()

    # 1. Initialize core components.
    db = DatabaseHandler()
    client = NHTSAClient()

    # 2. Identify target test IDs for fetching.
    # Excludes IDs that already exist in the database.
    existing_ids = db.get_existing_ids()
    all_possible_ids = range(settings.MIN_TEST_NO, settings.MAX_TEST_NO + 1)
    target_ids = [tid for tid in all_possible_ids if tid not in existing_ids]

    print(f"\n[*] Target Range: {settings.MIN_TEST_NO} ~ {settings.MAX_TEST_NO}")
    print(f"[*] Records to fetch: {len(target_ids)} (Existing: {len(existing_ids)})")

    if not target_ids:
        print("    - All data is up to date.")
        return

    # 3. Process fetching and saving in chunks for stability.
    CHUNK_SIZE = 50
    total_chunks = (len(target_ids) + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"[*] Processing in {total_chunks} chunks (Size: {CHUNK_SIZE})...\n")

    for i in range(0, len(target_ids), CHUNK_SIZE):
        chunk_ids = target_ids[i : i + CHUNK_SIZE]

        # Asynchronously fetch a batch of records from the API.
        records: List[NHTSARecord] = await client.fetch_batch(chunk_ids)

        # Save valid records to the database immediately.
        if records:
            db.save_records(records)
            print(
                f"    -> Saved {len(records)} records from chunk {i // CHUNK_SIZE + 1}/{total_chunks}"
            )
        # Optional: log empty chunks if necessary for debugging.
        # else:
        #     print(f"    -> Empty chunk {i // CHUNK_SIZE + 1}/{total_chunks}")

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n[Success] All finished in {duration}.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")