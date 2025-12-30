"""
Main entry point for the NHTSA data collection (Chunked Version).
"""

import asyncio
import sys
import warnings
from datetime import datetime
from typing import List

# Pydantic Settings 사용
from config import settings

# 통합된 Client 및 DB Handler 사용
from src.api.client import NHTSAClient
from src.utils.storage import DatabaseHandler
from src.core.models import NHTSARecord  # 타입 힌팅용


# ... (initialize_environment 함수는 그대로 유지) ...
def initialize_environment() -> None:
    if sys.platform == "win32":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def main() -> None:
    start_time = datetime.now()
    print(
        f"=== NHTSA Scanner V3 (Chunked Save) Started at {start_time.strftime('%H:%M:%S')} ==="
    )

    initialize_environment()

    # 1. 인프라 초기화
    db = DatabaseHandler()
    client = NHTSAClient()

    # 2. 수집 대상 식별
    min_test_no = settings.MIN_TEST_NO
    max_test_no = settings.MAX_TEST_NO
    existing_ids = db.get_existing_ids()

    all_possible_ids = range(min_test_no, max_test_no + 1)
    target_ids = [tid for tid in all_possible_ids if tid not in existing_ids]

    print(f"\n[*] Target Range: {min_test_no} ~ {max_test_no}")
    print(f"[*] Records to fetch: {len(target_ids)} (Existing: {len(existing_ids)})")

    if not target_ids:
        print("    - All data is up to date.")
        return

    # 3. 청크 단위 수집 및 저장 (안전성 확보)
    CHUNK_SIZE = 50  # 50개씩 끊어서 처리
    total_chunks = (len(target_ids) + CHUNK_SIZE - 1) // CHUNK_SIZE

    print(f"[*] Processing in {total_chunks} chunks (Size: {CHUNK_SIZE})...\n")

    for i in range(0, len(target_ids), CHUNK_SIZE):
        chunk_ids = target_ids[i : i + CHUNK_SIZE]

        # 데이터 수집
        records: List[NHTSARecord] = await client.fetch_batch(chunk_ids)

        # 유효한 데이터가 있으면 즉시 저장
        if records:
            db.save_records(records)
            print(
                f"    -> Saved {len(records)} records from chunk {i // CHUNK_SIZE + 1}/{total_chunks}"
            )
        else:
            # 빈 청크일 경우 진행 상황만 표시
            # print(f"    -> Empty chunk {i//CHUNK_SIZE + 1}/{total_chunks}")
            pass

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n[Success] All finished in {duration}.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
