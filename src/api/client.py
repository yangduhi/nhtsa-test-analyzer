import httpx
import asyncio
import math
from typing import List, Dict, Any
from src.core.models import NHTSATestMetadata


class NHTSAClient:
    def __init__(self, config: Dict[str, Any]):
        self.api_config = config.get("api", {})
        self.coll_config = config.get("collection", {})

        raw_url = self.api_config.get(
            "base_url", "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1"
        )
        self.base_url = raw_url.rstrip("/")

        self.headers = self.api_config.get("headers", {})
        self.timeout = self.coll_config.get("timeout_seconds", 60)
        max_tasks = self.coll_config.get("max_concurrent_tasks", 5)
        self.semaphore = asyncio.Semaphore(max_tasks)

    async def fetch_page(self, client: httpx.AsyncClient, page_num: int) -> List[dict]:
        """단일 페이지 수집"""
        url = f"{self.base_url}/vehicle-database-test-results"
        params = {"pageNumber": page_num}
        try:
            response = await client.get(url, params=params)
            if response.status_code != 200:
                return []
            data = response.json()
            return data.get("results", [])
        except Exception:
            return []

    async def fetch_all_data(self) -> List[NHTSATestMetadata]:
        """전체 데이터를 병렬로 빠르게 수집"""
        url = f"{self.base_url}/vehicle-database-test-results"

        async with httpx.AsyncClient(
            headers=self.headers, timeout=self.timeout
        ) as client:
            print("  [Init] Checking total records...")
            try:
                resp = await client.get(url, params={"pageNumber": 0})
                data = resp.json()

                meta = data.get("meta", {})
                pagination = meta.get("pagination", {})

                total_count = pagination.get("total", 0)
                page_size = pagination.get("count", 20)

                if total_count == 0:
                    print(f"  [!] Total count is 0. Meta dump: {meta}")
                    return []

                total_pages = math.ceil(total_count / page_size)
                print(
                    f"  [Init] Found {total_count} records ({total_pages} pages). Starting parallel fetch..."
                )

            except Exception as e:
                print(f"  [!] Initialization failed: {e}")
                return []

            async def protected_fetch(p_num):
                async with self.semaphore:
                    if p_num % 50 == 0:
                        print(f"  ... fetching page {p_num}/{total_pages}")
                    return await self.fetch_page(client, p_num)

            tasks = [protected_fetch(i) for i in range(total_pages)]
            pages_data = await asyncio.gather(*tasks)

            all_raw_results = []
            for page in pages_data:
                all_raw_results.extend(page)

            print(f"  [Done] Downloaded {len(all_raw_results)} raw records.")

            # [수정된 부분] 에러 원인 추적
            valid_models = []
            error_count = 0

            print("  [Parsing] Converting to data models...")
            for i, item in enumerate(all_raw_results):
                try:
                    valid_models.append(NHTSATestMetadata(**item))
                except Exception as e:
                    error_count += 1
                    # 첫 3개의 에러만 상세 출력 (화면 도배 방지)
                    if error_count <= 3:
                        print(f"    [!] Validation Error (Item {i}): {e}")
                        # 문제의 데이터 일부 출력
                        print(f"        Data sample: {str(item)[:100]}...")
                    continue

            if valid_models:
                print(f"  [Success] Successfully parsed {len(valid_models)} items.")
            else:
                print(f"  [Fail] All {len(all_raw_results)} items failed validation.")

            return valid_models
