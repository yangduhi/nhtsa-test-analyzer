import httpx
import asyncio
from typing import List, Dict, Any
from src.core.models import NHTSATestMetadata


class NHTSAClient:
    """NHTSA NRD API 클라이언트 (OData 지원)"""

    def __init__(self, config: Dict[str, Any]):
        self.api_config = config.get("api", {})
        self.coll_config = config.get("collection", {})

        # URL 설정
        raw_url = self.api_config.get(
            "base_url", "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1"
        )
        self.base_url = raw_url.rstrip("/")

        self.headers = self.api_config.get("headers", {})
        self.timeout = self.coll_config.get("timeout_seconds", 60)

        max_tasks = self.coll_config.get("max_concurrent_tasks", 2)
        self.semaphore = asyncio.Semaphore(max_tasks)

    async def fetch_tests_by_year(self, year: int) -> List[NHTSATestMetadata]:
        url = f"{self.base_url}/vehicle-database-test-results"

        # [핵심 수정] OData 문법($filter, $top) 적용
        # 일반 파라미터 대신 OData 표준을 따릅니다.
        params = {
            "$filter": f"modelYear eq {year}",  # 연도 필터링 문법
            "$top": "5000",  # 한 번에 가져올 최대 개수 (20개 제한 해제)
        }

        async with self.semaphore:
            async with httpx.AsyncClient(
                headers=self.headers, timeout=self.timeout
            ) as client:
                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    data = response.json()

                    # API 응답 파싱
                    results = data.get("Results") or data.get("results") or []

                    # [데이터 검증] 수동 필터링 (API가 필터를 무시할 경우를 대비)
                    metadata_list = [NHTSATestMetadata(**item) for item in results]

                    valid_data = [
                        m
                        for m in metadata_list
                        if m.model_year == year  # 연도가 일치하는 것만 남김
                    ]

                    return valid_data

                except Exception as e:
                    print(f"  [!] Error fetching {year}: {e}")
                    return []
