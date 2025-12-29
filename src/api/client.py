import httpx
import asyncio
from typing import List, Dict, Any
from src.core.models import NHTSATestMetadata, SignalMetadata


class NHTSAClient:
    """NHTSA NRD API 클라이언트"""

    def __init__(self, config: Dict[str, Any]):
        self.api_config = config.get("api", {})
        self.coll_config = config.get("collection", {})

        # YAML에서 URL 로드 (없으면 기본값)
        raw_url = self.api_config.get(
            "base_url", "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1"
        )
        self.base_url = raw_url.rstrip("/")

        self.headers = self.api_config.get("headers", {})
        self.timeout = self.coll_config.get("timeout_seconds", 30)

        max_tasks = self.coll_config.get("max_concurrent_tasks", 2)
        self.semaphore = asyncio.Semaphore(max_tasks)

    async def fetch_tests_by_year(self, year: int) -> List[NHTSATestMetadata]:
        url = f"{self.base_url}/vehicle-database-test-results"
        params = {"modelYear": year, "format": "json"}

        async with self.semaphore:
            async with httpx.AsyncClient(
                headers=self.headers, timeout=self.timeout
            ) as client:
                try:
                    response = await client.get(url, params=params)
                    if response.status_code == 404:
                        print(f"  [!] 404 Not Found: {url}")
                        return []
                    response.raise_for_status()

                    data = response.json()
                    results = data.get("Results") or data.get("results") or []
                    return [NHTSATestMetadata(**item) for item in results]
                except Exception as e:
                    print(f"  [!] Error fetching {year}: {e}")
                    return []
