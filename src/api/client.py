import httpx
import asyncio
from typing import List
from src.core.models import NHTSATestMetadata, SignalMetadata
from pathlib import Path

class NHTSAClient:
    """NHTSA NRD API 클라이언트"""
    BASE_URL = "https://nrd.api.nhtsa.dot.gov/Tests/Vehicle"

    def __init__(self):
        self.limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)

    async def fetch_tests_by_year(self, year: int) -> List[NHTSATestMetadata]:
        """특정 연도의 시험 목록을 비동기로 가져옵니다."""
        url = f"{self.BASE_URL}/ModelYear/{year}"
        
        async with httpx.AsyncClient(limits=self.limits, timeout=30.0) as client:
            try:
                response = await client.get(url, params={"format": "json"})
                response.raise_for_status()
                data = response.json()
                
                return [NHTSATestMetadata(**item) for item in data.get("Results", [])]
            except httpx.HTTPStatusError as e:
                print(f"HTTP error occurred: {e}")
                return []
            except Exception as e:
                print(f"An error occurred: {e}")
                return []

    # async def fetch_signals(self, test_num: int) -> List[SignalMetadata]:
    #     """특정 시험의 센서 목록 조회"""
    #     async with httpx.AsyncClient(headers=self.headers) as client:
    #         resp = await client.get(
    #             f"{self.base_url}/tests/{test_num}/signals?format=json"
    #         )
    #         if resp.status_code != 200:
    #             return []
    #         return [
    #             SignalMetadata(**item)
    #             for item in resp.json().get("results", [])
    #             if "ACCELERATION" in item.get("sensor", "").upper()
    #         ]

    # async def download_file(self, url: str, save_path: Path):
    #     """데이터 파일 스트리밍 다운로드"""
    #     async with httpx.AsyncClient(headers=self.headers) as client:
    #         async with client.stream("GET", url) as resp:
    #             with open(save_path, "wb") as f:
    #                 async for chunk in resp.aiter_bytes():
    #                     f.write(chunk)
