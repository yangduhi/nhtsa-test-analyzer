import asyncio
import httpx
from typing import Optional, List, Any
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.asyncio import tqdm

from config import settings

# [수정] 변경된 모델 이름(NHTSARecord)으로 import 수정
from src.core.models import NHTSARecord
from src.core.parser import parse_record_to_model


class NHTSAClient:
    """
    통합 API 클라이언트.
    비동기 세션 관리, 재시도 로직, 파싱 파이프라인을 캡슐화합니다.
    """

    def __init__(self):
        self.base_url = settings.BASE_URL
        self.headers = settings.API_HEADERS
        # 동시성 제어를 위한 세마포어
        self.sem = asyncio.Semaphore(settings.MAX_CONCURRENT_REQUESTS)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def _fetch_json(self, client: httpx.AsyncClient, url: str) -> Optional[dict]:
        """내부 Fetch 메서드 (재시도 로직 적용)"""
        try:
            resp = await client.get(url, timeout=settings.TIMEOUT_SECONDS)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP Error {e.response.status_code} for {url}")
            raise
        except Exception as e:
            logger.error(f"Network error for {url}: {e}")
            raise

    # [수정] 리턴 타입 힌트 변경 (NHTSATestMetadata -> NHTSARecord)
    async def fetch_and_parse_test(
        self, client: httpx.AsyncClient, test_id: int
    ) -> Optional[NHTSARecord]:
        """단일 테스트 ID 조회 및 Pydantic 모델 파싱"""
        url = f"{self.base_url}/vehicle-database-test-results/metadata/{test_id}"

        async with self.sem:  # 동시 요청 제한
            raw_data = await self._fetch_json(client, url)

            if not raw_data:
                return None

            # [수정] Parser 결과를 변수에 먼저 담습니다.
            parsed_data = parse_record_to_model(test_id, raw_data)

            # [추가] 유효한 데이터가 있을 때만 '성공(Success)' 로그를 띄웁니다.
            # 이 로그가 떠야 "아, 진짜로 수집되고 있구나" 하고 안심하실 수 있습니다.
            if parsed_data:
                logger.success(f"Found Data: Test ID {test_id}")

            return parsed_data

    # [수정] 리턴 타입 힌트 변경
    async def fetch_batch(self, target_ids: List[int]) -> List[NHTSARecord]:
        """다수의 ID를 병렬로 수집 (tqdm 프로그레스 바 적용)"""
        results = []

        # HTTP 연결 풀 제한 해제 (동시성 성능 확보)
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)

        async with httpx.AsyncClient(headers=self.headers, limits=limits) as client:
            tasks = [self.fetch_and_parse_test(client, tid) for tid in target_ids]

            # tqdm으로 진행 상황 시각화
            for f in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc="Fetching Records",
                unit="rec",
                ncols=100,
            ):
                res = await f
                if res:
                    results.append(res)

        logger.info(f"Batch fetch complete. Valid records: {len(results)}")
        return results
