# -*- coding: utf-8 -*-
"""
NHTSA API와의 통신을 담당하는 비동기 클라이언트 모듈.

이 모듈은 `httpx`를 기반으로, API 요청, 재시도 로직, 동시성 제어,
응답 파싱에 이르는 모든 네트워크 통신 과정을 캡슐화합니다.

Classes:
    NHTSAClient: NHTSA API와 상호작용하는 비동기 클라이언트.
"""

import asyncio
from typing import List, Optional

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.asyncio import tqdm

from config import settings
from src.core.models import NHTSARecord
from src.core.parser import parse_record_to_model


class NHTSAClient:
    """
    NHTSA API와 상호작용하며, 데이터 수집 및 파싱을 담당하는 비동기 클라이언트.

    Attributes:
        base_url (str): API의 기본 URL.
        headers (dict): 모든 요청에 사용될 공통 HTTP 헤더.
        semaphore (asyncio.Semaphore): API 서버 부하를 줄이기 위한 동시 요청 제한 장치.
    """

    def __init__(self):
        """NHTSAClient 인스턴스를 초기화합니다."""
        self.base_url = settings.BASE_URL
        self.headers = settings.API_HEADERS
        self.semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_REQUESTS)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_json(self, client: httpx.AsyncClient, url: str) -> Optional[dict]:
        """
        지정된 URL로부터 JSON 데이터를 비동기적으로 가져옵니다.

        네트워크 오류 발생 시 `tenacity`에 의해 설정된 전략에 따라 재시도합니다.
        404 Not Found의 경우, None을 반환하여 정상적인 실패로 처리합니다.

        Args:
            client (httpx.AsyncClient): HTTP 요청에 사용할 클라이언트.
            url (str): 데이터를 가져올 대상 URL.

        Returns:
            Optional[dict]: 성공 시 JSON 응답을 dict로, 404 발생 시 None을 반환.

        Raises:
            httpx.HTTPStatusError: 404 이외의 HTTP 에러 발생 시.
        """
        try:
            resp = await client.get(url, timeout=settings.TIMEOUT_SECONDS)
            if resp.status_code == 404:
                logger.debug(f"404 Not Found for URL: {url}")
                return None
            resp.raise_for_status()  # 4xx, 5xx 에러 발생 시 예외 발생
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP Error {e.response.status_code} for URL {url}")
            raise  # 재시도 로직에 의해 처리되도록 예외를 다시 발생시킴
        except Exception as e:
            logger.error(f"An unexpected network error occurred for {url}: {e}")
            raise

    async def fetch_and_parse_test(self, client: httpx.AsyncClient, test_id: int) -> Optional[NHTSARecord]:
        """
        단일 테스트 ID에 대한 메타데이터를 가져오고 파싱합니다.

        Args:
            client (httpx.AsyncClient): HTTP 요청에 사용할 클라이언트.
            test_id (int): 조회할 테스트의 ID.

        Returns:
            Optional[NHTSARecord]: 파싱에 성공한 경우 NHTSARecord 객체를,
                                   데이터가 없거나 유효하지 않은 경우 None을 반환.
        """
        url = f"{self.base_url}/vehicle-database-test-results/metadata/{test_id}"
        
        async with self.semaphore:
            raw_data = await self._fetch_json(client, url)

        if not raw_data:
            return None

        parsed_record = parse_record_to_model(test_id, raw_data)
        if parsed_record:
            logger.success(f"Successfully fetched and parsed Test ID: {test_id}")
        
        return parsed_record

    async def fetch_batch(self, target_ids: List[int]) -> List[NHTSARecord]:
        """
        여러 테스트 ID에 대한 메타데이터를 병렬로 수집합니다.

        `tqdm`을 사용하여 전체 진행 상황을 시각적으로 표시합니다.

        Args:
            target_ids (List[int]): 수집할 테스트 ID의 리스트.

        Returns:
            List[NHTSARecord]: 수집 및 파싱에 성공한 모든 레코드의 리스트.
        """
        valid_records = []
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)

        async with httpx.AsyncClient(headers=self.headers, limits=limits, http2=True) as client:
            tasks = [self.fetch_and_parse_test(client, tid) for tid in target_ids]
            
            # tqdm을 사용하여 비동기 작업의 진행률을 표시합니다.
            progress_bar = tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc="Fetching Records",
                unit="rec",
                ncols=100
            )

            for future in progress_bar:
                result = await future
                if result:
                    valid_records.append(result)

        logger.info(f"Batch fetch complete. Found {len(valid_records)} valid records.")
        return valid_records