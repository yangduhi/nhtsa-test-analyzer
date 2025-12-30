# -*- coding: utf-8 -*-
"""
NHTSA 원본 데이터 파일(TDMS) 다운로더 모듈.

이 스크립트는 데이터베이스의 'download_queue' 테이블을 주기적으로 폴링하여,
'PENDING' 상태의 파일들을 비동기적으로 다운로드합니다. 다운로드된 파일이
ZIP 아카이브일 경우, 자동으로 압축을 해제합니다.

주요 기능:
- 데이터베이스 기반의 다운로드 큐 시스템.
- `asyncio`와 `httpx`를 사용한 효율적인 비동기 다운로드.
- `tenacity`를 활용한 재시도 로직으로 네트워크 불안정성에 대응.
- ZIP 파일 자동 압축 해제.
- `tqdm`을 통한 진행 상황 시각화.
"""

import asyncio
import os
import sys
import warnings
import zipfile
from typing import List

import aiofiles
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.asyncio import tqdm

from config import settings
from src.utils.storage import DatabaseHandler, DownloadTask


class FileDownloader:
    """
    데이터베이스 큐를 기반으로 파일을 다운로드하고 관리하는 클래스.
    """

    def __init__(self, concurrent_downloads: int = 4):
        """
        FileDownloader를 초기화합니다.

        Args:
            concurrent_downloads (int): 동시에 처리할 최대 다운로드 수.
        """
        self.db = DatabaseHandler()
        self.base_dir = os.path.join(settings.DATA_ROOT, "downloads")
        os.makedirs(self.base_dir, exist_ok=True)
        self.semaphore = asyncio.Semaphore(concurrent_downloads)
        self.headers = settings.API_HEADERS

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _download_file(self, client: httpx.AsyncClient, url: str, dest_path: str) -> None:
        """
        주어진 URL에서 파일을 비동기적으로 스트리밍하여 다운로드합니다.

        Args:
            client (httpx.AsyncClient): HTTP 요청에 사용할 클라이언트.
            url (str): 다운로드할 파일의 URL.
            dest_path (str): 파일이 저장될 로컬 경로.
        
        Raises:
            httpx.HTTPStatusError: API 서버가 에러 상태 코드를 반환할 경우.
        """
        async with self.semaphore:
            async with client.stream("GET", url, follow_redirects=True, timeout=120.0) as resp:
                resp.raise_for_status()
                total_size = int(resp.headers.get("content-length", 0))
                
                # tqdm을 사용하여 파일 다운로드 진행률을 표시합니다.
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=os.path.basename(dest_path)) as progress:
                    async with aiofiles.open(dest_path, "wb") as f:
                        async for chunk in resp.aiter_bytes():
                            await f.write(chunk)
                            progress.update(len(chunk))

    async def _extract_zip(self, zip_path: str) -> None:
        """
        ZIP 파일을 동일한 디렉토리에 압축 해제합니다.

        CPU-bound 작업인 압축 해제를 `run_in_executor`를 사용하여
        이벤트 루프의 블로킹을 방지합니다.

        Args:
            zip_path (str): 압축 해제할 ZIP 파일의 경로.
        """
        try:
            loop = asyncio.get_event_loop()
            extract_dir = os.path.dirname(zip_path)

            def extract():
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(extract_dir)
            
            await loop.run_in_executor(None, extract)
            logger.info(f"Successfully extracted: {os.path.basename(zip_path)}")
            # 원본 ZIP 파일 삭제를 원할 경우 아래 라인의 주석을 해제하세요.
            # os.remove(zip_path)
        except zipfile.BadZipFile:
            logger.error(f"Invalid or corrupted zip file: {zip_path}")
            raise  # Re-raise to mark the task as ERROR

    async def execute_download_task(self, client: httpx.AsyncClient, task: DownloadTask) -> None:
        """
        단일 다운로드 작업을 실행하고, 결과에 따라 DB 상태를 업데이트합니다.

        Args:
            client (httpx.AsyncClient): HTTP 요청에 사용할 클라이언트.
            task (DownloadTask): 실행할 다운로드 작업 객체.
        """
        save_dir = os.path.join(self.base_dir, str(task.test_no))
        os.makedirs(save_dir, exist_ok=True)
        dest_path = os.path.join(save_dir, task.filename)

        # 파일이 이미 존재하고, 크기가 0 이상이면 'DONE'으로 처리.
        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            self.db.update_task_status(task.id, "DONE")
            logger.info(f"Skipping existing file: {task.filename}")
            return

        try:
            await self._download_file(client, task.url, dest_path)
            if dest_path.lower().endswith(".zip"):
                await self._extract_zip(dest_path)
            self.db.update_task_status(task.id, "DONE")
        except Exception as e:
            logger.warning(f"Task failed for {task.url}: {e}")
            self.db.update_task_status(task.id, "ERROR")

    async def process_batch(self, batch_size: int) -> bool:
        """
        지정된 수의 'PENDING' 작업을 가져와 병렬로 처리합니다.

        Args:
            batch_size (int): 한 번에 처리할 작업의 수.

        Returns:
            bool: 처리할 작업이 있었으면 True, 없었으면 False.
        """
        tasks = self.db.get_pending_tasks(limit=batch_size)
        if not tasks:
            return False

        async with httpx.AsyncClient(headers=self.headers, http2=True) as client:
            coroutines = [self.execute_download_task(client, task) for task in tasks]
            await asyncio.gather(*coroutines)
        
        return True


def initialize_environment() -> None:
    """
    Windows 환경에서 asyncio 실행을 위한 이벤트 루프 정책을 설정합니다.
    """
    if sys.platform == "win32":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def main() -> None:
    """
    다운로더를 실행하고, 주기적으로 다운로드 큐를 확인하여 작업을 처리합니다.
    """
    downloader = FileDownloader()
    print("=== NHTSA File Downloader & Extractor Started ===")
    
    while True:
        has_work = await downloader.process_batch(batch_size=10)
        if not has_work:
            print("Download queue is empty. Waiting for new tasks...")
            await asyncio.sleep(30)  # 큐가 비었을 때 대기 시간
            continue
        
        print("Batch processed. Waiting for a moment before next batch...")
        await asyncio.sleep(5)  # 배치 처리 후 짧은 대기


if __name__ == "__main__":
    initialize_environment()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user. Exiting.")