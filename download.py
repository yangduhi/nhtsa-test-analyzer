"""
Downloader module for NHTSA test analyzer.
Updated: Adds auto-unzip functionality for TDMS files.
"""

import asyncio
import os
import sys
import warnings
import sqlite3
import aiofiles
import httpx
import zipfile  # [추가] 압축 해제를 위한 모듈
from typing import List, Tuple
from loguru import logger
from tqdm.asyncio import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from src.utils.storage import DatabaseHandler


class FileDownloader:
    def __init__(self):
        self.db = DatabaseHandler()
        self.base_dir = os.path.join(settings.DATA_ROOT, "downloads")
        os.makedirs(self.base_dir, exist_ok=True)
        self.sem = asyncio.Semaphore(4)
        self.headers = settings.API_HEADERS

    async def _download_file(
        self, client: httpx.AsyncClient, url: str, dest_path: str
    ) -> bool:
        """파일 다운로드 및 (ZIP일 경우) 자동 압축 해제"""
        try:
            # 1. 파일 다운로드
            async with self.sem:
                async with client.stream(
                    "GET", url, follow_redirects=True, timeout=120.0
                ) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(dest_path, "wb") as f:
                        async for chunk in resp.aiter_bytes():
                            await f.write(chunk)

            # 2. [추가] ZIP 파일 자동 압축 해제 로직
            if dest_path.lower().endswith(".zip"):
                await self._extract_zip(dest_path)

            return True
        except Exception as e:
            logger.warning(f"Download/Extract failed: {url} -> {e}")
            return False

    async def _extract_zip(self, zip_path: str):
        """ZIP 파일을 동일한 폴더에 해제하고 원본은 (선택적으로) 삭제"""
        try:
            # 비동기 실행을 위해 run_in_executor 사용 (파일 I/O 블로킹 방지)
            loop = asyncio.get_event_loop()
            extract_dir = os.path.dirname(zip_path)

            def extract():
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(extract_dir)

            await loop.run_in_executor(None, extract)
            logger.info(f"Extracted: {os.path.basename(zip_path)}")

            # (선택) 압축 해제 후 zip 파일 삭제하려면 아래 주석 해제
            # os.remove(zip_path)

        except zipfile.BadZipFile:
            logger.error(f"Invalid zip file: {zip_path}")

    # ... (이하 get_pending_tasks, update_task_status 등 기존 메서드 동일) ...
    def get_pending_tasks(self, limit: int = 100) -> List[Tuple]:
        conn = _get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, test_no, file_type, url, filename 
                FROM download_queue 
                WHERE status = 'PENDING' 
                LIMIT ?
            """,
                (limit,),
            )
            return cursor.fetchall()
        finally:
            conn.close()

    def update_task_status(self, task_id: int, status: str):
        conn = _get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE download_queue SET status = ? WHERE id = ?", (status, task_id)
            )
            conn.commit()
        finally:
            conn.close()

    async def process_batch(self):
        tasks_data = self.get_pending_tasks(
            limit=10
        )  # 압축 해제 부하 고려하여 배치 크기 조절 권장 (50 -> 10)

        if not tasks_data:
            return False

        async with httpx.AsyncClient(headers=self.headers) as client:
            download_tasks = []

            for task in tasks_data:
                task_id, test_no, ftype, url, fname = task

                # 저장 경로: data/downloads/{test_no}/v06940.zip
                save_dir = os.path.join(self.base_dir, str(test_no))
                os.makedirs(save_dir, exist_ok=True)
                dest_path = os.path.join(save_dir, fname)

                download_tasks.append(
                    self.execute_download(client, task_id, url, dest_path)
                )

            # tqdm 설정
            for f in tqdm(
                asyncio.as_completed(download_tasks),
                total=len(download_tasks),
                desc="Processing Files",
                unit="file",
            ):
                await f

        return True

    async def execute_download(self, client, task_id, url, dest_path):
        # 파일이 이미 존재하면 스킵 (압축 해제된 파일 체크는 복잡하므로 zip 존재 여부만 체크)
        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            self.update_task_status(task_id, "DONE")
            return True

        success = await self._download_file(client, url, dest_path)
        status = "DONE" if success else "ERROR"
        self.update_task_status(task_id, status)
        return success


def _get_db_connection():
    return sqlite3.connect(settings.DB_PATH)


async def main():
    downloader = FileDownloader()
    print("=== NHTSA File Downloader & Extractor Started ===")

    while True:
        has_work = await downloader.process_batch()
        if not has_work:
            print("Queue empty. Waiting...")
            await asyncio.sleep(5)
            continue

        print("Batch processed.")
        await asyncio.sleep(1)


if __name__ == "__main__":
    if sys.platform == "win32":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Stopped.")
