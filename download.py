"""
NHTSA Data File Downloader (운영 최적화 버전).

주요 기능:
1. 재시도 로직 (Tenacity): 네트워크 불안정 시 최대 3회 재시도 및 지수 백오프 적용.
2. 에러 로깅 (Loguru): 다운로드 실패 항목을 'download_errors.log'에 기록.
3. 무결성 검증: 다운로드 후 파일 크기가 0이거나 파일이 없으면 실패로 간주하고 삭제.
4. 증분 다운로드: 이미 파일이 존재하고 크기가 0보다 크면 다운로드를 건너뜀.
"""

import argparse
import asyncio
import glob
import json
import os
import sys
from typing import Any, Coroutine, Dict, List

import aiofiles
import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from loguru import logger
from tqdm.asyncio import tqdm

import config

# --- 설정 및 경로 ---
BASE_DOWNLOAD_DIR: str = "data/raw"
SIGNAL_DIR_NAME: str = "signals"
REPORT_DIR_NAME: str = "reports"
ERROR_LOG_FILE: str = "download_errors.log"

# --- 로거 설정 ---
# 에러 레벨 이상의 로그만 파일에 기록 (10MB 단위로 로테이션)
logger.add(ERROR_LOG_FILE, rotation="10 MB", level="ERROR", encoding="utf-8")


@retry(
    stop=stop_after_attempt(3),  # 최대 3회 재시도
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 2초에서 10초 사이 지수 백오프
    retry=retry_if_exception_type(
        (aiohttp.ClientError, asyncio.TimeoutError, ValueError, IOError)
    ),
    reraise=True,
)
async def _download_core(
    session: aiohttp.ClientSession, url: str, save_path: str
) -> None:
    """실제 다운로드 및 무결성 검증을 수행하는 핵심 함수."""
    timeout = aiohttp.ClientTimeout(total=300)  # 파일당 최대 5분 제한

    async with session.get(url, timeout=timeout) as response:
        if response.status != 200:
            raise aiohttp.ClientError(f"HTTP Status {response.status}")

        # 저장 디렉토리 생성
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # 비동기 파일 쓰기
        async with aiofiles.open(save_path, mode="wb") as f:
            await f.write(await response.read())

    # [무결성 검증] 파일이 존재하지 않거나 크기가 0이면 예외 발생 (재시도 유도)
    if not os.path.exists(save_path) or os.path.getsize(save_path) == 0:
        if os.path.exists(save_path):
            os.remove(save_path)
        raise ValueError(f"Integrity check failed: {save_path} is empty")


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    save_path: str,
    semaphore: asyncio.Semaphore,
) -> bool:
    """동시성 제어 및 에러 로깅을 포함한 다운로드 래퍼 함수."""
    if not url or not isinstance(url, str) or url.lower() == "none":
        return False

    # 이미 파일이 존재하고 내용이 있다면 스킵
    if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
        return True

    try:
        async with semaphore:
            await _download_core(session, url, save_path)
            return True
    except Exception as e:
        # 재시도 끝에 최종 실패 시 로그 기록
        logger.error(
            f"DOWNLOAD_FAILED | URL: {url} | Path: {save_path} | Reason: {str(e)}"
        )
        return False


async def process_test_record(
    session: aiohttp.ClientSession,
    record: Dict[str, Any],
    download_reports: bool,
    semaphore: asyncio.Semaphore,
) -> Dict[str, int]:
    """단일 테스트 레코드의 TDMS 및 PDF 링크를 처리."""
    test_no = record.get("test_no")
    year = record.get("model_year", "unknown_year")

    # JSON 구조에서 links와 reports 추출
    links = record.get("links")
    if not isinstance(links, dict):
        links = {}
    reports = record.get("reports", [])

    results = {"signals": 0, "reports": 0}

    # 1. 신호 데이터 다운로드 (URL_TDMS 키 사용)
    tdms_url = links.get("URL_TDMS")
    if tdms_url:
        filename = os.path.basename(tdms_url)
        save_path = os.path.join(
            BASE_DOWNLOAD_DIR, SIGNAL_DIR_NAME, str(year), str(test_no), filename
        )
        if await download_file(session, tdms_url, save_path, semaphore):
            results["signals"] += 1

    # 2. PDF 리포트 다운로드 (옵션 활성화 시)
    if download_reports and reports:
        for rep in reports:
            pdf_url = rep.get("URL")
            if pdf_url and pdf_url.lower().endswith(".pdf"):
                filename = os.path.basename(pdf_url)
                save_path = os.path.join(
                    BASE_DOWNLOAD_DIR,
                    REPORT_DIR_NAME,
                    str(year),
                    str(test_no),
                    filename,
                )
                if await download_file(session, pdf_url, save_path, semaphore):
                    results["reports"] += 1
    return results


def load_metadata_records() -> List[Dict[str, Any]]:
    """nhtsa_data 폴더에서 모든 JSON 레코드를 로드."""
    json_files = glob.glob(os.path.join(config.OUTPUT_DIR, "nhtsa_*.json"))
    if not json_files:
        print(f"[!] No metadata JSON files found in '{config.OUTPUT_DIR}'.")
        return []

    all_records: List[Dict[str, Any]] = []
    for jf in json_files:
        with open(jf, "r", encoding="utf-8") as f:
            try:
                all_records.extend(json.load(f))
            except json.JSONDecodeError:
                print(f"[!] Warning: Could not decode JSON from {jf}.")
    return all_records


async def main(args: argparse.Namespace) -> None:
    """다운로드 프로세스 조율."""
    all_records = load_metadata_records()
    if not all_records:
        return

    print(f"[*] Found {len(all_records)} total test records.")
    print("[*] Downloading Signals (Target: TDMS zip)...")
    if args.download_reports:
        print("[*] Downloading Reports (Target: PDF)...")

    # 동시 다운로드 개수 제한 (서버 부하 및 차단 방지)
    sem = asyncio.Semaphore(5)

    total_signals = 0
    total_reports = 0

    async with aiohttp.ClientSession(headers=config.API_HEADERS) as session:
        tasks: List[Coroutine[Any, Any, Dict[str, int]]] = [
            process_test_record(session, rec, args.download_reports, sem)
            for rec in all_records
        ]

        # tqdm 프로그레스 바 출력
        for f in tqdm(
            asyncio.as_completed(tasks), total=len(tasks), desc="Downloading", ncols=100
        ):
            res = await f
            total_signals += res["signals"]
            total_reports += res["reports"]

    print("\n[Done] Download Finished.")
    print(f"    - Successfully Downloaded TDMS Zips: {total_signals}")
    print(f"    - Successfully Downloaded Report PDFs: {total_reports}")
    print(f"    - Errors logged to: {ERROR_LOG_FILE}")


if __name__ == "__main__":
    # Windows 환경에서의 비동기 정책 설정 (Python 3.8+ 대응)
    if sys.platform == "win32":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except AttributeError:
            pass

    parser = argparse.ArgumentParser(description="NHTSA File Downloader (TDMS & PDF)")
    parser.add_argument(
        "--download-reports",
        action="store_true",
        help="Download PDF reports in addition to TDMS signal files.",
    )
    cmd_args = parser.parse_args()

    try:
        asyncio.run(main(cmd_args))
    except KeyboardInterrupt:
        print("\n[!] Download interrupted by user.")
