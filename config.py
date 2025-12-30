# -*- coding: utf-8 -*-
"""
프로젝트의 모든 설정을 중앙에서 관리하는 모듈.

이 모듈은 Pydantic의 BaseSettings를 활용하여, 환경 변수나 .env 파일로부터
설정 값을 안전하게 로드합니다. 이를 통해 설정 값을 코드와 분리하여
유연성과 보안을 높입니다.

Attributes:
    settings (Settings): 프로젝트 전역에서 사용될 설정 객체 인스턴스.
"""

import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    애플리케이션의 모든 설정을 정의하고 관리하는 클래스.

    .env 파일에 정의된 환경 변수를 우선적으로 로드하며, 값이 없을 경우
    코드에 명시된 기본값을 사용합니다.

    Attributes:
        PROJECT_NAME (str): 프로젝트의 이름.
        VERSION (str): 프로젝트의 현재 버전.
        MIN_TEST_NO (int): 데이터 수집을 시작할 NHTSA 테스트 번호.
        MAX_TEST_NO (int): 데이터 수집을 종료할 NHTSA 테스트 번호.
        MAX_CONCURRENT_REQUESTS (int): API 동시 요청 제한 수.
        TIMEOUT_SECONDS (int): API 요청 시 타임아웃 시간(초).
        BASE_URL (str): NHTSA API의 기본 URL.
        API_HEADERS (dict): API 요청 시 사용할 HTTP 헤더.
        DATA_ROOT (str): 데이터 파일(DB, 로그 등)을 저장할 루트 디렉토리.
        DB_PATH (str): SQLite 데이터베이스 파일의 경로.
        LOG_DIR (str): 로그 파일을 저장할 디렉토리.
    """
    # --- Project Metadata ---
    PROJECT_NAME: str = "NHTSA_Data_Collection"
    VERSION: str = "1.0.0"

    # --- Data Collection Scope ---
    MIN_TEST_NO: int = 6931
    MAX_TEST_NO: int = 20000

    # --- Network Configuration ---
    MAX_CONCURRENT_REQUESTS: int = 2
    TIMEOUT_SECONDS: int = 60
    BASE_URL: str = "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1"

    # --- HTTP Headers for API Requests ---
    API_HEADERS: dict = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nhtsa.gov/",
    }

    # --- Storage Configuration ---
    DATA_ROOT: str = "data"
    DB_PATH: str = os.path.join(DATA_ROOT, "nhtsa_data.db")
    LOG_DIR: str = "logs"

    # Pydantic model configuration to load from a .env file.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


# Create a single, globally accessible instance of the settings.
settings = Settings()

# --- Directory Initialization ---
# Ensure that necessary data and log directories exist upon application startup.
os.makedirs(settings.DATA_ROOT, exist_ok=True)
os.makedirs(settings.LOG_DIR, exist_ok=True)
