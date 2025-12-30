# config.py
import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    프로젝트 전역 설정 관리 (Pydantic V2)
    .env 파일에서 환경 변수를 로드하며, 없을 경우 기본값을 사용합니다.
    """

    # Project Info
    PROJECT_NAME: str = "NHTSA_Frontal_Crash_Analysis"
    VERSION: str = "2.0.0"

    # Collection Scope
    MIN_TEST_NO: int = 6931
    MAX_TEST_NO: int = 20000

    # Network Settings
    MAX_CONCURRENT_REQUESTS: int = 2  # 403 방지용 보수적 설정
    TIMEOUT_SECONDS: int = 60
    BASE_URL: str = "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1"

    # Headers (Bot Detection Evasion)
    API_HEADERS: dict = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nhtsa.gov/",
    }

    # Storage Settings
    DATA_ROOT: str = "data"
    DB_PATH: str = os.path.join(DATA_ROOT, "nhtsa_data.db")  # SQLite DB 경로
    LOG_DIR: str = "./logs"

    # Analysis Settings
    CFC_FILTER_CLASS: int = 60

    # .env 파일 로드 설정
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


# 싱글톤 인스턴스 생성
settings = Settings()

# 디렉토리 자동 생성 (초기화 시점 실행)
os.makedirs(settings.DATA_ROOT, exist_ok=True)
os.makedirs(settings.LOG_DIR, exist_ok=True)
