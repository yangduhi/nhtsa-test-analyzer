# config.py
"""Configuration settings for the NHTSA data collection project.

This file centralizes all static configuration variables, such as API endpoints,
request parameters, directory paths, and analysis settings, to make them
easily accessible and manageable across the application.
"""

import os
from typing import Dict

# ---------------------------------------------------------
# 1. Project Settings
# ---------------------------------------------------------
PROJECT_NAME: str = "NHTSA_Frontal_Crash_Analysis"
VERSION: str = "1.0.1"  # 버전 업그레이드

# ---------------------------------------------------------
# 2. Collection Settings (데이터 수집)
# ---------------------------------------------------------
# YAML의 target_years: [2010, 2025]에 해당
# 2010년 데이터의 시작점인 Test No 6931번
MIN_TEST_NO: int = 6931

# [추가] 수집할 최대 Test ID (main.py에서 이동됨)
MAX_TEST_NO: int = 20000

# [중요] 403 방지를 위해 YAML 설정대로 2로 낮춤 (기존 30 -> 2)
MAX_CONCURRENT_REQUESTS: int = 2
TIMEOUT_SECONDS: int = 60

# ---------------------------------------------------------
# 3. API Settings (통신)
# ---------------------------------------------------------
BASE_URL: str = (
    "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1/vehicle-database-test-results"
)

# [중요] 봇 차단 방지 헤더 (YAML 내용 이식)
API_HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nhtsa.gov/",
}

# ---------------------------------------------------------
# 4. Storage Settings (저장 경로)
# ---------------------------------------------------------
# YAML의 raw_dir, processed_dir 등
DATA_ROOT: str = "data"
RAW_DIR: str = os.path.join(DATA_ROOT, "raw")
PROCESSED_DIR: str = os.path.join(DATA_ROOT, "processed")
LOG_DIR: str = "./logs"

# 사용할 최종 출력 폴더 지정 (main.py에서 사용)
OUTPUT_DIR: str = "nhtsa_data"

# ---------------------------------------------------------
# 5. Analysis Settings (추후 분석용)
# ---------------------------------------------------------
CFC_FILTER_CLASS: int = 60
UNIT_SYSTEM: str = "SI"
MAPPING_STRICTNESS: float = 0.8
