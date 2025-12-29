# config.py

import os

# ---------------------------------------------------------
# 1. Project Settings
# ---------------------------------------------------------
PROJECT_NAME = "NHTSA_Frontal_Crash_Analysis"
VERSION = "1.0.0"

# ---------------------------------------------------------
# 2. Collection Settings (데이터 수집)
# ---------------------------------------------------------
# YAML의 target_years: [2010, 2025]에 해당
# 2010년 데이터의 시작점인 Test No 6931번
MIN_TEST_NO = 6931

# [중요] 403 방지를 위해 YAML 설정대로 2로 낮춤 (기존 30 -> 2)
MAX_CONCURRENT_REQUESTS = 2
TIMEOUT_SECONDS = 60

# ---------------------------------------------------------
# 3. API Settings (통신)
# ---------------------------------------------------------
BASE_URL = (
    "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1/vehicle-database-test-results"
)

# [중요] 봇 차단 방지 헤더 (YAML 내용 이식)
API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nhtsa.gov/",
}

# ---------------------------------------------------------
# 4. Storage Settings (저장 경로)
# ---------------------------------------------------------
# YAML의 raw_dir, processed_dir 등
DATA_ROOT = "data"
RAW_DIR = os.path.join(DATA_ROOT, "raw")
PROCESSED_DIR = os.path.join(DATA_ROOT, "processed")
LOG_DIR = "./logs"

# 사용할 최종 출력 폴더 지정 (main.py에서 사용)
OUTPUT_DIR = "nhtsa_data"  # 또는 os.path.join(RAW_DIR, "json") 등으로 변경 가능

# ---------------------------------------------------------
# 5. Analysis Settings (추후 분석용)
# ---------------------------------------------------------
CFC_FILTER_CLASS = 60
UNIT_SYSTEM = "SI"
MAPPING_STRICTNESS = 0.8
