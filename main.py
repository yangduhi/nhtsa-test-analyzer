import asyncio
import aiohttp
import json
import os
import sys
import warnings
from datetime import datetime
from tqdm.asyncio import tqdm

# [설정 파일 임포트]
import config

# ---------------------------------------------------------
# 1. 환경 설정 및 초기화
# ---------------------------------------------------------

# Windows 환경에서 asyncio Loop 정책 설정 (DeprecationWarning 억제)
if sys.platform == "win32":
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 출력 디렉토리 확인 및 생성
if not os.path.exists(config.OUTPUT_DIR):
    os.makedirs(config.OUTPUT_DIR)

# ---------------------------------------------------------
# 2. 분석용 필드 매핑 정의 (Analytical Mapping)
# ---------------------------------------------------------
# API의 난해한 약어(VEHTWT)를 분석하기 쉬운 이름(spec_weight_kg)으로 변환합니다.
ANALYTICAL_MAPPING = {
    # [차량 제원 Specs]
    "VEHTWT": "spec_weight_kg",  # 차량 테스트 중량
    "VEHLEN": "spec_length_mm",  # 전장
    "VEHWID": "spec_width_mm",  # 전폭
    "WHLBAS": "spec_wheelbase_mm",  # 휠베이스
    "ENGINED": "spec_engine_desc",  # 엔진 설명
    "ENGDSP": "spec_engine_disp",  # 배기량 (리터)
    "BODYD": "spec_body_type",  # 바디 타입
    "VIN": "spec_vin",  # 차대번호
    # [충돌 물리량 Physics]
    "VEHSPD": "crash_speed_kph",  # 충돌 속도 (km/h)
    "CRBANG": "crash_angle_deg",  # 충돌 각도 (Crab Angle)
    "PDOF": "crash_pdof_deg",  # 주충격 방향 (Principal Direction of Force)
    # [손상 데이터 Damage]
    "VDI": "damage_vdi_code",  # 차량 변형 지수 (Vehicle Deformation Index)
    "TOTCRV": "damage_total_crush",  # 총 파손 깊이 (Total Crush)
    # [충돌 깊이 프로파일 Crush Profile]
    "DPD1": "crush_profile_c1",
    "DPD2": "crush_profile_c2",
    "DPD3": "crush_profile_c3",
    "DPD4": "crush_profile_c4",
    "DPD5": "crush_profile_c5",
    "DPD6": "crush_profile_c6",
}

# ---------------------------------------------------------
# 3. 유틸리티 함수
# ---------------------------------------------------------


async def fetch_json(session, url):
    """
    ID 기반 직접 조회를 위한 비동기 요청 함수
    """
    try:
        async with session.get(
            url, headers=config.API_HEADERS, timeout=config.TIMEOUT_SECONDS
        ) as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception:
        return None


def get_value_case_insensitive(record, possible_keys):
    """
    여러 키 후보 중 하나라도 존재하는 값을 찾아 반환합니다.
    """
    for key in possible_keys:
        if key in record and record[key] is not None:
            return record[key]
    return None


# ---------------------------------------------------------
# 4. 핵심 로직: ID 직접 순회 + 데이터 정제
# ---------------------------------------------------------


async def fetch_direct_range():
    """
    Test No 6931부터 13000번까지 직접 순회하며,
    Metadata 엔드포인트를 통해 상세 정보를 수집하고 정제합니다.
    """
    start_id = config.MIN_TEST_NO  # 6931
    end_id = 20000  # [설정] 전체 데이터 수집 목표 (2025년 포함 범위)

    print(f"[*] Starting Direct Scan from {start_id} to {end_id} (Analytical Mode)...")
    print(f"    - Endpoint: .../metadata/{{id}}")
    print(f"    - Concurrency: {config.MAX_CONCURRENT_REQUESTS}")

    base_detail_url = "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1/vehicle-database-test-results/metadata"

    valid_records = []
    # 동시 요청 수 제한 (Config 참조)
    sem = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)

    # ----------------------------------------------
    # 내부 함수: 분석용 데이터 추출 및 타입 변환
    # ----------------------------------------------
    def extract_analytical_data(raw_veh):
        extracted = {}
        for api_key, readable_key in ANALYTICAL_MAPPING.items():
            val = raw_veh.get(api_key)

            # 값 유효성 체크
            if val is not None and val != "":
                # 숫자형 데이터(kg, mm, kph, deg, c1~6)는 float으로 변환 시도
                if any(x in readable_key for x in ["_kg", "_mm", "_kph", "_deg", "_c"]):
                    try:
                        extracted[readable_key] = float(val)
                    except ValueError:
                        extracted[readable_key] = val  # 변환 실패 시 원본 문자열 유지
                else:
                    extracted[readable_key] = val
            else:
                extracted[readable_key] = None
        return extracted

    # ----------------------------------------------
    # 내부 함수: 개별 ID 처리 워커
    # ----------------------------------------------
    async def fetch_test_id(session, test_id):
        url = f"{base_detail_url}/{test_id}"

        async with sem:
            data = await fetch_json(session, url)
            if not data:
                return None

            # JSON 파싱 구조: results -> [0] -> TEST & VEHICLE
            results_wrapper = data.get("results", [])
            if not results_wrapper:
                return None

            first_wrapper = results_wrapper[0]

            # 1. 테스트 정보 추출 (Test Info)
            test_info = first_wrapper.get("TEST", {})

            # 2. 차량 정보 추출 (Vehicle List)
            vehicle_list = first_wrapper.get("VEHICLE", [])
            if not vehicle_list:
                return None

            # 3. 차량 선별 로직 (Target Selection)
            target_vehicle = None
            for veh in vehicle_list:
                # 실제 키: MAKED, YEAR, MODELD
                make = get_value_case_insensitive(
                    veh, ["MAKED", "vehicleMake", "Make", "make"]
                )
                year = get_value_case_insensitive(
                    veh, ["YEAR", "modelYear", "Year", "year"]
                )

                make = str(make).upper() if make else ""
                try:
                    year = int(year) if year else 0
                except:
                    year = 0

                # NHTSA 장벽/썰매 제외
                if make != "NHTSA" and year > 0:
                    target_vehicle = veh
                    break

            # 실제 차량이 없으면 리스트 첫 번째 사용 (Fallback)
            if not target_vehicle and vehicle_list:
                target_vehicle = vehicle_list[0]

            if target_vehicle:
                # [핵심] 분석하기 좋게 데이터 정제 (Flattening)
                analytical_data = extract_analytical_data(target_vehicle)

                return {
                    # A. 기본 식별자
                    "test_no": test_id,
                    # B. 테스트 분류 정보
                    "test_title": test_info.get("TITLE"),
                    "test_type": test_info.get("TSTTYPD"),  # 예: NCAP
                    "test_config": test_info.get("TSTCFND"),  # 예: VEHICLE INTO BARRIER
                    # C. 차량 기본 정보 (표준화)
                    "make": get_value_case_insensitive(
                        target_vehicle, ["MAKED", "Make"]
                    )
                    or "Unknown",
                    "model": get_value_case_insensitive(
                        target_vehicle, ["MODELD", "Model"]
                    )
                    or "Unknown",
                    "model_year": get_value_case_insensitive(
                        target_vehicle, ["YEAR", "Year"]
                    ),
                    # D. 분석용 데이터 (Flattened & Renamed)
                    **analytical_data,
                    # E. 원본 데이터 보존 (Raw Backup)
                    "raw_data": target_vehicle,
                }
            return None

    # ----------------------------------------------
    # 실행 루프
    # ----------------------------------------------
    async with aiohttp.ClientSession() as session:
        # 태스크 생성
        tasks = [fetch_test_id(session, tid) for tid in range(start_id, end_id + 1)]

        # tqdm 진행률 표시와 함께 실행
        for f in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="    - Processing Records",
        ):
            result = await f
            if result:
                valid_records.append(result)

    print(f"    - [Done] Collected {len(valid_records)} analytical records.")
    return valid_records


# ---------------------------------------------------------
# 5. 저장 단계
# ---------------------------------------------------------


def save_by_year(records):
    print(f"\n[*] Saving Data by Year to '{config.OUTPUT_DIR}'...")

    grouped = {}
    unknown_list = []

    for record in records:
        year = record.get("model_year")

        if year:
            try:
                year_int = int(year)
                # 2010 ~ 2030 범위 데이터만 그룹화
                if 2009 < year_int < 2030:
                    if year_int not in grouped:
                        grouped[year_int] = []
                    grouped[year_int].append(record)
                    continue
            except ValueError:
                pass
        unknown_list.append(record)

    # 연도별 파일 저장
    sorted_years = sorted(grouped.keys())
    total_saved = 0
    for year in sorted_years:
        items = grouped[year]
        filename = os.path.join(config.OUTPUT_DIR, f"nhtsa_{year}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=4, ensure_ascii=False)
        total_saved += len(items)
        print(f"    - Saved {len(items)} records to {filename}")

    # Unknown 파일 저장
    if unknown_list:
        filename = os.path.join(config.OUTPUT_DIR, "nhtsa_unknown.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(unknown_list, f, indent=4, ensure_ascii=False)
        print(f"    - Saved {len(unknown_list)} records to {filename}")

    print(f"[Summary] Total records saved: {total_saved + len(unknown_list)}")


# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------


async def main():
    start_time = datetime.now()
    print(f"=== NHTSA Direct Scanner Started at {start_time.strftime('%H:%M:%S')} ===")

    # 1. ID 직접 조회 및 정제
    records = await fetch_direct_range()

    if not records:
        print("[!] No records found. Check connection or ID range.")
        return

    # 2. 저장하기
    save_by_year(records)

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n[Success] All tasks finished in {duration}.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
