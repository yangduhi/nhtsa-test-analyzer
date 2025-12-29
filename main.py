import asyncio
import yaml
import sys
import json
from pathlib import Path

# [핵심] 프로젝트 루트를 절대 경로로 등록하여 src 패키지 인식 보장
ROOT = Path(__file__).parent.absolute()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.api.client import NHTSAClient


async def setup_environment(config: dict):
    """데이터 저장 및 로그를 위한 디렉토리 자동 생성"""
    paths = [config["storage"]["raw_dir"], config["paths"]["log_dir"]]
    for p in paths:
        path = Path(p)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            print(f"[*] Created directory: {p}")


async def run_collector(nhtsa: NHTSAClient, config: dict):
    """연도별 데이터 수집 및 필터링 저장 로직"""
    # 설정 로드
    years_range = config["collection"]["target_years"]
    years = list(range(years_range[0], years_range[1] + 1))

    # 필터링 키워드 (대문자로 변환하여 비교)
    target_type = config["collection"].get("crash_type", "Frontal").upper()
    raw_dir = Path(config["storage"]["raw_dir"])

    print(f"[*] Target Years: {years_range}")
    print(f"[*] Filtering for Keyword: '{target_type}'")

    for year in years:
        print(f"\n[*] Processing {year}...")
        tests = await nhtsa.fetch_tests_by_year(year)

        if not tests:
            print(f"  - No data found for {year}.")
            continue

        # --- [디버깅용 코드 시작] ---
        # 실제 API가 반환하는 test_type 값을 확인하기 위함
        sample_types = [t.test_type for t in tests[:5]]
        print(f"  [Debug] Sample Test Types: {sample_types}")
        # --- [디버깅용 코드 끝] ---

        # 필터링 로직: test_type 안에 target_type 문자열이 포함되어 있는지 확인
        filtered = [
            t for t in tests if t.test_type and target_type in t.test_type.upper()
        ]

        print(f"  - Total: {len(tests)}, Filtered: {len(filtered)}")

        # 필터링된 데이터 저장
        if filtered:
            save_path = raw_dir / f"nhtsa_{year}.json"
            save_data = [t.model_dump() for t in filtered]

            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(save_data, f, indent=4, ensure_ascii=False)
            print(f"  - Saved to {save_path}")


async def main():
    """메인 실행부"""
    config_path = ROOT / "config" / "settings.yaml"

    if not config_path.exists():
        print("[!] config/settings.yaml not found.")
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    await setup_environment(config)

    # 클라이언트 초기화
    nhtsa = NHTSAClient(config=config)

    # 수집 시작
    await run_collector(nhtsa, config)


if __name__ == "__main__":
    # Windows 환경 비동기 이슈 해결을 위한 설정
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
