import asyncio
import yaml
import sys
import json
from pathlib import Path

# 프로젝트 루트 경로 설정
ROOT = Path(__file__).parent.absolute()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.api.client import NHTSAClient


async def setup_environment(config: dict):
    """디렉토리 생성"""
    paths = [config["storage"]["raw_dir"], config["paths"]["log_dir"]]
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


async def run_collector(nhtsa: NHTSAClient, config: dict):
    """연도별 모든 데이터 수집 (필터링 없음)"""
    years_range = config["collection"]["target_years"]
    years = list(range(years_range[0], years_range[1] + 1))

    raw_dir = Path(config["storage"]["raw_dir"])

    print(f"[*] Target Years: {years_range}")
    print("[*] Collection Mode: FULL DATA (No Filtering)")

    for year in years:
        print(f"\n[*] Processing {year}...")

        # API 호출 (OData 적용된 client.py 사용)
        tests = await nhtsa.fetch_tests_by_year(year)

        if not tests:
            print(f"  - No data found for {year}.")
            continue

        # [디버깅] 다양한 충돌 유형이 섞여 들어오는지 확인
        sample_types = list(
            set([t.test_type for t in tests[:10]])
        )  # 중복제거하여 샘플 확인
        print(f"  [Debug] Fetched Types: {sample_types}")

        # [수정] 필터링 과정 없이 전체 데이터 저장
        # TestNo가 Primary Key 역할을 하므로 모든 데이터를 확보합니다.
        total_count = len(tests)
        save_path = raw_dir / f"nhtsa_all_{year}.json"

        # Pydantic 모델 -> Dict 변환
        save_data = [t.model_dump() for t in tests]

        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(save_data, f, indent=4, ensure_ascii=False)

        print(f"  - Saved {total_count} tests to {save_path}")


async def main():
    config_path = ROOT / "config" / "settings.yaml"

    if not config_path.exists():
        print("[!] config/settings.yaml not found.")
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    await setup_environment(config)
    nhtsa = NHTSAClient(config=config)

    try:
        await run_collector(nhtsa, config)
    except KeyboardInterrupt:
        print("\n[!] Stopped by user.")
    except Exception as e:
        print(f"\n[!] Critical Error: {e}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
