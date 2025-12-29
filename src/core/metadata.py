import asyncio
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
from loguru import logger
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class MetadataCrawler:
    def __init__(self, start_year: int = 2010, end_year: int = 2025, config: dict = None):
        self.start_year = start_year
        self.end_year = end_year
        self.base_url = "https://api.nhtsa.gov/SafetyRatings"
        self.headers = config.get("api", {}).get("headers", {}) if config else {}

        # 경로 설정
        self.raw_dir = Path("data/raw/metadata")
        self.processed_dir = Path("data/processed")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch_models_for_make_year(
        self, client: httpx.AsyncClient, year: int, make: str
    ) -> List[Dict[str, Any]]:
        """
        특정 연도와 제조사의 모델 목록을 호출합니다.
        """
        url = f"{self.base_url}/modelyear/{year}/make/{make}"
        response = await client.get(url, params={"format": "json"})
        response.raise_for_status()
        data = response.json().get("Results", [])
        return data

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch_vehicle_details(
        self, client: httpx.AsyncClient, year: int, make: str, model: str
    ) -> List[Dict[str, Any]]:
        """
        특정 연도, 제조사, 모델의 상세 차량 정보를 호출합니다.
        """
        url = f"{self.base_url}/modelyear/{year}/make/{make}/model/{model}"
        response = await client.get(url, params={"format": "json"})
        response.raise_for_status()
        data = response.json().get("Results", [])
        return data

    def process_to_master_csv(self, all_data: List[Dict[str, Any]]):
        """
        수집된 모든 데이터를 하나의 데이터프레임으로 통합하고 저장합니다.
        """
        if not all_data:
            logger.warning("No data to process.")
            return

        df = pd.DataFrame(all_data)

        # 필요한 컬럼만 추출 및 정렬
        cols_map = {
            "VehicleId": "vehicle_id",
            "ModelYear": "model_year",
            "Make": "make",
            "Model": "model",
            "VehicleDescription": "description",
        }
        # API 응답에 모든 키가 존재하지 않을 수 있으므로, 있는 키만 사용
        existing_cols = {k: v for k, v in cols_map.items() if k in df.columns}
        df = df.rename(columns=existing_cols)[list(existing_cols.values())]

        master_file = self.processed_dir / "metadata_master.csv"
        df.to_csv(master_file, index=False, encoding="utf-8-sig")
        logger.success(f"Master metadata created: {master_file} ({len(df)} records)")

    async def run(self):
        all_vehicle_details = []
        async with httpx.AsyncClient(timeout=30.0, headers=self.headers) as client:
            for year in range(self.start_year, self.end_year + 1):
                # 1. 연도의 모든 제조사 가져오기
                makes_url = f"{self.base_url}/modelyear/{year}"
                makes_response = await client.get(makes_url, params={"format": "json"})
                makes_response.raise_for_status()
                makes = makes_response.json().get("Results", [])
                logger.info(f"Year {year}: Found {len(makes)} makes.")

                # 2. 각 제조사/모델에 대한 상세 정보 병렬로 가져오기
                tasks = []
                for make_info in makes:
                    make_name = make_info["Make"]
                    models_data = await self.fetch_models_for_make_year(client, year, make_name)
                    for model_info in models_data:
                        model_name = model_info["Model"]
                        tasks.append(
                            self.fetch_vehicle_details(client, year, make_name, model_name)
                        )
                
                if tasks:
                    results = await asyncio.gather(*tasks)
                    # 2차원 리스트를 1차원으로 병합
                    flat_results = [item for sublist in results for item in sublist]
                    all_vehicle_details.extend(flat_results)
                    logger.info(f"Year {year}: Fetched details for {len(flat_results)} vehicles.")

        # 3. 모든 차량 정보를 CSV로 저장
        self.process_to_master_csv(all_vehicle_details)
        
        # Raw 데이터 저장
        raw_file = self.raw_dir / "all_vehicles.json"
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(all_vehicle_details, f, ensure_ascii=False, indent=4)
        logger.info(f"All vehicle details saved to {raw_file}")
