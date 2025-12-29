"""
Crawls the NHTSA Safety Ratings API to build a master metadata file of vehicles.

This module fetches vehicle information by iterating through years, makes, and
models to collect a comprehensive list of vehicle IDs and their descriptions.
"""

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


class MetadataCrawler:
    """Crawls NHTSA API for vehicle metadata based on year, make, and model.

    Attributes:
        start_year: The first year in the range to crawl.
        end_year: The last year in the range to crawl.
        base_url: The base URL for the Safety Ratings API.
        headers: HTTP headers for the requests.
        raw_dir: Directory to save raw JSON output.
        processed_dir: Directory to save the final processed CSV file.
    """

    def __init__(
        self,
        start_year: int = 2010,
        end_year: int = 2025,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initializes the MetadataCrawler.

        Args:
            start_year: The year to start crawling from.
            end_year: The year to end crawling at.
            config: A configuration dictionary, typically for API headers.
        """
        self.start_year: int = start_year
        self.end_year: int = end_year
        self.base_url: str = "https://api.nhtsa.gov/SafetyRatings"
        self.headers: Dict[str, str] = (
            config.get("api", {}).get("headers", {}) if config else {}
        )

        self.raw_dir: Path = Path("data/raw/metadata")
        self.processed_dir: Path = Path("data/processed")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch_models_for_make_year(
        self, client: httpx.AsyncClient, year: int, make: str
    ) -> List[Dict[str, Any]]:
        """Fetches a list of models for a given make and model year.

        Args:
            client: An httpx.AsyncClient instance.
            year: The model year.
            make: The vehicle manufacturer.

        Returns:
            A list of dictionaries, each representing a vehicle model.
        """
        url = f"{self.base_url}/modelyear/{year}/make/{make}"
        response = await client.get(url, params={"format": "json"})
        response.raise_for_status()
        data = response.json()
        return data.get("Results", [])

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch_vehicle_details(
        self, client: httpx.AsyncClient, year: int, make: str, model: str
    ) -> List[Dict[str, Any]]:
        """Fetches detailed vehicle information for a specific vehicle.

        Args:
            client: An httpx.AsyncClient instance.
            year: The model year.
            make: The vehicle manufacturer.
            model: The vehicle model.

        Returns:
            A list of dictionaries containing detailed vehicle trims/variants.
        """
        url = f"{self.base_url}/modelyear/{year}/make/{make}/model/{model}"
        response = await client.get(url, params={"format": "json"})
        response.raise_for_status()
        data = response.json()
        return data.get("Results", [])

    def process_to_master_csv(self, all_data: List[Dict[str, Any]]) -> None:
        """Converts the collected data into a single master CSV file.

        Args:
            all_data: A list of all vehicle detail dictionaries collected.
        """
        if not all_data:
            logger.warning("No data to process.")
            return

        df = pd.DataFrame(all_data)

        cols_map = {
            "VehicleId": "vehicle_id",
            "ModelYear": "model_year",
            "Make": "make",
            "Model": "model",
            "VehicleDescription": "description",
        }
        existing_cols = {k: v for k, v in cols_map.items() if k in df.columns}
        
        df = df[list(existing_cols.keys())].rename(columns=existing_cols)

        master_file = self.processed_dir / "metadata_master.csv"
        df.to_csv(master_file, index=False, encoding="utf-8-sig")
        logger.success(f"Master metadata created: {master_file} ({len(df)} records)")

    async def run(self) -> None:
        """Executes the end-to-end crawling and processing workflow."""
        all_vehicle_details: List[Dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30.0, headers=self.headers) as client:
            for year in range(self.start_year, self.end_year + 1):
                makes_url = f"{self.base_url}/modelyear/{year}"
                makes_response = await client.get(makes_url, params={"format": "json"})
                makes_response.raise_for_status()
                makes = makes_response.json().get("Results", [])
                logger.info(f"Year {year}: Found {len(makes)} makes.")

                tasks = []
                for make_info in makes:
                    make_name = make_info.get("Make")
                    if not make_name:
                        continue
                    
                    models_data = await self.fetch_models_for_make_year(
                        client, year, make_name
                    )
                    for model_info in models_data:
                        model_name = model_info.get("Model")
                        if not model_name:
                            continue
                        tasks.append(
                            self.fetch_vehicle_details(
                                client, year, make_name, model_name
                            )
                        )

                if tasks:
                    results = await asyncio.gather(*tasks)
                    flat_results = [item for sublist in results for item in sublist]
                    all_vehicle_details.extend(flat_results)
                    logger.info(
                        f"Year {year}: Fetched details for {len(flat_results)} vehicles."
                    )

        self.process_to_master_csv(all_vehicle_details)

        raw_file = self.raw_dir / "all_vehicles.json"
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(all_vehicle_details, f, ensure_ascii=False, indent=4)
        logger.info(f"All vehicle details saved to {raw_file}")
