"""
API client for interacting with the NHTSA (National Highway Traffic Safety Administration) API.

This module provides a client to fetch vehicle crash test data, handling
pagination, concurrent requests, and data validation.
"""

import asyncio
import math
from typing import Any, Dict, List

import httpx
from src.core.models import NHTSATestMetadata


class NHTSAClient:
    """A client for fetching data from the NHTSA vehicle safety API.

    Handles API configuration, concurrent request management, and pagination
    to efficiently download and parse crash test metadata.

    Attributes:
        base_url: The base URL for the NHTSA API.
        headers: The HTTP headers to use for API requests.
        timeout: The timeout in seconds for each request.
        semaphore: An asyncio.Semaphore to limit concurrent requests.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initializes the NHTSAClient.

        Args:
            config: A dictionary containing 'api' and 'collection' settings.
        """
        api_config: Dict[str, Any] = config.get("api", {})
        coll_config: Dict[str, Any] = config.get("collection", {})

        raw_url: str = api_config.get(
            "base_url", "https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1"
        )
        self.base_url: str = raw_url.rstrip("/")

        self.headers: Dict[str, str] = api_config.get("headers", {})
        self.timeout: int = coll_config.get("timeout_seconds", 60)
        max_tasks: int = coll_config.get("max_concurrent_tasks", 5)
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(max_tasks)

    async def fetch_page(
        self, client: httpx.AsyncClient, page_num: int
    ) -> List[Dict[str, Any]]:
        """Fetches a single page of test results from the API.

        Args:
            client: An httpx.AsyncClient instance.
            page_num: The page number to fetch.

        Returns:
            A list of raw result dictionaries from the API page, or an
            empty list if the request fails.
        """
        url = f"{self.base_url}/vehicle-database-test-results"
        params = {"pageNumber": page_num}
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()
            return data.get("results", [])
        except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as e:
            print(f"  [!] Error fetching page {page_num}: {e}")
            return []

    async def fetch_all_data(self) -> List[NHTSATestMetadata]:
        """Fetches all data records from the API in parallel.

        This method first determines the total number of pages and then
        creates concurrent tasks to fetch all pages, respecting the semaphore
        limit. It then parses the raw results into Pydantic models.

        Returns:
            A list of NHTSATestMetadata objects containing the validated
            test data.
        """
        url = f"{self.base_url}/vehicle-database-test-results"

        async with httpx.AsyncClient(
            headers=self.headers, timeout=self.timeout
        ) as client:
            print("  [Init] Checking total records...")
            try:
                resp = await client.get(url, params={"pageNumber": 0})
                resp.raise_for_status()
                data = resp.json()

                meta = data.get("meta", {})
                pagination = meta.get("pagination", {})
                total_count = pagination.get("total", 0)
                page_size = pagination.get("count", 20)

                if total_count == 0:
                    print(f"  [!] Total count is 0. Meta dump: {meta}")
                    return []

                total_pages = math.ceil(total_count / page_size)
                print(
                    f"  [Init] Found {total_count} records ({total_pages} pages). "
                    "Starting parallel fetch..."
                )

            except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as e:
                print(f"  [!] Initialization failed: {e}")
                return []

            async def protected_fetch(p_num: int) -> List[Dict[str, Any]]:
                """A wrapper to fetch a page within the semaphore context."""
                async with self.semaphore:
                    if p_num % 50 == 0:
                        print(f"  ... fetching page {p_num}/{total_pages}")
                    return await self.fetch_page(client, p_num)

            tasks = [protected_fetch(i) for i in range(total_pages)]
            pages_data = await asyncio.gather(*tasks)

            all_raw_results: List[Dict[str, Any]] = []
            for page in pages_data:
                all_raw_results.extend(page)

            print(f"  [Done] Downloaded {len(all_raw_results)} raw records.")
            return self._parse_results(all_raw_results)

    def _parse_results(
        self, raw_results: List[Dict[str, Any]]
    ) -> List[NHTSATestMetadata]:
        """Parses raw API results into a list of Pydantic models.

        Args:
            raw_results: A list of dictionaries from the API.

        Returns:
            A list of validated NHTSATestMetadata objects.
        """
        valid_models: List[NHTSATestMetadata] = []
        error_count = 0
        print("  [Parsing] Converting to data models...")
        for i, item in enumerate(raw_results):
            try:
                valid_models.append(NHTSATestMetadata(**item))
            except Exception as e:
                error_count += 1
                if error_count <= 3:
                    print(f"    [!] Validation Error (Item {i}): {e}")
                    print(f"        Data sample: {str(item)[:100]}...")
                continue

        if error_count > 3:
            print(f"    ... and {error_count - 3} more validation errors.")

        if valid_models:
            print(f"  [Success] Successfully parsed {len(valid_models)} items.")
        else:
            print(f"  [Fail] All {len(raw_results)} items failed validation.")

        return valid_models
