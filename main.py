import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent))

import asyncio
import yaml
from src.core.metadata import MetadataCrawler
import httpx

async def main():
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    crawler = MetadataCrawler(config=config)
    async with httpx.AsyncClient(timeout=30.0, headers=crawler.headers) as client:
        makes_url = f"{crawler.base_url}/modelyear/2010"
        makes_response = await client.get(makes_url, params={"format": "json"})
        makes_response.raise_for_status()
        makes = makes_response.json().get("Results", [])
        print(makes)

if __name__ == "__main__":
    asyncio.run(main())
