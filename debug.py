import asyncio
import aiohttp
import json

# 테스트할 ID (최신 데이터 중 하나)
TEST_TARGET_ID = 15501 

async def debug_single_id():
    url = f"https://nrd.api.nhtsa.dot.gov/nhtsa/vehicle/api/v1/vehicle-database-test-results/metadata/{TEST_TARGET_ID}"
    print(f"[*] Requesting: {url}")
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            print(f"[*] Status Code: {response.status}")
            
            if response.status != 200:
                print("[!] API Error. Stopping.")
                return

            data = await response.json()
            
            # 1. Raw Data 확인
            results = data.get('results', [])
            print(f"[*] Results Count: {len(results)}")
            
            if not results:
                print("[!] 'results' list is empty.")
                return

            first_item = results[0]
            vehicle_list = first_item.get('VEHICLE', [])
            print(f"[*] Vehicle List Count: {len(vehicle_list)}")

            # 2. 필터링 로직 시뮬레이션
            print("-" * 30)
            print("[*] Checking Filter Logic:")
            
            for i, veh in enumerate(vehicle_list):
                make = veh.get('MAKED') or veh.get('Make')
                year = veh.get('YEAR') or veh.get('modelYear')
                
                print(f"    [Vehicle {i}] MAKE: {make}, YEAR: {year}")
                
                # 필터 조건 확인
                make_str = str(make).upper() if make else ''
                try: year_int = int(year) if year else 0
                except: year_int = 0
                
                if make_str != 'NHTSA' and year_int > 0:
                    print("    -> ✅ PASS (This vehicle would be saved)")
                else:
                    print("    -> ❌ FAIL (Skipped by filter: Make=NHTSA or Year=0)")

if __name__ == "__main__":
    asyncio.run(debug_single_id())