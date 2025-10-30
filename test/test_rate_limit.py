# test_rate_limit.py

import requests
import time

# API 엔드포인트 주소
URL = "http://localhost:8000/api/market-stats/"
# URL = "http://localhost:8000/api/realtime-price/" # 실시간 API 테스트 시

# 1분 안에 100회를 초과하도록 110회 요청
REQUEST_COUNT = 60

print(f"--- API Rate Limit Test Start ---")
print(f"Target URL: {URL}")
print(f"Sending {REQUEST_COUNT} requests...")

start_time = time.time()
rate_limit_hit = False

for i in range(1, REQUEST_COUNT + 1):
    try:
        response = requests.get(URL)
        
        # HTTP 429: Too Many Requests
        if response.status_code == 429:
            print(f"\n[Request {i}] ✅ SUCCESS: Rate Limit Hit! (429 Too Many Requests)")
            rate_limit_hit = True
            break
        
        # HTTP 200: OK
        elif response.status_code == 200:
            # 너무 많은 로그를 피하기 위해 10번에 한 번만 출력
            if i % 10 == 0:
                print(f"[Request {i}] OK (Status: 200)")
        
        # Other Errors
        else:
            print(f"\n[Request {i}] ❌ FAILED: Received status code {response.status_code}")
            print(response.json())
            break
            
    except requests.ConnectionError:
        print("\n❌ FAILED: Could not connect to server. Is Django running?")
        break
    except Exception as e:
        print(f"\n❌ FAILED: An unexpected error occurred: {e}")
        break

end_time = time.time()
print(f"\n--- Test Finished in {end_time - start_time:.2f} seconds ---")

if not rate_limit_hit:
    print("⚠️ WARNING: Rate limit was not hit. Check settings.py `DEFAULT_THROTTLE_RATES`.")