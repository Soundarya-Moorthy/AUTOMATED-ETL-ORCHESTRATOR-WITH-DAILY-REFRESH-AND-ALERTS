
import time
import requests

def retry_request(url, params, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    raise Exception("API request failed after retries")
