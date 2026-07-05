import requests
import json

# Test MiniMax
url = "http://127.0.0.1:8000/board/models"
data = {
    "provider_key": "minimax",
    "api_key": "test-key",
    "base_url": "https://api.minimaxi.com/v1"
}

print(f"URL: {url}")
print(f"Data: {json.dumps(data)}")

try:
    response = requests.post(url, json=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
    
    # Test DeepSeek
    data2 = {
        "provider_key": "deepseek",
        "api_key": "test-key",
        "base_url": "https://api.deepseek.com/v1"
    }
    print(f"\n--- Testing DeepSeek ---")
    print(f"Data: {json.dumps(data2)}")
    response2 = requests.post(url, json=data2)
    print(f"Status Code: {response2.status_code}")
    print(f"Response: {response2.text}")
    
except Exception as e:
    print(f"Error: {e}")