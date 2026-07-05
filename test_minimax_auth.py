import requests
import json

# Test different MiniMax API endpoints and headers
test_cases = [
    {
        "name": "Bearer token",
        "url": "https://api.minimaxi.com/v1/models",
        "headers": {
            "Authorization": "Bearer test-key",
            "Content-Type": "application/json"
        }
    },
    {
        "name": "X-API-Key",
        "url": "https://api.minimaxi.com/v1/models", 
        "headers": {
            "X-API-Key": "test-key",
            "Content-Type": "application/json"
        }
    },
    {
        "name": "No auth",
        "url": "https://api.minimaxi.com/v1/models",
        "headers": {
            "Content-Type": "application/json"
        }
    }
]

for test in test_cases:
    print(f"\n--- Testing {test['name']} ---")
    try:
        response = requests.get(test['url'], headers=test['headers'])
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Models found: {len(data.get('models', []))}")
            if 'models' in data:
                for model in data['models'][:3]:  # Show first 3 models
                    print(f"  - {model.get('id', 'Unknown')}")
        else:
            print(f"Response: {response.text[:200]}")
    except Exception as e:
        print(f"Error: {e}")