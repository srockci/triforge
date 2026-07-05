import requests, sys

# Get all runs
r = requests.get('http://127.0.0.1:8000/board/runs', timeout=5)
runs = r.json().get('runs', [])
print(f'Total runs: {len(runs)}')
for rr in runs[:3]:
    print(f'  {rr["run_id"]} - {rr["status"]}')

if runs:
    rid = runs[0]['run_id']
    rd = requests.get(f'http://127.0.0.1:8000/board/runs/{rid}', timeout=5)
    print(f'\nDetail for {rid}:')
    print(f'  Status: {rd.status_code}')
    data = rd.json()
    print(f'  Keys: {list(data.keys())[:8]}')
    print(f'  Phase: {data.get("phase")}')
    print(f'  Status: {data.get("status")}')
