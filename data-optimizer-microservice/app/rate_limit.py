import time
from fastapi import HTTPException

requests_log = {}
MAX_REQ = 10
WINDOW = 60

def rate_limiter(api_key):
    now = time.time()
    logs = requests_log.get(api_key, [])
    logs = [t for t in logs if now - t < WINDOW]
    if len(logs) >= MAX_REQ:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    logs.append(now)
    requests_log[api_key] = logs
