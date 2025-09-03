import time
from fastapi import HTTPException, Request
import os

RATE_LIMIT = int(os.getenv("DATA_OPT_RATE_LIMIT", "10"))   # requests
RATE_PERIOD = int(os.getenv("DATA_OPT_RATE_PERIOD", "60")) # seconds

_client_requests = {}

def rate_limiter(request: Request):
    client_ip = request.client.host
    now = time.time()
    arr = _client_requests.setdefault(client_ip, [])
    arr[:] = [ts for ts in arr if now - ts < RATE_PERIOD]
    if len(arr) >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    arr.append(now)
    return True