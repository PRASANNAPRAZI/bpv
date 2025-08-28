from fastapi import HTTPException

TRIGGER_KEY = "trigger-secret"
READ_KEYS = {"read-secret-1", "read-secret-2"}

def verify_api_key(api_key: str):
    if api_key == TRIGGER_KEY or api_key in READ_KEYS:
        return api_key
    raise HTTPException(status_code=401, detail="Invalid API Key")
