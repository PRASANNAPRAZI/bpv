from fastapi import FastAPI, Request, HTTPException, Depends#, Query
from app.pipeline import run_workflow
# from app.auth import verify_api_key
from app.rate_limit import rate_limiter
from pathlib import Path
from fastapi.responses import JSONResponse
import json
import gzip
from app.auth import create_token, verify_token

app = FastAPI(title="Data Optimizer")

@app.post("/login")
def login(username: str, password: str):
    if username == "admin" and password == "password123":
        token = create_token({"sub": username, "role": "admin"})
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/secure-data")
def secure_data(user=Depends(verify_token)):
    return {"message": "This is protected data", "user": user}


@app.post("/optimize")
async def optimize(request: Request, _auth: bool = Depends(verify_token)):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Expected a list of records (JSON array)")

    results = run_workflow(payload)
    return {"status": "success", "results": results,}


@app.get("/retrieve")
def retrieve(_auth: bool = Depends(verify_token), _rl: bool = Depends(rate_limiter)):
    out_file = Path("outputs/logic_results.json")
    if not out_file.exists():
        raise HTTPException(status_code=404, detail="No processed results found")

    try:
        content = json.loads(out_file.read_text(encoding="utf-8"))
        return {"status": "success", "data": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading output file: {str(e)}")



BLOB_CONTAINER = Path("azure_blob")

def read_blob_file(asset_id: str):
    file_path = BLOB_CONTAINER / f"{asset_id}.blob"
    if not file_path.exists():
        raise FileNotFoundError(f"No blob found for asset_id={asset_id}")
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        return json.load(f)

@app.get("/read_blob/{asset_id}")
async def read_blob(asset_id: str, _auth: bool = Depends(verify_token)):
    if api_key != "trigger-secret":
        raise HTTPException(status_code=401, detail="Invalid API key")

    try:
        data = read_blob_file(asset_id)
        return {"asset_id": asset_id, "data": data}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read blob: {e}")


# Error handlers
@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"status": "error", "detail": exc.detail})
