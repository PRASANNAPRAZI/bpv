from fastapi import FastAPI, Request, HTTPException, Depends
from app.pipeline import run_workflow, load_refined_data
from app.auth import verify_api_key
from app.rate_limit import rate_limiter

app = FastAPI()

# Workflow Trigger
@app.post("/optimize")
async def optimize(request: Request, api_key: str = Depends(verify_api_key)):
    data = await request.json()
    result = run_workflow(data)
    return {"message": "Workflow completed", "asset_ids": [r["asset_id"] for r in result]}

# Data Retrieval
@app.get("/retrieve")
async def retrieve(api_key: str = Depends(verify_api_key)):
    rate_limiter(api_key)   # Apply per-key rate limiting
    return load_refined_data()
