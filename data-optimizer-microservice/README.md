# Data Optimizer Microservice

## 📌 Overview
This Python-based microservice simulates a **data optimization workflow** with FastAPI,
machine learning (HuggingFace Transformers), metadata extraction (spaCy / NLTK),
reinforcement learning (Q-learning), and external storage simulation (Azure Blob / SharePoint).

---

## 🛠️ Setup Instructions

```bash
docker-compose up --build
docker-compose down
```

Dependencies:
- fastapi
- uvicorn
- pandas
- nltk
- spacy
- transformers
- azure-storage-blob
- shareplum

---

## 📡 Sample API Calls

### 1. Trigger Optimization
```bash
curl -X POST "http://127.0.0.1:8000/optimize?api_key=trigger-secret"  -H "Content-Type: application/json"  -d @input_data.json
```

### 2. Retrieve Results
```bash
curl -X GET "http://127.0.0.1:8000/retrieve?api_key=trigger-secret"
```

---

## 🚀 Deployment Plan
- **Local Development**: Uvicorn + FastAPI.

---

## 📂 External Storage Implementation

### Azure Blob Simulation
- Uses `azure-storage-blob` to upload final data.
- Simulated via `.blob` files in local directory.
- Metadata (`timestamp`, `asset_id`) stored alongside.

### SharePoint Simulation
- Uses `shareplum` with mock credentials.
- Uploads to a simulated endpoint.

---

## 🔐 Data Privacy & Compliance
- Identifiable fields (e.g., `name`) are replaced with `[MASKED]` in logs/outputs.
- Metadata & logs stored securely in JSON files.
- API requires authentication with API key.

---

## 📁 Project Structure
```
data-optimizer-microservice/
│── app/
│   ├── auth.py
│   ├── main.py
│   ├── pipeline.py
│   ├── rate_limit.py
│   ├── storage.py
│── outputs/
│── requirements.txt
│── README.md
│── input_data.json
```

---

## Auth (auth.py)

   Simple API key check.

   Keys:

      - trigger-secret → for /optimize

      - read-secret-* → for /retrieve

## API Entrypoint (main.py)

   - POST /optimize → Runs workflow (ingest → clean → ML → save).

   - GET /retrieve → Returns results (rate limited, API key required).

## Pipeline (pipeline.py)

   - Ingest & Clean → Fill missing values, save cleaned_data.json.

   - Metadata Extraction → Mask names, save metadata.json.

   - AI/ML Refinement → Score + Q-learning tweak, assign asset_id.

   - Save results to refined_data.json + log.json.

## Rate Limiting (rate_limit.py)

   - Max 10 requests per minute per API key.

## Storage Simulation (storage.py)

   Simulates:

      - Azure Blob → Saves as .blob files.

      - SharePoint → Saves metadata in sharepoint_list.json.