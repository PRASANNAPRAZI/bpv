import json
import uuid
import gzip
from pathlib import Path
from datetime import datetime

# Base Storage
class BaseStorage:
    def save(self, asset_id: str, data: dict):
        raise NotImplementedError

# Azure Blob Simulation
class AzureBlobSim(BaseStorage):
    def __init__(self, container="azure_blob"):
        Path(container).mkdir(exist_ok=True)
        self.container = container

    def save(self, asset_id: str, data: dict):
        file_path = Path(self.container)/f"{asset_id}.blob"
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return {"status": "success", "blob": str(file_path)}


# SharePoint Simulation
class SharePointSim(BaseStorage):
    def __init__(self, list_name="sharepoint/sharepoint_list.json"):
        self.list_file = Path(list_name)
        if not self.list_file.exists():
            with open(self.list_file, "w", encoding="utf-8") as f:
                json.dump([], f)

    def save(self, asset_id: str, data: dict):
        with open(self.list_file, "rt", encoding="utf-8") as f:
            existing = json.load(f)

        entry = {
            "asset_id": asset_id,
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "size": len(json.dumps(data)),
            },
            "data": data,
        }
        existing.append(entry)

        with open(self.list_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)

        return {"status": "success", "entry_count": len(existing)}


# Storage Manager
class StorageManager:
    def __init__(self, backend="local"):
        if backend == "azure":
            self.engine = AzureBlobSim()
        elif backend == "sharepoint":
            self.engine = SharePointSim()
        else:
            raise ValueError(f"Unknown backend: {backend}")

    def save(self, asset_id: str, data: dict):
        return self.engine.save(asset_id, data)


# Persist Final Results
def persist_final_results(results, backend):
    storage = StorageManager(backend=backend)
    responses = []
    for record in results:
        asset_id = record.get("asset_id", f"asset_{uuid.uuid4().hex[:6]}")
        resp = storage.save(asset_id, record)
        responses.append({"asset_id": asset_id, "storage": resp})
    return responses
