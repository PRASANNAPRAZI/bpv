import json, os
from datetime import datetime
from pathlib import Path

def save_to_storage(refined):
    Path("external_storage").mkdir(exist_ok=True)
    for item in refined:
        fname = f"external_storage/{item['asset_id']}.blob"
        json.dump(item, open(fname,"w"), indent=2)
    with open("external_storage/sharepoint_list.json","w") as f:
        json.dump({"uploaded": len(refined), "time": datetime.utcnow().isoformat()}, f)
