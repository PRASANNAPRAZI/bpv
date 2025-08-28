import pandas as pd, json, os, uuid, re, spacy
from datetime import datetime
from pathlib import Path

# Step 1: Ingest & Clean
def ingest_and_clean(data):
    df = pd.DataFrame(data)
    df["rating"] = df["rating"].fillna(df["rating"].mean())
    df["timestamp"] = df["timestamp"].fillna(datetime.utcnow().isoformat())
    cleaned = df.to_dict(orient="records")
    Path("outputs").mkdir(exist_ok=True)
    json.dump(cleaned, open("outputs/cleaned_data.json","w"), indent=2)
    return cleaned

# Step 2: Metadata Extraction (simple regex + masking)
def extract_metadata(cleaned):
    nlp = spacy.load("en_core_web_sm")

    metadata = []
    for record in cleaned:
        text = record["text"]
        doc = nlp(text)

        entities = []
        masked_text = text
        for ent in doc.ents:
            if ent.label_ in ["PERSON", "ORG"]:  # only mask names/orgs
                entities.append({"text": ent.text, "label": ent.label_})
                masked_text = masked_text.replace(ent.text, "[MASKED]")

        record["masked_text"] = masked_text
        metadata.append({
            "asset_id": record.get("asset_id", ""),
            "entities": entities
        })

    with open("outputs/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    return metadata
    #     text = record["text"]
    #     names = re.findall(r"\b[A-Z][a-z]+\b", text)
    #     record["masked_text"] = re.sub(r"\b[A-Z][a-z]+\b", "[MASKED]", text)
    #     metadata.append({"asset_id": record.get("asset_id",""), "entities": names})
    # json.dump(metadata, open("outputs/metadata.json","w"), indent=2)
    # return metadata

# Step 3: ML + Q-learning (lightweight)
def ml_refinement(cleaned):
    refined = []
    for record in cleaned:
        score = record["rating"] / 10  # dummy "DistilBERT score"
        q_value = score + 0.1          # RL refinement
        asset_id = f"asset_{uuid.uuid4().hex[:6]}"
        refined.append({
            "asset_id": asset_id,
            "refined_score": round(q_value, 2),
            "masked_text": record["masked_text"],
            "timestamp": record["timestamp"]
        })
    json.dump(refined, open("outputs/refined_data.json","w"), indent=2)
    json.dump(refined, open("outputs/log.json","w"), indent=2)
    return refined

# Main pipeline runner
def run_workflow(data):
    cleaned = ingest_and_clean(data)
    extract_metadata(cleaned)
    refined = ml_refinement(cleaned)
    from app.storage import save_to_storage
    save_to_storage(refined)
    return refined

def load_refined_data():
    return json.load(open("outputs/refined_data.json"))
