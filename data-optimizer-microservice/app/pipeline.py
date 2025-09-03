import pandas as pd, json, os, uuid, re, spacy, random
from datetime import datetime
from pathlib import Path
from transformers import pipeline
from app.storage import persist_final_results

output_dir = Path("outputs")
output_dir.mkdir(exist_ok=True)

# Step 1: Data Ingestion
def ingest_and_clean(data):
    try:
        df = pd.DataFrame(data)

        if "text" in df.columns:
            df["text"] = (
                df["text"]
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
            )

        # Clean rating
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")  # force numeric
        missing_ratings = df["rating"].isna().sum()
        df["rating"] = df["rating"].fillna("No Rating")

        # Clean timestamp
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        missing_timestamps = df["timestamp"].isna().sum()
        df["timestamp"] = df["timestamp"].fillna(f'Current Datetime - {datetime.utcnow()}')

        # Remove Duplicate
        before = len(df)
        df = df.drop_duplicates(subset=["text", "timestamp"])
        after = len(df)

        cleaned = df.to_dict(orient="records")

        # Save cleaned data
        cleaned_path = output_dir/"cleaned_data.json"
        with cleaned_path.open("w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2, ensure_ascii=False, default=str)

        # Save metadata log
        log = {
            "ingested_records": len(data),
            "cleaned_records": len(cleaned),
            "missing_ratings_filled": int(missing_ratings),
            "missing_timestamps_filled": int(missing_timestamps),
            "duplicates_removed": before - after,
            "run_at": datetime.utcnow().isoformat()
        }
        log_path = output_dir/"cleaned_data_log.json"
        with log_path.open("w", encoding="utf-8") as f:
            json.dump(log, f, indent=2, ensure_ascii=False)

        return cleaned

    except Exception as e:
        error_log = {"error": str(e), "run_at": datetime.utcnow().isoformat()}
        error_path = output_dir/"cleaned_data_error_log.json"
        with error_path.open("w", encoding="utf-8") as f:
            json.dump(error_log, f, indent=2)
        print(f"Error during ingestion: {e}")
        return []

# Step 2: Metadata Extraction
from spacy.pipeline import EntityRuler

def add_custom_entities(nlp):
    ruler = nlp.add_pipe("entity_ruler", before="ner")
    patterns = [
        {"label": "ORG", "pattern": "HR"},
        {"label": "ORG", "pattern": "Marketing"}
    ]
    ruler.add_patterns(patterns)
    return nlp

nlp = spacy.load("en_core_web_sm")
# nlp = add_custom_entities(nlp)

def extract_metadata(cleaned):

    metadata = []

    for record in cleaned:
        text = record["text"]
        doc = nlp(text)

        entities = []
        masked_text = list(text)

        for ent in doc.ents:
            if ent.label_ in ["PERSON", "ORG", "DATE"]:
                entity_map = {"HR": "Human Resources", "Marketing": "Marketing Department"}
                normalized = entity_map.get(ent.text, ent.text)
                entities.append({
                    "text": ent.text,
                    "normalized": normalized,
                    "label": ent.label_,
                })
                masked_text[ent.start_char:ent.end_char] = "[MASKED]"

        masked_text = "".join(masked_text)
        asset_id = f"asset_{uuid.uuid4().hex[:6]}"
        enriched = {
            "asset_id": asset_id,
            "entities": entities,
            "rating": record.get("rating"),
            "timestamp": record.get("timestamp"),
            "masked_text": masked_text
        }
        metadata.append(enriched)

    path = output_dir/"metadata.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

    return metadata


# Step 3: Use Zero-Shot Classification + RL refinement.

zero_shot = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

CATEGORIES = [
    "Excellent work",
    "Confusing explanation",
    "Neutral feedback",
    "Negative feedback",
]

CATEGORY_TO_RATING = {
    "Excellent work": 9,
    "Confusing explanation": 4,
    "Neutral feedback": 5,
    "Negative feedback": 3,
}

# Reinforcement Learning Setup

ACTIONS = ["decrease", "same", "increase"]
Q_TABLE = {}
ALPHA, GAMMA, EPSILON = 0.5, 0.9, 0.2


def q_update(state, action, reward, next_state):

    if state not in Q_TABLE:
        Q_TABLE[state] = {a: 0 for a in ACTIONS}
    old_value = Q_TABLE[state][action]
    next_max = max(Q_TABLE.get(next_state, {a: 0 for a in ACTIONS}).values())
    Q_TABLE[state][action] = old_value + ALPHA * (reward + GAMMA * next_max - old_value)


def choose_action(state):

    if state not in Q_TABLE:
        Q_TABLE[state] = {a: 0 for a in ACTIONS}
    if random.random() < EPSILON:  # explore
        return random.choice(ACTIONS)
    return max(Q_TABLE[state], key=Q_TABLE[state].get)


def process_logic(cleaned_records: list):
    results = []
    total_reward = 0

    for i, record in enumerate(cleaned_records, start=1):

        text = record["text"]
        actual_rating = record.get("rating")

        # Zero-Shot classification
        prediction = zero_shot(text, candidate_labels=CATEGORIES)
        best_idx = prediction["scores"].index(max(prediction["scores"]))
        category = prediction["labels"][best_idx]
        confidence = prediction["scores"][best_idx]

        predicted_rating = CATEGORY_TO_RATING.get(category, 5)

        # RL refinement
        action = choose_action(category)
        refined_rating = predicted_rating + (
            1 if action == "increase" else -1 if action == "decrease" else 0
        )
        refined_rating = max(1, min(10, refined_rating))

        # Reward
        if actual_rating is None or actual_rating == "No Rating":
            reward = 0
        else:
            reward = -abs(refined_rating - actual_rating)

        total_reward += reward
        q_update(category, action, reward, category)

        print(f"[Record {i}] {text}")
        print(f"Category: {category} (conf={confidence:.3f})")
        print(f"Predicted={predicted_rating}, Refined={refined_rating}, Action={action}")
        print(f"Actual={actual_rating}, Reward={reward}")
        print(f"Q-Table[{category}] = {Q_TABLE[category]}\n")

        asset_id = f"asset_{uuid.uuid4().hex[:6]}"
        results.append({
            "asset_id": asset_id,
            "text": text,
            "category": category,
            "confidence": round(confidence, 3),
            "predicted_rating": predicted_rating,
            "refined_rating": refined_rating,
            "action": action,
            "reward": reward,
        })

    # Save cleaned data
    refined_data_path = output_dir/"refined_data.json"
    with refined_data_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    # Summary
    avg_reward = total_reward / len(cleaned_records) if cleaned_records else 0
    summary = {
        "average_reward": round(avg_reward, 3),
        "q_table": Q_TABLE,
    }

    # Save results
    path = output_dir/"logic_results.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump({"results": results, "summary": summary}, f, indent=2, ensure_ascii=False)

    return results


# Main pipeline runner
def run_workflow(data):
    cleaned = ingest_and_clean(data)
    metadata = extract_metadata(cleaned)
    refined = process_logic(cleaned)
    persist_final_results(refined, backend="azure")
    persist_final_results(refined, backend="sharepoint")

    return refined
