import json
from pathlib import Path
import time

CORPUS = Path.home() / "Node_Temp/training/corpus"
LOG = Path.home() / "Node_Temp/metadata/ingest_logs/ingest.log"
PROCESSED_STATE = Path.home() / "Node_Temp/metadata/ingest_logs/processed.json"

def log(msg):
    with open(LOG, "a") as f:
        f.write(msg + "\n")

def load_processed():
    if PROCESSED_STATE.exists():
        with open(PROCESSED_STATE, "r") as f:
            return set(json.load(f))
    return set()

def save_processed(processed):
    PROCESSED_STATE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROCESSED_STATE, "w") as f:
        json.dump(list(processed), f)

def entry_key(entry):
    return (
        entry.get("id")
        or entry.get("arxiv_id")
        or entry.get("topic")
        or entry.get("term")
    )

def process_file(file, processed):
    changed = False
    with open(file, "r") as f:
        for line in f:
            try:
                entry = json.loads(line)
                key = entry_key(entry)
                if not key or key in processed:
                    continue
                log(f"INGESTED: {key}")
                processed.add(key)
                changed = True
            except:
                log(f"FAILED: {file}")
    return changed

def run():
    processed = load_processed()
    while True:
        changed = False
        for f in CORPUS.glob("*.jsonl"):
            if process_file(f, processed):
                changed = True
        if changed:
            save_processed(processed)
        time.sleep(5)

if __name__ == "__main__":
    run()
