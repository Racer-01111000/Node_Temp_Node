#!/usr/bin/env python3
"""
PRX Quantum ingest via arXiv journal-ref search.

Searches arXiv for papers with journal-ref containing "PRX Quantum".
These are peer-reviewed papers published in Physical Review X Quantum
that authors have also deposited on arXiv.

Rate: respects arXiv's polite use policy (3s between requests).
State: runtime/prx_ingest_state.json tracks offset.
Output: training/corpus/prx_aps_feed.jsonl + validation queue.
"""
import hashlib
import json
import sys
import time
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

BASE = Path.home() / "NODE"
OUT = BASE / "training/corpus/prx_aps_feed.jsonl"
STATE = BASE / "runtime/prx_ingest_state.json"
VALIDATION_QUEUE = BASE / "runtime/validation_queue.jsonl"

ARXIV_BASE = "http://export.arxiv.org/api/query"
# journal-ref search for PRX Quantum papers
SEARCH_QUERY = 'jrn:"PRX Quantum"'
PAGE_SIZE = 20
SECONDS_BETWEEN_REQUESTS = 3
NS = {"atom": "http://www.w3.org/2005/Atom"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_state():
    if STATE.exists():
        try:
            return json.loads(STATE.read_text())
        except Exception:
            pass
    return {"offset": 0, "total_ingested": 0}


def save_state(state):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2) + "\n")


def load_seen_hashes():
    seen = set()
    for path in (OUT, VALIDATION_QUEUE):
        if not path.exists():
            continue
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    ch = r.get("content_hash") or hashlib.sha256(
                        r.get("content", "").encode()
                    ).hexdigest()
                    seen.add(ch)
                except Exception:
                    pass
    return seen


def fetch(offset):
    params = urllib.parse.urlencode({
        "search_query": SEARCH_QUERY,
        "start": offset,
        "max_results": PAGE_SIZE,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    })
    url = f"{ARXIV_BASE}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "NODE/1.0 (kestrel-node research)"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8")


def parse(xml_data):
    root = ET.fromstring(xml_data)
    entries = []
    for entry in root.findall("atom:entry", NS):
        title_el = entry.find("atom:title", NS)
        summary_el = entry.find("atom:summary", NS)
        id_el = entry.find("atom:id", NS)
        if title_el is None or summary_el is None:
            continue
        title = title_el.text.strip().replace("\n", " ")
        summary = summary_el.text.strip().replace("\n", " ")
        arxiv_id = ""
        if id_el is not None:
            raw = id_el.text.strip().rstrip("/").split("/")[-1]
            arxiv_id = raw.split("v")[0]
        content = f"{title}: {summary}"
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        entries.append({
            "topic": "prx_quantum",
            "type": "paper",
            "content": content,
            "state": "UNVERIFIED",
            "arxiv_id": arxiv_id,
            "source": "prx_quantum_via_arxiv",
            "source_url": f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else "",
            "peer_reviewed": True,
            "journal": "PRX Quantum",
            "content_hash": content_hash,
            "ingested_at": utc_now(),
        })
    return entries


def append_corpus(entry):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def append_queue(entry):
    VALIDATION_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with VALIDATION_QUEUE.open("a") as f:
        f.write(json.dumps({
            "id": entry["content_hash"][:16],
            "source": "prx_quantum",
            "source_url": entry["source_url"],
            "content": entry["content"],
            "content_hash": entry["content_hash"],
            "topic": entry["topic"],
            "arxiv_id": entry["arxiv_id"],
            "peer_reviewed": True,
            "validation_status": "pending",
            "added_at": utc_now(),
        }, ensure_ascii=False) + "\n")


def main():
    state = load_state()
    seen = load_seen_hashes()
    offset = state["offset"]

    print(f"[prx_ingest] fetching offset={offset} query='{SEARCH_QUERY}'")
    try:
        xml_data = fetch(offset)
    except Exception as exc:
        print(f"[prx_ingest] fetch error: {exc}", file=sys.stderr)
        return 1

    entries = parse(xml_data)
    if not entries:
        print("[prx_ingest] no entries — resetting offset to 0")
        state["offset"] = 0
        save_state(state)
        return 0

    added = 0
    skipped = 0
    for e in entries:
        if e["content_hash"] in seen:
            skipped += 1
            continue
        append_corpus(e)
        append_queue(e)
        seen.add(e["content_hash"])
        added += 1

    state["offset"] = offset + PAGE_SIZE
    state["total_ingested"] = state.get("total_ingested", 0) + added
    save_state(state)

    print(f"[prx_ingest] done: added={added} skipped={skipped} next_offset={state['offset']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
