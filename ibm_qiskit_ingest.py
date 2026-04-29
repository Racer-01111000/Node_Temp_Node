#!/usr/bin/env python3
"""
IBM/Qiskit ingest via Semantic Scholar API.

Searches for papers about Qiskit and IBM Quantum hardware.
These are technical papers describing real quantum hardware and
the software stack for programming it — authoritative, tool-specific.

Rate: S2 key rate limit enforced (62s between requests).
State: runtime/ibm_qiskit_ingest_state.json
Output: training/corpus/ibm_qiskit_feed.jsonl + validation queue.
"""
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE = Path.home() / "NODE"
OUT = BASE / "training/corpus/ibm_qiskit_feed.jsonl"
STATE = BASE / "runtime/ibm_qiskit_ingest_state.json"
RATE_STATE = BASE / "runtime/ibm_qiskit_rate_state.json"
VALIDATION_QUEUE = BASE / "runtime/validation_queue.jsonl"

S2_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
S2_FIELDS = "paperId,title,abstract,year,authors,citationCount,externalIds,url,openAccessPdf"
MIN_SECONDS = 62

TOPICS = [
    "Qiskit quantum computing",
    "IBM Quantum hardware",
    "quantum volume IBM",
    "Qiskit runtime circuit execution",
    "IBM quantum error mitigation",
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_state():
    if STATE.exists():
        try:
            return json.loads(STATE.read_text())
        except Exception:
            pass
    return {"topic_offsets": {}, "total_ingested": 0}


def save_state(state):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2) + "\n")


def load_rate_state():
    if RATE_STATE.exists():
        try:
            return json.loads(RATE_STATE.read_text())
        except Exception:
            pass
    return {"last_request_at": None}


def save_rate_state(state):
    RATE_STATE.parent.mkdir(parents=True, exist_ok=True)
    RATE_STATE.write_text(json.dumps(state, indent=2) + "\n")


def enforce_rate_limit():
    rs = load_rate_state()
    last = rs.get("last_request_at")
    if last:
        try:
            from datetime import datetime as dt
            last_dt = dt.fromisoformat(last)
            elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
            wait = MIN_SECONDS - elapsed
            if wait > 0:
                print(f"[ibm_qiskit_ingest] rate gate sleeping {wait:.1f}s")
                time.sleep(wait)
        except Exception:
            pass


def mark_request():
    rs = load_rate_state()
    rs["last_request_at"] = utc_now()
    save_rate_state(rs)


def load_seen_ids():
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
                    pid = r.get("paper_id") or r.get("id")
                    if pid:
                        seen.add(pid)
                except Exception:
                    pass
    return seen


def api_get(query, offset, limit, api_key):
    params = {"query": query, "offset": offset, "limit": limit, "fields": S2_FIELDS}
    url = S2_SEARCH_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    req.add_header("x-api-key", api_key)
    req.add_header("User-Agent", "NODE/1.0 (kestrel-node research)")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode()), resp.status, None
    except urllib.error.HTTPError as exc:
        return None, exc.code, str(exc)
    except Exception as exc:
        return None, 0, str(exc)


def build_record(paper, topic):
    pid = paper.get("paperId", "")
    title = (paper.get("title") or "").strip()
    abstract = (paper.get("abstract") or "").strip()
    ext_ids = paper.get("externalIds") or {}
    arxiv_id = ext_ids.get("ArXiv", "")
    doi = ext_ids.get("DOI", "")
    s2_url = paper.get("url") or f"https://www.semanticscholar.org/paper/{pid}"
    source_url = f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else s2_url
    content = f"{title}: {abstract[:600]}" if abstract else title
    content_hash = hashlib.sha256(content.encode()).hexdigest()
    return {
        "topic": "ibm_qiskit",
        "type": "paper",
        "content": content,
        "state": "UNVERIFIED",
        "source": "ibm_qiskit_via_s2",
        "source_url": source_url,
        "paper_id": pid,
        "title": title,
        "abstract": abstract[:1000],
        "year": paper.get("year"),
        "authors": [a.get("name", "") for a in (paper.get("authors") or [])],
        "citation_count": paper.get("citationCount") or 0,
        "arxiv_id": arxiv_id,
        "doi": doi,
        "content_hash": content_hash,
        "ingested_at": utc_now(),
    }


def append_corpus(entry):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def append_queue(entry):
    VALIDATION_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with VALIDATION_QUEUE.open("a") as f:
        f.write(json.dumps({
            "id": entry["paper_id"],
            "source": "ibm_qiskit",
            "source_url": entry["source_url"],
            "content": entry["content"],
            "content_hash": entry["content_hash"],
            "topic": entry["topic"],
            "arxiv_id": entry["arxiv_id"],
            "validation_status": "pending",
            "added_at": utc_now(),
        }, ensure_ascii=False) + "\n")


def main():
    api_key = os.environ.get("S2_API_KEY", "").strip()
    if not api_key:
        print("[ibm_qiskit_ingest] S2_API_KEY not set", file=sys.stderr)
        return 1

    state = load_state()
    seen = load_seen_ids()
    total_new = 0

    for topic in TOPICS:
        offset = state["topic_offsets"].get(topic, 0)
        print(f"[ibm_qiskit_ingest] query='{topic}' offset={offset}")

        enforce_rate_limit()
        data, status, err = api_get(topic, offset, 10, api_key)
        mark_request()

        if status == 429:
            print("[ibm_qiskit_ingest] 429 rate limited — stopping pass", file=sys.stderr)
            break
        if err:
            print(f"[ibm_qiskit_ingest] error: {err}", file=sys.stderr)
            continue

        papers = data.get("data") or []
        next_offset = data.get("next", offset + len(papers))
        new_count = 0

        for paper in papers:
            pid = paper.get("paperId", "")
            if not pid or pid in seen:
                continue
            record = build_record(paper, topic)
            append_corpus(record)
            append_queue(record)
            seen.add(pid)
            new_count += 1
            total_new += 1

        state["topic_offsets"][topic] = next_offset
        state["total_ingested"] = state.get("total_ingested", 0) + new_count
        save_state(state)
        print(f"[ibm_qiskit_ingest] '{topic}': new={new_count}")

    print(f"[ibm_qiskit_ingest] pass complete: total_new={total_new}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
