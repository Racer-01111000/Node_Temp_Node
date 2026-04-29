#!/usr/bin/env python3
"""
One-shot enrichment: reads arxiv_feed.jsonl, resolves each paper title via
the arXiv search API, and appends proper validation queue entries.

Safe:
- Never modifies corpus files.
- Skips papers already present in the validation queue (by content hash).
- Appends only to runtime/validation_queue.jsonl.
"""
import hashlib
import json
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

BASE = Path.home() / "NODE"
ARXIV_FEED = BASE / "training/corpus/arxiv_feed.jsonl"
QUEUE_PATH = BASE / "runtime/validation_queue.jsonl"
ARXIV_API = "http://export.arxiv.org/api/query"
NS = "http://www.w3.org/2005/Atom"

SECONDS_BETWEEN_REQUESTS = 3


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_seen_hashes():
    seen = set()
    if not QUEUE_PATH.exists():
        return seen
    with QUEUE_PATH.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("content_hash"):
                    seen.add(rec["content_hash"])
            except Exception:
                pass
    return seen


def content_hash(content):
    return hashlib.sha256(content.encode()).hexdigest()


def parse_title(content):
    if ": " in content:
        return content.split(": ", 1)[0].strip()
    return content[:100].strip()


def arxiv_lookup(title):
    params = urllib.parse.urlencode({
        "search_query": f'ti:"{title}"',
        "max_results": 1,
        "sortBy": "relevance",
    })
    url = f"{ARXIV_API}?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NODE/1.0 (kestrel-node research)"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            xml = resp.read().decode("utf-8")
    except Exception as exc:
        return None, None, f"request error: {exc}"

    try:
        root = ET.fromstring(xml)
        entries = root.findall(f"{{{NS}}}entry")
        if not entries:
            return None, None, "no results"
        entry = entries[0]
        entry_id = entry.findtext(f"{{{NS}}}id", "").strip()
        # entry_id is like https://arxiv.org/abs/2211.02350v1
        arxiv_id = entry_id.rstrip("/").split("/")[-1].split("v")[0]
        abs_url = f"https://arxiv.org/abs/{arxiv_id}"
        found_title = entry.findtext(f"{{{NS}}}title", "").strip().replace("\n", " ")
        return arxiv_id, abs_url, found_title
    except Exception as exc:
        return None, None, f"parse error: {exc}"


def append_queue(entry):
    QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with QUEUE_PATH.open("a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main():
    if not ARXIV_FEED.exists():
        print("arxiv_feed.jsonl not found")
        return

    seen_hashes = load_seen_hashes()
    records = []
    with ARXIV_FEED.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                pass

    print(f"Found {len(records)} arXiv records, {len(seen_hashes)} already in queue")
    added = 0
    skipped = 0

    for i, rec in enumerate(records):
        content = rec.get("content", "")
        if not content:
            skipped += 1
            continue

        chash = content_hash(content)
        if chash in seen_hashes:
            print(f"[{i+1}/{len(records)}] SKIP (already queued): {content[:60]}")
            skipped += 1
            continue

        title = parse_title(content)
        print(f"[{i+1}/{len(records)}] Resolving: {title[:60]}")

        arxiv_id, abs_url, found = arxiv_lookup(title)

        if arxiv_id:
            print(f"  -> arxiv_id={arxiv_id} url={abs_url} matched='{found[:50]}'")
            source_url = abs_url
        else:
            print(f"  -> not found ({found}), queuing without URL")
            source_url = ""
            arxiv_id = ""

        entry = {
            "id": chash[:16],
            "source": "arxiv_feed",
            "source_url": source_url,
            "content": content,
            "content_hash": chash,
            "topic": rec.get("topic", "arxiv_quantum"),
            "arxiv_id": arxiv_id,
            "validation_status": "pending",
            "added_at": utc_now(),
        }
        append_queue(entry)
        seen_hashes.add(chash)
        added += 1

        if i < len(records) - 1:
            time.sleep(SECONDS_BETWEEN_REQUESTS)

    print(f"\nDone: added={added} skipped={skipped}")


if __name__ == "__main__":
    main()
