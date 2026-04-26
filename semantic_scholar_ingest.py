#!/usr/bin/env python3
"""
Semantic Scholar API ingest for Node_Temp_Node corpus.

Security:
- Reads S2_API_KEY from environment only.
- Never logs or stores the key.
- Never auto-promotes records.

Rate policy:
- Semantic Scholar key is IP-bound.
- Maximum allowed cadence: 1 request per minute.
- Project implementation uses 62 seconds between request attempts.
- Dry-run still obeys rate limit because dry-run still calls the real API.
"""
import argparse
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

BASE = Path.home() / "Node_Temp_Node"
CORPUS_FILE = BASE / "training/corpus/semantic_scholar_feed.jsonl"
LOG_FILE = BASE / "metadata/ingest_logs/semantic_scholar.log"
STATE_FILE = BASE / "metadata/ingest_logs/semantic_scholar_state.json"
RATE_STATE_FILE = BASE / "runtime/semantic_scholar_rate_state.json"
VALIDATION_QUEUE = BASE / "runtime/validation_queue.jsonl"
CONFIG_PATH = BASE / "config/source_registry.json"

S2_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
S2_FIELDS = "paperId,title,abstract,year,authors,citationCount,externalIds,url,openAccessPdf"

DEFAULT_TOPICS = [
    "quantum error correction",
    "variational quantum eigensolver",
    "quantum machine learning",
    "noisy intermediate-scale quantum",
    "quantum circuit optimization",
    "topological quantum computing",
    "quantum advantage",
    "quantum entanglement",
    "quantum cryptography",
    "quantum simulation",
]

DEFAULT_LIMIT_PER_QUERY = 10
MIN_SECONDS_BETWEEN_S2_REQUESTS = 62
BACKOFF_429_INITIAL = 600
BACKOFF_429_MAX = 3600


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(ts: str):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def log_write(msg: str, level: str = "INFO") -> None:
    line = f"{utc_now()} [{level}] {msg}\n"
    sys.stdout.write(line)
    sys.stdout.flush()
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


def load_config() -> dict:
    cfg = {
        "min_seconds_between_requests": MIN_SECONDS_BETWEEN_S2_REQUESTS,
        "limit_per_query": DEFAULT_LIMIT_PER_QUERY,
        "backoff_initial": BACKOFF_429_INITIAL,
        "backoff_max": BACKOFF_429_MAX,
    }

    if not CONFIG_PATH.exists():
        return cfg

    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        ss = data.get("sources", {}).get("semantic_scholar", {})
        rate = ss.get("rate_limit", {})

        cfg["min_seconds_between_requests"] = int(
            rate.get("min_seconds_between_requests", MIN_SECONDS_BETWEEN_S2_REQUESTS)
        )
        cfg["limit_per_query"] = int(ss.get("limit_per_query", DEFAULT_LIMIT_PER_QUERY))
        cfg["backoff_initial"] = int(rate.get("backoff_429_initial", BACKOFF_429_INITIAL))
        cfg["backoff_max"] = int(rate.get("backoff_429_max", BACKOFF_429_MAX))
    except Exception as exc:
        log_write(f"Config read warning, using safe defaults: {exc}", "WARN")

    if cfg["min_seconds_between_requests"] < MIN_SECONDS_BETWEEN_S2_REQUESTS:
        log_write(
            f"Config requested {cfg['min_seconds_between_requests']}s; enforcing {MIN_SECONDS_BETWEEN_S2_REQUESTS}s minimum.",
            "WARN",
        )
        cfg["min_seconds_between_requests"] = MIN_SECONDS_BETWEEN_S2_REQUESTS

    if cfg["backoff_initial"] < BACKOFF_429_INITIAL:
        cfg["backoff_initial"] = BACKOFF_429_INITIAL

    if cfg["backoff_max"] < cfg["backoff_initial"]:
        cfg["backoff_max"] = cfg["backoff_initial"]

    return cfg


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"topic_offsets": {}, "total_ingested": 0, "total_429s": 0}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def load_rate_state() -> dict:
    if RATE_STATE_FILE.exists():
        try:
            return json.loads(RATE_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "last_request_at": None,
        "min_seconds_between_requests": MIN_SECONDS_BETWEEN_S2_REQUESTS,
        "last_status_code": None,
    }


def save_rate_state(state: dict) -> None:
    RATE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    RATE_STATE_FILE.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def enforce_s2_rate_limit(min_seconds: int) -> None:
    state = load_rate_state()
    last = parse_iso(state.get("last_request_at"))

    if last is not None:
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        remaining = min_seconds - elapsed
        if remaining > 0:
            log_write(f"Semantic Scholar rate gate sleeping {remaining:.1f}s")
            time.sleep(remaining)

    state["min_seconds_between_requests"] = min_seconds
    save_rate_state(state)


def mark_s2_request_attempt(status_code: int) -> None:
    state = load_rate_state()
    state["last_request_at"] = utc_now()
    state["last_status_code"] = status_code
    state["min_seconds_between_requests"] = MIN_SECONDS_BETWEEN_S2_REQUESTS
    save_rate_state(state)


def load_seen_ids() -> set:
    seen = set()
    for path in (CORPUS_FILE, VALIDATION_QUEUE):
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as f:
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


def append_corpus(record: dict) -> None:
    CORPUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with CORPUS_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_queue(entry: dict) -> None:
    VALIDATION_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with VALIDATION_QUEUE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def api_get(url: str, params: dict, api_key: str, timeout: int = 30) -> tuple[dict | None, int, str | None]:
    full_url = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full_url)
    req.add_header("x-api-key", api_key)
    req.add_header("User-Agent", "Node_Temp_Node/1.0 (kestrel-node research)")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            return json.loads(resp.read().decode("utf-8")), status, None
    except urllib.error.HTTPError as exc:
        return None, exc.code, f"HTTP {exc.code}: {exc.reason}"
    except urllib.error.URLError as exc:
        return None, 0, f"URL error: {exc.reason}"
    except Exception as exc:
        return None, 0, str(exc)[:200]


def build_record(paper: dict, topic: str) -> dict:
    pid = paper.get("paperId", "")
    title = (paper.get("title") or "").strip()
    abstract = (paper.get("abstract") or "").strip()
    year = paper.get("year")
    authors = [a.get("name", "") for a in (paper.get("authors") or [])]
    citation_count = paper.get("citationCount") or 0
    ext_ids = paper.get("externalIds") or {}
    arxiv_id = ext_ids.get("ArXiv", "")
    doi = ext_ids.get("DOI", "")
    s2_url = paper.get("url") or f"https://www.semanticscholar.org/paper/{pid}"
    oap = paper.get("openAccessPdf") or {}
    pdf_url = oap.get("url", "") if isinstance(oap, dict) else ""

    content = f"{title}: {abstract[:600]}" if abstract else title
    content_hash = hashlib.sha256(content.encode()).hexdigest()

    return {
        "topic": topic,
        "type": "paper",
        "content": content,
        "state": "UNVERIFIED",
        "source": "semantic_scholar",
        "source_url": s2_url,
        "paper_id": pid,
        "title": title,
        "abstract": abstract[:1000],
        "year": year,
        "authors": authors,
        "citation_count": citation_count,
        "arxiv_id": arxiv_id,
        "doi": doi,
        "pdf_url": pdf_url,
        "ingested_at": utc_now(),
        "content_hash": content_hash,
    }


def build_queue_entry(record: dict) -> dict:
    return {
        "id": record["paper_id"],
        "source": "semantic_scholar",
        "source_url": record["source_url"],
        "content": record["content"],
        "topic": record["topic"],
        "arxiv_id": record.get("arxiv_id", ""),
        "validation_status": "pending",
        "added_at": utc_now(),
    }


def ingest_topic(
    topic: str,
    api_key: str,
    offset: int,
    limit: int,
    seen_ids: set,
    dry_run: bool,
    min_seconds_between_requests: int,
) -> tuple[int, int, int | str | None]:
    params = {"query": topic, "offset": offset, "limit": limit, "fields": S2_FIELDS}
    log_write(f'Fetching query="{topic}" offset={offset} limit={limit}')

    enforce_s2_rate_limit(min_seconds_between_requests)
    data, status_code, err_msg = api_get(S2_SEARCH_URL, params, api_key)
    mark_s2_request_attempt(status_code)

    if status_code == 429:
        return 0, offset, 429

    if err_msg is not None:
        log_write(f"Fetch error: {err_msg}", "ERROR")
        return 0, offset, err_msg

    papers = data.get("data") or []
    total_available = data.get("total", 0)
    next_offset = data.get("next", offset + len(papers))

    new_count = 0
    for paper in papers:
        pid = paper.get("paperId", "")
        if not pid or pid in seen_ids:
            continue

        record = build_record(paper, topic)

        if dry_run:
            log_write(f'[dry-run] would ingest paper_id={pid} title="{paper.get("title", "")[:60]}"')
        else:
            append_corpus(record)
            append_queue(build_queue_entry(record))
            seen_ids.add(pid)
            new_count += 1
            log_write(f'New: paper_id={pid} title="{paper.get("title", "")[:60]}"')

    log_write(
        f'Done: query="{topic}" fetched={len(papers)} new={new_count} '
        f'total_available={total_available}'
    )
    return new_count, next_offset, None


def run(once: bool, dry_run: bool, status_only: bool, query: str | None, limit: int | None) -> int:
    api_key = os.environ.get("S2_API_KEY", "").strip()
    if not api_key:
        print("S2_API_KEY not set", file=sys.stderr)
        return 1

    cfg = load_config()
    state = load_state()

    if status_only:
        seen_ids = load_seen_ids()
        print(json.dumps({
            "total_ingested": state.get("total_ingested", 0),
            "total_429s": state.get("total_429s", 0),
            "corpus_records": len(seen_ids),
            "corpus_file": str(CORPUS_FILE),
            "corpus_exists": CORPUS_FILE.exists(),
            "api_key_configured": True,
            "min_seconds_between_requests": cfg["min_seconds_between_requests"],
            "rate_state_file": str(RATE_STATE_FILE),
            "rate_state": load_rate_state(),
        }, indent=2))
        return 0

    seen_ids = load_seen_ids()
    topics = [query] if query else DEFAULT_TOPICS
    effective_limit = int(limit or cfg["limit_per_query"])
    min_seconds = int(cfg["min_seconds_between_requests"])
    backoff = int(cfg["backoff_initial"])
    total_new = 0

    log_write(
        f"Starting Semantic Scholar ingest | topics={len(topics)} "
        f"limit_per_query={effective_limit} min_seconds_between_requests={min_seconds}"
    )

    for topic in topics:
        offset = state["topic_offsets"].get(topic, 0)
        new_count, next_offset, err = ingest_topic(
            topic,
            api_key,
            offset,
            effective_limit,
            seen_ids,
            dry_run,
            min_seconds,
        )

        if err == 429:
            state["total_429s"] = state.get("total_429s", 0) + 1
            log_write(f'429 rate-limited on query="{topic}", backing off {backoff}s', "WARN")
            save_state(state)
            if once:
                return 0
            time.sleep(backoff)
            backoff = min(backoff * 2, int(cfg["backoff_max"]))
            continue

        if err is not None:
            if once:
                return 1
            time.sleep(min_seconds)
            continue

        backoff = int(cfg["backoff_initial"])

        if not dry_run:
            state["topic_offsets"][topic] = next_offset
            state["total_ingested"] = state.get("total_ingested", 0) + new_count
            save_state(state)

        total_new += new_count

        if once:
            break

        time.sleep(min_seconds)

    log_write(f"Ingest pass complete | total_new={total_new}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Semantic Scholar API ingest")
    parser.add_argument("--once", action="store_true", help="Ingest one query/request cycle then exit")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched without writing")
    parser.add_argument("--status", action="store_true", help="Show current ingest state and exit")
    parser.add_argument("--query", help="Override with single topic query")
    parser.add_argument("--limit", type=int, help="Papers per query")
    args = parser.parse_args()

    return run(
        once=args.once,
        dry_run=args.dry_run,
        status_only=args.status,
        query=args.query,
        limit=args.limit,
    )


if __name__ == "__main__":
    raise SystemExit(main())
