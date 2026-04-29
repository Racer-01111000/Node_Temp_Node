#!/usr/bin/env python3
"""
Continuous w3m-based validation loop for NODE corpus candidates.

Target: 60 validation attempts per hour (1 per minute globally).
Per-domain cooldown: 120 seconds (configurable via config/source_registry.json).
Failure backoff: 600 seconds per domain.

Safety:
  - Does NOT auto-promote records.
  - Does NOT mutate corpus files.
  - Writes only to metadata/validation_logs/ and metadata/truth_gate_reports/.
  - Respects domain cooldown and per-minute global rate.

Usage:
  python3 validate_w3m_loop.py            # continuous loop
  python3 validate_w3m_loop.py --once     # single attempt then exit
  python3 validate_w3m_loop.py --dry-run  # show next candidate, don't fetch
"""
import argparse
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

BASE = Path.home() / "NODE"
CONFIG_PATH = BASE / "config/source_registry.json"
QUEUE_PATH = BASE / "runtime/validation_queue.jsonl"
STATE_PATH = BASE / "runtime/validation_state.json"
LOG_PATH = BASE / "metadata/validation_logs/w3m_validation.jsonl"
REPORTS_DIR = BASE / "metadata/truth_gate_reports"

# Defaults (overridden by config/source_registry.json)
ATTEMPTS_PER_HOUR = 60
MIN_SECONDS = 60
PER_DOMAIN_SECONDS = 120
FAILURE_BACKOFF = 600
MAX_FETCH_SECONDS = 30
MAX_TEXT_BYTES = 250000

TERMINAL_VALIDATION_STATUSES = frozenset({"done", "failed_permanent"})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text())
            return data.get("validation", {})
        except Exception:
            pass
    return {}


def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            pass
    return {
        "domain_last_attempt": {},
        "failure_counts": {},
        "total_attempts": 0,
        "last_attempt_at": None,
        "attempts_this_hour": 0,
        "hour_window_start": utc_now(),
    }


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def append_log(entry: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def write_report(candidate_id: str, report: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"{candidate_id}.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def load_queue() -> list:
    if not QUEUE_PATH.exists():
        return []
    records = []
    with QUEUE_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return [r for r in records if r.get("validation_status") not in TERMINAL_VALIDATION_STATUSES]


def update_queue_record(candidate_id: str, updates: dict) -> None:
    if not QUEUE_PATH.exists():
        return
    lines = QUEUE_PATH.read_text(encoding="utf-8").splitlines()
    new_lines = []
    for line in lines:
        try:
            rec = json.loads(line)
            if rec.get("id") == candidate_id or rec.get("arxiv_id") == candidate_id:
                rec.update(updates)
            new_lines.append(json.dumps(rec))
        except json.JSONDecodeError:
            new_lines.append(line)
    QUEUE_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc or url[:40]
    except Exception:
        return url[:40]


def can_attempt_domain(state: dict, domain: str, cooldown: int) -> bool:
    last = state["domain_last_attempt"].get(domain, 0)
    failures = state["failure_counts"].get(domain, 0)
    if failures >= 3:
        backoff = state.get("_failure_backoff", FAILURE_BACKOFF)
        return (time.time() - last) >= backoff
    return (time.time() - last) >= cooldown


def candidate_id_of(candidate: dict) -> str:
    for field in ("id", "arxiv_id", "paper_id"):
        if candidate.get(field):
            return str(candidate[field])
    raw = json.dumps(candidate, sort_keys=True).encode()
    return "auto_" + hashlib.sha256(raw).hexdigest()[:12]


def best_url_for(candidate: dict) -> str | None:
    for field in ("source_url", "url", "arxiv_url", "doi_url"):
        val = candidate.get(field, "").strip()
        if val and val.startswith("http"):
            return val
    arxiv_id = candidate.get("arxiv_id", "").strip()
    if arxiv_id:
        return f"https://arxiv.org/abs/{arxiv_id}"
    return None


def fetch_w3m(url: str, timeout: int, max_bytes: int) -> tuple[str | None, str | None]:
    try:
        result = subprocess.run(
            ["w3m", "-dump", "-T", "text/html", url],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            return None, f"w3m exit {result.returncode}: {result.stderr[:200].strip()}"
        return result.stdout[:max_bytes], None
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except FileNotFoundError:
        return None, "w3m_not_found"
    except Exception as exc:
        return None, str(exc)[:200]


def content_match_score(fetched_text: str, candidate: dict) -> dict:
    content = candidate.get("content", "")
    title = ""
    if ": " in content:
        title = content.split(": ", 1)[0]
    elif content:
        title = content[:80]

    text_lower = fetched_text.lower()
    title_found = bool(title) and title.lower() in text_lower

    # Check for a few key topic words as secondary signal
    topic = candidate.get("topic", "")
    topic_words = [w for w in topic.replace("_", " ").split() if len(w) > 4]
    topic_hits = sum(1 for w in topic_words if w.lower() in text_lower)

    return {
        "title_found": title_found,
        "title_checked": title[:100],
        "topic_word_hits": topic_hits,
        "topic_word_count": len(topic_words),
    }


def validate_candidate(candidate: dict, state: dict, cfg: dict) -> dict:
    cid = candidate_id_of(candidate)
    url = best_url_for(candidate)

    report = {
        "candidate_id": cid,
        "attempted_at": utc_now(),
        "url": url,
        "status": "no_url",
        "text_hash": None,
        "match": None,
        "error": None,
        "promoted": False,
    }

    if not url:
        report["status"] = "no_url"
        return report

    domain = domain_of(url)
    cooldown = cfg.get("per_domain_min_seconds", PER_DOMAIN_SECONDS)
    report["_failure_backoff"] = cfg.get("failure_backoff_seconds", FAILURE_BACKOFF)

    if not can_attempt_domain(state, domain, cooldown):
        report["status"] = "domain_cooldown"
        return report

    state["domain_last_attempt"][domain] = time.time()
    state["total_attempts"] = state.get("total_attempts", 0) + 1
    state["last_attempt_at"] = utc_now()

    text, error = fetch_w3m(
        url,
        timeout=cfg.get("max_fetch_seconds", MAX_FETCH_SECONDS),
        max_bytes=cfg.get("max_text_bytes", MAX_TEXT_BYTES),
    )

    if error:
        fc = state["failure_counts"].get(domain, 0) + 1
        state["failure_counts"][domain] = fc
        report["status"] = "fetch_error"
        report["error"] = error
        return report

    state["failure_counts"][domain] = 0
    report["text_hash"] = hashlib.sha256(text.encode()).hexdigest()
    report["match"] = content_match_score(text, candidate)

    if report["match"]["title_found"]:
        report["status"] = "validated"
    else:
        report["status"] = "content_mismatch"

    # Never auto-promote — truth gate requires corroboration
    report["promoted"] = False
    return report


def seconds_until_next_slot(state: dict, min_seconds: int) -> float:
    last_str = state.get("last_attempt_at")
    if not last_str:
        return 0.0
    try:
        from datetime import datetime as _dt
        last_ts = _dt.fromisoformat(last_str).timestamp()
        elapsed = time.time() - last_ts
        return max(0.0, min_seconds - elapsed)
    except Exception:
        return 0.0


def run(once: bool = False, dry_run: bool = False) -> int:
    cfg = load_config()
    min_seconds = cfg.get("min_seconds_between_attempts", MIN_SECONDS)

    print(f"[validate_w3m_loop] started | once={once} dry_run={dry_run} "
          f"min_interval={min_seconds}s", flush=True)

    while True:
        state = load_state()
        state["_failure_backoff"] = cfg.get("failure_backoff_seconds", FAILURE_BACKOFF)

        wait = seconds_until_next_slot(state, min_seconds)
        if wait > 0 and not once:
            print(f"[validate_w3m_loop] rate-limit: sleeping {wait:.1f}s", flush=True)
            time.sleep(wait)
            state = load_state()

        queue = load_queue()
        if not queue:
            print("[validate_w3m_loop] queue empty — nothing to validate", flush=True)
            if once:
                return 0
            time.sleep(min_seconds)
            continue

        cooldown = cfg.get("per_domain_min_seconds", PER_DOMAIN_SECONDS)
        candidate = None
        for c in queue:
            u = best_url_for(c)
            d = domain_of(u) if u else ""
            if not d or can_attempt_domain(state, d, cooldown):
                candidate = c
                break

        if candidate is None:
            print("[validate_w3m_loop] all candidates on domain cooldown, sleeping", flush=True)
            time.sleep(cooldown)
            continue

        cid = candidate_id_of(candidate)
        url = best_url_for(candidate)

        print(f"[validate_w3m_loop] candidate={cid} url={url}", flush=True)

        if dry_run:
            print(f"[validate_w3m_loop] dry-run: would validate {cid}", flush=True)
            return 0

        report = validate_candidate(candidate, state, cfg)
        save_state(state)

        log_entry = {**report, "candidate": {k: v for k, v in candidate.items() if not k.startswith("_")}}
        append_log(log_entry)
        write_report(cid, report)
        update_queue_record(cid, {"validation_status": report["status"], "last_validated_at": utc_now()})

        print(f"[validate_w3m_loop] result: {json.dumps(report)}", flush=True)

        if once:
            return 0

        time.sleep(1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Continuous w3m corpus validator")
    parser.add_argument("--once", action="store_true", help="Validate one candidate then exit")
    parser.add_argument("--dry-run", action="store_true", help="Show next candidate without fetching")
    args = parser.parse_args()
    return run(once=args.once, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
