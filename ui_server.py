import json
import os
import shlex
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, str(Path(__file__).parent))
try:
    import kestrel_persona as _persona
    _PERSONA_OK = True
except Exception:
    _PERSONA_OK = False

BASE = Path.home() / "NODE"
CORPUS = BASE / "training/corpus"
SUBSTRATE = BASE / "training/substrate"
INGEST_LOG = BASE / "metadata/ingest_logs/ingest.log"
PROCESSED = BASE / "metadata/ingest_logs/processed.json"

BRIDGE_INBOX = Path.home() / "GPT_Firefox_extension/control/inbox/current.json"
BRIDGE_OUTBOX = Path.home() / "GPT_Firefox_extension/control/outbox"

VALIDATION_QUEUE = BASE / "runtime/validation_queue.jsonl"
VALIDATION_STATE = BASE / "runtime/validation_state.json"
VALIDATION_LOG = BASE / "metadata/validation_logs/w3m_validation.jsonl"
OFFLOAD_LIST = BASE / "runtime/offload_list.jsonl"
ARCHIVE_DIR = BASE / "archive/offloaded_batches"

SS_LOG = BASE / "metadata/ingest_logs/semantic_scholar.log"
SS_STATE = BASE / "metadata/ingest_logs/semantic_scholar_state.json"

UPLOAD_DIR = Path.home() / "incoming/uploads"
TELEGRAM_DIR = Path.home() / "incoming/telegram"
W3M_FETCH_QUEUE = BASE / "runtime/w3m_fetch_queue.jsonl"
WEB_CAPTURE_DIR = BASE / "staging/web_capture"

PAPER_SOURCES = ["arxiv_feed.jsonl", "semantic_scholar_feed.jsonl"]

ACCEPTED_STATES = frozenset({"ACCEPTED", "accepted"})
ELEVATED_STATES = frozenset({"ELEVATED", "elevated"})
ELEVATED_MODES = frozenset({"corpus_candidate"})

SEARCH_ROOTS = [
    Path.home() / "incoming",
    Path.home() / "NODE",
]

PROTECTED_NAMES = frozenset({
    ".ssh", ".config", ".local", ".git", ".env",
    "secrets", "tokens", "__pycache__",
})
PROTECTED_SUFFIXES = frozenset({".key", ".pem", ".token", ".secret", ".pyc"})

HOST_ADDR = "127.0.0.1"
PORT = 7700


# ---------------------------------------------------------------------------
# Data readers
# ---------------------------------------------------------------------------

def load_jsonl(path: Path) -> list:
    if not path.exists():
        return []
    records = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def extract_title(content: str) -> str:
    if not content:
        return "(no content)"
    # arxiv format: "Title of Paper: Abstract text..."
    idx = content.find(": ")
    if 0 < idx < 150:
        return content[:idx]
    return content[:100] + ("…" if len(content) > 100 else "")


def load_all_corpus() -> list:
    records = []
    if not CORPUS.exists():
        return records
    for f in sorted(CORPUS.glob("*.jsonl")):
        for rec in load_jsonl(f):
            rec.setdefault("_source", f.name)
            records.append(rec)
    return records


def load_papers() -> list:
    results = []
    for fname in PAPER_SOURCES:
        path = CORPUS / fname
        if not path.exists():
            continue
        for rec in load_jsonl(path):
            r_type = rec.get("type", "")
            r_topic = rec.get("topic", "")
            if r_type == "paper" or "arxiv" in r_topic or "semantic_scholar" in r_topic:
                content = rec.get("content", "")
                out = dict(rec)
                out["_title"] = extract_title(content)
                out["_preview"] = content[:220] + ("…" if len(content) > 220 else "")
                out["_source"] = fname
                results.append(out)
    return results


def load_accepted() -> list:
    results = []
    for rec in load_all_corpus():
        tg = rec.get("truth_gate") or {}
        tg_status = tg.get("status", "") if isinstance(tg, dict) else ""
        state = (
            rec.get("state")
            or rec.get("status")
            or rec.get("promotion_status")
            or ""
        )
        if state in ACCEPTED_STATES or tg_status in ACCEPTED_STATES:
            results.append(rec)
    return results


def load_elevated() -> list:
    results = []
    for rec in load_all_corpus():
        state = (
            rec.get("state")
            or rec.get("status")
            or rec.get("promotion_status")
            or ""
        )
        if state in ELEVATED_STATES or rec.get("mode", "") in ELEVATED_MODES:
            results.append(rec)
    return results


def truth_gate_summary() -> dict:
    all_records = load_all_corpus()
    by_state: dict = {}
    for rec in all_records:
        state = rec.get("state") or rec.get("status") or "unknown"
        by_state[state] = by_state.get(state, 0) + 1

    recent_outbox = []
    if BRIDGE_OUTBOX.exists():
        for a in sorted(BRIDGE_OUTBOX.glob("*.json"), reverse=True)[:10]:
            try:
                d = json.loads(a.read_text())
                recent_outbox.append({
                    "id": d.get("id"),
                    "status": d.get("status"),
                    "target": d.get("target"),
                    "finished_at": d.get("finished_at"),
                })
            except Exception:
                pass

    return {
        "total": len(all_records),
        "by_state": by_state,
        "processed_key_count": processed_count(),
        "doctrine": (
            "Promotion requires either two verified supporting sources "
            "OR two indirectly supporting facts."
        ),
        "recent_bridge_results": recent_outbox,
    }


# ---------------------------------------------------------------------------
# Existing helpers
# ---------------------------------------------------------------------------

def corpus_files():
    return sorted(CORPUS.glob("*.jsonl")) if CORPUS.exists() else []


def substrate_files():
    return sorted(SUBSTRATE.iterdir()) if SUBSTRATE.exists() else []


def processed_count():
    if PROCESSED.exists():
        try:
            return len(json.loads(PROCESSED.read_text()))
        except Exception:
            return "error"
    return 0


def log_tail(n=20):
    if not INGEST_LOG.exists():
        return ["(no log yet)"]
    lines = INGEST_LOG.read_text().splitlines()
    return lines[-n:] if lines else ["(empty)"]


def search_files(query: str) -> list:
    results = []
    q = query.lower().strip()
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        try:
            for p in root.rglob("*"):
                if any(part in PROTECTED_NAMES for part in p.parts):
                    continue
                if p.suffix in PROTECTED_SUFFIXES:
                    continue
                if p.is_file() and q in p.name.lower():
                    results.append(str(p))
                if len(results) >= 50:
                    return results
        except PermissionError:
            continue
    return results


def validate_approved_path(source_path: str) -> Path:
    p = Path(source_path).resolve()
    approved = [r.resolve() for r in SEARCH_ROOTS]
    if not any(str(p).startswith(str(r)) for r in approved):
        raise ValueError(f"Path not in approved roots: {source_path}")
    return p


def queue_manual_ingest(source_path: str, mode: str, source_type: str, tags: str) -> dict:
    p = validate_approved_path(source_path)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    job_id = f"manual-ingest-{ts}"
    command = " ".join([
        "python3", "manual_ingest.py",
        "--path", shlex.quote(str(p)),
        "--mode", shlex.quote(mode),
        "--type", shlex.quote(source_type),
        "--tags", shlex.quote(tags),
    ])
    task = {
        "id": job_id,
        "target": "NODE_WORKLOAD",
        "cwd": ".",
        "command": command,
        "use_root_helper": False,
        "approval_mode": "auto_if_policy_allows",
        "status": "queued",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    BRIDGE_INBOX.parent.mkdir(parents=True, exist_ok=True)
    BRIDGE_INBOX.write_text(json.dumps(task, indent=2) + "\n", encoding="utf-8")
    return {"job_id": job_id, "status": "queued", "command": command}


def validation_summary() -> dict:
    # Queue counts
    total_q, pending_q, done_q = 0, 0, 0
    if VALIDATION_QUEUE.exists():
        with VALIDATION_QUEUE.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    total_q += 1
                    if r.get("validation_status") in ("done", "failed_permanent", "validated"):
                        done_q += 1
                    else:
                        pending_q += 1
                except json.JSONDecodeError:
                    pass

    # State
    state = {}
    if VALIDATION_STATE.exists():
        try:
            state = json.loads(VALIDATION_STATE.read_text())
        except Exception:
            pass

    # Recent log entries
    recent_log = []
    if VALIDATION_LOG.exists():
        try:
            lines = VALIDATION_LOG.read_text(encoding="utf-8").splitlines()
            for line in reversed(lines[-20:]):
                if line.strip():
                    try:
                        recent_log.append(json.loads(line))
                    except Exception:
                        pass
            recent_log = recent_log[:10]
        except Exception:
            pass

    return {
        "queue_total": total_q,
        "queue_pending": pending_q,
        "queue_done": done_q,
        "total_attempts": state.get("total_attempts", 0),
        "last_attempt_at": state.get("last_attempt_at"),
        "target_per_hour": 60,
        "recent_log": recent_log,
        "semantic_scholar": ss_ingest_summary(),
    }


def offload_summary() -> dict:
    total, pending, archived_count = 0, 0, 0
    recent = []
    if OFFLOAD_LIST.exists():
        with OFFLOAD_LIST.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    total += 1
                    if r.get("archived"):
                        archived_count += 1
                    else:
                        pending += 1
                    recent.append(r)
                except json.JSONDecodeError:
                    pass
    recent = list(reversed(recent))[:10]

    archives = []
    if ARCHIVE_DIR.exists():
        for mf in sorted(ARCHIVE_DIR.glob("*.manifest.json"), reverse=True)[:5]:
            try:
                archives.append(json.loads(mf.read_text()))
            except Exception:
                archives.append({"path": str(mf), "error": "unreadable"})

    return {
        "offload_total": total,
        "pending_archive": pending,
        "already_archived": archived_count,
        "archive_trigger": 100,
        "archives": archives,
        "recent_offloads": recent,
    }


def ss_ingest_summary() -> dict:
    state = {}
    if SS_STATE.exists():
        try:
            state = json.loads(SS_STATE.read_text())
        except Exception:
            pass

    # Count SS candidates in validation queue
    ss_queued = 0
    if VALIDATION_QUEUE.exists():
        with VALIDATION_QUEUE.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    if r.get("source") == "semantic_scholar":
                        ss_queued += 1
                except Exception:
                    pass

    # Recent log lines — capture INFO lines and flag 429/backoff events
    recent_lines: list = []
    backoff_events: list = []
    if SS_LOG.exists():
        try:
            lines = SS_LOG.read_text(encoding="utf-8").splitlines()
            for line in reversed(lines[-50:]):
                if not line.strip():
                    continue
                if "429" in line or "backoff" in line.lower():
                    backoff_events.append(line)
                elif len(recent_lines) < 10:
                    recent_lines.append(line)
            backoff_events = backoff_events[:5]
        except Exception:
            pass

    return {
        "api_key_configured": bool(os.environ.get("S2_API_KEY")),
        "total_ingested": state.get("total_ingested", 0),
        "total_429s": state.get("total_429s", 0),
        "ss_queued": ss_queued,
        "recent_log": recent_lines,
        "backoff_events": backoff_events,
    }


def get_ingest_status() -> dict:
    recent = []
    if BRIDGE_OUTBOX.exists():
        for a in sorted(BRIDGE_OUTBOX.glob("manual-ingest-*.json"), reverse=True)[:5]:
            try:
                recent.append(json.loads(a.read_text()))
            except Exception:
                pass
    return {"recent": recent}


# ---------------------------------------------------------------------------
# Directory browser
# ---------------------------------------------------------------------------

def browse_directory(path: str | None) -> dict:
    approved = [r.resolve() for r in SEARCH_ROOTS]

    if path is None:
        entries = []
        for root in SEARCH_ROOTS:
            if root.exists():
                entries.append({"name": root.name, "path": str(root.resolve()), "type": "dir", "size": None})
        return {"cwd": None, "parent": None, "entries": entries}

    p = Path(path).resolve()
    if not any(str(p).startswith(str(r)) or p == r for r in approved):
        raise ValueError(f"Path outside approved roots: {path}")
    if not p.exists() or not p.is_dir():
        raise ValueError(f"Not a directory: {path}")

    parent_p = p.parent
    approved_strs = {str(r) for r in approved}
    if str(p) in approved_strs:
        parent = "__roots__"
    elif parent_p != p and any(str(parent_p).startswith(str(r)) or parent_p == r for r in approved):
        parent = str(parent_p)
    else:
        parent = None

    entries = []
    try:
        for child in sorted(p.iterdir(), key=lambda x: (x.is_file(), x.name.lower())):
            if child.name.startswith('.'):
                continue
            if any(part in PROTECTED_NAMES for part in child.parts):
                continue
            if child.suffix in PROTECTED_SUFFIXES:
                continue
            size = None
            if child.is_file():
                try:
                    size = child.stat().st_size
                except Exception:
                    pass
            entries.append({
                "name": child.name,
                "path": str(child),
                "type": "dir" if child.is_dir() else "file",
                "size": size,
            })
    except PermissionError:
        pass

    return {"cwd": str(p), "parent": parent, "entries": entries}


# ---------------------------------------------------------------------------
# File intake helpers
# ---------------------------------------------------------------------------

def upload_file(filename: str, data: bytes) -> dict:
    safe_name = Path(filename).name
    if not safe_name or safe_name.startswith('.'):
        raise ValueError("Invalid filename")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / safe_name
    if dest.exists():
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dest = UPLOAD_DIR / f"{dest.stem}_{ts}{dest.suffix}"
    dest.write_bytes(data)
    return {"path": str(dest), "filename": dest.name, "size": len(data)}


def queue_w3m_fetch(url: str, source_type: str, tags: str) -> dict:
    if not url.startswith(("http://", "https://")):
        raise ValueError("URL must start with http:// or https://")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    job_id = f"w3m-{ts}"
    entry = {
        "id": job_id,
        "url": url,
        "source_type": source_type,
        "tags": tags,
        "status": "queued",
        "queued_at": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(WEB_CAPTURE_DIR),
    }
    W3M_FETCH_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with W3M_FETCH_QUEUE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return {"job_id": job_id, "status": "queued", "url": url}


def list_telegram_files() -> list:
    if not TELEGRAM_DIR.exists():
        return []
    results = []
    try:
        for p in sorted(TELEGRAM_DIR.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if p.is_file() and not p.name.startswith('.'):
                results.append({"path": str(p), "name": p.name, "size": p.stat().st_size})
            if len(results) >= 20:
                break
    except PermissionError:
        pass
    return results


# ---------------------------------------------------------------------------
# HTML render
# ---------------------------------------------------------------------------

def render() -> str:
    cf = corpus_files()
    sf = substrate_files()
    pc = processed_count()
    tail = log_tail()
    s2_ok = bool(os.environ.get("S2_API_KEY"))
    s2_status_html = (
        '<span class="status">configured</span>'
        if s2_ok
        else '<span style="color:#f90">MISSING — set S2_API_KEY in environment</span>'
    )

    corpus_rows = "".join(
        f"<tr><td>{f.name}</td><td>{f.stat().st_size:,} bytes</td></tr>"
        for f in cf
    )
    substrate_rows = "".join(f"<tr><td>{f.name}</td></tr>" for f in sf)
    log_lines = "\n".join(tail)

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>NODE</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: monospace; background: #0d0d0d; color: #c8ffc8; margin: 0; padding: 0; }}
  header {{ padding: 1rem 2rem 0; border-bottom: 1px solid #222; }}
  h1 {{ color: #00ff88; margin: 0 0 0.4rem; }}
  .tabs {{ display: flex; flex-wrap: wrap; margin-top: 0.5rem; }}
  .tab {{ padding: 6px 16px; cursor: pointer; border: 1px solid #333;
          border-bottom: none; background: #111; color: #888;
          font-family: monospace; font-size: 0.9em; user-select: none; }}
  .tab.active {{ background: #0d0d0d; color: #00ff88; border-color: #444; }}
  .tab:hover:not(.active) {{ color: #c8ffc8; }}
  .panel {{ display: none; padding: 1.5rem 2rem; }}
  .panel.active {{ display: block; }}
  h2 {{ color: #88ffcc; border-bottom: 1px solid #333; padding-bottom: 4px; margin-top: 1.5rem; }}
  h2:first-child {{ margin-top: 0; }}
  h3 {{ color: #88ffcc; margin-top: 1.3rem; margin-bottom: 0.2rem; font-size: 0.95em; border-left: 2px solid #444; padding-left: 8px; }}
  .intake-section {{ border: 1px solid #2a2a2a; background: #0f0f0f; padding: 0.8rem 1rem; margin-bottom: 0.5rem; max-width: 700px; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 860px; margin-bottom: 1rem; }}
  td, th {{ border: 1px solid #333; padding: 4px 10px; text-align: left; vertical-align: top; }}
  th {{ color: #aaa; }}
  .path {{ color: #aaffff; font-size: 0.85em; }}
  .status {{ color: #00ff88; font-weight: bold; }}
  pre {{ background: #111; padding: 0.8rem; border: 1px solid #333;
         overflow-x: auto; font-size: 0.8em; white-space: pre-wrap; max-width: 900px; }}
  .badge {{ display: inline-block; padding: 1px 7px; border-radius: 3px;
            font-size: 0.78em; border: 1px solid #444; color: #aaa; }}
  .badge.unverified {{ border-color: #555; color: #888; }}
  .badge.verified {{ border-color: #00aa55; color: #00cc66; background: #001a0d; }}
  .badge.accepted {{ border-color: #00ff88; color: #00ff88; background: #002211; }}
  .badge.elevated {{ border-color: #88aaff; color: #88aaff; background: #001133; }}
  .badge.unknown {{ border-color: #555; color: #666; }}
  .online {{ display: inline-block; padding: 2px 8px; border-radius: 3px;
             background: #003322; border: 1px solid #00ff88; color: #00ff88; font-weight: bold; }}
  .note {{ color: #888; font-size: 0.85em; margin: 0.3rem 0 1rem; line-height: 1.5; }}
  .empty-note {{ color: #555; font-style: italic; padding: 0.8rem;
                 border: 1px dashed #333; max-width: 700px; margin-top: 0.5rem; }}
  /* Paper cards */
  .card {{ border: 1px solid #2a2a2a; background: #111; padding: 0.8rem 1rem;
           margin-bottom: 0.7rem; max-width: 860px; }}
  .card-title {{ color: #c8ffc8; font-weight: bold; margin-bottom: 0.3rem; }}
  .card-meta {{ font-size: 0.82em; color: #888; margin-bottom: 0.4rem; }}
  .card-preview {{ font-size: 0.82em; color: #9aaa9a; line-height: 1.5; }}
  /* Form */
  .field {{ margin-bottom: 1.1rem; }}
  .field label {{ display: block; color: #88ffcc; margin-bottom: 4px; font-size: 0.9em; }}
  .field input[type=text], .field select {{
    width: 100%; max-width: 600px; background: #111; color: #c8ffc8;
    border: 1px solid #444; padding: 6px 10px; font-family: monospace; font-size: 0.9em;
  }}
  .field input[type=text]:focus, .field select:focus {{ outline: none; border-color: #00ff88; }}
  .field input[type=file] {{
    color: #c8ffc8; background: #111; border: 1px solid #444;
    padding: 5px 8px; font-family: monospace; font-size: 0.85em; flex: 1;
  }}
  .inline {{ display: flex; gap: 8px; max-width: 600px; }}
  .inline input {{ flex: 1; min-width: 0; }}
  .btn {{ background: #003322; color: #00ff88; border: 1px solid #00ff88;
          padding: 6px 16px; cursor: pointer; font-family: monospace;
          font-size: 0.9em; white-space: nowrap; }}
  .btn:hover {{ background: #004433; }}
  .btn.primary {{ background: #002244; border-color: #0088ff; color: #88ccff; }}
  .btn.primary:hover {{ background: #003355; }}
  .search-results {{ max-width: 600px; border: 1px solid #444; background: #111;
                     max-height: 200px; overflow-y: auto; margin-top: 2px; display: none; }}
  .result {{ padding: 5px 10px; cursor: pointer; font-size: 0.85em; color: #aaffff; }}
  .result:hover {{ background: #1a2a1a; }}
  .msg {{ margin-top: 10px; font-size: 0.85em; min-height: 1.2em; }}
  .msg.ok {{ color: #00ff88; }}
  .msg.err {{ color: #ff5555; }}
  .msg.pending {{ color: #aaa; }}
  .loading {{ color: #555; font-style: italic; padding: 0.5rem 0; }}
  .tg-table td:first-child {{ color: #88ccff; width: 220px; }}
  .tg-table td:last-child {{ color: #c8ffc8; }}
</style>
</head>
<body>
<header>
  <h1>NODE</h1>
  <p style="margin:0 0 0.5rem">Status: <span class="online">ONLINE</span></p>
  <nav class="tabs">
    <div class="tab active" data-tab="status">Status</div>
    <div class="tab" data-tab="papers">Papers</div>
    <div class="tab" data-tab="accepted">Accepted</div>
    <div class="tab" data-tab="elevated">Elevated</div>
    <div class="tab" data-tab="truthgate">Truth Gate</div>
    <div class="tab" data-tab="manualingest">Manual Ingest</div>
    <div class="tab" data-tab="validation">Validation</div>
    <div class="tab" data-tab="offload">Offload Archive</div>
    <div class="tab" data-tab="logs">Logs</div>
    <div class="tab" data-tab="console">Console</div>
  </nav>
</header>

<div id="tab-status" class="panel active">
  <h2>Corpus <small>({len(cf)} files)</small></h2>
  <p class="path">{CORPUS}</p>
  <table>
  <tr><th>File</th><th>Size</th></tr>
  {corpus_rows or '<tr><td colspan="2">(none)</td></tr>'}
  </table>
  <h2>Substrate <small>({len(sf)} files)</small></h2>
  <p class="path">{SUBSTRATE}</p>
  <table>
  <tr><th>File</th></tr>
  {substrate_rows or '<tr><td>(none)</td></tr>'}
  </table>
  <h2>Ingest State</h2>
  <table>
  <tr><th>Item</th><th>Value</th></tr>
  <tr><td>Corpus path</td><td class="path">{CORPUS}</td></tr>
  <tr><td>Substrate path</td><td class="path">{SUBSTRATE}</td></tr>
  <tr><td>Ingest log</td><td class="path">{INGEST_LOG}</td></tr>
  <tr><td>Processed state</td><td class="path">{PROCESSED}</td></tr>
  <tr><td>Processed key count</td><td>{pc}</td></tr>
  <tr><td>Semantic Scholar API</td><td>{s2_status_html}</td></tr>
  </table>
</div>

<div id="tab-papers" class="panel">
  <h2>Papers <span id="papers-count" style="color:#888;font-size:0.8em;font-weight:normal"></span></h2>
  <p class="path">{CORPUS}</p>
  <div id="papers-container"><p class="loading">Loading…</p></div>
</div>

<div id="tab-accepted" class="panel">
  <h2>Accepted</h2>
  <p class="note">Records that have passed truth-gate corroboration.</p>
  <div id="accepted-container"><p class="loading">Loading…</p></div>
</div>

<div id="tab-elevated" class="panel">
  <h2>Elevated</h2>
  <p class="note">Records promoted to LLaMA corpus candidates.</p>
  <div id="elevated-container"><p class="loading">Loading…</p></div>
</div>

<div id="tab-truthgate" class="panel">
  <h2>Truth Gate</h2>
  <p class="note">Promotion requires: two verified supporting sources OR two indirectly supporting facts.</p>
  <div id="truthgate-container"><p class="loading">Loading…</p></div>
</div>

<div id="tab-manualingest" class="panel">
  <h2>Manual Ingest</h2>
  <p class="note">
    Stages local material for pipeline evaluation. Material is NOT automatically promoted.<br>
    Promotion requires truth-gate corroboration: two verified supporting sources OR two indirectly supporting facts.
  </p>

  <div class="field">
    <label>Source path <small style="color:#666">(filled by any route below, or type directly)</small></label>
    <input type="text" id="source-path" placeholder="/home/rick/incoming/...">
  </div>

  <h3>A — Browse existing NODE files</h3>
  <div class="intake-section">
    <p class="note" style="margin-top:0">Navigate folders on NODE. Click a file to fill Source path. Or search by keyword below.</p>
    <div id="fb-breadcrumb" style="font-size:0.85em;color:#888;margin-bottom:0.4rem;padding:4px 6px;background:#0a0a0a;border:1px solid #222">Loading…</div>
    <div id="fb-entries" style="max-height:240px;overflow-y:auto;border:1px solid #333;background:#0a0a0a;margin-bottom:0.5rem"><div class="loading" style="padding:0.5rem">Loading…</div></div>
    <div style="display:flex;gap:8px;max-width:600px">
      <input type="text" id="search-query" placeholder="search by filename keyword"
             style="flex:1;background:#111;color:#c8ffc8;border:1px solid #444;padding:5px 8px;font-family:monospace;font-size:0.85em"
             onkeydown="if(event.key==='Enter')searchFiles()">
      <button class="btn" onclick="searchFiles()">Search</button>
    </div>
    <div class="search-results" id="search-results"></div>
  </div>

  <h3>B — Upload from HOST</h3>
  <div class="intake-section">
    <p class="note" style="margin-top:0">Browser file picker → /home/rick/incoming/uploads/ → Source path auto-filled.</p>
    <div class="field" style="margin-bottom:0">
      <div class="inline">
        <input type="file" id="upload-file-input">
        <button class="btn" onclick="uploadFile()">Upload to NODE</button>
      </div>
      <div class="msg" id="upload-msg"></div>
    </div>
  </div>

  <h3>C — Telegram intake</h3>
  <div class="intake-section">
    <p class="note" style="margin-top:0">Files sent to the Telegram bot land in <span class="path">/home/rick/incoming/telegram/</span>. Click a file to fill Source path.</p>
    <div id="telegram-files-container"><p class="loading">Loading…</p></div>
    <button class="btn" onclick="loadTelegramFiles()" style="margin-top:0.5rem">Refresh</button>
  </div>

  <h3>D — Fetch URL with w3m</h3>
  <div class="intake-section">
    <p class="note" style="margin-top:0">w3m fetches and validates the web source → staged to staging/web_capture/ → truth gate.</p>
    <div class="field" style="margin-bottom:0">
      <div class="inline">
        <input type="text" id="w3m-url" placeholder="https://arxiv.org/abs/...">
        <button class="btn" onclick="fetchW3m()">Fetch with w3m</button>
      </div>
      <div class="msg" id="w3m-msg"></div>
    </div>
  </div>

  <hr style="border:none;border-top:1px solid #333;margin:1.5rem 0">
  <div class="field">
    <label>Mode</label>
    <select id="ingest-mode">
      <option value="stage_only">stage_only — copy to staging, do not promote</option>
      <option value="truth_gate">truth_gate — run verification / corroboration checks</option>
      <option value="corpus_candidate">corpus_candidate — convert to candidate format, not yet promoted</option>
    </select>
  </div>
  <div class="field">
    <label>Source type</label>
    <select id="source-type">
      <option value="notes">notes</option>
      <option value="paper">paper</option>
      <option value="transcript">transcript</option>
      <option value="code">code</option>
      <option value="documentation">documentation</option>
      <option value="web_capture">web_capture</option>
      <option value="unknown">unknown</option>
    </select>
  </div>
  <div class="field">
    <label>Topic / tags <small style="color:#666">(comma-separated)</small></label>
    <input type="text" id="ingest-tags"
           placeholder="quantum_error_correction, qiskit, hybrid_quantum_classical">
  </div>
  <button class="btn primary" onclick="queueIngest()">Queue manual ingest</button>
  <div class="msg" id="ingest-msg"></div>
</div>

<div id="tab-validation" class="panel">
  <h2>Validation</h2>
  <p class="note">
    Continuous w3m validation — target 60 attempts/hour. Per-domain cooldown 120s.<br>
    Writes to <span class="path">{VALIDATION_LOG}</span>
  </p>
  <div id="validation-container"><p class="loading">Loading…</p></div>
</div>

<div id="tab-offload" class="panel">
  <h2>Offload Archive</h2>
  <p class="note">
    Consumed/trained documents accumulate in <span class="path">{OFFLOAD_LIST}</span>.<br>
    Archive created when 100 truth-gate-passed documents are pending.
  </p>
  <div id="offload-container"><p class="loading">Loading…</p></div>
</div>

<div id="tab-logs" class="panel">
  <h2>Recent Ingest Log <small>(last 20 lines)</small></h2>
  <p class="path">{INGEST_LOG}</p>
  <pre>{log_lines or "(empty)"}</pre>
</div>

<div id="tab-console" class="panel">
  <h2>Console</h2>
  <div id="console-log" style="
    font-family:monospace; font-size:0.88em; background:#070707;
    border:1px solid #222; padding:0.8rem 1rem; min-height:240px;
    max-height:480px; overflow-y:auto; margin-bottom:0.7rem;
    white-space:pre-wrap; color:#c8ffc8; line-height:1.5;
  "></div>
  <div style="display:flex; gap:8px; max-width:700px">
    <input type="text" id="console-input"
      placeholder="Tell Kestrel what you need…"
      style="flex:1; background:#111; color:#c8ffc8; border:1px solid #444;
             padding:6px 10px; font-family:monospace; font-size:0.9em;"
      onkeydown="if(event.key==='Enter')consoleSubmit()">
    <button class="btn" onclick="consoleSubmit()">Send</button>
  </div>
</div>

<script>
(function () {{
  var tabLoaders = {{
    'papers':     loadPapers,
    'accepted':   loadAccepted,
    'elevated':   loadElevated,
    'truthgate':  loadTruthGate,
    'validation': loadValidation,
    'offload':    loadOffload,
    'console':    loadConsole
  }};
  var tabLoaded = {{}};

  document.querySelectorAll('.tab').forEach(function (tab) {{
    tab.addEventListener('click', function () {{
      var name = tab.dataset.tab;
      document.querySelectorAll('.tab').forEach(function (t) {{ t.classList.remove('active'); }});
      document.querySelectorAll('.panel').forEach(function (p) {{ p.classList.remove('active'); }});
      tab.classList.add('active');
      document.getElementById('tab-' + name).classList.add('active');
      if (tabLoaders[name] && !tabLoaded[name]) {{
        tabLoaders[name]();
        tabLoaded[name] = true;
      }}
    }});
  }});

  document.addEventListener('click', function (e) {{
    if (!e.target.closest('#search-results') && !e.target.closest('.inline')) {{
      var el = document.getElementById('search-results');
      if (el) el.style.display = 'none';
    }}
  }});

  document.querySelectorAll('.tab').forEach(function (tab) {{
    tab.addEventListener('click', function () {{
      if (tab.dataset.tab === 'manualingest') {{
        loadTelegramFiles();
        fbLoad(null);
      }}
    }});
  }});

  fbLoad(null);
}})();

function stateBadge(state) {{
  var s = (state || 'unknown').toLowerCase();
  return '<span class="badge ' + s + '">' + (state || 'unknown') + '</span>';
}}

// --- Papers ---
function loadPapers() {{
  fetch('/api/papers')
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      var records = data.records || [];
      var el = document.getElementById('papers-container');
      var cnt = document.getElementById('papers-count');
      cnt.textContent = '(' + records.length + ' records)';
      if (!records.length) {{
        el.innerHTML = '<p class="empty-note">No paper records found in ' +
          'training/corpus/arxiv_feed.jsonl or semantic_scholar_feed.jsonl.</p>';
        return;
      }}
      el.innerHTML = records.map(function (r) {{
        return '<div class="card">' +
          '<div class="card-title">' + esc(r._title) + '</div>' +
          '<div class="card-meta">' +
            'topic: ' + esc(r.topic || '—') + ' &nbsp;|&nbsp; ' +
            'type: ' + esc(r.type || '—') + ' &nbsp;|&nbsp; ' +
            'source: ' + esc(r._source || '—') + ' &nbsp;|&nbsp; ' +
            stateBadge(r.state) +
          '</div>' +
          '<div class="card-preview">' + esc(r._preview) + '</div>' +
          '</div>';
      }}).join('');
    }})
    .catch(function () {{
      document.getElementById('papers-container').innerHTML =
        '<p class="empty-note" style="color:#f55">Failed to load papers.</p>';
    }});
}}

// --- Accepted ---
function loadAccepted() {{
  fetch('/api/accepted')
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      var records = data.records || [];
      renderRecords('accepted-container', records,
        'No accepted records yet. Truth-gate promotion has not produced accepted records.');
    }})
    .catch(function () {{
      document.getElementById('accepted-container').innerHTML =
        '<p class="empty-note" style="color:#f55">Failed to load accepted records.</p>';
    }});
}}

// --- Elevated ---
function loadElevated() {{
  fetch('/api/elevated')
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      var records = data.records || [];
      renderRecords('elevated-container', records,
        'No elevated records yet. Corpus candidates have not been promoted.');
    }})
    .catch(function () {{
      document.getElementById('elevated-container').innerHTML =
        '<p class="empty-note" style="color:#f55">Failed to load elevated records.</p>';
    }});
}}

function renderRecords(containerId, records, emptyMsg) {{
  var el = document.getElementById(containerId);
  if (!records.length) {{
    el.innerHTML = '<p class="empty-note">' + esc(emptyMsg) + '</p>';
    return;
  }}
  var rows = records.map(function (r) {{
    return '<tr>' +
      '<td>' + esc(r.topic || r.term || '—') + '</td>' +
      '<td>' + esc(r.type || '—') + '</td>' +
      '<td>' + stateBadge(r.state || r.status) + '</td>' +
      '<td class="path">' + esc(r._source || '—') + '</td>' +
      '<td style="color:#9aaa9a;font-size:0.85em">' +
        esc((r.content || r.meaning || '').slice(0, 120)) + '</td>' +
      '</tr>';
  }}).join('');
  el.innerHTML = '<table><tr>' +
    '<th>Topic / Term</th><th>Type</th><th>State</th><th>Source</th><th>Content</th>' +
    '</tr>' + rows + '</table>';
}}

// --- Truth Gate ---
function loadTruthGate() {{
  fetch('/api/truth-gate')
    .then(function (r) {{ return r.json(); }})
    .then(function (d) {{
      var el = document.getElementById('truthgate-container');
      var byState = d.by_state || {{}};
      var stateRows = Object.keys(byState).sort().map(function (s) {{
        return '<tr><td>' + stateBadge(s) + '</td><td>' + byState[s] + '</td></tr>';
      }}).join('');
      var recentRows = (d.recent_bridge_results || []).map(function (r) {{
        return '<tr><td class="path">' + esc(r.id || '—') + '</td>' +
          '<td>' + esc(r.status || '—') + '</td>' +
          '<td>' + esc(r.target || '—') + '</td>' +
          '<td style="font-size:0.8em;color:#666">' + esc(r.finished_at || '') + '</td></tr>';
      }}).join('');
      el.innerHTML =
        '<table class="tg-table"><tr><th colspan="2">Corpus Summary</th></tr>' +
        '<tr><td>Total records</td><td>' + d.total + '</td></tr>' +
        '<tr><td>Processed key count</td><td>' + d.processed_key_count + '</td></tr>' +
        '<tr><td>Doctrine</td><td style="color:#888;font-size:0.85em">' + esc(d.doctrine) + '</td></tr>' +
        '</table>' +
        '<h2 style="margin-top:1.2rem">By State</h2>' +
        '<table><tr><th>State</th><th>Count</th></tr>' + (stateRows || '<tr><td colspan="2">(none)</td></tr>') + '</table>' +
        '<h2>Recent Bridge Results</h2>' +
        '<table><tr><th>ID</th><th>Status</th><th>Target</th><th>Finished</th></tr>' +
        (recentRows || '<tr><td colspan="4">(none)</td></tr>') + '</table>';
    }})
    .catch(function () {{
      document.getElementById('truthgate-container').innerHTML =
        '<p class="empty-note" style="color:#f55">Failed to load truth gate summary.</p>';
    }});
}}

// --- File browser (Route A) ---
var fbCwd = null;
function fbLoad(path) {{
  var entriesEl = document.getElementById('fb-entries');
  var breadEl = document.getElementById('fb-breadcrumb');
  entriesEl.innerHTML = '<div class="loading" style="padding:0.5rem">Loading…</div>';
  var url = '/api/browse' + (path != null ? '?path=' + encodeURIComponent(path) : '');
  fetch(url, {{ cache: 'no-store' }})
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      if (data.error) {{
        entriesEl.innerHTML = '<div style="color:#f55;padding:0.5rem">' + esc(data.error) + '</div>';
        return;
      }}
      fbCwd = data.cwd;
      var crumb = data.cwd || '/ (roots)';
      var upTarget = data.parent === '__roots__' ? null : data.parent;
      var upBtn = data.parent !== null
        ? '<button class="btn" onclick="fbLoad(' + (upTarget !== null ? JSON.stringify(upTarget) : 'null') + ')" style="padding:2px 8px;font-size:0.8em;margin-right:8px">↑ up</button>'
        : '';
      breadEl.innerHTML = upBtn + '<span style="color:#aaffff">' + esc(crumb) + '</span>';
      entriesEl.innerHTML = '';
      if (!data.entries.length) {{
        entriesEl.innerHTML = '<div style="color:#555;padding:0.5rem;font-style:italic">(empty)</div>';
        return;
      }}
      data.entries.forEach(function (e) {{
        var d = document.createElement('div');
        d.style.cssText = 'padding:5px 10px;cursor:pointer;border-bottom:1px solid #1a1a1a;display:flex;align-items:center;gap:8px;font-family:monospace;font-size:0.85em';
        if (e.type === 'dir') {{
          d.innerHTML = '<span style="color:#f5c542">&#128193;</span><span style="color:#c8ffc8">' + esc(e.name) + '/</span>';
          d.addEventListener('click', function () {{ fbLoad(e.path); }});
        }} else {{
          var sz = e.size !== null ? '<span style="color:#444;font-size:0.8em;margin-left:6px">' + e.size + 'B</span>' : '';
          d.innerHTML = '<span style="color:#555">&#128196;</span><span style="color:#aaffff">' + esc(e.name) + '</span>' + sz;
          d.addEventListener('click', function () {{
            document.getElementById('source-path').value = e.path;
            document.querySelectorAll('#fb-entries div').forEach(function(x){{x.style.background='';}});
            d.style.background = '#001a0d';
            var msg = document.getElementById('ingest-msg');
            msg.className = 'msg ok';
            msg.textContent = 'Selected: ' + e.path;
          }});
        }}
        d.addEventListener('mouseover', function () {{ if (!d.style.background || d.style.background !== 'rgb(0, 26, 13)') d.style.background='#1a2a1a'; }});
        d.addEventListener('mouseout',  function () {{ if (d.style.background !== 'rgb(0, 26, 13)') d.style.background=''; }});
        entriesEl.appendChild(d);
      }});
    }})
    .catch(function () {{
      entriesEl.innerHTML = '<div style="color:#f55;padding:0.5rem">Browse failed.</div>';
    }});
}}

// --- Manual ingest search / queue ---
function searchFiles() {{
  var q = document.getElementById('search-query').value.trim();
  var el = document.getElementById('search-results');
  var msg = document.getElementById('ingest-msg');

  if (!el) {{
    if (msg) {{
      msg.className = 'msg err';
      msg.textContent = 'Search UI error: missing search-results element.';
    }}
    return;
  }}

  el.style.display = 'block';
  el.innerHTML = '';

  if (!q) {{
    el.innerHTML = '<div class="result" style="color:#f5c542">(enter a search term)</div>';
    if (msg) {{
      msg.className = 'msg err';
      msg.textContent = 'Enter a filename keyword or path fragment first.';
    }}
    return;
  }}

  el.innerHTML = '<div class="result" style="color:#888">(searching...)</div>';
  if (msg) {{
    msg.className = 'msg pending';
    msg.textContent = 'Searching files...';
  }}

  fetch('/api/search-files?q=' + encodeURIComponent(q), {{ cache: 'no-store' }})
    .then(function (r) {{
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    }})
    .then(function (data) {{
      var results = data.results || [];
      el.innerHTML = '';
      el.style.display = 'block';

      if (!results.length) {{
        el.innerHTML = '<div class="result" style="color:#888">(no results)</div>';
        if (msg) {{
          msg.className = 'msg err';
          msg.textContent = 'No matching files found.';
        }}
        return;
      }}

      results.forEach(function (p) {{
        var d = document.createElement('div');
        d.className = 'result';
        d.textContent = p;
        d.style.display = 'block';
        d.style.padding = '6px 10px';
        d.style.cursor = 'pointer';
        d.style.borderBottom = '1px solid #222';
        d.addEventListener('click', function () {{
          document.getElementById('source-path').value = p;
          el.style.display = 'none';
          if (msg) {{
            msg.className = 'msg ok';
            msg.textContent = 'Selected: ' + p;
          }}
        }});
        el.appendChild(d);
      }});

      if (msg) {{
        msg.className = 'msg ok';
        msg.textContent = 'Found ' + results.length + ' file(s). Click one to fill Source path.';
      }}
    }})
    .catch(function (err) {{
      el.style.display = 'block';
      el.innerHTML = '<div class="result" style="color:#ff5555">(search error)</div>';
      if (msg) {{
        msg.className = 'msg err';
        msg.textContent = 'Search failed: ' + err.message;
      }}
    }});
}}


function queueIngest() {{
  var source_path = document.getElementById('source-path').value.trim();
  var mode        = document.getElementById('ingest-mode').value;
  var source_type = document.getElementById('source-type').value;
  var tags        = document.getElementById('ingest-tags').value.trim();
  var msg         = document.getElementById('ingest-msg');
  if (!source_path) {{
    msg.className = 'msg err'; msg.textContent = 'Source path is required.'; return;
  }}
  msg.className = 'msg pending'; msg.textContent = 'Queuing…';
  fetch('/api/queue-manual-ingest', {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: JSON.stringify({{ source_path: source_path, mode: mode, source_type: source_type, tags: tags }})
  }})
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      if (data.error) {{
        msg.className = 'msg err'; msg.textContent = 'Error: ' + data.error;
      }} else {{
        msg.className = 'msg ok'; msg.textContent = 'Queued: ' + data.job_id;
      }}
    }})
    .catch(function () {{ msg.className = 'msg err'; msg.textContent = 'Request failed.'; }});
}}

// --- Upload from HOST ---
function uploadFile() {{
  var input = document.getElementById('upload-file-input');
  var msg = document.getElementById('upload-msg');
  if (!input.files || !input.files[0]) {{
    msg.className = 'msg err'; msg.textContent = 'Select a file first.'; return;
  }}
  var file = input.files[0];
  msg.className = 'msg pending'; msg.textContent = 'Uploading ' + file.name + '…';
  fetch('/api/upload-file?filename=' + encodeURIComponent(file.name), {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/octet-stream' }},
    body: file
  }})
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      if (data.error) {{
        msg.className = 'msg err'; msg.textContent = 'Upload error: ' + data.error;
      }} else {{
        document.getElementById('source-path').value = data.path;
        msg.className = 'msg ok';
        msg.textContent = 'Uploaded: ' + data.path + ' (' + data.size + ' bytes)';
      }}
    }})
    .catch(function (err) {{
      msg.className = 'msg err'; msg.textContent = 'Upload failed: ' + err.message;
    }});
}}

// --- Telegram intake ---
function loadTelegramFiles() {{
  var el = document.getElementById('telegram-files-container');
  if (!el) return;
  el.innerHTML = '<p class="loading">Loading…</p>';
  fetch('/api/telegram-files', {{ cache: 'no-store' }})
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      var files = data.files || [];
      if (!files.length) {{
        el.innerHTML = '<p class="note" style="color:#555">(no files in /home/rick/incoming/telegram/ yet)</p>';
        return;
      }}
      el.innerHTML = '';
      files.forEach(function (f) {{
        var d = document.createElement('div');
        d.className = 'result';
        d.style.cssText = 'display:block;padding:6px 10px;border-bottom:1px solid #222;cursor:pointer;margin-bottom:2px;color:#aaffff';
        d.textContent = f.name + ' (' + f.size + ' bytes)';
        d.addEventListener('click', function () {{
          document.getElementById('source-path').value = f.path;
          var msg = document.getElementById('ingest-msg');
          msg.className = 'msg ok';
          msg.textContent = 'Selected: ' + f.path;
        }});
        el.appendChild(d);
      }});
    }})
    .catch(function () {{
      el.innerHTML = '<p class="note" style="color:#f55">Failed to load Telegram files.</p>';
    }});
}}

// --- Fetch URL with w3m ---
function fetchW3m() {{
  var url = document.getElementById('w3m-url').value.trim();
  var msg = document.getElementById('w3m-msg');
  var source_type = document.getElementById('source-type').value;
  var tags = document.getElementById('ingest-tags').value.trim();
  if (!url) {{ msg.className = 'msg err'; msg.textContent = 'Enter a URL first.'; return; }}
  msg.className = 'msg pending'; msg.textContent = 'Queuing w3m fetch…';
  fetch('/api/queue-w3m-fetch', {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: JSON.stringify({{ url: url, source_type: source_type, tags: tags }})
  }})
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      if (data.error) {{
        msg.className = 'msg err'; msg.textContent = 'Error: ' + data.error;
      }} else {{
        msg.className = 'msg ok';
        msg.textContent = 'Queued: ' + data.job_id + ' — w3m will fetch ' + data.url;
      }}
    }})
    .catch(function () {{ msg.className = 'msg err'; msg.textContent = 'Request failed.'; }});
}}

// --- Console ---
var _consolePending = false;
function _consoleAppend(who, text) {{
  var log = document.getElementById('console-log');
  var prefix = who === 'kestrel' ? 'Kestrel\\n' : '→ ';
  log.textContent += prefix + text + '\\n\\n';
  log.scrollTop = log.scrollHeight;
}}

function loadConsole() {{
  fetch('/api/dialog', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{text: '__ready__'}})
  }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      if (d.response) _consoleAppend('kestrel', d.response);
    }})
    .catch(function() {{
      _consoleAppend('kestrel', 'Kestrel online. (status unavailable)');
    }});
}}

function consoleSubmit() {{
  if (_consolePending) return;
  var inp = document.getElementById('console-input');
  var text = inp.value.trim();
  if (!text) return;
  _consoleAppend('user', text);
  inp.value = '';
  _consolePending = true;
  fetch('/api/dialog', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{text: text}})
  }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      _consoleAppend('kestrel', d.response || d.error || '(no response)');
      _consolePending = false;
    }})
    .catch(function() {{
      _consoleAppend('kestrel', 'Request failed.');
      _consolePending = false;
    }});
}}

// --- Validation ---
function loadValidation() {{
  fetch('/api/validation')
    .then(function (r) {{ return r.json(); }})
    .then(function (d) {{
      var el = document.getElementById('validation-container');
      var logRows = (d.recent_log || []).map(function (e) {{
        var st = e.status || '—';
        var color = st === 'validated' ? '#00cc66' : st === 'fetch_error' ? '#f55' : '#aaa';
        return '<tr>' +
          '<td style="font-size:0.8em;color:#666">' + esc(e.attempted_at || '') + '</td>' +
          '<td style="color:' + color + '">' + esc(st) + '</td>' +
          '<td class="path" style="font-size:0.8em">' + esc(e.url || '—') + '</td>' +
          '<td style="font-size:0.8em;color:#888">' + esc(e.error || '') + '</td>' +
          '</tr>';
      }}).join('');
      var ss = d.semantic_scholar || {{}};
      var ssKeyStatus = ss.api_key_configured
        ? '<span style="color:#00cc66">configured</span>'
        : '<span style="color:#f90">MISSING — set S2_API_KEY</span>';
      var ssBackoffRows = (ss.backoff_events || []).map(function (line) {{
        return '<tr><td style="font-size:0.78em;color:#f90">' + esc(line) + '</td></tr>';
      }}).join('');
      var ssLogRows = (ss.recent_log || []).map(function (line) {{
        return '<tr><td style="font-size:0.78em;color:#888">' + esc(line) + '</td></tr>';
      }}).join('');
      el.innerHTML =
        '<table class="tg-table"><tr><th colspan="2">w3m Validation State</th></tr>' +
        '<tr><td>Queue total</td><td>' + d.queue_total + '</td></tr>' +
        '<tr><td>Pending</td><td>' + d.queue_pending + '</td></tr>' +
        '<tr><td>Done</td><td>' + d.queue_done + '</td></tr>' +
        '<tr><td>Total attempts</td><td>' + d.total_attempts + '</td></tr>' +
        '<tr><td>Last attempt</td><td style="font-size:0.85em;color:#888">' + esc(d.last_attempt_at || '(none)') + '</td></tr>' +
        '<tr><td>Target rate</td><td>' + d.target_per_hour + ' / hour</td></tr>' +
        '</table>' +
        '<h2>Recent w3m Validation Log</h2>' +
        '<table><tr><th>Attempted at</th><th>Status</th><th>URL</th><th>Error</th></tr>' +
        (logRows || '<tr><td colspan="4">(no log entries)</td></tr>') +
        '</table>' +
        '<h2 style="margin-top:1.5rem">Semantic Scholar Ingest</h2>' +
        '<table class="tg-table"><tr><th colspan="2">S2 State</th></tr>' +
        '<tr><td>API key</td><td>' + ssKeyStatus + '</td></tr>' +
        '<tr><td>Total ingested</td><td>' + (ss.total_ingested || 0) + '</td></tr>' +
        '<tr><td>Candidates queued</td><td>' + (ss.ss_queued || 0) + '</td></tr>' +
        '<tr><td>Total 429s</td><td>' + (ss.total_429s || 0) + '</td></tr>' +
        '</table>' +
        (ssBackoffRows ? '<h2>Recent 429 / Backoff Events</h2><table>' + ssBackoffRows + '</table>' : '') +
        '<h2>Recent S2 Ingest Log</h2>' +
        '<table><tr><th>Log line</th></tr>' +
        (ssLogRows || '<tr><td style="color:#555">(no log yet — run semantic_scholar_ingest.py)</td></tr>') +
        '</table>';
    }})
    .catch(function () {{
      document.getElementById('validation-container').innerHTML =
        '<p class="empty-note" style="color:#f55">Failed to load validation data.</p>';
    }});
}}

// --- Offload Archive ---
function loadOffload() {{
  fetch('/api/offload')
    .then(function (r) {{ return r.json(); }})
    .then(function (d) {{
      var el = document.getElementById('offload-container');
      var archiveRows = (d.archives || []).map(function (a) {{
        return '<tr>' +
          '<td class="path" style="font-size:0.8em">' + esc(a.batch_id || '—') + '</td>' +
          '<td>' + (a.record_count || '—') + '</td>' +
          '<td style="font-size:0.8em;color:#666">' + esc(a.created_at || '') + '</td>' +
          '</tr>';
      }}).join('');
      var offloadRows = (d.recent_offloads || []).map(function (r) {{
        return '<tr>' +
          '<td class="path" style="font-size:0.8em">' + esc(r.doc_id || '—') + '</td>' +
          '<td>' + stateBadge(r.truth_gate_status) + '</td>' +
          '<td>' + esc(r.archived ? '✓ archived' : 'pending') + '</td>' +
          '<td style="font-size:0.8em;color:#666">' + esc(r.consumed_at || r.trained_at || '') + '</td>' +
          '</tr>';
      }}).join('');
      var ready = d.pending_archive >= d.archive_trigger;
      el.innerHTML =
        '<table class="tg-table"><tr><th colspan="2">Offload Status</th></tr>' +
        '<tr><td>Total offload records</td><td>' + d.offload_total + '</td></tr>' +
        '<tr><td>Pending archive</td><td>' + d.pending_archive +
          (ready ? ' <span style="color:#f90"> ← threshold reached</span>' : '') + '</td></tr>' +
        '<tr><td>Already archived</td><td>' + d.already_archived + '</td></tr>' +
        '<tr><td>Archive trigger</td><td>' + d.archive_trigger + ' records</td></tr>' +
        '</table>' +
        '<h2>Archive Batches <small>(' + (d.archives || []).length + ')</small></h2>' +
        '<table><tr><th>Batch ID</th><th>Records</th><th>Created</th></tr>' +
        (archiveRows || '<tr><td colspan="3">(no archives yet)</td></tr>') + '</table>' +
        '<h2>Recent Offload Entries</h2>' +
        '<table><tr><th>Doc ID</th><th>Truth Gate</th><th>Status</th><th>Consumed at</th></tr>' +
        (offloadRows || '<tr><td colspan="4">(offload list is empty)</td></tr>') + '</table>';
    }})
    .catch(function () {{
      document.getElementById('offload-container').innerHTML =
        '<p class="empty-note" style="color:#f55">Failed to load offload data.</p>';
    }});
}}

function esc(s) {{
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}}
</script>

<script>
(function() {{
  var loaderMap = {{
    papers:     loadPapers,
    accepted:   loadAccepted,
    elevated:   loadElevated,
    truthgate:  loadTruthGate,
    validation: loadValidation,
    offload:    loadOffload,
    console:    loadConsole
  }};
  var loaded = {{}};

  document.addEventListener('click', function(e) {{
    var tab = e.target.closest('.tab');
    if (!tab) return;
    var name = tab.getAttribute('data-tab');
    if (!name) return;

    document.querySelectorAll('.tab').forEach(function(t) {{ t.classList.remove('active'); }});
    document.querySelectorAll('.panel').forEach(function(p) {{ p.classList.remove('active'); }});
    tab.classList.add('active');
    var panel = document.getElementById('tab-' + name);
    if (panel) panel.classList.add('active');

    if (name === 'manualingest') {{
      loadTelegramFiles();
      fbLoad(null);
    }} else if (loaderMap[name] && !loaded[name]) {{
      loaderMap[name]();
      loaded[name] = true;
    }}
  }}, true);
}})();
</script>

</body>
</html>"""


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        p = parsed.path

        if p in ("/", ""):
            body = render().encode()
            self._respond(200, "text/html; charset=utf-8", body)
        elif p == "/api/papers":
            self._json({"records": load_papers()})
        elif p == "/api/accepted":
            self._json({"records": load_accepted()})
        elif p == "/api/elevated":
            self._json({"records": load_elevated()})
        elif p == "/api/truth-gate":
            self._json(truth_gate_summary())
        elif p == "/api/search-files":
            qs = parse_qs(parsed.query)
            q = (qs.get("q") or [""])[0].strip()
            self._json({"results": search_files(q) if q else []})
        elif p == "/api/validation":
            self._json(validation_summary())
        elif p == "/api/offload":
            self._json(offload_summary())
        elif p == "/api/manual-ingest-status":
            self._json(get_ingest_status())
        elif p == "/api/telegram-files":
            self._json({"files": list_telegram_files()})
        elif p == "/api/browse":
            qs = parse_qs(parsed.query)
            browse_path = (qs.get("path") or [None])[0]
            try:
                self._json(browse_directory(browse_path))
            except Exception as exc:
                self._json({"error": str(exc)}, status=400)
        else:
            self._respond(404, "text/plain; charset=utf-8", b"Not found")

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        if path == "/api/queue-manual-ingest":
            try:
                data = json.loads(raw)
                result = queue_manual_ingest(
                    data.get("source_path", ""),
                    data.get("mode", "stage_only"),
                    data.get("source_type", "unknown"),
                    data.get("tags", ""),
                )
                self._json(result)
            except Exception as exc:
                self._json({"error": str(exc)}, status=400)

        elif path == "/api/upload-file":
            try:
                qs = parse_qs(urlparse(self.path).query)
                filename = (qs.get("filename") or ["upload"])[0]
                result = upload_file(filename, raw)
                self._json(result)
            except Exception as exc:
                self._json({"error": str(exc)}, status=400)

        elif path == "/api/queue-w3m-fetch":
            try:
                data = json.loads(raw)
                result = queue_w3m_fetch(
                    data.get("url", ""),
                    data.get("source_type", "web_capture"),
                    data.get("tags", ""),
                )
                self._json(result)
            except Exception as exc:
                self._json({"error": str(exc)}, status=400)

        elif path == "/api/dialog":
            try:
                data = json.loads(raw)
                text = data.get("text", "").strip()
                if not _PERSONA_OK:
                    self._json({"response": "Persona module unavailable."})
                elif text == "__ready__":
                    self._json({"response": _persona.build_ready_state()})
                else:
                    self._json({"response": _persona.handle(text)})
            except Exception as exc:
                self._json({"error": str(exc)}, status=400)

        else:
            self._respond(404, "text/plain; charset=utf-8", b"Not found")

    def _respond(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self._respond(status, "application/json", body)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    server = HTTPServer((HOST_ADDR, PORT), Handler)
    print(f"NODE UI: http://{HOST_ADDR}:{PORT}")
    server.serve_forever()
