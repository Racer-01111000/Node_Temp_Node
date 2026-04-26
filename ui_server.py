import json
import shlex
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

BASE = Path.home() / "Node_Temp_Node"
CORPUS = BASE / "training/corpus"
SUBSTRATE = BASE / "training/substrate"
INGEST_LOG = BASE / "metadata/ingest_logs/ingest.log"
PROCESSED = BASE / "metadata/ingest_logs/processed.json"

BRIDGE_INBOX = Path.home() / "GPT_Firefox_extension/control/inbox/current.json"
BRIDGE_OUTBOX = Path.home() / "GPT_Firefox_extension/control/outbox"

SEARCH_ROOTS = [
    Path.home() / "incoming",
    Path.home() / "Node_Temp_Node",
]

PROTECTED_NAMES = frozenset({
    ".ssh", ".config", ".local", ".git", ".env",
    "secrets", "tokens", "__pycache__",
})
PROTECTED_SUFFIXES = frozenset({".key", ".pem", ".token", ".secret", ".pyc"})

HOST_ADDR = "127.0.0.1"
PORT = 7700


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


def get_ingest_status() -> dict:
    recent = []
    if BRIDGE_OUTBOX.exists():
        artifacts = sorted(
            BRIDGE_OUTBOX.glob("manual-ingest-*.json"), reverse=True
        )[:5]
        for a in artifacts:
            try:
                recent.append(json.loads(a.read_text()))
            except Exception:
                pass
    return {"recent": recent}


def render() -> str:
    cf = corpus_files()
    sf = substrate_files()
    pc = processed_count()
    tail = log_tail()

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
<title>Node_Temp_Node</title>
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
  table {{ border-collapse: collapse; width: 100%; max-width: 800px; margin-bottom: 1rem; }}
  td, th {{ border: 1px solid #333; padding: 4px 10px; text-align: left; }}
  th {{ color: #aaa; }}
  .path {{ color: #aaffff; font-size: 0.85em; }}
  .status {{ color: #00ff88; font-weight: bold; }}
  pre {{ background: #111; padding: 0.8rem; border: 1px solid #333;
         overflow-x: auto; font-size: 0.8em; white-space: pre-wrap; max-width: 900px; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px;
            background: #003322; border: 1px solid #00ff88; color: #00ff88; }}
  .stub {{ color: #555; font-style: italic; padding: 1rem;
           border: 1px dashed #333; max-width: 600px; }}
  .note {{ color: #888; font-size: 0.85em; margin: 0.3rem 0 1rem; line-height: 1.5; }}
  /* Form */
  .field {{ margin-bottom: 1.1rem; }}
  .field label {{ display: block; color: #88ffcc; margin-bottom: 4px; font-size: 0.9em; }}
  .field input[type=text], .field select {{
    width: 100%; max-width: 600px; background: #111; color: #c8ffc8;
    border: 1px solid #444; padding: 6px 10px;
    font-family: monospace; font-size: 0.9em;
  }}
  .field input[type=text]:focus, .field select:focus {{
    outline: none; border-color: #00ff88;
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
</style>
</head>
<body>
<header>
  <h1>Node_Temp_Node</h1>
  <p style="margin:0 0 0.5rem">Status: <span class="status badge">ONLINE</span></p>
  <nav class="tabs">
    <div class="tab active" data-tab="status">Status</div>
    <div class="tab" data-tab="papers">Papers</div>
    <div class="tab" data-tab="accepted">Accepted</div>
    <div class="tab" data-tab="elevated">Elevated</div>
    <div class="tab" data-tab="truthgate">Truth Gate</div>
    <div class="tab" data-tab="manualingest">Manual Ingest</div>
    <div class="tab" data-tab="logs">Logs</div>
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
  </table>
</div>

<div id="tab-papers" class="panel">
  <h2>Papers</h2>
  <p class="stub">Corpus paper browser — not yet implemented.</p>
</div>

<div id="tab-accepted" class="panel">
  <h2>Accepted</h2>
  <p class="stub">Accepted (truth-gate passed) records — not yet implemented.</p>
</div>

<div id="tab-elevated" class="panel">
  <h2>Elevated</h2>
  <p class="stub">Elevated records (promoted to LLaMA corpus candidates) — not yet implemented.</p>
</div>

<div id="tab-truthgate" class="panel">
  <h2>Truth Gate</h2>
  <p class="stub">Truth gate queue and results — not yet implemented.</p>
  <p class="note">Promotion requires: two verified supporting sources OR two indirectly supporting facts.</p>
</div>

<div id="tab-manualingest" class="panel">
  <h2>Manual Ingest</h2>
  <p class="note">
    Stages selected local material for pipeline evaluation. Material is NOT automatically promoted.<br>
    Promotion requires truth-gate corroboration: two verified supporting sources OR two indirectly supporting facts.
  </p>

  <div class="field">
    <label>Source path</label>
    <input type="text" id="source-path" placeholder="/home/rick/incoming/notes.txt">
  </div>

  <div class="field">
    <label>Search repo files</label>
    <div class="inline">
      <input type="text" id="search-query" placeholder="filename keyword or path fragment"
             onkeydown="if(event.key==='Enter')searchFiles()">
      <button class="btn" onclick="searchFiles()">Search files</button>
    </div>
    <div class="search-results" id="search-results"></div>
  </div>

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

<div id="tab-logs" class="panel">
  <h2>Recent Ingest Log <small>(last 20 lines)</small></h2>
  <p class="path">{INGEST_LOG}</p>
  <pre>{log_lines or "(empty)"}</pre>
</div>

<script>
(function () {{
  document.querySelectorAll('.tab').forEach(function (tab) {{
    tab.addEventListener('click', function () {{
      document.querySelectorAll('.tab').forEach(function (t) {{ t.classList.remove('active'); }});
      document.querySelectorAll('.panel').forEach(function (p) {{ p.classList.remove('active'); }});
      tab.classList.add('active');
      document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    }});
  }});

  document.addEventListener('click', function (e) {{
    if (!e.target.closest('#search-results') && !e.target.closest('.inline')) {{
      var el = document.getElementById('search-results');
      if (el) el.style.display = 'none';
    }}
  }});
}})();

function searchFiles() {{
  var q = document.getElementById('search-query').value.trim();
  if (!q) return;
  var el = document.getElementById('search-results');
  el.style.display = 'block';
  el.innerHTML = '<div class="result" style="color:#666">(searching…)</div>';
  fetch('/api/search-files?q=' + encodeURIComponent(q))
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      var results = data.results || [];
      if (!results.length) {{
        el.innerHTML = '<div class="result" style="color:#666">(no results)</div>';
        return;
      }}
      el.innerHTML = '';
      results.forEach(function (p) {{
        var d = document.createElement('div');
        d.className = 'result';
        d.textContent = p;
        d.addEventListener('click', function () {{
          document.getElementById('source-path').value = p;
          el.style.display = 'none';
        }});
        el.appendChild(d);
      }});
    }})
    .catch(function () {{
      el.innerHTML = '<div class="result" style="color:#f55">(search error)</div>';
    }});
}}

function queueIngest() {{
  var source_path = document.getElementById('source-path').value.trim();
  var mode        = document.getElementById('ingest-mode').value;
  var source_type = document.getElementById('source-type').value;
  var tags        = document.getElementById('ingest-tags').value.trim();
  var msg         = document.getElementById('ingest-msg');

  if (!source_path) {{
    msg.className = 'msg err';
    msg.textContent = 'Source path is required.';
    return;
  }}
  msg.className = 'msg pending';
  msg.textContent = 'Queuing…';

  fetch('/api/queue-manual-ingest', {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: JSON.stringify({{ source_path: source_path, mode: mode, source_type: source_type, tags: tags }})
  }})
    .then(function (r) {{ return r.json(); }})
    .then(function (data) {{
      if (data.error) {{
        msg.className = 'msg err';
        msg.textContent = 'Error: ' + data.error;
      }} else {{
        msg.className = 'msg ok';
        msg.textContent = 'Queued: ' + data.job_id;
      }}
    }})
    .catch(function () {{
      msg.className = 'msg err';
      msg.textContent = 'Request failed.';
    }});
}}
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        p = parsed.path

        if p in ("/", ""):
            body = render().encode()
            self._respond(200, "text/html; charset=utf-8", body)
        elif p == "/api/search-files":
            qs = parse_qs(parsed.query)
            q = (qs.get("q") or [""])[0].strip()
            self._json({"results": search_files(q) if q else []})
        elif p == "/api/manual-ingest-status":
            self._json(get_ingest_status())
        else:
            self._respond(404, "text/plain; charset=utf-8", b"Not found")

    def do_POST(self):
        if urlparse(self.path).path == "/api/queue-manual-ingest":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
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
    print(f"Node_Temp_Node UI: http://{HOST_ADDR}:{PORT}")
    server.serve_forever()
