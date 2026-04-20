import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BASE = Path.home() / "Node_Temp"
CORPUS = BASE / "training/corpus"
SUBSTRATE = BASE / "training/substrate"
INGEST_LOG = BASE / "metadata/ingest_logs/ingest.log"
PROCESSED = BASE / "metadata/ingest_logs/processed.json"
HOST = "127.0.0.1"
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


def log_tail(n=10):
    if not INGEST_LOG.exists():
        return ["(no log yet)"]
    lines = INGEST_LOG.read_text().splitlines()
    return lines[-n:] if lines else ["(empty)"]


def render():
    cf = corpus_files()
    sf = substrate_files()
    pc = processed_count()
    tail = log_tail()

    corpus_rows = "".join(
        f"<tr><td>{f.name}</td><td>{f.stat().st_size:,} bytes</td></tr>" for f in cf
    )
    substrate_rows = "".join(f"<tr><td>{f.name}</td></tr>" for f in sf)
    log_lines = "\n".join(tail)

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Node_Temp</title>
<style>
  body {{ font-family: monospace; background: #0d0d0d; color: #c8ffc8; margin: 2rem; }}
  h1 {{ color: #00ff88; }}
  h2 {{ color: #88ffcc; border-bottom: 1px solid #333; padding-bottom: 4px; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 1rem; }}
  td, th {{ border: 1px solid #333; padding: 4px 10px; text-align: left; }}
  th {{ color: #aaa; }}
  .path {{ color: #aaffff; font-size: 0.85em; }}
  .status {{ color: #00ff88; font-weight: bold; }}
  pre {{ background: #111; padding: 0.8rem; border: 1px solid #333; overflow-x: auto; font-size: 0.8em; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px;
            background: #003322; border: 1px solid #00ff88; color: #00ff88; }}
</style>
</head>
<body>
<h1>Node_Temp</h1>
<p>Status: <span class="status badge">ONLINE</span></p>

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
<tr><td>Ingest log</td><td class="path">{INGEST_LOG}</td></tr>
<tr><td>Processed state</td><td class="path">{PROCESSED}</td></tr>
<tr><td>Processed key count</td><td>{pc}</td></tr>
</table>

<h2>Recent Ingest Log</h2>
<pre>{log_lines}</pre>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = render().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress access logs


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), Handler)
    print(f"Node_Temp UI: http://{HOST}:{PORT}")
    server.serve_forever()
