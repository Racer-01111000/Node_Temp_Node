#!/usr/bin/env python3
"""
Telegram bot for NODE / kestrel-memory integration.

Text commands are forwarded to kestrel-memory handlers (unchanged behavior).
File/document attachments are downloaded to ~/incoming/telegram/ for Manual Ingest.

Token is read from ~/.telegram_token (KEY=VALUE format) — never committed.
"""
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

_CFG_FILE = Path.home() / ".telegram_token"

def _load_config() -> dict:
    if not _CFG_FILE.exists():
        raise RuntimeError(f"Missing config: {_CFG_FILE}  (needs TOKEN= and CHAT_ID=)")
    cfg = {}
    for line in _CFG_FILE.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip()
    return cfg

_cfg = _load_config()
TOKEN   = _cfg["TOKEN"]
CHAT_ID = _cfg["CHAT_ID"]
API     = f"https://api.telegram.org/bot{TOKEN}"

RUNTIME_DIR  = Path.home() / "kestrel-memory/runtime"
STAGED_DIR   = Path.home() / "kestrel-memory/knowledge/staged"
TELEGRAM_DIR = Path.home() / "incoming/telegram"
OFFSET_FILE  = Path("/tmp/tg_bot_offset")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def send(text: str) -> None:
    try:
        requests.post(f"{API}/sendMessage",
                      json={"chat_id": CHAT_ID, "text": text[:4000]},
                      timeout=10)
    except Exception:
        pass


def _safe_name(raw: str) -> str:
    name = Path(raw).name          # strip any path component
    name = re.sub(r"[^\w.\-]", "_", name)
    name = re.sub(r"_+", "_", name)
    return (name[:200] or "file")


def _download(file_id: str, filename: str) -> Optional[Path]:
    try:
        r = requests.get(f"{API}/getFile", params={"file_id": file_id}, timeout=15)
        r.raise_for_status()
        file_path = r.json()["result"]["file_path"]
        url = f"https://api.telegram.org/file/bot{TOKEN}/{file_path}"
        content = requests.get(url, timeout=120).content

        TELEGRAM_DIR.mkdir(parents=True, exist_ok=True)
        ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe = _safe_name(filename)
        dest = TELEGRAM_DIR / f"{ts}_{safe}"
        if dest.exists():
            dest = TELEGRAM_DIR / f"{ts}_{os.getpid()}_{safe}"
        dest.write_bytes(content)
        return dest
    except Exception as exc:
        send(f"Download error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Document / attachment handler
# ---------------------------------------------------------------------------

def handle_attachment(msg: dict) -> None:
    # document (most common for files)
    for key in ("document", "video", "audio", "voice"):
        f = msg.get(key)
        if f:
            fname = f.get("file_name") or f.get("title") or key
            dest = _download(f["file_id"], fname)
            if dest:
                send(
                    f"Saved: {dest}\n\n"
                    f"Open the Manual Ingest UI → Telegram section → click the file → "
                    f"choose type and mode → Queue."
                )
            return

    # photo: pick largest size
    photos = msg.get("photo")
    if photos:
        p = max(photos, key=lambda x: x.get("file_size", 0))
        dest = _download(p["file_id"], "photo.jpg")
        if dest:
            send(
                f"Saved: {dest}\n\n"
                f"Open the Manual Ingest UI → Telegram section → click the file → "
                f"choose type and mode → Queue."
            )


# ---------------------------------------------------------------------------
# Text / command handler  (kestrel-memory behavior, unchanged)
# ---------------------------------------------------------------------------

HELP = (
    "Kestrel commands:\n"
    "update / status — sufficiency summary + promotion gate count\n"
    "confirm — last verify_claims.log entries\n"
    "staged — list staged items and epistemic levels\n"
    "promote — run promotion queue\n"
    "verify — run verify_claims.sh\n"
    "help — this message\n"
    "<url> — fetch and stage as new entry\n"
    "<text >100 chars> — stage as claim\n"
    "Send a file/document → saved to ~/incoming/telegram/ for Manual Ingest"
)


def handle_text(text: str) -> None:
    low = text.lower()

    if any(k in low for k in ("update", "status")):
        reviews = sorted(RUNTIME_DIR.glob("sufficiency_review_*.txt"))
        summary = ""
        if reviews:
            lines = reviews[-1].read_text().splitlines()
            summary = " ".join(
                l for l in lines
                if re.match(r"^(STATUS:|  PASS:|  WARN:|  FAIL:|  Total:)", l)
            )
        gate_count: object = "unknown"
        try:
            gf = Path.home() / ".kestrel-node/runtime/state/promotion_gate.json"
            d = json.loads(gf.read_text())
            items = d if isinstance(d, list) else d.get("items", d.get("queue", []))
            gate_count = len(items)
        except Exception:
            pass
        send(f"Sufficiency: {summary} | Promotion gate: {gate_count} item(s)")

    elif "confirm" in low:
        log = RUNTIME_DIR / "verify_claims.log"
        last = ""
        if log.exists():
            lines = [l for l in log.read_text().splitlines() if l.strip()]
            last = "\n".join(lines[-5:])
        send(f"Last verify_claims.log entries:\n{last}")

    elif "staged" in low:
        lines = []
        for f in sorted(STAGED_DIR.glob("*.json")):
            try:
                d = json.loads(f.read_text())
                title = d.get("title") or d.get("id") or f.name
                level = d.get("epistemic_level") or d.get("epistemic_status") or "?"
                lines.append(f"{title} [{level}]")
            except Exception:
                pass
        send("Staged items:\n" + ("\n".join(lines) if lines else "No staged JSON items."))

    elif "promote" in low:
        r = subprocess.run(
            ["bash", str(RUNTIME_DIR / "run_promotion_queue.sh")],
            capture_output=True, text=True, timeout=60
        )
        send("Promotion result:\n" + (r.stdout + r.stderr)[-1000:])

    elif "verify" in low:
        r = subprocess.run(
            ["bash", str(RUNTIME_DIR / "verify_claims.sh")],
            capture_output=True, text=True, timeout=60
        )
        send("verify_claims result:\n" + (r.stdout + r.stderr)[-1000:])

    elif "help" in low:
        send(HELP)

    elif text.lower().startswith("http"):
        ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        slug = f"tg_url_{ts}"
        try:
            html    = requests.get(text, timeout=15).text
            content = re.sub(r"<[^>]+>", " ", html)
            content = re.sub(r"\s+", " ", content).strip()[:3000]
        except Exception:
            send(f"Failed to fetch URL: {text}")
            return
        d = {
            "id": slug, "title": text, "source": text,
            "epistemic_level": "claim", "review_status": "pending",
            "content": content,
        }
        (STAGED_DIR / f"{slug}.json").write_text(json.dumps(d, indent=2))
        send(f"Staged URL as {slug}.json [claim]")

    elif len(text) > 100:
        ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        slug = f"tg_ingest_{ts}"
        d = {
            "id": slug, "title": text[:60], "source": "telegram",
            "epistemic_level": "claim", "review_status": "pending",
            "content": text,
        }
        (STAGED_DIR / f"{slug}.json").write_text(json.dumps(d, indent=2))
        send(f"Staged as {slug}.json [claim]")

    else:
        send(HELP)


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def main() -> None:
    offset = int(OFFSET_FILE.read_text().strip()) if OFFSET_FILE.exists() else 0

    while True:
        try:
            r = requests.get(
                f"{API}/getUpdates",
                params={"offset": offset, "timeout": 30},
                timeout=40,
            )
            updates = r.json().get("result", [])
        except Exception:
            time.sleep(5)
            continue

        for update in updates:
            uid = update["update_id"]
            msg = update.get("message", {})

            if any(k in msg for k in ("document", "photo", "video", "audio", "voice")):
                handle_attachment(msg)
            elif "text" in msg:
                handle_text(msg["text"])

            offset = uid + 1
            OFFSET_FILE.write_text(str(offset))


if __name__ == "__main__":
    main()
