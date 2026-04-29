#!/usr/bin/env python3
"""
Kestrel interaction / persona layer.

Shapes HOW Kestrel expresses things — not WHAT is true.

LAYER 1 (epistemic engine) decides: what happened, what is true, what is allowed.
LAYER 2 (this module) decides: how to say it, how to greet, how to present status.

Truth first. Expression second. Always.
"""
import json
import random
import re
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_NODE_BASE    = Path.home() / "NODE"
_GATE_FILE    = Path.home() / ".kestrel-node/runtime/state/promotion_gate.json"
_INGEST_FILE  = Path.home() / ".kestrel-node/runtime/state/ingest_state.json"
_RUNTIME_DIR  = Path.home() / "kestrel-memory/runtime"
_STAGED_DIR   = Path.home() / "kestrel-memory/knowledge/staged"
_TZ_OFFSET    = timedelta(hours=7)  # Asia/Saigon

_OLLAMA_URL   = "http://127.0.0.1:11434"
_OLLAMA_MODEL = "phi3:mini"

_CORPUS_DIRS  = [
    _NODE_BASE / "training/llama_ready",
    _NODE_BASE / "training/candidates",
]

_KESTREL_SYSTEM = (
    "You are Kestrel, a quantum-ready AI assistant built on curated, peer-reviewed "
    "quantum computing research. You reason from validated papers — not speculation. "
    "When the corpus supports an answer, cite it. When it doesn't, say so plainly. "
    "You are concise, precise, and technically grounded. Rick is your operator."
)


# ---------------------------------------------------------------------------
# Ollama / RAG
# ---------------------------------------------------------------------------

def _ollama_available(model: str = _OLLAMA_MODEL) -> bool:
    try:
        req = urllib.request.Request(f"{_OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            models = [m.get("name", "") for m in data.get("models", [])]
            return any(model in m for m in models)
    except Exception:
        return False


def _corpus_search(query: str, n: int = 3) -> list:
    """Return up to n corpus records most relevant to query by keyword overlap."""
    keywords = set(re.sub(r"[^\w\s]", "", query.lower()).split()) - {
        "the", "a", "an", "is", "in", "of", "to", "and", "for", "with", "what",
        "how", "why", "does", "can", "are", "be", "that", "this", "it", "on",
    }
    if not keywords:
        return []
    scored = []
    for corpus_dir in _CORPUS_DIRS:
        if not corpus_dir.exists():
            continue
        for path in corpus_dir.glob("*.json"):
            try:
                rec = json.loads(path.read_text())
                text = (
                    (rec.get("corpus_record") or {}).get("content", "")
                    or rec.get("content", "")
                    or rec.get("title", "")
                ).lower()
                score = sum(1 for kw in keywords if kw in text)
                if score > 0:
                    scored.append((score, rec))
            except Exception:
                pass
    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored[:n]]


def _build_rag_context(records: list) -> str:
    parts = []
    for i, rec in enumerate(records, 1):
        cr = rec.get("corpus_record") or {}
        content = cr.get("content") or rec.get("content") or rec.get("title") or ""
        title = rec.get("title") or content.split(": ", 1)[0]
        arxiv = rec.get("arxiv_id") or cr.get("arxiv_id") or ""
        ref = f"arXiv:{arxiv}" if arxiv else rec.get("paper_id", "")
        parts.append(f"[{i}] {title} ({ref})\n{content[:500]}")
    return "\n\n".join(parts)


def _ollama_ask(user_text: str, context: str = "", model: str = _OLLAMA_MODEL) -> Optional[str]:
    prompt_parts = [_KESTREL_SYSTEM]
    if context:
        prompt_parts.append(f"\nRelevant corpus excerpts:\n{context}")
    prompt_parts.append(f"\nOperator: {user_text}")
    prompt = "\n".join(prompt_parts)

    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"num_predict": 400},
    }).encode()
    req = urllib.request.Request(
        f"{_OLLAMA_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read())
            return data.get("response", "").strip()
    except Exception:
        return None


def kestrel_llm_response(text: str) -> str:
    """Route substantive query through corpus RAG + Ollama. Falls back gracefully."""
    if not _ollama_available():
        return (
            "Ollama is not running. Start it with:\n"
            "  sudo systemctl start ollama\n"
            "  ollama pull phi3:mini"
        )
    records = _corpus_search(text)
    context = _build_rag_context(records) if records else ""
    reply = _ollama_ask(text, context)
    if reply is None:
        return build_error_soft("Ollama call failed")
    if records:
        sources = ", ".join(
            r.get("arxiv_id") or r.get("paper_id", "?")
            for r in records
        )
        reply += f"\n\n[corpus: {sources}]"
    return reply


# ---------------------------------------------------------------------------
# Live state
# ---------------------------------------------------------------------------

def _count_staged_manual() -> int:
    staging = _NODE_BASE / "staging/manual"
    if not staging.exists():
        return 0
    return len(list(staging.glob("*.json")))


def _count_gate_eligible() -> int:
    try:
        d = json.loads(_GATE_FILE.read_text())
        items = (
            d if isinstance(d, list)
            else d.get("eligible_items", d.get("items", d.get("queue", [])))
        )
        return len(items)
    except Exception:
        return 0


def _node_reachable() -> bool:
    import socket
    try:
        s = socket.create_connection(("127.0.0.1", 7700), timeout=0.5)
        s.close()
        return True
    except Exception:
        return False


def _last_ingest() -> str:
    try:
        d = json.loads(_INGEST_FILE.read_text())
        return d.get("last_ingest_at", "—")
    except Exception:
        return "—"


def live_status() -> dict:
    return {
        "staged":         _count_staged_manual(),
        "gate_eligible":  _count_gate_eligible(),
        "node_reachable": _node_reachable(),
        "last_ingest":    _last_ingest(),
    }


# ---------------------------------------------------------------------------
# Time / greeting word
# ---------------------------------------------------------------------------

def _local_hour() -> int:
    return (datetime.now(timezone.utc) + _TZ_OFFSET).hour


def _time_word() -> str:
    h = _local_hour()
    if 5 <= h < 12:
        return "Good morning"
    if 12 <= h < 18:
        return "Good afternoon"
    if 18 <= h < 22:
        return "Good evening"
    return "Good night"


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def build_greeting(status: Optional[dict] = None) -> str:
    if status is None:
        status = live_status()
    node_line = "NODE: reachable" if status["node_reachable"] else "NODE: unreachable"
    return (
        f"{_time_word()}, Rick. Kestrel online.\n"
        f"{node_line}\n"
        f"Staged: {status['staged']}\n"
        f"Eligible at gate: {status['gate_eligible']}"
    )


def build_farewell() -> str:
    h = _local_hour()
    if h >= 20 or h < 5:
        return "Goodnight, Rick."
    return "Later, Rick."


def build_ack() -> str:
    return random.choice(["Confirmed.", "Understood.", "Got it."])


def build_help() -> str:
    return (
        "Commands: update · staged · confirm · promote · verify\n"
        "Ask me anything about quantum computing — I'll search the corpus.\n"
        "Send a URL to stage it. Send a file to ingest it."
    )


def build_status_summary(status: Optional[dict] = None) -> str:
    if status is None:
        status = live_status()
    node_line = "NODE: reachable" if status["node_reachable"] else "NODE: unreachable"
    return (
        f"{node_line}\n"
        f"Staged: {status['staged']}\n"
        f"Eligible at gate: {status['gate_eligible']}\n"
        f"Last ingest: {status['last_ingest']}"
    )


def build_ready_state() -> str:
    st = live_status()
    node_line = "NODE: reachable" if st["node_reachable"] else "NODE: unreachable"
    return (
        f"Kestrel online.\n"
        f"Corpus search ready.\n"
        f"{node_line}\n"
        f"Staged: {st['staged']} · Gate eligible: {st['gate_eligible']}\n"
        f"Awaiting operator input."
    )


def build_error_soft(detail: str = "") -> str:
    return f"Can't reach that right now: {detail}" if detail else "Something didn't work. Try again."


def build_error_hard(detail: str = "") -> str:
    return f"Error: {detail}" if detail else "Hard error — check logs."


def build_action_confirm(action: str) -> str:
    return f"Done: {action}"


# ---------------------------------------------------------------------------
# Intent classification
# ---------------------------------------------------------------------------

_GREETING_WORDS   = {"hello", "hi", "hey", "morning", "afternoon", "evening", "yo",
                      "howdy", "hiya", "sup", "greetings", "heya"}
_GREETING_PHRASES = {"good morning", "good afternoon", "good evening", "good night",
                     "good day", "good to see"}
_FAREWELL_WORDS   = {"bye", "goodbye", "goodnight", "later", "cya", "night", "adios",
                     "cheers", "done", "exit", "quit", "logout"}
_FAREWELL_PHRASES = {"see you", "see ya", "good night", "going offline",
                     "signing off", "i'm done", "im done", "that's all", "thats all"}
_ACK_WORDS        = {"ok", "okay", "got", "sure", "yep", "yes", "ack", "noted",
                     "thanks", "thank", "ty", "k", "kk", "cool", "great", "perfect",
                     "yup", "roger", "copy", "np", "no problem", "good"}
_STATUS_WORDS     = {"status", "state", "where", "things", "overview",
                     "summary", "how's", "hows"}
_HELP_WORDS       = {"help", "commands", "options", "menu", "what"}
_COMMAND_FIRST    = {"update", "confirm", "staged", "promote", "verify"}


def classify_intent(text: str) -> str:
    """
    Returns one of: GREETING, FAREWELL, ACK, STATUS, HELP, COMMAND.
    COMMAND is the default / fall-through for anything substantive.
    """
    t = text.strip().lower()
    if not t:
        return "ACK"

    # Multi-word phrase checks (highest priority for social inputs)
    for phrase in _GREETING_PHRASES:
        if t.startswith(phrase):
            return "GREETING"
    for phrase in _FAREWELL_PHRASES:
        if phrase in t:
            return "FAREWELL"

    # Single-token or short inputs
    words = set(re.sub(r"[^\w\s]", "", t).split())
    if len(t) <= 30:
        if words & _GREETING_WORDS:
            return "GREETING"
        if words & _FAREWELL_WORDS:
            return "FAREWELL"
        if words & _ACK_WORDS:
            return "ACK"
        if words & _STATUS_WORDS:
            return "STATUS"
        if words & _HELP_WORDS or t == "?":
            return "HELP"

    # Longer input — route by first word
    first = t.split()[0] if t.split() else ""
    if first in _COMMAND_FIRST:
        return "COMMAND"
    if t.startswith("http"):
        return "COMMAND"
    if len(t) > 100:
        return "COMMAND"

    return "COMMAND"


# ---------------------------------------------------------------------------
# Command dispatch  (kestrel-memory operations, unchanged logic)
# ---------------------------------------------------------------------------

def _run_shell(cmd: list, timeout: int = 60) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()[-1500:]
    except subprocess.TimeoutExpired:
        return "Timed out."
    except Exception as exc:
        return f"Error: {exc}"


def _cmd_update() -> str:
    reviews = sorted(_RUNTIME_DIR.glob("sufficiency_review_*.txt"))
    summary = ""
    if reviews:
        lines = reviews[-1].read_text().splitlines()
        summary = " ".join(
            l for l in lines
            if re.match(r"^(STATUS:|  PASS:|  WARN:|  FAIL:|  Total:)", l)
        )
    gate = _count_gate_eligible()
    return f"Sufficiency: {summary}\nPromotion gate: {gate} item(s)"


def _cmd_confirm() -> str:
    log = _RUNTIME_DIR / "verify_claims.log"
    if not log.exists():
        return "No verify_claims.log found."
    lines = [l for l in log.read_text().splitlines() if l.strip()]
    return "Last verify entries:\n" + "\n".join(lines[-5:])


def _cmd_staged() -> str:
    lines = []
    for f in sorted(_STAGED_DIR.glob("*.json")):
        try:
            d = json.loads(f.read_text())
            title = d.get("title") or d.get("id") or f.name
            level = d.get("epistemic_level") or d.get("epistemic_status") or "?"
            lines.append(f"{title} [{level}]")
        except Exception:
            pass
    return "Staged:\n" + ("\n".join(lines) if lines else "Nothing staged.")


def _cmd_promote() -> str:
    out = _run_shell(["bash", str(_RUNTIME_DIR / "run_promotion_queue.sh")])
    return f"Promotion:\n{out}" if out else "Promotion ran — no output."


def _cmd_verify() -> str:
    out = _run_shell(["bash", str(_RUNTIME_DIR / "verify_claims.sh")])
    return f"Verify:\n{out}" if out else "Verify ran — no output."


def _cmd_fetch_url(url: str) -> str:
    try:
        import requests as _req
        ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        slug = f"tg_url_{ts}"
        html = _req.get(url, timeout=15).text
        content = re.sub(r"<[^>]+>", " ", html)
        content = re.sub(r"\s+", " ", content).strip()[:3000]
        if not content:
            return f"Fetched nothing from {url}"
        d = {
            "id": slug, "title": url, "source": url,
            "epistemic_level": "claim", "review_status": "pending",
            "content": content,
        }
        (_STAGED_DIR / f"{slug}.json").write_text(json.dumps(d, indent=2))
        return f"Staged URL as {slug}.json [claim]"
    except Exception as exc:
        return build_error_soft(str(exc))


def _cmd_stage_text(text: str) -> str:
    ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = f"tg_ingest_{ts}"
    d = {
        "id": slug, "title": text[:60], "source": "dialog",
        "epistemic_level": "claim", "review_status": "pending",
        "content": text,
    }
    (_STAGED_DIR / f"{slug}.json").write_text(json.dumps(d, indent=2))
    return f"Staged as {slug}.json [claim]"


def dispatch_command(text: str) -> str:
    low = text.lower().strip()
    first = low.split()[0] if low.split() else ""

    if first in ("update", "status"):
        return _cmd_update()
    if first == "confirm":
        return _cmd_confirm()
    if first == "staged":
        return _cmd_staged()
    if first == "promote":
        return _cmd_promote()
    if first == "verify":
        return _cmd_verify()
    if low.startswith("http"):
        return _cmd_fetch_url(text.strip())
    if len(text) > 100:
        return kestrel_llm_response(text)

    return kestrel_llm_response(text)


# ---------------------------------------------------------------------------
# Top-level handler
# ---------------------------------------------------------------------------

def handle(text: str) -> str:
    """Classify intent and return the appropriate Kestrel response."""
    intent = classify_intent(text)
    if intent == "GREETING":
        return build_greeting()
    if intent == "FAREWELL":
        return build_farewell()
    if intent == "ACK":
        return build_ack()
    if intent == "STATUS":
        return build_status_summary()
    if intent == "HELP":
        return build_help()
    return dispatch_command(text)
