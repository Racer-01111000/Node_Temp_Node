#!/usr/bin/env python3
"""
LLaMA consumer for NODE training/llama_ready/ pipeline.

Reads training/candidates/ for corroborated papers, optionally promotes
operator-approved candidates to training/llama_ready/, then sends them
to a local Ollama instance for embedding/ingestion.

Two modes:
  --promote <paper_id>   Operator-triggered: move candidate → llama_ready
  --consume              Process all llama_ready records via Ollama API
  --status               Show pipeline state

Ollama is called with a configurable model (default: llama3).
Does NOT auto-promote. Promote is always explicit operator action.
"""
import argparse
import json
import shutil
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

BASE = Path.home() / "NODE"
CANDIDATES_DIR = BASE / "training/candidates"
LLAMA_READY_DIR = BASE / "training/llama_ready"
OFFLOAD_LIST = BASE / "runtime/offload_list.jsonl"
CONSUME_LOG = BASE / "metadata/ingest_logs/llama_consume.log"

OLLAMA_URL = "http://127.0.0.1:11434"
DEFAULT_MODEL = "llama3"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def log(msg):
    line = f"{utc_now()} {msg}"
    print(line, flush=True)
    CONSUME_LOG.parent.mkdir(parents=True, exist_ok=True)
    with CONSUME_LOG.open("a") as f:
        f.write(line + "\n")


def ollama_available(model):
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            models = [m.get("name", "") for m in data.get("models", [])]
            return any(model in m for m in models)
    except Exception:
        return False


def ollama_generate(model, prompt):
    payload = json.dumps({"model": model, "prompt": prompt, "stream": False}).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read()), None
    except urllib.error.URLError as exc:
        return None, str(exc)
    except Exception as exc:
        return None, str(exc)


def promote(paper_id, model):
    src = CANDIDATES_DIR / f"{paper_id}.json"
    if not src.exists():
        print(f"[llama_consume] candidate not found: {paper_id}", file=sys.stderr)
        return 1

    artifact = json.loads(src.read_text())
    LLAMA_READY_DIR.mkdir(parents=True, exist_ok=True)
    dst = LLAMA_READY_DIR / f"{paper_id}.json"
    artifact["promoted_to_llama_ready_at"] = utc_now()
    artifact["promoted"] = True
    dst.write_text(json.dumps(artifact, indent=2) + "\n")
    log(f"[promote] {paper_id} → llama_ready/")

    # Update offload_list entry
    if OFFLOAD_LIST.exists():
        lines = OFFLOAD_LIST.read_text().splitlines()
        new_lines = []
        for line in lines:
            try:
                rec = json.loads(line)
                if rec.get("doc_id") == paper_id:
                    rec["promoted_to_llama_ready"] = True
                    rec["promoted_at"] = utc_now()
                new_lines.append(json.dumps(rec))
            except Exception:
                new_lines.append(line)
        OFFLOAD_LIST.write_text("\n".join(new_lines) + "\n")

    print(f"[llama_consume] promoted {paper_id} to llama_ready/")
    return 0


def consume(model):
    if not ollama_available(model):
        print(f"[llama_consume] Ollama not reachable or model '{model}' not loaded", file=sys.stderr)
        print(f"  Start: ollama serve")
        print(f"  Pull:  ollama pull {model}")
        return 1

    ready_files = sorted(LLAMA_READY_DIR.glob("*.json")) if LLAMA_READY_DIR.exists() else []
    if not ready_files:
        print("[llama_consume] llama_ready/ is empty — nothing to consume")
        return 0

    consumed = 0
    for path in ready_files:
        artifact = json.loads(path.read_text())
        if artifact.get("llama_consumed"):
            continue

        content = ""
        if artifact.get("corpus_record"):
            content = artifact["corpus_record"].get("content", "")
        if not content:
            content = artifact.get("title", "")

        if not content:
            log(f"[consume] skip {path.stem}: no content")
            continue

        prompt = (
            f"You are building a quantum computing knowledge base. "
            f"Read and summarize the key technical contribution of this paper:\n\n{content[:2000]}"
        )

        log(f"[consume] sending {path.stem} to {model}")
        result, err = ollama_generate(model, prompt)

        if err:
            log(f"[consume] error on {path.stem}: {err}")
            continue

        artifact["llama_consumed"] = True
        artifact["llama_consumed_at"] = utc_now()
        artifact["llama_model"] = model
        artifact["llama_response"] = result.get("response", "")[:500]
        path.write_text(json.dumps(artifact, indent=2) + "\n")
        consumed += 1
        log(f"[consume] done {path.stem}")

    print(f"[llama_consume] consumed={consumed}")
    return 0


def status():
    candidates = list(CANDIDATES_DIR.glob("*.json")) if CANDIDATES_DIR.exists() else []
    ready = list(LLAMA_READY_DIR.glob("*.json")) if LLAMA_READY_DIR.exists() else []
    consumed = [f for f in ready if json.loads(f.read_text()).get("llama_consumed")]

    print(json.dumps({
        "candidates": len(candidates),
        "llama_ready": len(ready),
        "llama_consumed": len(consumed),
        "ollama_up": ollama_available(DEFAULT_MODEL),
        "offload_list_records": sum(1 for _ in open(OFFLOAD_LIST)) if OFFLOAD_LIST.exists() else 0,
    }, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description="NODE LLaMA consumer")
    parser.add_argument("--promote", metavar="PAPER_ID", help="Promote candidate to llama_ready")
    parser.add_argument("--consume", action="store_true", help="Send llama_ready records to Ollama")
    parser.add_argument("--status", action="store_true", help="Show pipeline state")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Ollama model (default: {DEFAULT_MODEL})")
    args = parser.parse_args()

    if args.status:
        return status()
    if args.promote:
        return promote(args.promote, args.model)
    if args.consume:
        return consume(args.model)
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
