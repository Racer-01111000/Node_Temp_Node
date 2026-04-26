#!/usr/bin/env python3
"""
Offload manager for trained/consumed Node_Temp_Node corpus documents.

Reads runtime/offload_list.jsonl.
When the list reaches the archive trigger count (default 100), creates a
tar.gz archive with a manifest JSON. Marks archived entries. Preserves
all source provenance and truth-gate reports.

Safety:
  - Does NOT delete provenance.
  - Does NOT mutate accepted/elevated status.
  - Does NOT archive unverified material as trained.
  - Does NOT auto-promote anything.
  - Archives only when triggered by reaching the count threshold.

Usage:
  python3 offload_trained.py              # check and archive if threshold reached
  python3 offload_trained.py --status     # show current offload list count
  python3 offload_trained.py --dry-run    # show what would be archived
  python3 offload_trained.py --force      # archive even if below threshold
"""
import argparse
import hashlib
import json
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

BASE = Path.home() / "Node_Temp_Node"
CONFIG_PATH = BASE / "config/source_registry.json"
OFFLOAD_PATH = BASE / "runtime/offload_list.jsonl"
ARCHIVE_DIR = BASE / "archive/offloaded_batches"
REPORTS_DIR = BASE / "metadata/truth_gate_reports"

DEFAULT_TRIGGER = 100


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text())
            return data.get("offload", {})
        except Exception:
            pass
    return {}


def load_offload_list() -> list:
    if not OFFLOAD_PATH.exists():
        return []
    records = []
    with OFFLOAD_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def save_offload_list(records: list) -> None:
    OFFLOAD_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OFFLOAD_PATH.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def pending_records(records: list) -> list:
    return [r for r in records if not r.get("archived", False)]


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


def archive_batch(batch: list, trigger: int, dry_run: bool = False) -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # find next batch number
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    existing = sorted(ARCHIVE_DIR.glob("llama_offload_*.tar.gz"))
    batch_num = len(existing) + 1
    batch_id = f"llama_offload_{ts}_{batch_num:04d}"
    archive_path = ARCHIVE_DIR / f"{batch_id}.tar.gz"
    manifest_path = ARCHIVE_DIR / f"{batch_id}.manifest.json"

    manifest = {
        "batch_id": batch_id,
        "created_at": utc_now(),
        "record_count": len(batch),
        "trigger_count": trigger,
        "records": [],
        "truth_gate_reports_included": [],
        "archived": not dry_run,
    }

    if dry_run:
        print(f"[dry-run] would create {archive_path}")
        print(f"[dry-run] {len(batch)} records to archive")
        for r in batch[:5]:
            print(f"  {r.get('doc_id', '?')} | {r.get('source', '?')} | {r.get('truth_gate_status', '?')}")
        if len(batch) > 5:
            print(f"  ... and {len(batch) - 5} more")
        return {"status": "dry_run", "batch_id": batch_id, "count": len(batch)}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        docs_dir = tmp / "documents"
        docs_dir.mkdir()
        reports_dir = tmp / "truth_gate_reports"
        reports_dir.mkdir()

        for rec in batch:
            doc_id = rec.get("doc_id", "unknown")
            safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in doc_id)

            doc_path = docs_dir / f"{safe_id}.json"
            doc_path.write_text(json.dumps(rec, indent=2) + "\n", encoding="utf-8")

            manifest["records"].append({
                "doc_id": doc_id,
                "source": rec.get("source"),
                "source_url": rec.get("source_url"),
                "title": rec.get("title"),
                "truth_gate_status": rec.get("truth_gate_status"),
                "promotion_status": rec.get("promotion_status"),
                "consumed_at": rec.get("trained_at") or rec.get("consumed_at"),
                "content_hash": rec.get("content_hash"),
            })

            # include truth-gate report if present
            report_src = REPORTS_DIR / f"{doc_id}.json"
            if report_src.exists():
                report_dst = reports_dir / f"{doc_id}.json"
                report_dst.write_bytes(report_src.read_bytes())
                manifest["truth_gate_reports_included"].append(doc_id)

        manifest_tmp = tmp / "manifest.json"
        manifest_tmp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(docs_dir, arcname="documents")
            tar.add(reports_dir, arcname="truth_gate_reports")
            tar.add(manifest_tmp, arcname="manifest.json")

    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"[offload_trained] archived {len(batch)} records → {archive_path}")
    return {"status": "archived", "batch_id": batch_id, "path": str(archive_path), "count": len(batch)}


def run(dry_run: bool = False, force: bool = False, status_only: bool = False) -> int:
    cfg = load_config()
    trigger = cfg.get("archive_trigger_count", DEFAULT_TRIGGER)

    all_records = load_offload_list()
    pending = pending_records(all_records)

    if status_only:
        print(json.dumps({
            "total_offload_records": len(all_records),
            "pending_archive": len(pending),
            "already_archived": len(all_records) - len(pending),
            "archive_trigger": trigger,
            "ready_to_archive": len(pending) >= trigger,
            "existing_archives": len(list(ARCHIVE_DIR.glob("*.tar.gz"))) if ARCHIVE_DIR.exists() else 0,
        }, indent=2))
        return 0

    if len(pending) == 0:
        print("[offload_trained] offload list is empty — nothing to archive")
        return 0

    if len(pending) < trigger and not force:
        print(f"[offload_trained] {len(pending)}/{trigger} pending — threshold not reached")
        return 0

    # Safety: do not archive unverified material as trained
    safe_batch = [
        r for r in pending
        if r.get("truth_gate_status") in ("ACCEPTED", "ELEVATED", "accepted", "elevated")
        or r.get("promotion_status") in ("accepted", "elevated")
    ]

    if not safe_batch:
        print("[offload_trained] no truth-gate-passed records in offload list — nothing to archive safely")
        return 0

    result = archive_batch(safe_batch, trigger, dry_run=dry_run)

    if not dry_run and result.get("status") == "archived":
        archived_ids = {r.get("doc_id") for r in safe_batch}
        for rec in all_records:
            if rec.get("doc_id") in archived_ids:
                rec["archived"] = True
                rec["archived_batch"] = result["batch_id"]
                rec["archived_at"] = utc_now()
        save_offload_list(all_records)
        print(f"[offload_trained] marked {len(safe_batch)} records as archived")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Offload manager for trained corpus documents")
    parser.add_argument("--status", action="store_true", help="Show offload list status and exit")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be archived without doing it")
    parser.add_argument("--force", action="store_true", help="Archive even if below threshold")
    args = parser.parse_args()
    return run(dry_run=args.dry_run, force=args.force, status_only=args.status)


if __name__ == "__main__":
    raise SystemExit(main())
