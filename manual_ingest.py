#!/usr/bin/env python3
"""
Controlled manual ingest into Node_Temp_Node staging.

Stages local material for pipeline evaluation. Does NOT promote to corpus.
Promotion requires truth-gate corroboration (two verified sources or two
indirect supporting facts).
"""
import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

APPROVED_ROOTS = [
    Path.home() / "incoming",
    Path.home() / "Node_Temp_Node",
]

STAGING = Path.home() / "Node_Temp_Node/staging/manual"

VALID_MODES = {"stage_only", "truth_gate", "corpus_candidate"}
VALID_TYPES = {"notes", "paper", "transcript", "code", "documentation", "web_capture", "unknown"}


def validate_path(source_path: str) -> Path:
    p = Path(source_path).resolve()
    approved = [r.resolve() for r in APPROVED_ROOTS]
    if not any(str(p).startswith(str(r)) for r in approved):
        raise ValueError(f"Path not in approved roots: {source_path}")
    if not p.exists():
        raise FileNotFoundError(f"Path does not exist: {p}")
    return p


def stage(p: Path, mode: str, source_type: str, tags: str) -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    job_id = f"manual-{ts}"
    STAGING.mkdir(parents=True, exist_ok=True)

    if p.is_file():
        dest = STAGING / f"{job_id}_{p.name}"
        shutil.copy2(p, dest)
        staged_files = [str(dest)]
    else:
        dest_dir = STAGING / job_id
        shutil.copytree(str(p), str(dest_dir))
        staged_files = [str(f) for f in dest_dir.rglob("*") if f.is_file()]

    tag_list = [t.strip() for t in tags.split(",") if t.strip()]

    record = {
        "id": job_id,
        "source_path": str(p),
        "mode": mode,
        "source_type": source_type,
        "tags": tag_list,
        "staged_files": staged_files,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "staged",
        "promoted": False,
    }

    manifest = STAGING / f"{job_id}.json"
    manifest.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return record


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manually stage local material into Node_Temp_Node pipeline."
    )
    parser.add_argument("--path", required=True, help="Source file or directory path")
    parser.add_argument("--mode", default="stage_only", choices=sorted(VALID_MODES))
    parser.add_argument("--type", dest="source_type", default="unknown",
                        choices=sorted(VALID_TYPES))
    parser.add_argument("--tags", default="", help="Comma-separated topic tags")
    args = parser.parse_args()

    try:
        p = validate_path(args.path)
        record = stage(p, args.mode, args.source_type, args.tags)
        print(json.dumps(record, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc), "status": "failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
