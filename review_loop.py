#!/usr/bin/env python3
"""
HOST-side review loop for NODE truth-gate pipeline.

Reads truth_gate_reports/, checks two-source corroboration, and writes
review artifacts. Does NOT auto-promote. Does NOT mutate corpus records.

Corroboration standard (from README doctrine):
  CORROBORATED: paper validated by w3m AND confirmed in semantic_scholar_feed
                by matching arxiv_id — two independent sources agree.
  SINGLE_SOURCE: w3m validated but no S2 cross-reference found.

Outputs:
  staging/review_queue/<paper_id>.json  — review artifact per paper
  training/candidates/<paper_id>.json   — CORROBORATED papers only
  runtime/offload_list.jsonl            — appended when candidate is written

Operator controls promotion from candidates/ → llama_ready/ separately.
"""
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path.home() / "NODE"
REPORTS_DIR = BASE / "metadata/truth_gate_reports"
REVIEW_DIR = BASE / "staging/review_queue"
CANDIDATES_DIR = BASE / "training/candidates"
OFFLOAD_LIST = BASE / "runtime/offload_list.jsonl"
STATE_FILE = BASE / "runtime/review_loop_state.json"

CORPUS_FILES = [
    BASE / "training/corpus/arxiv_feed.jsonl",
    BASE / "training/corpus/semantic_scholar_feed.jsonl",
]
S2_FEED = BASE / "training/corpus/semantic_scholar_feed.jsonl"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"processed_report_ids": [], "total_corroborated": 0, "total_single": 0}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")


def load_s2_by_arxiv_id():
    index = {}
    if not S2_FEED.exists():
        return index
    with S2_FEED.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                aid = r.get("arxiv_id", "").strip()
                if aid:
                    index[aid] = r
            except Exception:
                pass
    return index


def load_corpus_by_content_hash():
    index = {}
    for path in CORPUS_FILES:
        if not path.exists():
            continue
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    content = r.get("content", "")
                    ch = r.get("content_hash") or hashlib.sha256(content.encode()).hexdigest()
                    if ch not in index:
                        index[ch] = r
                except Exception:
                    pass
    return index


def write_review_artifact(paper_id, artifact):
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    path = REVIEW_DIR / f"{paper_id}.json"
    path.write_text(json.dumps(artifact, indent=2) + "\n")


def write_candidate(paper_id, artifact):
    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
    path = CANDIDATES_DIR / f"{paper_id}.json"
    path.write_text(json.dumps(artifact, indent=2) + "\n")


def append_offload(entry):
    OFFLOAD_LIST.parent.mkdir(parents=True, exist_ok=True)
    with OFFLOAD_LIST.open("a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main():
    state = load_state()
    processed = set(state.get("processed_report_ids", []))

    if not REPORTS_DIR.exists():
        print("[review_loop] no truth_gate_reports dir yet — nothing to review")
        return 0

    reports = sorted(REPORTS_DIR.glob("*.json"))
    if not reports:
        print("[review_loop] no reports found")
        return 0

    s2_index = load_s2_by_arxiv_id()
    corpus_index = load_corpus_by_content_hash()

    new_corroborated = 0
    new_single = 0
    skipped = 0

    for report_path in reports:
        paper_id = report_path.stem
        if paper_id in processed:
            skipped += 1
            continue

        try:
            report = json.loads(report_path.read_text())
        except Exception as exc:
            print(f"[review_loop] bad report {paper_id}: {exc}", file=sys.stderr)
            continue

        if report.get("status") != "validated":
            processed.add(paper_id)
            continue

        arxiv_id = ""
        title = ""
        content = ""
        source_url = report.get("url", "")

        # Recover arxiv_id from the report URL
        url = report.get("url", "")
        if "arxiv.org/abs/" in url:
            arxiv_id = url.split("arxiv.org/abs/")[-1].split("v")[0].strip()

        # Recover content from corpus
        corpus_record = None
        for cf in CORPUS_FILES:
            if not cf.exists():
                continue
            with cf.open() as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                        r_arxiv = r.get("arxiv_id", "").strip()
                        r_url = r.get("source_url", "")
                        if (arxiv_id and r_arxiv == arxiv_id) or (source_url and r_url == source_url):
                            corpus_record = r
                            break
                    except Exception:
                        pass
            if corpus_record:
                break

        if corpus_record:
            content = corpus_record.get("content", "")
            title = content.split(": ", 1)[0] if ": " in content else content[:80]
            arxiv_id = arxiv_id or corpus_record.get("arxiv_id", "")

        # Check for S2 corroboration
        s2_match = s2_index.get(arxiv_id) if arxiv_id else None
        corroborated = s2_match is not None

        corroboration_level = "CORROBORATED" if corroborated else "SINGLE_SOURCE"
        evidence = {
            "w3m_validation": {
                "url": url,
                "text_hash": report.get("text_hash"),
                "match": report.get("match"),
                "attempted_at": report.get("attempted_at"),
            }
        }
        if s2_match:
            evidence["semantic_scholar"] = {
                "paper_id": s2_match.get("paper_id"),
                "source_url": s2_match.get("source_url"),
                "citation_count": s2_match.get("citation_count"),
                "year": s2_match.get("year"),
            }

        artifact = {
            "paper_id": paper_id,
            "arxiv_id": arxiv_id,
            "title": title,
            "corroboration_level": corroboration_level,
            "reviewed_at": utc_now(),
            "promoted": False,
            "evidence": evidence,
            "corpus_record": corpus_record,
        }

        write_review_artifact(paper_id, artifact)
        print(f"[review_loop] {paper_id} → {corroboration_level}")

        if corroborated:
            write_candidate(paper_id, artifact)
            append_offload({
                "doc_id": paper_id,
                "arxiv_id": arxiv_id,
                "title": title,
                "truth_gate": corroboration_level,
                "candidate_path": str(CANDIDATES_DIR / f"{paper_id}.json"),
                "added_at": utc_now(),
                "consumed": False,
            })
            new_corroborated += 1
        else:
            new_single += 1

        processed.add(paper_id)

    state["processed_report_ids"] = list(processed)
    state["total_corroborated"] = state.get("total_corroborated", 0) + new_corroborated
    state["total_single"] = state.get("total_single", 0) + new_single
    save_state(state)

    print(
        f"[review_loop] done: corroborated={new_corroborated} "
        f"single_source={new_single} skipped={skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
