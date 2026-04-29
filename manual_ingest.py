#!/usr/bin/env python3
"""
Controlled manual ingest into NODE staging.

Stages local material for pipeline evaluation. Does NOT promote to corpus.
Promotion requires truth-gate corroboration (two verified sources or two
indirect supporting facts).
"""
import argparse
import json
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

APPROVED_ROOTS = [
    Path.home() / "incoming",
    Path.home() / "NODE",
]

STAGING = Path.home() / "NODE/staging/manual"

VALID_MODES = {"stage_only", "truth_gate", "corpus_candidate"}
VALID_TYPES = {"notes", "paper", "transcript", "code", "documentation", "web_capture", "unknown"}

# Minimum embedded-text characters before we consider the PDF "text-bearing"
_MIN_TEXT_CHARS = 100
# Max pages to OCR (keeps runtime bounded for large PDFs)
_OCR_MAX_PAGES = 3


def validate_path(source_path: str) -> Path:
    p = Path(source_path).resolve()
    approved = [r.resolve() for r in APPROVED_ROOTS]
    if not any(str(p).startswith(str(r)) for r in approved):
        raise ValueError(f"Path not in approved roots: {source_path}")
    if not p.exists():
        raise FileNotFoundError(f"Path does not exist: {p}")
    return p


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------

def _run(cmd: list, timeout: int = 60) -> Tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except FileNotFoundError:
        return -1, "", f"command not found: {cmd[0]}"


def _extract_embedded(pdf_path: Path, txt_path: Path) -> Tuple[str, str]:
    """Try pdftotext. Returns (method, status)."""
    rc, _, err = _run(["pdftotext", str(pdf_path), str(txt_path)])
    if rc != 0:
        return "embedded_text", "failed"
    text = txt_path.read_text(encoding="utf-8", errors="replace") if txt_path.exists() else ""
    if len(text.strip()) >= _MIN_TEXT_CHARS:
        return "embedded_text", "ok"
    return "embedded_text", "empty"


def _extract_ocr(pdf_path: Path, txt_path: Path) -> Tuple[str, str]:
    """pdftoppm → tesseract on first N pages. Returns (method, status)."""
    with tempfile.TemporaryDirectory(prefix="manual_ingest_ocr_") as tmp:
        tmp_p = Path(tmp)
        page_prefix = str(tmp_p / "page")
        rc, _, _ = _run(
            ["pdftoppm", str(pdf_path), page_prefix,
             "-png", "-f", "1", "-l", str(_OCR_MAX_PAGES)],
            timeout=60,
        )
        if rc != 0:
            return "ocr_fallback", "failed"

        pages = sorted(tmp_p.glob("page-*.png"))
        if not pages:
            return "ocr_fallback", "failed"

        parts = []
        for pg in pages:
            out_base = str(tmp_p / pg.stem)
            rc2, _, _ = _run(["tesseract", str(pg), out_base], timeout=30)
            out_txt = Path(out_base + ".txt")
            if rc2 == 0 and out_txt.exists():
                parts.append(out_txt.read_text(encoding="utf-8", errors="replace"))

        combined = "\n\n".join(parts).strip()
        if not combined:
            return "ocr_fallback", "empty"

        txt_path.write_text(combined, encoding="utf-8")
        return "ocr_fallback", "ok"


def extract_pdf_text(pdf_path: Path, txt_path: Path) -> Tuple[str, str, int]:
    """
    Extract text from a PDF. Returns (extraction_method, extraction_status, char_count).

    Order:
      1. pdftotext (embedded text)
      2. OCR fallback via pdftoppm + tesseract, only if embedded text is absent/short
    """
    method, status = _extract_embedded(pdf_path, txt_path)

    if status == "ok":
        chars = len(txt_path.read_text(encoding="utf-8", errors="replace"))
        return method, status, chars

    # Embedded text absent or too short — try OCR
    method, status = _extract_ocr(pdf_path, txt_path)
    if status == "ok":
        chars = len(txt_path.read_text(encoding="utf-8", errors="replace"))
        return method, status, chars

    # Nothing worked
    if not txt_path.exists():
        txt_path.write_text("", encoding="utf-8")
    return "no_text_extracted", "empty", 0


# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------

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

    # PDF extraction (single-file only)
    extraction_method = "not_pdf"
    extraction_status = "skipped"
    extracted_text_path = None
    extracted_text_chars = 0

    if p.is_file() and p.suffix.lower() == ".pdf":
        txt_name = dest.stem + ".txt"
        txt_dest = STAGING / txt_name
        extraction_method, extraction_status, extracted_text_chars = extract_pdf_text(dest, txt_dest)
        extracted_text_path = str(txt_dest)
        staged_files.append(str(txt_dest))

    record = {
        "id": job_id,
        "source_path": str(p),
        "mode": mode,
        "source_type": source_type,
        "tags": tag_list,
        "staged_files": staged_files,
        "extracted_text_path": extracted_text_path,
        "extraction_method": extraction_method,
        "extraction_status": extraction_status,
        "extracted_text_chars": extracted_text_chars,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "staged",
        "promoted": False,
    }

    manifest = STAGING / f"{job_id}.json"
    manifest.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return record


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manually stage local material into NODE pipeline."
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
