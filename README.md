# Node_Temp_Node

NODE-side corpus acquisition, validation, truth-gate, and LLaMA quantum training preparation pipeline.

## Role in the system

| Component | Role |
|-----------|------|
| **NODE** | worker / gatherer / verifier / corpus store / truth-gate runner |
| **HOST** | operator console / controller / GitHub push authority / final system-action authority |
| **Telegram** | exception channel only — notifies Rick when something needs attention |
| **GitHub** | source code + approved project history |
| **LLaMA** | downstream consumer of curated, truth-gated quantum corpus material |

Rick is the operator authority for system actions and scope. Rick is not the proof standard for corpus truth. Truth promotion requires corroboration — see doctrine below.

## Pipeline stages

```
acquisition  !=  evidence  !=  sufficiency  !=  promotion
```

1. **Acquisition** — record exists in corpus
2. **Evidence** — record validated by w3m and/or structured metadata
3. **Sufficiency** — truth gate decides corroboration threshold is met
4. **Promotion** — material enters accepted/elevated/LLaMA candidate corpus

## Truth promotion doctrine

A candidate record can be promoted only when it has either:
1. Two other verified supporting sources, OR
2. Two indirectly supporting facts

Promotion is never automatic. The truth gate evaluates. Rick approves system actions — not individual truth claims.

## Sources

| Source | Output |
|--------|--------|
| arXiv | `training/corpus/arxiv_feed.jsonl` |
| Semantic Scholar | `training/corpus/semantic_scholar_feed.jsonl` |
| IBM / Qiskit docs | `training/corpus/ibm_qiskit_feed.jsonl` |
| PRX Quantum / APS | `training/corpus/prx_aps_feed.jsonl` |
| Local / Manual ingest | `staging/manual/<job_id>/` |
| Lexicon | `training/corpus/lexicon.jsonl` |
| Generated concepts | `training/corpus/generated_core.jsonl`, `training/corpus/quantum_core.jsonl` |

## Validation

`validate_w3m_loop.py` — continuous w3m-based validation

- Target: 60 validation attempts per hour (1 per minute globally)
- Per-domain cooldown: 120 seconds
- Failure backoff: 600 seconds per domain after 3 consecutive failures
- Writes: `metadata/validation_logs/w3m_validation.jsonl`
- Writes: `metadata/truth_gate_reports/<candidate_id>.json`
- Never auto-promotes

Validation queue: `runtime/validation_queue.jsonl`
Validation state: `runtime/validation_state.json`

## Offload / archive policy

`offload_trained.py` — offload manager

When a document is consumed/trained/marked complete, add it to `runtime/offload_list.jsonl`.
When the list reaches 100 truth-gate-passed documents, an archive is created:
- `archive/offloaded_batches/llama_offload_YYYYMMDD_HHMMSS_NNNN.tar.gz`
- `archive/offloaded_batches/llama_offload_YYYYMMDD_HHMMSS_NNNN.manifest.json`

Provenance and truth-gate reports are always preserved. Unverified material is never archived as trained.

## Directory structure

```
config/
  source_registry.json      — source definitions and validation/offload config

training/
  corpus/                   — active corpus JSONL files
  substrate/                — training substrate
  candidates/               — LLaMA corpus candidates (truth-gate passed)
  llama_ready/              — final LLaMA-ready records

staging/
  manual/                   — manually ingested material staging area

metadata/
  ingest_logs/              — ingest logs and processed state
  validation_logs/          — w3m validation log
  truth_gate_reports/       — per-candidate truth gate reports

runtime/
  validation_queue.jsonl    — candidates awaiting w3m validation
  validation_state.json     — validator state (domain timers, counts)
  offload_list.jsonl        — consumed/trained documents awaiting archive
  fetch_queue.json
  resume_state.json
  gap_state.json

archive/
  offloaded_batches/        — tar.gz archives of consumed LLaMA training batches

handoff/                    — handoff documents
```

## Scripts

| Script | Purpose |
|--------|---------|
| `real_ingest.py` | One-shot bounded arXiv fetch |
| `ingest_loop.py` | Incremental ingest loop (deduplicates) |
| `generate_corpus.py` | Generate synthetic concept baseline (UNVERIFIED) |
| `manual_ingest.py` | Stage manually selected local material |
| `validate_w3m_loop.py` | Continuous w3m validation (60/hour target) |
| `offload_trained.py` | Archive consumed training documents |
| `ui_server.py` | Local web UI on 127.0.0.1:7700 |

## UI access (via SSH tunnel)

The UI runs on the NODE at `127.0.0.1:7700`. To access from HOST:

```bash
ssh -f -N -L 7700:127.0.0.1:7700 rick@<NODE_TAILSCALE_IP>
# then open http://127.0.0.1:7700 in Firefox on HOST
```
