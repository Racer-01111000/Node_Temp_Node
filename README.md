# Node_Temp

Temporary reconstructed node-role workspace for Kestrel/OpenClaw while the physical NODE is offline.

## Purpose
This repo is a truthful emulation of the NODE role:
- bounded external acquisition
- staging of fetched artifacts
- support for retrieval/testing workflows
- no authority over sufficiency, promotion, or final review

## Important limits
Node_Temp is **not** the original physical NODE.
It does **not** claim to contain unsynced files or exact local state from the dark node.
It is a role reconstruction based on known contracts, paths, and workflow behavior.

## Known workspace structure
- docs/
- evals/
- metadata/
- training/
- runtime/
- staging/external_research/
- handoff/

## Known runtime responsibilities
- NODE performs bounded fetch and artifact staging
- HOST owns sufficiency decisions
- HOST owns review/promotion/resume decisions
- acquisition is not evidence
- evidence is not sufficiency
- sufficiency is not promotion

## Known runtime files
- runtime/fetch_queue.json
- runtime/resume_state.json
- runtime/gap_state.json

## Intended use
Use Node_Temp to:
- continue node-role development
- emulate bounded retrieval logic
- stage temporary artifacts
- test controller/runtime flow
- prepare clean re-entry for the real NODE later
