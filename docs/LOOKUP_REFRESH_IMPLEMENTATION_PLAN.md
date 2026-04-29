# Lookup Refresh Implementation Plan

This document turns the production-safe lookup refresh design into concrete repo work.

Related documents:

- [PRODUCTION_LOOKUP_UPDATE_DESIGN.md](/Users/adibakbar/Software_Development/disd-nas/docs/PRODUCTION_LOOKUP_UPDATE_DESIGN.md:1)
- [RUN_PIPELINE.md](/Users/adibakbar/Software_Development/disd-nas/docs/RUN_PIPELINE.md:46)
- [DB_MIGRATIONS.md](/Users/adibakbar/Software_Development/disd-nas/docs/DB_MIGRATIONS.md:1)

## Current Scaffold Added

This repo now has a backend-owned maintenance entrypoint:

- [backend/app/maintenance/apply_lookup_refresh.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/maintenance/apply_lookup_refresh.py:1)

Current behavior:

- `apply_lookup_refresh.py`
  - validates a staged batch
  - persists validation rows and diff rows
  - supports dry-run summary
  - supports guarded apply for `state` and `postcode`
  - mirrors those two tables into the runtime schema when present

This is intentional. The safe first step is:

- stage
- validate
- diff
- guarded apply on a small stable subset

## Target Schemas

- canonical lookup schema:
  - `nas_lookup`
- staging lookup schema:
  - `nas_lookup_staging`

Recommended staging tables:

- `state_stage`
- `district_stage`
- `mukim_stage`
- `locality_stage`
- `postcode_stage`
- `pbt_stage`
- `state_boundary_stage`
- `district_boundary_stage`
- `mukim_boundary_stage`
- `postcode_boundary_stage`
- `lookup_refresh_batch`

## Phase 1

Goal:

- make staging repeatable
- make validation observable
- avoid production writes to canonical tables

Status:

- implemented

Deliverables:

- admin/API-owned lookup staging workflow
- `apply_lookup_refresh.py` dry-run summary
- batch ID tracking

## Phase 2

Goal:

- add deterministic validation gates before apply

Status:

- implemented

Implementation targets:

1. Add staging validation report table:
   - `lookup_refresh_validation`
2. Add checks for:
   - duplicate business keys
   - null business keys
   - unmatched canonical IDs
   - large row count deltas
3. Add threshold flags:
   - warning
   - blocked

Suggested output shape:

```json
{
  "refresh_batch_id": "lookup_refresh_...",
  "status": "blocked",
  "checks": [
    {
      "check_name": "postcode_boundary_unmatched_postcode_id",
      "severity": "error",
      "row_count": 12
    }
  ]
}
```

Implemented checks:

- duplicate `state_code`
- missing `state_code`
- missing `state_name`
- duplicate `postcode`
- missing `postcode`
- missing `postcode_name`
- unmatched boundary linkage IDs
- `state` / `postcode` row-delta thresholds
- postcode locality resolution hint

## Phase 3

Goal:

- implement safe apply logic for dimension tables

Status:

- partially implemented

Order:

1. `state`
2. `district`
3. `mukim`
4. `postcode`
5. `locality`
6. `pbt`

Recommended strategy:

- match canonical rows by stable business key
- reuse existing IDs
- insert only new keys
- update non-key attributes in place
- avoid delete as the default

Why this order:

- `state`, `district`, `mukim`, and `postcode` are the main canonical anchors
- `locality` is trickier because name-only matching may be ambiguous

Implemented now:

- `state`
  - update by `state_code`
  - insert new rows with stable `state_id` allocation based on existing max ID
- `postcode`
  - update by `postcode`
  - insert new rows with stable `postcode_id` allocation based on existing max ID
  - resolve `locality_id` conservatively from canonical locality rows when possible
- runtime mirror
  - mirror `state` and `postcode` changes into `PGSCHEMA` when those tables exist

Not implemented yet:

- `district`
- `mukim`
- `locality`
- `pbt`
- canonical delete/inactivation policy

## Phase 4

Goal:

- implement safe apply logic for boundary tables

Recommended strategy:

- require canonical IDs to be resolved in staging first
- replace only boundary rows, not reference dimensions
- keep activation explicit

This can reuse the direction already present in:

- [backend/app/repositories/boundary_admin_repository.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/repositories/boundary_admin_repository.py:39)

## Phase 5

Goal:

- integrate runtime repair/backfill

When needed:

- a business key remaps to a different canonical ID
- boundaries materially change spatial assignment

Candidate tools:

- [backend/app/maintenance/backfill_lookup_refs.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/maintenance/backfill_lookup_refs.py:1)
- [backend/app/maintenance/backfill_spatial_refs.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/maintenance/backfill_spatial_refs.py:1)

## Suggested CLI Workflow

Stage lookup changes through the admin/API workflow.

Validate:

```bash
python -m backend.app.maintenance.apply_lookup_refresh \
  --schema nas_lookup \
  --staging-schema nas_lookup_staging \
  --refresh-batch-id lookup_refresh_20260422T120000Z
```

Future apply:

```bash
python -m backend.app.maintenance.apply_lookup_refresh \
  --schema nas_lookup \
  --staging-schema nas_lookup_staging \
  --refresh-batch-id lookup_refresh_20260422T120000Z \
  --apply
```

At the moment, the last command is intentionally blocked.

## Minimal Next Code Steps

The next useful implementation slice is:

1. add validation tables and thresholds
2. implement merge/upsert for `state` and `postcode`
3. persist a diff summary per refresh batch
4. only then open `--apply` for those tables

That gives the repo a real production-safe path without pretending the full dimension graph is already solved.
