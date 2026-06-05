# Deferred: Delete ETL legacy cluster

## Status

Blocked by jobs/ module migration

## Legacy cluster — files to delete after jobs/ migration

```
transform/address/_parsing.py
transform/address/_lookups.py
transform/address/_validation.py
transform/address/_spatial.py
transform/address/_utils.py
transform/address/normalize.py
transform/address/unified.py
transform/address/input_normalizer.py
transform/address/text.py
transform/address/naskod_utils.py
transform/__init__.py (old re-exports)
transform/address/__init__.py (old re-exports)
pipeline/orchestrator.py (shim)
```

## What holds them in place

```
jobs/retry_failed.py → orchestrator shim → normalize.py
jobs/retry_failed_rows.py → orchestrator shim → normalize.py
jobs/_common.py → naskod_utils, old transform functions
```

## What needs to happen first

Rewrite jobs/ to use the new Polars pipeline:

1. **jobs/_common.py**
   Replace old transform chain with:
   - `normalise_chunk()` from `transform/address/normalise.py`
   - `enrich_lookup()` from `transform/address/lookup.py`
   - `validate_chunk()` from `pipeline/stages/validate.py`
   - `assign_naskod()` from `pipeline/stages/naskod.py`

2. **jobs/retry_failed_rows.py**
   Already has good logic — just update the transform calls
   to use new Polars functions.
   Input: corrections CSV + original failed parquet
   Output: re-processed success/warning/failed splits

3. **jobs/retry_failed.py**
   Same pattern as retry_failed_rows.py.

## After jobs/ migration

Run: `grep -rln 'import pandas' nas-processor/`
→ should return zero results

Then delete entire legacy cluster.

## Files to keep permanently (not in legacy cluster)

```
pipeline/syncer.py          — OpenSearch sync, standalone CLI
bootstrap/bootstrap_lookups.py — DB seeding, standalone
load/postgres.py            — Parquet → Postgres CLI
load/loader.py              — Parquet writer utilities
audit/audit_log.py          — Audit logging
```
