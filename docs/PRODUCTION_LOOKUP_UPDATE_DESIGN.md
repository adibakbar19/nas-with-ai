# Production Lookup Update Design

This document defines the production lookup update flow.

## Why This Exists

Lookup and boundary data should be staged, validated, and applied through backend-owned data workflows.

It is not a good default for routine production updates because it can:

- replace whole tables in one operation
- change canonical IDs if source ordering changes
- invalidate existing runtime references
- make future FK enforcement fragile
- make rollback harder when a bad lookup payload is loaded

## Target Principles

Production lookup updates should follow these rules:

- canonical IDs must remain stable
- updates must be validated before activation
- active tables must not be destructively overwritten during normal refresh
- boundary and lookup updates should be versioned
- runtime ETL should read from stable canonical tables only
- backfills must be explicit and observable

## Recommended Production Model

Use a four-layer model:

1. Source payloads
2. Staging tables
3. Canonical tables
4. Activation and backfill jobs

### 1. Source Payloads

Incoming source files should be stored and tracked as upload artifacts:

- lookup CSV files
- boundary shapefiles / GeoJSON / JSON uploads
- version label
- uploaded by
- uploaded at

These are not active production tables. They are inputs to validation.

### 2. Staging Tables

Every production refresh should load into staging tables first, for example:

- `nas_lookup_staging.state_stage`
- `nas_lookup_staging.district_stage`
- `nas_lookup_staging.mukim_stage`
- `nas_lookup_staging.locality_stage`
- `nas_lookup_staging.postcode_stage`
- `nas_lookup_staging.state_boundary_stage`
- `nas_lookup_staging.district_boundary_stage`
- `nas_lookup_staging.mukim_boundary_stage`
- `nas_lookup_staging.postcode_boundary_stage`

Staging tables should be replaceable and disposable.

They should include:

- the source payload data
- normalized text/code columns
- canonical ID candidates where available
- validation status columns
- load batch / version metadata

### 3. Canonical Tables

The active lookup tables under `LOOKUP_SCHEMA` should remain durable:

- `state`
- `district`
- `mukim`
- `locality`
- `postcode`
- `pbt`
- `state_boundary`
- `district_boundary`
- `mukim_boundary`
- `postcode_boundary`

These tables should be updated by merge/upsert logic, not full overwrite.

### 4. Activation And Backfill Jobs

After validation passes:

- merge new rows into canonical tables
- update changed attributes in place
- preserve canonical IDs whenever the business key matches
- mark removed rows as inactive when appropriate instead of hard delete
- run explicit runtime backfills if lookup remaps affect runtime references

Examples:

- if a boundary changes but `postcode_id` is unchanged, no address ID remap is needed
- if a locality or postcode is split/merged, run a targeted backfill job such as
  [backend/app/maintenance/backfill_lookup_refs.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/maintenance/backfill_lookup_refs.py:1)
  or [backend/app/maintenance/backfill_spatial_refs.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/maintenance/backfill_spatial_refs.py:1)

## Data Flow

```mermaid
flowchart LR
    src["Source Files / Boundary Uploads"]
    stage["Staging Tables"]
    validate["Validation + Diff Checks"]
    merge["Canonical Merge / Upsert"]
    activate["Version Activation"]
    lookup["Active LOOKUP_SCHEMA Tables"]
    backfill["Targeted Runtime Backfill"]
    runtime["Runtime Address Tables"]

    src --> stage
    stage --> validate
    validate --> merge
    merge --> activate
    activate --> lookup
    lookup --> backfill
    backfill --> runtime
```

## Stable ID Strategy

The most important production rule is this:

- IDs must come from stable business keys, not row ordering

Bad pattern:

- regenerate `postcode_id` with `row_number()` on every rebuild

Good pattern:

- match existing canonical row by business key
- reuse existing `postcode_id`
- insert a new `postcode_id` only when the business key is truly new

Recommended natural-key matching:

- `state`: `state_code`
- `district`: `(state_id, district_code)` or a validated equivalent
- `mukim`: prefer `mukim_id` from source, else `(district_id, mukim_code)`
- `locality`: business-key design should be reviewed carefully; avoid name-only matching if duplicates exist
- `postcode`: `postcode`
- `postcode_boundary`: `postcode_id`
- `district_boundary`: `district_id`
- `mukim_boundary`: `mukim_id`
- `state_boundary`: `state_id`

## Validation Gates

Before activation, validate at least:

- row count delta against previous version
- duplicate business keys
- nulls in required business-key columns
- invalid or empty geometries
- unmatched canonical IDs
- unexpected large deletes
- unexpected large remaps
- geometry overlap/conflict checks where relevant

Examples:

- postcode boundaries with no matching `postcode_id` should fail validation
- district boundaries with duplicate `district_id` should fail validation
- a refresh that removes 30% of postcode rows should require manual approval

## Recommended Update Modes

### Lookups

For lookup dimensions:

- load source into staging
- normalize
- join to canonical tables by business key
- update non-key attributes in place
- insert new rows
- optionally mark missing rows inactive rather than delete

### Boundaries

For boundaries:

- use versioned uploads
- validate geometry
- resolve canonical IDs in staging
- activate by replacing only the canonical boundary table contents, not the reference dimensions

This repo already has the beginning of that model for boundaries:

- version tracking in
  [backend/app/repositories/boundary_admin_repository.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/repositories/boundary_admin_repository.py:39)
- service layer in
  [backend/app/services/boundary_admin_service.py](/Users/adibakbar/Software_Development/disd-nas/backend/app/services/boundary_admin_service.py:19)

## Operational Workflow

Production-safe refresh flow:

1. Upload source files or boundary package.
2. Load into staging tables.
3. Run validation report.
4. Review row-count and mapping diff.
5. Merge into canonical lookup tables.
6. Activate new boundary version if relevant.
7. Run targeted backfill jobs if runtime IDs or spatial assignments need repair.
8. Refresh `lookup_version`.
9. Monitor ETL and API behavior.

## Runtime Update Path

- use the lookup staging API/workflow for tabular lookup changes
- use `backend/app/maintenance/apply_lookup_refresh.py` for validated merge/upsert flows
- keep boundary upload/activation for geometry lifecycle

The current scaffold for that split is documented in
[LOOKUP_REFRESH_IMPLEMENTATION_PLAN.md](/Users/adibakbar/Software_Development/disd-nas/docs/LOOKUP_REFRESH_IMPLEMENTATION_PLAN.md:1).

## Suggested Repo Evolution

### Short Term

- use boundary admin upload/activate for production boundary changes
- avoid repeated full overwrite of `nas_lookup` in production

### Medium Term

- add staging schemas and staging tables
- build diff/validation reports
- replace overwrite writes with merge/upsert flows
- preserve IDs by business key

### Long Term

- add hard FK constraints where stable IDs are guaranteed
- add approval workflow for high-impact lookup refreshes
- add audit trail for lookup version activation and runtime remaps

## FK Guidance

Hard FK constraints become practical only after:

- canonical IDs are stable
- canonical tables are not destructively overwritten
- refreshes are merge/upsert based

Until then, the safer intermediate model is:

- canonical ID linkage columns
- validation checks
- indexes
- explicit backfill jobs

That is the state this repo is moving toward now.
