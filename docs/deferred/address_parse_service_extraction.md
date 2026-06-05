# Address parse — event-driven via worker

## Revised decision

Address parse moves to an async event-driven pattern,
not a synchronous HTTP endpoint.

## New flow

POST /api/v1/address/parse (nas-processor-api)
→ validates request
→ publishes "address_parse_requested" event to Valkey
→ returns {"job_id": "...", "status": "queued"} 202

Worker (nas-processor-worker) adds new job type:
→ consumes address_parse_requested events
→ runs ETL address transforms on the submitted addresses
→ writes results to DB table: nas.parsed_address_results
→ updates job status to completed

GET /api/v1/address/parse/jobs/{job_id} (nas-processor-api)
→ returns job status + results when ready

## Why this is better

- nas-processor-api stays lightweight (no ETL deps)
- Worker already has all heavy deps (pandas, polars, ETL)
- Large address batches supported without HTTP timeout
- Same infrastructure as batch ingest

## For single-address instant lookup

Use GET /api/v1/db/addresses?q={address} (already built)
or autocomplete service (to be built)

These query already-processed data — no ETL needed

## When to build

After admin routes land in nas-processor-api.
Requires new Valkey topic: address_parse_requested
Requires new DB table: nas.parsed_address_results
Requires new job type handler in nas-processor-worker

## Dependencies (worker-side only)

- etl/ (full directory)
- shared/nas_config (for load_config)
- config/config.json (pipeline config file)
- Postgres access (for lookup tables via load_lookup_frames)
- AWS credentials (for Bedrock LLM)
- pandas, polars, geopandas, shapely, openai, boto3

## Blocked by

Nothing — can be done independently after admin routes are complete.
