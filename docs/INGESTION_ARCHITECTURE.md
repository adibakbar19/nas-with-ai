# Ingestion Architecture

```mermaid
flowchart LR
    subgraph ClientSide["Client Side"]
        client["Agency Client / Frontend"]
    end

    subgraph ApiTier["API Tier"]
        api["NAS API\n/api/v1/ingest/*"]
        ingest["IngestService"]
    end

    subgraph RuntimeDb["Runtime Postgres"]
        jobdb["ingest_job\nmultipart_upload_session"]
        runtimetables["Normalized runtime tables\nschema: PGSCHEMA"]
    end

    subgraph Storage["Object Storage"]
        s3["S3 / S3-compatible bucket"]
    end

    subgraph QueueLayer["Queue Layer"]
        queue["Valkey Stream"]
    end

    subgraph WorkerTier["Worker Tier"]
        worker["queue_consumer.py"]
        pipeline["etl.pipeline"]
        loadpg["etl.load.postgres"]
        searchworker["Search sync handler"]
        loades["backend.app.search\nSearchSyncService"]
    end

    subgraph Search["Search"]
        es["Elasticsearch"]
    end

    subgraph LookupDb["Lookup / Boundary Data"]
        lookup["Lookup schema\nnas_lookup + boundary tables"]
    end

    client -->|"1. Direct upload\nsmall files"| api
    client -->|"1a. Multipart initiate / part-url / complete\nlarge files"| api
    api --> ingest

    ingest -->|"2. Store uploaded file\nor presign multipart URLs"| s3
    client -->|"3. Upload parts with presigned URLs"| s3

    ingest -->|"4. Create / update job state"| jobdb
    ingest -->|"5. Publish bulk ingest event"| queue

    queue -->|"6. Consume queued job"| worker
    worker -->|"7. Claim / update job status"| jobdb
    worker -->|"8. Download source file"| s3

    worker --> pipeline
    pipeline -->|"9. Read config + transform + validate"| lookup
    pipeline -->|"10. Write cleaned / warning / failed outputs"| s3

    worker --> loadpg
    loadpg -->|"11. Load normalized records"| runtimetables
    loadpg -->|"12. Join / reference curated lookups"| lookup

    worker -->|"13. Publish search_sync_requested"| queue
    queue -->|"14. Consume search sync event"| searchworker
    searchworker -->|"15. Index from Postgres truth"| loades
    loades --> es

    worker -->|"16. Persist final status,\npaths, progress, errors"| jobdb
    api -->|"17. Job polling / status APIs"| jobdb
    client -->|"18. Poll job status"| api
```

## Notes

- The API accepts uploads and creates ingest jobs, but in production it should stay stateless.
- The worker is the component that actually executes the ingest pipeline when `INGEST_EXECUTION_MODE=queue_worker`.
- Source files are stored in object storage before processing.
- Job state and multipart session state live in Postgres runtime tables.
- The queue backend is Valkey Streams.
- Elasticsearch sync is backend-owned and indexes from Postgres, not parquet.
- Search sync is queued as `search_sync_requested` after the DB load succeeds, so ingest completion is not failed by an Elasticsearch outage.
- Lookup and boundary data are separate from runtime ingest tables and are used during validation/enrichment.

## Detailed Ingest Flow

```mermaid
flowchart TB
    client["Agency Client / Frontend"]
    api["API\n/api/v1/ingest/*"]
    service["IngestService"]
    s3["Object Storage Bucket\nsource objects + retry CSVs"]
    jobdb["Runtime Postgres\ningest_job"]
    multipartdb["Runtime Postgres\nmultipart_upload_session"]
    queue["Queue\nValkey Stream"]
    worker["Worker\nqueue_consumer.py"]
    staging["Local Staging\nuploads/<job_id>/<file>"]
    logs["Job Log\nlogs/jobs/<job_id>.log"]
    outputs["Final Parquet Outputs\noutput/uploads/<job_id>/cleaned|warnings|failed"]
    checkpoints["Stage Checkpoints\noutput/uploads/<job_id>/checkpoints"]
    loadpg["etl.load.postgres"]
    runtime["Runtime Tables\nschema: PGSCHEMA"]
    lookup["Lookup + Boundary Tables\nschema: LOOKUP_SCHEMA"]
    searchqueue["Event\nsearch_sync_requested"]
    esload["backend.app.search\nSearchSyncService"]
    es["Elasticsearch"]

    client -->|"Direct upload"| api
    client -->|"Multipart initiate / part-url / complete"| api
    api --> service

    service -->|"Store direct upload as sha256/<hash>"| s3
    service -->|"Create multipart session"| multipartdb
    client -->|"Upload parts with presigned URLs"| s3
    service -->|"Complete multipart upload\nmultipart/<session_id>/<file>"| s3

    service -->|"Create job record\nobject_name, paths, load flags"| jobdb
    service -->|"Publish bulk ingest event"| queue
    queue --> worker

    worker -->|"Claim queued job\nset status=running"| jobdb
    worker -->|"Download source object"| s3
    worker --> staging
    worker --> logs

    worker -->|"Run target checkpoint pipeline"| checkpoints
    checkpoints -->|"Extract -> text clean -> spatial enrich/validate -> NASKOD"| outputs
    checkpoints -->|"Optional sync when CHECKPOINT_STORE=s3|minio"| s3

    outputs -->|"Success + warning parquet"| loadpg
    loadpg -->|"Read parquet + build normalized tables"| runtime
    loadpg -->|"Join reference data"| lookup

    worker -->|"Publish search sync event"| searchqueue
    searchqueue -->|"Worker consumes"| esload
    runtime -->|"Read canonical address rows"| esload
    esload --> es
    worker -->|"Persist final status,\npaths, errors, progress"| jobdb
    api -->|"Poll jobs / status"| jobdb
```

## Pipeline Internals

```mermaid
flowchart LR
    source["Downloaded Source File"]
    extract["10_extract_raw\nread CSV / Excel / JSON\nattach record_id"]
    resume["11_extract_resume_filtered\noptional failed-only subset"]
    textclean["20_clean_text\nparse_full_address()\ntext normalization only"]
    spatial["30_spatial_enrich_validate\nlookup joins + boundary mapping\nPBT assignment + PASS/WARN/FAIL"]
    status["90_record_status\nrow-level processing state"]
    finalok["40_success_final\nPASS rows + naskod"]
    finalwarn["41_warning_final\nWARNING rows + naskod"]
    finalfail["42_failed_final\nFAILED rows + naskod + error_reason"]
    published["Published Parquet Outputs\ncleaned / warnings / failed"]
    dbload["etl.load.postgres\noptional"]
    runtime["Runtime Postgres Tables"]
    esload["backend.app.search\nSearchSyncService"]
    search["Elasticsearch"]

    source --> extract
    extract --> resume
    extract --> textclean
    resume --> textclean

    extract --> status
    textclean --> status

    textclean --> spatial
    spatial --> status

    spatial --> finalok
    spatial --> finalwarn
    spatial --> finalfail

    finalok --> status
    finalwarn --> status
    finalfail --> status

    finalok --> published
    finalwarn --> published
    finalfail --> published

    published --> dbload
    dbload --> runtime

    runtime --> esload
    esload --> search
```

## Retry Failed Rows Flow

```mermaid
flowchart LR
    parent["Parent Job\nfailed parquet exists"]
    usercsv["Corrections CSV\nmust include record_id"]
    retryapi["POST /api/v1/ingest/jobs/{job_id}/retry-failed-rows"]
    retryobj["Bucket Object\nretry_failed_rows/<parent>/<retry>/<file>"]
    retryjob["Retry Job Record\njob_type=retry_failed_rows"]
    retryworker["Worker"]
    failedset["Parent Failed Parquet"]
    merge["Join corrections by record_id\nreplace editable fields"]
    reclean["20_clean_text\nre-run text normalization"]
    revalidate["30_spatial_enrich_validate\nre-run spatial enrichment + validation"]
    naskod["40_generate_naskod"]
    retryout["Retry Outputs\ncleaned / warnings / failed"]
    loadpg["etl.load.postgres\noptional"]
    runtime["Runtime Postgres"]

    usercsv --> retryapi
    retryapi -->|"Upload corrections CSV"| retryobj
    retryapi -->|"Create retry job"| retryjob
    retryjob --> retryworker

    retryworker -->|"Download corrections CSV"| retryobj
    retryworker -->|"Read source_failed_path"| failedset
    failedset --> merge
    retryobj --> merge
    merge --> reclean
    reclean --> revalidate
    revalidate --> naskod
    naskod --> retryout
    retryout --> loadpg
    loadpg --> runtime
```

## Refactor Map

- Target stage `20_clean_text`
  Current owner: `clean_addresses()` in [etl/transform/address/normalize.py](/Users/adibakbar/Software_Development/disd-nas/etl/transform/address/normalize.py:1048)
  Keep here:
  - address column detection and structured-address composition
  - `parse_full_address(...)`
  - text normalization for premise, street, locality, postcode, state, district, mukim
  - non-spatial lookup canonicalization and fuzzy matching
  Move out:
  - `_assign_postcode_from_boundary(...)`
  - `_assign_admin_from_boundaries(...)`
  - `_assign_pbt(...)`

- Target stage `30_spatial_enrich_validate`
  Current owners: spatial work inside `clean_addresses()` plus status split in `validate_addresses()`
  New responsibility:
  - load postcode/admin/PBT boundary datasets
  - run GeoPandas/Shapely intersections and conflict flags
  - enrich PBT and boundary-derived admin fields
  - run `validate_addresses()` after spatial flags are present
  Candidate extracted helpers:
  - `enrich_spatial_components(df, config=..., postcode_boundaries=..., admin_boundaries=..., pbt_boundaries=...)`
  - `split_validated_outputs(df, require_mukim=...)`

- Target stage `40_generate_naskod`
  Current owner: finalize block in [etl/pipeline/etl.pipeline](/Users/adibakbar/Software_Development/disd-nas/etl/pipeline/etl.pipeline:919)
  Keep here:
  - `add_standard_naskod(...)`
  - final PASS / WARNING / FAILED output shaping
  - final parquet writes

- Pipeline changes in [etl/pipeline/etl.pipeline](/Users/adibakbar/Software_Development/disd-nas/etl/pipeline/etl.pipeline:535)
  Replace current flow:
  - `extract -> clean -> validate -> finalize`
  With target flow:
  - `extract -> clean_text -> spatial_enrich_validate -> generate_naskod`
  Rename checkpoints to make stage intent explicit:
  - `20_clean_text`
  - `30_spatial_validated_success`
  - `31_spatial_validated_failed`
  - `40_success_final`
  - `41_warning_final`
  - `42_failed_final`

- Retry flow changes in [etl/jobs/retry_failed_rows.py](/Users/adibakbar/Software_Development/disd-nas/etl/jobs/retry_failed_rows.py:1)
  Replace:
  - `clean_addresses(...) -> validate_addresses(...) -> add_standard_naskod(...)`
  With:
  - `clean_text_addresses(...) -> enrich_spatial_components(...) -> validate_addresses(...) -> add_standard_naskod(...)`
  This keeps retry behavior aligned with the main pipeline.

- Recommended extraction order
  1. Split `clean_addresses()` into:
     - `clean_text_addresses(...)`
     - `enrich_spatial_components(...)`
  2. Update `etl.pipeline` to use the new stage boundary names and checkpoints.
  3. Update `etl.jobs.retry_failed_rows` to call the same split functions.
  4. Only after behavior is stable, rename old checkpoint folders or add migration/backward-compat handling for resume logic.

- Main risk areas
  - checkpoint resume compatibility, because current `--resume` logic expects old stage names
  - row-level status rebuilding in `90_record_status`
  - retry flow assumptions, because failed parquet currently already contains some spatially-derived columns
  - test coverage around boundary conflicts and PBT assignment after function extraction
