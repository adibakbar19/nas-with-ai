# Runtime ERD

This ERD covers the main runtime tables visible in this repo:

- ingest runtime tables from Alembic migrations
- auth/admin tables from Alembic migrations
- address-domain tables created and maintained by `etl/load/postgres.py`
- lookup hierarchy and review tables referenced by repositories and ETL

Important caveat:

- some relationships are enforced as real foreign keys in migrations
- some address-domain relationships are inferred from ETL/repository code and may not be declared as DB-level FKs in every environment

## Mermaid ERD

```mermaid
erDiagram
    AGENCY_USER {
        text user_id PK
        text agency_id
        text username UK
        text display_name
        text email
        text password_hash
        text password_algo
        text role
        text status
        text created_by
        timestamptz created_at
        timestamptz updated_at
        timestamptz last_login_at
        timestamptz disabled_at
    }

    AGENCY_API_KEY {
        text key_id PK
        text agency_id
        text label
        text secret_hash
        text secret_preview
        text status
        text created_by
        text revoked_reason
        timestamptz created_at
        timestamptz updated_at
        timestamptz last_used_at
        timestamptz expires_at
        timestamptz revoked_at
    }

    INGEST_JOB {
        text job_id PK
        text agency_id
        text status
        jsonb data
        timestamptz created_at
        timestamptz updated_at
    }

    MULTIPART_UPLOAD_SESSION {
        text session_id PK
        text agency_id
        text status
        text bucket
        text object_name
        text upload_id
        text file_name
        text content_type
        bigint content_bytes
        int part_size
        text job_id
        jsonb data
        timestamptz created_at
        timestamptz updated_at
    }

    API_IDEMPOTENCY_REQUEST {
        text agency_id PK
        text operation PK
        text idempotency_key PK
        text request_fingerprint
        text status
        text resource_type
        text resource_id
        jsonb response
        timestamptz created_at
        timestamptz updated_at
    }

    STATE {
        int state_id PK
        text state_name
        text state_code
    }

    DISTRICT {
        int district_id PK
        text district_name
        text district_code
        int state_id FK
    }

    MUKIM {
        int mukim_id PK
        text mukim_name
        text mukim_code
        int district_id FK
    }

    LOCALITY {
        int locality_id PK
        text locality_code
        text locality_name
        int mukim_id FK
    }

    POSTCODE {
        int postcode_id PK
        text postcode
        int locality_id FK
    }

    PBT {
        int pbt_id PK
        text pbt_name
        geometry boundary_geom
        bool is_active
        timestamptz created_at
        timestamptz updated_at
    }

    STREET {
        int street_id PK
        text street_name_prefix
        text street_name
        int locality_id FK
        int pbt_id FK
        timestamptz created_at
        timestamptz updated_at
    }

    ADDRESS_TYPE {
        int address_type_id PK
        text property_type
        text property_code
        text ownership_structure
    }

    STANDARDIZED_ADDRESS {
        int address_id PK
        text premise_no
        text building_name
        text floor_level
        text unit_no
        text lot_no
        int street_id FK
        int locality_id FK
        int mukim_id FK
        int district_id FK
        int state_id FK
        int postcode_id FK
        int pbt_id FK
        int address_type_id FK
        double latitude
        double longitude
        geometry geom
        text country
        text validation_status
        timestamptz validation_date
        int validation_by
        text checksum
        text canonical_address_key
        timestamptz created_at
        timestamptz updated_at
    }

    NASKOD {
        int naskod_id PK
        int address_id FK
        text code
        bool is_vanity
        bool verified
        text status
        timestamptz generated_at
    }

    ADDRESS_ALIAS {
        int alias_id PK
        int address_id FK
        text raw_address_variant
        text normalized_address_variant
        text alias_checksum
        timestamptz created_at
        timestamptz updated_at
    }

    ADDRESS_MATCH_REVIEW {
        int review_id PK
        text candidate_canonical_address_key
        text candidate_checksum
        text candidate_raw_address_variant
        text candidate_normalized_address_variant
        int candidate_state_id
        int candidate_district_id
        int candidate_postcode_id
        int candidate_street_id
        int candidate_locality_id
        int matched_address_id FK
        text matched_canonical_address_key
        int match_score
        text match_reasons
        text review_status
        text reviewed_by
        timestamptz reviewed_at
        text review_note
        timestamptz created_at
        timestamptz updated_at
    }

    STATE_BOUNDARY {
        int state_boundary_id PK
        int state_id FK
        geometry boundary_geom
    }

    DISTRICT_BOUNDARY {
        int district_boundary_id PK
        int district_id FK
        int state_id FK
        geometry boundary_geom
    }

    MUKIM_BOUNDARY {
        int mukim_boundary_id PK
        int mukim_id FK
        int district_id FK
        int state_id FK
        geometry boundary_geom
    }

    POSTCODE_BOUNDARY {
        int postcode_boundary_id PK
        int postcode_id FK
        geometry boundary_geom
    }

    STATE ||--o{ DISTRICT : contains
    DISTRICT ||--o{ MUKIM : contains
    MUKIM ||--o{ LOCALITY : contains
    LOCALITY ||--o{ POSTCODE : contains
    LOCALITY ||--o{ STREET : contains
    PBT ||--o{ STREET : governs

    STATE ||--o{ STANDARDIZED_ADDRESS : classifies
    DISTRICT ||--o{ STANDARDIZED_ADDRESS : classifies
    MUKIM ||--o{ STANDARDIZED_ADDRESS : classifies
    LOCALITY ||--o{ STANDARDIZED_ADDRESS : classifies
    POSTCODE ||--o{ STANDARDIZED_ADDRESS : classifies
    STREET ||--o{ STANDARDIZED_ADDRESS : classifies
    PBT ||--o{ STANDARDIZED_ADDRESS : classifies
    ADDRESS_TYPE ||--o{ STANDARDIZED_ADDRESS : types

    STANDARDIZED_ADDRESS ||--o{ ADDRESS_ALIAS : has
    STANDARDIZED_ADDRESS ||--o{ NASKOD : assigned
    STANDARDIZED_ADDRESS ||--o{ ADDRESS_MATCH_REVIEW : matched_by

    STATE ||--o{ STATE_BOUNDARY : boundary
    STATE ||--o{ DISTRICT_BOUNDARY : boundary_context
    DISTRICT ||--o{ DISTRICT_BOUNDARY : boundary
    STATE ||--o{ MUKIM_BOUNDARY : boundary_context
    DISTRICT ||--o{ MUKIM_BOUNDARY : boundary_context
    MUKIM ||--o{ MUKIM_BOUNDARY : boundary
    POSTCODE ||--o{ POSTCODE_BOUNDARY : boundary

    INGEST_JOB ||--o{ MULTIPART_UPLOAD_SESSION : created_from
```

## Scope Notes

### Explicit FK relationships in repo migrations

These are explicitly added by migrations:

- `address_alias.address_id -> standardized_address.address_id`
- `address_match_review.matched_address_id -> standardized_address.address_id`

### Logical or inferred relationships

These are strongly implied by ETL/repository code, but may not be declared as DB FKs in every environment:

- `district.state_id -> state.state_id`
- `mukim.district_id -> district.district_id`
- `locality.mukim_id -> mukim.mukim_id`
- `postcode.locality_id -> locality.locality_id`
- `street.locality_id -> locality.locality_id`
- `street.pbt_id -> pbt.pbt_id`
- `standardized_address.*_id -> lookup/reference tables`
- `naskod.address_id -> standardized_address.address_id`
- boundary tables to their matching lookup/reference tables

Recommended FK-style boundary linkage:

- `state_boundary.state_id -> state.state_id`
- `district_boundary.district_id -> district.district_id`
- `district_boundary.state_id -> state.state_id`
- `mukim_boundary.mukim_id -> mukim.mukim_id`
- `mukim_boundary.district_id -> district.district_id`
- `mukim_boundary.state_id -> state.state_id`
- `postcode_boundary.postcode_id -> postcode.postcode_id`

This is the cleaner target relational model, even though the current ETL still partially derives postcode/admin matches from boundary source text columns before resolving to canonical lookup IDs.

### Agency scoping relationships

These are application-level, not DB FK-level:

- `agency_user.agency_id -> ingest_job.agency_id`
- `agency_api_key.agency_id -> ingest_job.agency_id`
- `agency_user.agency_id -> multipart_upload_session.agency_id`
- `agency_api_key.agency_id -> multipart_upload_session.agency_id`
- `api_idempotency_request.agency_id` is also agency-scoped

The app enforces agency visibility by filtering on `agency_id`, not by foreign-key constraints.

## Table Families

### Auth and tenancy

- `agency_user`
- `agency_api_key`
- `api_idempotency_request`

### Ingest runtime

- `ingest_job`
- `multipart_upload_session`

### Address master data

- `state`
- `district`
- `mukim`
- `locality`
- `postcode`
- `street`
- `pbt`
- `address_type`
- `standardized_address`
- `naskod`

### Match and review

- `address_alias`
- `address_match_review`

### Spatial governance

- `state_boundary`
- `district_boundary`
- `mukim_boundary`
- `postcode_boundary`

In the target model, each boundary row should belong to one canonical lookup row by FK-style ID linkage, not only by text labels such as postcode/state/district names.

## Source Basis

This ERD was derived from:

- Alembic migrations under `backend/app/db/migrations/versions/`
- ETL table creation and write logic in `etl/load/postgres.py`
- repository joins in:
  - `backend/app/repositories/address_read_repository.py`
  - `backend/app/repositories/address_match_review_repository.py`
  - `backend/app/repositories/lookup_admin_repository.py`
