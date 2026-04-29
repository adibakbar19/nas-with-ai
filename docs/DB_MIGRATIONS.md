# DB Migrations Runbook

This project uses Alembic for versioned runtime schema changes.

Current migration entrypoints:

- `alembic.ini`
- `backend/app/db/migrations/env.py`
- `backend/app/db/migrations/versions/`

Current runtime tables managed by Alembic:

- `ingest_job`
- `multipart_upload_session`

## Core Rules

- Do not edit an old migration after it has been applied in a shared environment.
- Create a new revision for every schema change.
- Keep schema changes in Alembic revisions.
- Keep large or operationally sensitive data backfills out of app startup.
- Prefer additive changes first, then backfill, then cleanup later.

## Common Commands

Create a new revision:

```bash
alembic revision -m "add foo column to ingest_job"
```

Check current version:

```bash
alembic current
```

See revision history:

```bash
alembic history
```

Apply all pending migrations:

```bash
alembic upgrade head
```

Roll back one revision:

```bash
alembic downgrade -1
```

Run migrations via Docker Compose:

```bash
docker compose up db-migrate
```

## Normal Workflow

1. Change application code or repository logic that needs a schema update.
2. Create a new Alembic revision.
3. Implement the `upgrade()` logic.
4. Implement `downgrade()` only if rollback is realistic and safe.
5. Apply locally with `alembic upgrade head`.
6. Test the app against the migrated schema.
7. Commit the code change and the revision together.

## Additive Change Example

Use one migration when the change is backward-compatible, such as:

- adding a nullable column
- adding a new table
- adding an index

Example: add `source_etag` to `multipart_upload_session`.

Create the revision:

```bash
alembic revision -m "add source_etag to multipart upload session"
```

Edit the new file under `backend/app/db/migrations/versions/`:

```python
from __future__ import annotations

from alembic import op


revision = "20260407_0002"
down_revision = "20260407_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    op.execute(
        f'ALTER TABLE "{schema}"."multipart_upload_session" '
        'ADD COLUMN IF NOT EXISTS source_etag TEXT NULL'
    )


def downgrade() -> None:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    op.execute(
        f'ALTER TABLE "{schema}"."multipart_upload_session" '
        'DROP COLUMN IF EXISTS source_etag'
    )
```

This is safe because:

- old code still works if it ignores the new column
- new code can deploy after the column exists

## Backfill Pattern

Backfills should be explicit. Use one of these approaches:

- Small deterministic backfill: keep it inside the Alembic revision.
- Large backfill: run it as a separate script or one-off job.

Use a separate job when the backfill:

- touches many rows
- may take minutes or hours
- needs batching or resumability
- could lock hot tables for too long

### Small Backfill In Revision

Example: initialize `source_etag` from existing JSON data.

```python
def upgrade() -> None:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    op.execute(
        f'ALTER TABLE "{schema}"."multipart_upload_session" '
        'ADD COLUMN IF NOT EXISTS source_etag TEXT NULL'
    )
    op.execute(
        f'''
        UPDATE "{schema}"."multipart_upload_session"
        SET source_etag = data->>'etag'
        WHERE source_etag IS NULL
          AND data ? 'etag'
        '''
    )
```

This is acceptable only if the update is small and quick.

### Large Backfill As Separate Step

For a large migration, use expand, migrate, contract:

1. Expand:
   Add the new column or table in Alembic.
2. Migrate:
   Run a one-off backfill script in batches.
3. Contract:
   Drop old columns only after reads and writes have moved fully to the new shape.

Pseudo-example for a large backfill script:

```python
from __future__ import annotations

import psycopg


def main() -> None:
    dsn = "postgresql://..."
    schema = "nas"
    batch_size = 1000

    with psycopg.connect(dsn) as conn:
        while True:
            with conn.cursor() as cur:
                cur.execute(
                    f'''
                    WITH rows AS (
                      SELECT session_id
                      FROM "{schema}"."multipart_upload_session"
                      WHERE source_etag IS NULL
                      ORDER BY session_id
                      LIMIT %s
                    )
                    UPDATE "{schema}"."multipart_upload_session" t
                    SET source_etag = t.data->>'etag'
                    FROM rows
                    WHERE t.session_id = rows.session_id
                    RETURNING t.session_id
                    ''',
                    (batch_size,),
                )
                updated = cur.fetchall()
            conn.commit()
            if not updated:
                break


if __name__ == "__main__":
    main()
```

Backfill scripts should be:

- idempotent
- restart-safe
- batch-based
- observable through logs or progress counters

## Rollback Guidance

Do not assume every migration is cleanly reversible.

Safe rollback candidates:

- dropping a newly added index
- dropping a newly added nullable column that no code uses yet
- dropping a new table that has no important data

Unsafe rollback candidates:

- destructive data rewrites
- type changes that truncate data
- dropping columns after data has already moved

Example rollback for an additive change:

```python
def downgrade() -> None:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    op.execute(
        f'ALTER TABLE "{schema}"."multipart_upload_session" '
        'DROP COLUMN IF EXISTS source_etag'
    )
```

Rollback commands:

```bash
alembic downgrade -1
alembic downgrade 20260407_0001
```

If a revision includes a risky data migration, the safer rollback strategy is usually:

1. deploy a code fix
2. create a forward repair migration
3. avoid a destructive downgrade in production

## Expand, Migrate, Contract

Use this for non-trivial schema changes.

Example: replace `status TEXT` with a stricter representation later.

1. Expand:
   Add `status_v2` while keeping `status`.
2. Migrate:
   Backfill `status_v2` from `status`.
3. App transition:
   Make the app read both fields or write both fields temporarily.
4. Contract:
   Remove `status` only after all code reads `status_v2`.

This avoids breaking mixed-version deployments.

## Production Notes

- Run `alembic upgrade head` on every deploy. It is a no-op when there are no new revisions.
- In this repo, Docker Compose handles that via the `db-migrate` service before `api` and `worker`.
- Never rely on request-path code to create or mutate schema.
- If a migration can fail halfway, make sure rerunning it is safe.

## Repo-Specific Notes

- Runtime DB config is resolved from `.env` and environment variables in `backend/app/db/migration_settings.py`.
- The runtime migration schema defaults to `INGEST_JOB_STATE_SCHEMA`, then `PGSCHEMA`, then `nas`.
- The initial baseline revision is `backend/app/db/migrations/versions/20260407_0001_create_runtime_tables.py`.

## Checklist Before Merging

- New revision created under `backend/app/db/migrations/versions/`
- `upgrade()` implemented
- `downgrade()` either implemented or intentionally left minimal with known limits
- `alembic upgrade head` tested locally
- App code tested against upgraded schema
- Large backfills moved out of request path and startup path
