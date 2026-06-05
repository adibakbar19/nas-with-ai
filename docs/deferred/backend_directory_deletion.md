# Deferred: Delete backend/ directory

## Status

Blocked — db-migrate and db-bootstrap still use Dockerfile.backend

## What still uses backend/

- docker-compose.yml db-migrate service:
  dockerfile: Dockerfile.backend
  Runs Alembic migrations from backend/app/db/

- docker-compose.yml db-bootstrap service:
  dockerfile: Dockerfile.backend
  Runs lookup seeding from etl/ + bootstrap scripts

## What needs to happen before deletion

1. Move Alembic migrations into nas-processor/
   Create nas-processor/alembic/ with the migrations
   Create nas-processor/Dockerfile.migrate
   Update docker-compose db-migrate to use it

2. Move db-bootstrap into nas-processor/
   Create nas-processor/Dockerfile.bootstrap
   Update docker-compose db-bootstrap to use it

3. Once both services no longer reference Dockerfile.backend or backend/app/db/:
   rm -rf backend/
   rm Dockerfile.backend

## Also deferred

etl/ move inside nas-processor/ — currently works as sibling directory
in worker container, move deferred until Alembic migration is resolved
(they can be done together)

## Estimated effort

1-2 sessions. Alembic migration move is the main work.
