# Deferred: address_match_review flat schema reconciliation

## Status

Deferred from Priority 4 (schema contract migration)

## Problem

address_match_review_repository.py references address_id as an integer FK to standardized_address.address_id. The flat schema has no address_id column — it uses record_id (text) as primary key.

## What needs to happen

1. Decide whether match_review rows should reference record_id (text FK) or keep a separate integer surrogate key on standardized_address
2. Write an Alembic migration to update the FK column
3. Update address_match_review_repository.py to use record_id
4. Test the full match review flow against the flat schema

## Why deferred

The match review feature was not working against the flat table before this migration either (aspirational code). Fixing it requires a product decision on the FK strategy, not just a mechanical code change.
