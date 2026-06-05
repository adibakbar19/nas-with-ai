# Implementation Plan: Autocomplete Service (Remaining Work)

## Overview

The core autocomplete service implementation is complete — all modules (app.py, config.py, search.py, cache.py), the Dockerfile, Docker Compose integration, and 64 tests (property-based, unit, and integration) are passing. The only remaining gap is Requirement 2.7: proper validation error messages when numeric environment variables (`PORT` or `CACHE_TTL_SECONDS`) contain non-integer values. The current `Settings` class uses bare `int()` conversion which raises a generic `ValueError` traceback rather than a clear error message indicating which configuration variable is invalid.

## Tasks

- [x] 1. Implement configuration validation with descriptive error messages
  - [x] 1.1 Update `config.py` Settings class to validate numeric env vars with clear error messages
    - Wrap `int()` conversions for `PORT` and `CACHE_TTL_SECONDS` in try/except blocks
    - On `ValueError`, raise a `SystemExit` or `ValueError` with a message like: `"Invalid configuration: PORT must be an integer, got '{value}'"`
    - Similarly for `CACHE_TTL_SECONDS`: `"Invalid configuration: CACHE_TTL_SECONDS must be a positive integer, got '{value}'"`
    - Validate that `PORT` is in range 1–65535 and `CACHE_TTL_SECONDS` is a positive integer
    - _Requirements: 2.4, 2.6, 2.7_

  - [x] 1.2 Write unit tests for invalid configuration values
    - Test that setting `PORT` to a non-integer string (e.g., `"abc"`) raises an error with message mentioning `PORT`
    - Test that setting `CACHE_TTL_SECONDS` to a non-integer string raises an error with message mentioning `CACHE_TTL_SECONDS`
    - Test that setting `PORT` to `"0"` or `"70000"` (out of range 1–65535) raises an error
    - Test that setting `CACHE_TTL_SECONDS` to `"0"` or `"-1"` (non-positive) raises an error
    - _Requirements: 2.4, 2.6, 2.7_

- [x] 2. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All previous tasks from the initial implementation plan are complete (64 tests passing)
- The only remaining gap is Requirement 2.7: descriptive error reporting for invalid numeric config
- Docker and Docker Compose integration (Requirement 8) is fully implemented in `docker-compose.yml`
- Property tests validate universal correctness properties from the design document
- Tasks marked with `*` are optional and can be skipped for faster MVP

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] }
  ]
}
```
