# Requirements Document

## Introduction

The Autocomplete Service is a standalone, lightweight, read-only FastAPI microservice that provides fast address autocomplete suggestions for the National Address System (NAS). It queries domain-specific OpenSearch indices using `bool_prefix` multi_match (for `search_as_you_type` fields) or `match_phrase_prefix` (for simpler indices) and uses Redis (Valkey) caching to minimize latency. The service runs on port 8002, requires no authentication, and has no dependency on PostgreSQL.

The `domain` parameter routes queries to the appropriate OpenSearch index (`autocomplete-{domain}`). The initial "address" domain maps to the existing `nas_addresses` index and its `autocomplete` field (type: `search_as_you_type`). Future domains may use dedicated indices with a `value` field.

## Glossary

- **Autocomplete_Service**: The standalone FastAPI microservice providing autocomplete suggestions on port 8002
- **OpenSearch_Backend**: The OpenSearch 2.13 cluster used to store and query autocomplete indices
- **Redis_Cache**: The Valkey (Redis-compatible) instance used to cache autocomplete results
- **Domain**: A logical namespace identifying which autocomplete index to query (e.g., "address", "street", "locality"); the "address" domain maps to the existing `nas_addresses` index
- **Cache_Key**: The Redis key in format `ac:{domain}:{query}` used to store cached results
- **Suggestion**: A string value returned as an autocomplete match (the `address_clean` field for the "address" domain)
- **Health_Endpoint**: The `/health` route used for container liveness probes
- **Autocomplete_Endpoint**: The `GET /autocomplete` route that returns suggestions
- **Domain_Config**: A mapping that defines per-domain index name, query field, and query strategy

## Requirements

### Requirement 1: Service Skeleton and Health Check

**User Story:** As a platform engineer, I want the Autocomplete Service to expose a health endpoint, so that container orchestrators can verify service liveness.

#### Acceptance Criteria

1. THE Autocomplete_Service SHALL expose a `GET /health` endpoint that returns HTTP 200 with a JSON body containing `{"status": "ok", "service": "autocomplete-service"}` and a `Content-Type` header of `application/json`
2. THE Autocomplete_Service SHALL listen on port 8002 and respond to the `GET /health` endpoint within 2 seconds under normal operating conditions
3. THE Autocomplete_Service SHALL use the `python:3.12-slim` base image in its Dockerfile
4. THE Autocomplete_Service SHALL include `fastapi` (>=0.111), `uvicorn` (>=0.29), `httpx` (>=0.27), and `redis` (>=5.0) in its Python dependencies
5. WHEN a Docker healthcheck probes the `/health` endpoint, THE Autocomplete_Service SHALL be considered healthy if the endpoint returns HTTP 200 within the configured timeout of 5 seconds, checked at an interval of 10 seconds with up to 5 retries

### Requirement 2: Configuration Management

**User Story:** As a platform engineer, I want the Autocomplete Service to read configuration from environment variables with sensible defaults, so that deployment is flexible across environments.

#### Acceptance Criteria

1. THE Autocomplete_Service SHALL read the `OPENSEARCH_URL` setting from environment variables with a default value of `http://opensearch:9200`
2. THE Autocomplete_Service SHALL read the `REDIS_URL` setting from environment variables with a default value of `redis://valkey:6379/0`
3. THE Autocomplete_Service SHALL read the `HOST` setting from environment variables with a default value of `0.0.0.0`
4. THE Autocomplete_Service SHALL read the `PORT` setting from environment variables with a default value of `8002`, parsing the value as an integer in the range 1 to 65535
5. THE Autocomplete_Service SHALL define a `DOMAIN_CONFIG` dictionary mapping domain names to their index name, query type, query fields, and result field, including at minimum an "address" domain entry with index `nas_addresses`, query type `bool_prefix`, query fields `["autocomplete", "autocomplete._2gram", "autocomplete._3gram"]`, and result field `address_clean`
6. THE Autocomplete_Service SHALL read the `CACHE_TTL_SECONDS` setting from environment variables with a default value of 300, parsing the value as a positive integer representing the cache time-to-live in seconds
7. IF a numeric environment variable (`PORT` or `CACHE_TTL_SECONDS`) contains a non-integer value, THEN THE Autocomplete_Service SHALL fail to start and report an error message indicating the invalid configuration variable

### Requirement 3: Redis Cache Lookup

**User Story:** As a user, I want autocomplete responses to be fast, so that the typing experience feels instantaneous.

#### Acceptance Criteria

1. WHEN a request arrives at the Autocomplete_Endpoint, THE Autocomplete_Service SHALL normalize the `q` parameter by trimming leading/trailing whitespace and converting to lowercase, then check the Redis_Cache for key `ac:{domain}:{normalized_query}` before querying the OpenSearch_Backend
2. WHEN a cache hit occurs for key `ac:{domain}:{normalized_query}`, THE Autocomplete_Service SHALL return the cached suggestions without querying the OpenSearch_Backend
3. THE Autocomplete_Service SHALL store cached values as JSON-serialized lists of strings with a TTL of 300 seconds (5 minutes)
4. WHEN constructing the Cache_Key, THE Autocomplete_Service SHALL include the `limit` parameter in the key format `ac:{domain}:{normalized_query}:{limit}` so that requests with different limits are cached independently

### Requirement 4: OpenSearch Query

**User Story:** As a user, I want autocomplete suggestions to match the beginning of my input, so that I see relevant results as I type.

#### Acceptance Criteria

1. WHEN no cache hit exists for key `ac:{domain}:{query}`, THE Autocomplete_Service SHALL query the OpenSearch index determined by the Domain_Config for the given domain with a connection timeout of 10 seconds
2. WHEN the domain is "address", THE Autocomplete_Service SHALL query the `nas_addresses` index using a `multi_match` query with type `bool_prefix` on fields `["autocomplete", "autocomplete._2gram", "autocomplete._3gram"]`
3. WHEN the Domain_Config for the requested domain specifies a `query_type` of `match_phrase_prefix`, THE Autocomplete_Service SHALL query the configured index using a `match_phrase_prefix` query on the first field listed in the domain's `query_fields` configuration
4. WHEN the OpenSearch_Backend returns results, THE Autocomplete_Service SHALL extract the field specified by the Domain_Config `result_field` for the given domain as suggestion strings, excluding any hits where the result field is missing or empty
5. WHEN the `limit` query parameter is provided with a value between 1 and 100 inclusive, THE Autocomplete_Service SHALL restrict OpenSearch results to the specified limit
6. WHEN the `limit` query parameter is not provided, THE Autocomplete_Service SHALL default to returning a maximum of 10 suggestions
7. IF the `limit` query parameter is provided with a value less than 1 or greater than 100, THEN THE Autocomplete_Service SHALL return HTTP 400 with an error message indicating the valid range is 1 to 100

### Requirement 5: Cache Write-Through

**User Story:** As a platform engineer, I want OpenSearch results to be cached, so that repeated queries avoid redundant backend calls.

#### Acceptance Criteria

1. WHEN the OpenSearch_Backend returns results, THE Autocomplete_Service SHALL store the result as a JSON-serialized list of strings in the Redis_Cache with key `ac:{domain}:{query}` and a TTL of 300 seconds
2. WHEN the OpenSearch_Backend returns an empty result set, THE Autocomplete_Service SHALL cache the empty list in the Redis_Cache with key `ac:{domain}:{query}` and a TTL of 300 seconds to prevent repeated queries for non-matching prefixes
3. IF the Redis_Cache is unreachable during a cache write, THEN THE Autocomplete_Service SHALL return the OpenSearch results to the caller without caching and without raising an error

### Requirement 6: Autocomplete Endpoint Response Format

**User Story:** As a frontend developer, I want a consistent response format from the autocomplete endpoint, so that I can parse and display suggestions predictably.

#### Acceptance Criteria

1. THE Autocomplete_Endpoint SHALL accept `GET /autocomplete` with query parameters `q` (required, string), `domain` (required, string), and `limit` (optional, integer with minimum value of 1 and maximum value of 100, defaulting to 10)
2. WHEN the request is valid, THE Autocomplete_Service SHALL return HTTP 200 with a JSON response containing a `suggestions` field whose value is an array of strings
3. IF the `q` parameter is missing or contains only whitespace, THEN THE Autocomplete_Service SHALL return HTTP 400 with a JSON body containing a `detail` field that indicates the `q` parameter is required
4. IF the `domain` parameter is missing or contains only whitespace, THEN THE Autocomplete_Service SHALL return HTTP 400 with a JSON body containing a `detail` field that indicates the `domain` parameter is required
5. IF the `domain` parameter does not match a key in the Domain_Config, THEN THE Autocomplete_Service SHALL return HTTP 400 with a JSON body containing a `detail` field that lists the valid domain values
6. IF the `limit` parameter is less than 1 or greater than 100, THEN THE Autocomplete_Service SHALL return HTTP 400 with a JSON body containing a `detail` field that indicates the allowed range

### Requirement 7: Error Handling

**User Story:** As a platform engineer, I want the service to degrade gracefully when backends are unavailable, so that partial failures do not crash the service.

#### Acceptance Criteria

1. IF the Redis_Cache is unreachable or returns an error during a cache read, THEN THE Autocomplete_Service SHALL bypass the cache and query the OpenSearch_Backend directly without returning an error to the caller
2. IF the Redis_Cache is unreachable or returns an error during a cache write, THEN THE Autocomplete_Service SHALL return the OpenSearch_Backend results to the caller without caching them and without returning an error
3. IF the OpenSearch_Backend does not respond within 10 seconds, THEN THE Autocomplete_Service SHALL return HTTP 503 with a JSON body containing a `detail` field describing the connectivity failure
4. IF the OpenSearch_Backend returns a non-2xx HTTP response, THEN THE Autocomplete_Service SHALL return HTTP 502 with a JSON body containing a `detail` field that includes the upstream HTTP status code
5. WHILE any backend is experiencing failures, THE Autocomplete_Service SHALL remain running and continue accepting new requests on port 8002

### Requirement 8: Docker and Docker Compose Integration

**User Story:** As a platform engineer, I want the Autocomplete Service to run as a Docker container within the existing NAS Docker Compose stack, so that deployment is consistent with other services.

#### Acceptance Criteria

1. THE Autocomplete_Service SHALL have a Dockerfile with build context `./autocomplete-service`
2. THE Autocomplete_Service SHALL expose port 8002 in its Dockerfile and map it to host port 8002 in `docker-compose.yml`
3. THE Autocomplete_Service SHALL be defined as a service in `docker-compose.yml` with environment variables `OPENSEARCH_URL=http://opensearch:9200` and `REDIS_URL=redis://valkey:6379/0`
4. THE Autocomplete_Service SHALL depend on the `opensearch` service with condition `service_healthy` and the `valkey` service with condition `service_started` in Docker Compose
5. THE Autocomplete_Service SHALL include a healthcheck that probes the `/health` endpoint using a Python urllib request to `http://localhost:8002/health` with an interval of 10 seconds, a timeout of 5 seconds, and a maximum of 5 retries
6. THE Autocomplete_Service SHALL use restart policy `unless-stopped` in Docker Compose
