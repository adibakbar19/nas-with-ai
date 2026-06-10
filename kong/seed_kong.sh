#!/bin/bash
# Seeds Kong via Admin API with all NAS services, routes, and plugins.
# Safe to re-run — PUT is idempotent for services/routes.
set -e

KONG="http://localhost:8100"

wait_for_kong() {
  echo "Waiting for Kong Admin API..."
  until curl -sf "$KONG/status" > /dev/null 2>&1; do
    echo "  not ready, retrying in 3s..."
    sleep 3
  done
  echo "Kong is ready."
}

put_service() {
  local name=$1 url=$2 read_timeout=${3:-60000}
  echo "→ Service: $name"
  curl -sf -X PUT "$KONG/services/$name" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$name\",\"url\":\"$url\",\"connect_timeout\":10000,\"read_timeout\":$read_timeout,\"write_timeout\":60000}" > /dev/null && echo "  ✓"
}

put_route() {
  local name=$1 service=$2
  shift 2
  local paths_json
  paths_json=$(python3 -c "import sys, json; paths = sys.argv[1:]; print(json.dumps(paths))" "$@")
  echo "→ Route: $name ($@)"
  curl -sf -X PUT "$KONG/services/$service/routes/$name" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$name\",\"paths\":$paths_json,\"strip_path\":false,\"preserve_host\":false}" > /dev/null && echo "  ✓"
}

post_plugin_service() {
  local service=$1 name=$2 config=$3
  echo "→ Plugin $name on $service"
  # Delete existing first to avoid duplicates
  EXISTING=$(curl -sf "$KONG/services/$service/plugins" \
    | python3 -c "import sys,json
plugins = json.load(sys.stdin)['data']
match = [p['id'] for p in plugins if p['name'] == '$name']
print(match[0] if match else '')")
  if [ -n "$EXISTING" ]; then
    curl -sf -X DELETE "$KONG/plugins/$EXISTING" > /dev/null
  fi
  curl -sf -X POST "$KONG/services/$service/plugins" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$name\",\"config\":$config}" > /dev/null && echo "  ✓"
}

post_global_plugin() {
  local name=$1 config=$2
  echo "→ Global plugin: $name"
  # Delete existing first
  EXISTING=$(curl -sf "$KONG/plugins" \
    | python3 -c "import sys,json
plugins = json.load(sys.stdin)['data']
match = [p['id'] for p in plugins if p['name'] == '$name' and not p.get('service') and not p.get('route')]
print(match[0] if match else '')")
  if [ -n "$EXISTING" ]; then
    curl -sf -X DELETE "$KONG/plugins/$EXISTING" > /dev/null
  fi
  curl -sf -X POST "$KONG/plugins" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$name\",\"config\":$config}" > /dev/null && echo "  ✓"
}

wait_for_kong

echo ""
echo "--- Services ---"
put_service "ingestion-api" "http://ingestion-api:3000"
put_service "nas-processor-api" "http://nas-processor-api:8001"
put_service "address-search-service" "http://address-search-service:8003" 15000
put_service "queue-service" "http://queue-service:8005" 30000
put_service "event-streaming-service" "http://event-streaming-service:8004" 30000

echo ""
echo "--- Routes ---"
put_route "ingest-routes" "ingestion-api" "/api/v1/ingest"
put_route "address-routes" "nas-processor-api" "/api/v1/addresses" "/api/v1/address" "/api/v1/review" "/api/v1/admin" "/api/v1/search"
put_route "search-public" "address-search-service" "/api/search" "/api/autocomplete" "/api/suggest"
put_route "search-protected" "address-search-service" "/api/validate" "/api/batch-validate" "/api/spatial" "/api/geocode"
put_route "queue-routes" "queue-service" "/api/queue" "/api/jobs"
put_route "events-routes" "event-streaming-service" "/api/events" "/api/webhooks" "/api/deliveries"

echo ""
echo "--- Rate Limiting per Service ---"
post_plugin_service "ingestion-api" "rate-limiting" '{"minute":300,"policy":"local","fault_tolerant":true}'
post_plugin_service "nas-processor-api" "rate-limiting" '{"minute":200,"policy":"local","fault_tolerant":true}'
post_plugin_service "address-search-service" "rate-limiting" '{"minute":100,"limit_by":"ip","policy":"local","fault_tolerant":true}'
post_plugin_service "queue-service" "rate-limiting" '{"minute":120,"policy":"local","fault_tolerant":true}'
post_plugin_service "event-streaming-service" "rate-limiting" '{"minute":120,"policy":"local","fault_tolerant":true}'

echo ""
echo "--- Global Plugins ---"
post_global_plugin "cors" '{"origins":["http://localhost","http://localhost:5173","http://frontend-vue"],"methods":["GET","POST","PUT","PATCH","DELETE","OPTIONS"],"headers":["Accept","Authorization","Content-Type","X-API-Key","X-Service-Key","X-Requested-With"],"credentials":true,"max_age":3600}'
post_global_plugin "correlation-id" '{"header_name":"X-Request-ID","generator":"uuid","echo_downstream":true}'

echo ""
echo "=== Seed complete ==="
echo "Services: $(curl -sf $KONG/services | python3 -c "import sys,json; print(len(json.load(sys.stdin)['data']))")"
echo "Routes:   $(curl -sf $KONG/routes | python3 -c "import sys,json; print(len(json.load(sys.stdin)['data']))")"
echo "Plugins:  $(curl -sf $KONG/plugins | python3 -c "import sys,json; print(len(json.load(sys.stdin)['data']))")"
echo ""
echo "Next step: bash kong/setup_jwt.sh"
echo "Then open Kong Manager: http://localhost:8002"
