#!/bin/bash
# Adds JWT validation plugin to Kong for Keycloak tokens.
# Run after seed_kong.sh. Safe to re-run.
set -e

KONG="http://localhost:8100"
KEYCLOAK="http://localhost:8080"
REALM="org-realm"

echo "=== Setting up Kong JWT for Keycloak ==="

# Get issuer from real token
TOKEN=$(curl -sf -X POST "$KEYCLOAK/realms/$REALM/protocol/openid-connect/token" \
  -d "client_id=nas-agency-a" \
  -d "client_secret=test-secret-agency-a" \
  -d "grant_type=client_credentials" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

ISSUER=$(echo $TOKEN | cut -d. -f2 | python3 -c "import sys, base64, json
t = sys.stdin.read().strip()
t += '=' * (4 - len(t) % 4)
print(json.loads(base64.b64decode(t))['iss'])")
echo "Token issuer: $ISSUER"

# Get RSA public key
PEM=$(python3 << 'PYEOF'
import urllib.request, json, base64, sys
url = 'http://localhost:8080/realms/org-realm/protocol/openid-connect/certs'
with urllib.request.urlopen(url) as r:
    keys = json.loads(r.read())['keys']
sig_key = next(k for k in keys if k.get('use') == 'sig')
try:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
    from cryptography.hazmat.primitives import serialization
    n = int.from_bytes(base64.urlsafe_b64decode(sig_key['n']+'=='), 'big')
    e = int.from_bytes(base64.urlsafe_b64decode(sig_key['e']+'=='), 'big')
    pub = RSAPublicNumbers(e, n).public_key()
    print(pub.public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode())
except ImportError:
    print('ERROR: pip install cryptography', file=sys.stderr)
    sys.exit(1)
PYEOF
)

[ -z "$PEM" ] && { echo "Failed to get key"; exit 1; }

# Create consumer for Keycloak
curl -sf -X PUT "$KONG/consumers/keycloak-issuer" \
  -H "Content-Type: application/json" \
  -d '{"username":"keycloak-issuer"}' > /dev/null
echo "Consumer created: keycloak-issuer"

# Remove old JWT credential if exists
OLD=$(curl -sf "$KONG/consumers/keycloak-issuer/jwt" \
  | python3 -c "import sys,json
creds = json.load(sys.stdin).get('data',[])
print(creds[0]['id'] if creds else '')")
[ -n "$OLD" ] && \
  curl -sf -X DELETE "$KONG/consumers/keycloak-issuer/jwt/$OLD" > /dev/null && \
  echo "Removed old credential"

# Add JWT credential with Keycloak public key
PEM_JSON=$(python3 -c "import json,sys; print(json.dumps(sys.stdin.read()))" <<< "$PEM")
curl -sf -X POST "$KONG/consumers/keycloak-issuer/jwt" \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"$ISSUER\",\"algorithm\":\"RS256\",\"rsa_public_key\":$PEM_JSON}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('Credential id:', d.get('id','ERROR:'+str(d)))"

# Add JWT plugin to protected services
for service in "ingestion-api" "nas-processor-api"; do
  EXISTING=$(curl -sf "$KONG/services/$service/plugins" \
    | python3 -c "import sys,json
plugins = json.load(sys.stdin)['data']
match = [p['id'] for p in plugins if p['name'] == 'jwt']
print(match[0] if match else '')")
  [ -n "$EXISTING" ] && \
    curl -sf -X DELETE "$KONG/plugins/$EXISTING" > /dev/null
  curl -sf -X POST "$KONG/services/$service/plugins" \
    -H "Content-Type: application/json" \
    -d '{"name":"jwt","config":{"header_names":["authorization"],"claims_to_verify":["exp"],"key_claim_name":"iss","secret_is_base64":false,"run_on_preflight":false}}' \
    > /dev/null && echo "JWT plugin added to $service"
done

# Verify
echo ""
echo "Testing JWT..."
NO_TOKEN=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/api/v1/ingest/jobs)
echo "No token → HTTP $NO_TOKEN (expect 401)"
WITH_TOKEN=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/api/v1/ingest/jobs \
  -H "Authorization: Bearer $TOKEN")
echo "Valid token → HTTP $WITH_TOKEN (expect 200/404)"
echo ""
echo "=== JWT setup complete ==="
echo "Open Kong Manager: http://localhost:8002"
