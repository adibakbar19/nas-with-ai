#!/usr/bin/env python3
"""Keycloak seed script — idempotent setup of realm, clients, roles, and Redis routing.

Requires: httpx, redis
Run after Keycloak is healthy.

Notes on defensive fallbacks:
- SSL disable: Keycloak 24 in start-dev mode still sets sslRequired=external on the
  master realm, blocking HTTP token requests from outside the container. The script
  detects the 403 "HTTPS required" response and uses `docker exec kcadm.sh` to
  disable SSL on the master realm before retrying.
- Redis fallback: In Docker Compose environments where Valkey/Redis does not expose
  port 6379 to the host, direct Redis connections fail. The script falls back to
  `docker exec valkey-cli SET` to seed routing config inside the container.
"""

from __future__ import annotations

import json
import os
import sys

import httpx
import redis


# ── Configuration ────────────────────────────────────────────────────────────

KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "http://localhost:8080").rstrip("/")
KEYCLOAK_ADMIN_USER = os.environ.get("KEYCLOAK_ADMIN_USER", "admin")
KEYCLOAK_ADMIN_PASSWORD = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin")
KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "org-realm")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

INGESTION_CLIENT_ID = "ingestion-api"
PROCESSOR_API_CLIENT_ID = "nas-processor-api"
AGENCY_CLIENT_ID = "nas-agency-a"
AGENCY_CLIENT_SECRET = "test-secret-agency-a"
ADMIN_CLIENT_ID = "nas-admin"
ADMIN_CLIENT_SECRET = "test-secret-admin"
REALM_ROLE_PLATFORM_ADMIN = "platform_admin"

CLIENT_ROLES = [
    "ingest.upload",
    "ingest.read",
    "ingest.start",
    "ingest.retry",
    "multipart.write",
    "multipart.read",
]

REDIS_ROUTING = {
    "source": "nas",
    "agency": "agency-a",
    "topic": "ingest.nas",
}

ADMIN_REDIS_ROUTING = {
    "source": "admin",
    "agency": "admin",
    "topic": "",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_admin_token(client: httpx.Client) -> str:
    """Get admin access token from master realm.
    
    If HTTPS is required (common in Keycloak 24+ dev mode), disable it
    via kcadm.sh inside the container first.
    """
    resp = client.post(
        f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token",
        data={
            "client_id": "admin-cli",
            "username": KEYCLOAK_ADMIN_USER,
            "password": KEYCLOAK_ADMIN_PASSWORD,
            "grant_type": "password",
        },
    )
    if resp.status_code == 403 and "HTTPS required" in resp.text:
        print("  ⚠ HTTPS required on master realm — disabling via kcadm.sh...")
        import subprocess
        container = os.environ.get("KEYCLOAK_CONTAINER", "nas-keycloak")
        subprocess.run(
            [
                "docker", "exec", container,
                "/opt/keycloak/bin/kcadm.sh",
                "config", "credentials",
                "--server", "http://localhost:8080",
                "--realm", "master",
                "--user", KEYCLOAK_ADMIN_USER,
                "--password", KEYCLOAK_ADMIN_PASSWORD,
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "docker", "exec", container,
                "/opt/keycloak/bin/kcadm.sh",
                "update", "realms/master",
                "-s", "sslRequired=NONE",
            ],
            check=True,
            capture_output=True,
        )
        print("  ✓ SSL requirement disabled on master realm")
        # Retry token request
        resp = client.post(
            f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token",
            data={
                "client_id": "admin-cli",
                "username": KEYCLOAK_ADMIN_USER,
                "password": KEYCLOAK_ADMIN_PASSWORD,
                "grant_type": "password",
            },
        )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("✓ admin token acquired")
    return token


def create_realm(client: httpx.Client, headers: dict) -> None:
    """Create realm if it does not exist, with SSL disabled for dev."""
    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms",
        headers=headers,
        json={"realm": KEYCLOAK_REALM, "enabled": True, "sslRequired": "NONE"},
    )
    if resp.status_code == 409:
        print(f"✓ realm '{KEYCLOAK_REALM}' exists, skipping")
    elif resp.status_code == 201:
        print(f"✓ realm '{KEYCLOAK_REALM}' created")
    else:
        resp.raise_for_status()


def get_clients(client: httpx.Client, headers: dict) -> list[dict]:
    """Get all clients in the realm."""
    resp = client.get(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients",
        headers=headers,
    )
    resp.raise_for_status()
    return resp.json()


def find_client(clients: list[dict], client_id: str) -> dict | None:
    """Find a client by clientId."""
    for c in clients:
        if c.get("clientId") == client_id:
            return c
    return None


def create_ingestion_client(client: httpx.Client, headers: dict) -> str:
    """Create the ingestion-api client (bearer-only). Returns internal ID."""
    clients = get_clients(client, headers)
    existing = find_client(clients, INGESTION_CLIENT_ID)
    if existing:
        print(f"✓ client '{INGESTION_CLIENT_ID}' exists, skipping")
        return existing["id"]

    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients",
        headers=headers,
        json={
            "clientId": INGESTION_CLIENT_ID,
            "enabled": True,
            "bearerOnly": True,
            "publicClient": False,
        },
    )
    if resp.status_code == 409:
        print(f"✓ client '{INGESTION_CLIENT_ID}' exists, skipping")
        clients = get_clients(client, headers)
        return find_client(clients, INGESTION_CLIENT_ID)["id"]
    resp.raise_for_status()
    print(f"✓ client '{INGESTION_CLIENT_ID}' created")

    # Fetch to get internal id
    clients = get_clients(client, headers)
    return find_client(clients, INGESTION_CLIENT_ID)["id"]


def create_agency_client(client: httpx.Client, headers: dict) -> str:
    """Create the test agency client (confidential, service account). Returns internal ID."""
    clients = get_clients(client, headers)
    existing = find_client(clients, AGENCY_CLIENT_ID)
    if existing:
        print(f"✓ client '{AGENCY_CLIENT_ID}' exists, skipping")
        return existing["id"]

    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients",
        headers=headers,
        json={
            "clientId": AGENCY_CLIENT_ID,
            "enabled": True,
            "serviceAccountsEnabled": True,
            "publicClient": False,
            "secret": AGENCY_CLIENT_SECRET,
        },
    )
    if resp.status_code == 409:
        print(f"✓ client '{AGENCY_CLIENT_ID}' exists, skipping")
        clients = get_clients(client, headers)
        return find_client(clients, AGENCY_CLIENT_ID)["id"]
    resp.raise_for_status()
    print(f"✓ client '{AGENCY_CLIENT_ID}' created")

    clients = get_clients(client, headers)
    return find_client(clients, AGENCY_CLIENT_ID)["id"]


def create_client_roles(client: httpx.Client, headers: dict, ingestion_id: str) -> None:
    """Create roles on the ingestion-api client."""
    for role_name in CLIENT_ROLES:
        resp = client.post(
            f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{ingestion_id}/roles",
            headers=headers,
            json={"name": role_name},
        )
        if resp.status_code == 409:
            print(f"  ✓ role '{role_name}' exists, skipping")
        elif resp.status_code == 201:
            print(f"  ✓ role '{role_name}' created")
        else:
            resp.raise_for_status()
    print("✓ client roles configured")


def assign_roles_to_service_account(
    client: httpx.Client, headers: dict, agency_id: str, ingestion_id: str
) -> None:
    """Assign all ingestion-api roles to nas-agency-a service account user."""
    # Get service account user
    resp = client.get(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{agency_id}/service-account-user",
        headers=headers,
    )
    resp.raise_for_status()
    sa_user_id = resp.json()["id"]

    # Get available roles for this client
    resp = client.get(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{ingestion_id}/roles",
        headers=headers,
    )
    resp.raise_for_status()
    all_roles = resp.json()

    # Filter to only the roles we want
    roles_to_assign = [r for r in all_roles if r["name"] in CLIENT_ROLES]

    if not roles_to_assign:
        print("✓ no roles to assign (roles not found on client)")
        return

    # Assign roles
    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/users/{sa_user_id}/role-mappings/clients/{ingestion_id}",
        headers=headers,
        json=roles_to_assign,
    )
    if resp.status_code == 204:
        print(f"✓ {len(roles_to_assign)} roles assigned to service account")
    elif resp.status_code == 409:
        print("✓ roles already assigned, skipping")
    else:
        resp.raise_for_status()
        print(f"✓ {len(roles_to_assign)} roles assigned to service account")


def create_processor_api_client(client: httpx.Client, headers: dict) -> str:
    """Create the nas-processor-api client (bearer-only, validates tokens)."""
    clients = get_clients(client, headers)
    existing = find_client(clients, PROCESSOR_API_CLIENT_ID)
    if existing:
        print(f"✓ client '{PROCESSOR_API_CLIENT_ID}' exists, skipping")
        return existing["id"]

    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients",
        headers=headers,
        json={
            "clientId": PROCESSOR_API_CLIENT_ID,
            "enabled": True,
            "bearerOnly": True,
            "publicClient": False,
        },
    )
    if resp.status_code == 409:
        print(f"✓ client '{PROCESSOR_API_CLIENT_ID}' exists, skipping")
        clients = get_clients(client, headers)
        return find_client(clients, PROCESSOR_API_CLIENT_ID)["id"]
    resp.raise_for_status()
    print(f"✓ client '{PROCESSOR_API_CLIENT_ID}' created")

    clients = get_clients(client, headers)
    return find_client(clients, PROCESSOR_API_CLIENT_ID)["id"]


# api.access role exists solely to trigger Keycloak audience mapping.
# When a service account has this role on a client, Keycloak includes
# that client in the token's aud claim. Without it, the bearer-only
# client rejects the token even if the signature is valid.

def create_audience_roles(client: httpx.Client, headers: dict) -> None:
    """Create api.access roles on bearer-only clients and assign to service accounts.

    This ensures service account tokens include the correct audience claims.
    """
    clients_list = get_clients(client, headers)
    proc_client = find_client(clients_list, PROCESSOR_API_CLIENT_ID)
    if not proc_client:
        print(f"  ⚠ client '{PROCESSOR_API_CLIENT_ID}' not found, skipping audience roles")
        return
    proc_id = proc_client["id"]

    # Create api.access role on nas-processor-api
    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{proc_id}/roles",
        headers=headers,
        json={"name": "api.access"},
    )
    if resp.status_code == 409:
        print("  ✓ role 'api.access' on nas-processor-api exists, skipping")
    elif resp.status_code == 201:
        print("  ✓ role 'api.access' on nas-processor-api created")
    else:
        resp.raise_for_status()

    # Get the role
    role_resp = client.get(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{proc_id}/roles/api.access",
        headers=headers,
    )
    role_resp.raise_for_status()
    role = role_resp.json()

    # Assign to nas-agency-a and nas-admin service accounts
    for target_client_id in (AGENCY_CLIENT_ID, ADMIN_CLIENT_ID):
        target = find_client(clients_list, target_client_id)
        if not target:
            continue
        sa_resp = client.get(
            f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{target['id']}/service-account-user",
            headers=headers,
        )
        sa_resp.raise_for_status()
        sa_user_id = sa_resp.json()["id"]
        assign_resp = client.post(
            f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/users/{sa_user_id}/role-mappings/clients/{proc_id}",
            headers=headers,
            json=[role],
        )
        if assign_resp.status_code == 204:
            print(f"  ✓ api.access assigned to {target_client_id}")
        elif assign_resp.status_code == 409:
            print(f"  ✓ api.access already assigned to {target_client_id}, skipping")
        else:
            assign_resp.raise_for_status()
            print(f"  ✓ api.access assigned to {target_client_id}")
    print("✓ audience roles configured")


def create_admin_client(client: httpx.Client, headers: dict) -> str:
    """Create the nas-admin client (confidential, service account). Returns internal ID."""
    clients = get_clients(client, headers)
    existing = find_client(clients, ADMIN_CLIENT_ID)
    if existing:
        print(f"✓ client '{ADMIN_CLIENT_ID}' exists, skipping")
        return existing["id"]

    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients",
        headers=headers,
        json={
            "clientId": ADMIN_CLIENT_ID,
            "enabled": True,
            "serviceAccountsEnabled": True,
            "publicClient": False,
            "secret": ADMIN_CLIENT_SECRET,
        },
    )
    if resp.status_code == 409:
        print(f"✓ client '{ADMIN_CLIENT_ID}' exists, skipping")
        clients = get_clients(client, headers)
        return find_client(clients, ADMIN_CLIENT_ID)["id"]
    resp.raise_for_status()
    print(f"✓ client '{ADMIN_CLIENT_ID}' created")

    clients = get_clients(client, headers)
    return find_client(clients, ADMIN_CLIENT_ID)["id"]


def create_realm_role(client: httpx.Client, headers: dict, role_name: str) -> None:
    """Create a realm-level role."""
    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/roles",
        headers=headers,
        json={"name": role_name},
    )
    if resp.status_code == 409:
        print(f"✓ realm role '{role_name}' exists, skipping")
    elif resp.status_code == 201:
        print(f"✓ realm role '{role_name}' created")
    else:
        resp.raise_for_status()


def assign_realm_role_to_service_account(
    client: httpx.Client, headers: dict, client_internal_id: str, role_name: str
) -> None:
    """Assign a realm role to a client's service account."""
    # Get service account user
    resp = client.get(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{client_internal_id}/service-account-user",
        headers=headers,
    )
    resp.raise_for_status()
    sa_user_id = resp.json()["id"]

    # Get the realm role
    resp = client.get(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/roles/{role_name}",
        headers=headers,
    )
    resp.raise_for_status()
    role = resp.json()

    # Assign realm role to service account
    resp = client.post(
        f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/users/{sa_user_id}/role-mappings/realm",
        headers=headers,
        json=[role],
    )
    if resp.status_code == 204:
        print(f"✓ realm role '{role_name}' assigned to {ADMIN_CLIENT_ID} service account")
    elif resp.status_code == 409:
        print(f"✓ realm role '{role_name}' already assigned, skipping")
    else:
        resp.raise_for_status()
        print(f"✓ realm role '{role_name}' assigned to {ADMIN_CLIENT_ID} service account")


def seed_redis_routing() -> None:
    """Seed Redis with client routing config."""
    import subprocess

    entries = [
        (f"apikey:{AGENCY_CLIENT_ID}", json.dumps(REDIS_ROUTING)),
        (f"apikey:{ADMIN_CLIENT_ID}", json.dumps(ADMIN_REDIS_ROUTING)),
    ]

    for key, value in entries:
        try:
            r = redis.from_url(REDIS_URL, decode_responses=True)
            r.set(key, value)
            r.close()
        except redis.ConnectionError:
            container = os.environ.get("REDIS_CONTAINER", "nas-valkey")
            subprocess.run(
                ["docker", "exec", container, "valkey-cli", "SET", key, value],
                check=True,
                capture_output=True,
            )
        print(f"✓ Redis routing set: {key}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"Seeding Keycloak at {KEYCLOAK_URL}")
    print(f"  realm: {KEYCLOAK_REALM}")
    print(f"  redis: {REDIS_URL}")
    print()

    try:
        with httpx.Client(timeout=30.0) as client:
            token = get_admin_token(client)
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            create_realm(client, headers)
            ingestion_id = create_ingestion_client(client, headers)
            create_processor_api_client(client, headers)
            agency_id = create_agency_client(client, headers)
            admin_id = create_admin_client(client, headers)
            create_client_roles(client, headers, ingestion_id)
            assign_roles_to_service_account(client, headers, agency_id, ingestion_id)
            create_realm_role(client, headers, REALM_ROLE_PLATFORM_ADMIN)
            assign_realm_role_to_service_account(client, headers, admin_id, REALM_ROLE_PLATFORM_ADMIN)
            create_audience_roles(client, headers)

        seed_redis_routing()

        print()
        print("✓ Keycloak seed complete")
        return 0

    except httpx.HTTPStatusError as exc:
        print(f"\n✗ HTTP error: {exc.response.status_code} — {exc.response.text}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"\n✗ Unexpected error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
