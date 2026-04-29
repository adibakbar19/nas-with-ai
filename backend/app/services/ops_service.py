from typing import Any

import requests

from backend.app.core.settings import get_settings
from backend.app.object_store import ObjectStoreSettings, build_s3_client


class OpsService:
    def health(self) -> dict[str, Any]:
        settings = get_settings()
        es_status = "down"
        object_storage_status = "down"
        try:
            response = requests.get(settings.es_url, timeout=5)
            if response.status_code < 300:
                es_status = "up"
        except requests.RequestException:
            es_status = "down"
        try:
            object_store = ObjectStoreSettings.from_env()
            client = build_s3_client(object_store)
            client.head_bucket(Bucket=object_store.bucket)
            object_storage_status = "up"
        except Exception:
            object_storage_status = "down"
        return {
            "service": "nas-api",
            "status": "ok",
            "elasticsearch": es_status,
            "object_storage": object_storage_status,
            "minio": object_storage_status,
        }

    @staticmethod
    def root() -> dict[str, Any]:
        return {"service": "nas-api", "status": "ok"}
