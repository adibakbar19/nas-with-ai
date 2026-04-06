from typing import Any

from backend.app.services.errors import ServiceError


class OpsService:
    @staticmethod
    def _runtime():
        from backend.app import main as runtime

        return runtime

    def health(self) -> dict[str, Any]:
        runtime = self._runtime()
        es_status = "down"
        object_storage_status = "down"
        try:
            response = runtime.requests.get(runtime.ES_URL, timeout=5)
            if response.status_code < 300:
                es_status = "up"
        except runtime.requests.RequestException:
            es_status = "down"
        try:
            runtime._get_object_store_client()
            object_storage_status = "up"
        except Exception:
            object_storage_status = "down"
        return {
            "service": "nas-api",
            "status": "ok",
            "elasticsearch": es_status,
            "object_storage": object_storage_status,
            "minio": object_storage_status,
            "ingest_execution_mode": runtime.INGEST_EXECUTION_MODE,
        }

    def list_jobs(self, *, limit: int) -> dict[str, Any]:
        runtime = self._runtime()
        runs = runtime._load_audit_runs(limit=limit)
        return {"count": len(runs), "items": runs}

    def get_job(self, run_id: str) -> dict[str, Any]:
        runtime = self._runtime()
        runs = runtime._load_audit_runs(limit=1000)
        for run in runs:
            if run["run_id"] == run_id:
                return run
        raise ServiceError(status_code=404, detail="run_id not found")

    @staticmethod
    def root() -> dict[str, Any]:
        return {"service": "nas-api", "status": "ok"}
