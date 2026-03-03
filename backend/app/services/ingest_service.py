import threading
import uuid
from datetime import datetime, timezone
from typing import Any, BinaryIO

from backend.app.services.errors import ServiceError


class IngestService:
    @staticmethod
    def _runtime():
        from backend.app import main as runtime

        return runtime

    def upload(
        self,
        *,
        file_name: str,
        file_obj: BinaryIO,
        content_type: str | None,
        auto_start: bool,
        load_to_db: bool = True,
        resume_from_checkpoint: bool = True,
        resume_failed_only: bool = True,
    ) -> dict[str, Any]:
        runtime = self._runtime()

        # Ingest always continues with DB load + checkpoint resume behavior.
        load_to_db = True
        resume_from_checkpoint = True
        resume_failed_only = True
        config_path = "config/config.json"
        source_type = runtime._infer_source_type(file_name)
        job_id = uuid.uuid4().hex
        object_name = f"{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{job_id}_{file_name}"
        success_path = runtime.OUTPUT_UPLOADS_DIR / job_id / "cleaned"
        failed_path = runtime.OUTPUT_UPLOADS_DIR / job_id / "failed"
        checkpoint_root = runtime.OUTPUT_UPLOADS_DIR / job_id / "checkpoints"

        try:
            client = runtime._get_minio_client()
            client.put_object(
                runtime.MINIO_BUCKET,
                object_name,
                file_obj,
                length=-1,
                part_size=10 * 1024 * 1024,
                content_type=content_type or "application/octet-stream",
            )
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"minio upload failed: {exc}") from exc

        runtime._set_job(
            job_id,
            status="uploaded",
            created_at=runtime._now_iso(),
            file_name=file_name,
            source_type=source_type,
            config_path=config_path,
            object_name=object_name,
            bucket=runtime.MINIO_BUCKET,
            success_path=str(success_path),
            failed_path=str(failed_path),
            checkpoint_root=str(checkpoint_root),
            load_to_db=load_to_db,
            load_status="pending" if load_to_db else "skipped",
            resume_from_checkpoint=resume_from_checkpoint,
            resume_failed_only=resume_failed_only,
        )

        if auto_start:
            runtime._set_job(job_id, status="queued", persist=True)
            self._queue_or_run(job_id)

        return {
            "job_id": job_id,
            "status": "queued" if auto_start else "uploaded",
            "object_name": object_name,
            "load_to_db": load_to_db,
        }

    def start_job(self, job_id: str) -> dict[str, Any]:
        runtime = self._runtime()
        job = runtime._get_job_for_api(job_id)
        if not job:
            raise ServiceError(status_code=404, detail="job_id not found")
        if runtime.INGEST_EXECUTION_MODE == "queue_worker":
            runtime._set_job(job_id, persist=False, **{k: v for k, v in job.items() if k != "job_id"})
        status = str(job.get("status") or "").lower()
        if status in {"running", "queued", "pausing"}:
            raise ServiceError(status_code=409, detail=f"job cannot be started from status '{status}'")
        runtime._set_job(
            job_id,
            status="queued",
            ended_at=None,
            error=None,
            pause_requested=False,
            resume_from_checkpoint=True,
            resume_failed_only=True,
            last_log_line="Queued to resume from checkpoint",
            load_status="pending" if job.get("load_to_db") else "skipped",
        )
        self._queue_or_run(job_id)
        return {"job_id": job_id, "status": "queued"}

    def pause_job(self, job_id: str) -> dict[str, Any]:
        runtime = self._runtime()
        if runtime.INGEST_EXECUTION_MODE == "queue_worker":
            job = runtime._get_job_for_api(job_id)
            if not job:
                raise ServiceError(status_code=404, detail="job_id not found")

            status = str(job.get("status") or "").lower()
            if status in {"paused", "completed", "failed", "interrupted"}:
                raise ServiceError(status_code=409, detail=f"cannot pause job in status '{status}'")
            if status == "queued":
                runtime._set_job(
                    job_id,
                    status="paused",
                    ended_at=runtime._now_iso(),
                    pause_requested=False,
                    progress_stage="paused",
                    last_log_line="Paused before process start",
                    load_status="paused" if job.get("load_to_db") else "skipped",
                )
                return {"job_id": job_id, "status": "paused", "phase": "queued"}
            if status != "running":
                raise ServiceError(status_code=409, detail=f"job is not running (status: {status or 'unknown'})")

            runtime._set_job(
                job_id,
                status="pausing",
                pause_requested=True,
                progress_stage="pausing",
                last_log_line="Pause requested; worker will stop at safe point",
            )
            return {"job_id": job_id, "status": "pausing", "phase": "pipeline"}

        job = runtime._get_job(job_id)
        if not job:
            raise ServiceError(status_code=404, detail="job_id not found")

        status = str(job.get("status") or "").lower()
        if status in {"paused", "completed", "failed", "interrupted"}:
            raise ServiceError(status_code=409, detail=f"cannot pause job in status '{status}'")
        if status == "queued":
            runtime._set_job(
                job_id,
                status="paused",
                ended_at=runtime._now_iso(),
                progress_stage="paused",
                last_log_line="Paused before process start",
                load_status="paused" if job.get("load_to_db") else "skipped",
            )
            return {"job_id": job_id, "status": "paused", "phase": "queued"}
        if status != "running":
            raise ServiceError(status_code=409, detail=f"job is not running (status: {status or 'unknown'})")

        paused, phase = runtime._request_pause(job_id)
        if not paused and phase == "db_load":
            raise ServiceError(
                status_code=409,
                detail="pause is only supported during pipeline stage; db load pause may cause partial writes",
            )
        if not paused:
            raise ServiceError(status_code=409, detail="no active process found for this job")

        runtime._set_job(
            job_id,
            status="pausing",
            progress_stage="pausing",
            last_log_line=f"Pause requested during {phase or 'pipeline'} stage",
        )
        return {"job_id": job_id, "status": "pausing", "phase": phase or "pipeline"}

    def list_jobs(self, *, limit: int) -> dict[str, Any]:
        runtime = self._runtime()
        if runtime.INGEST_EXECUTION_MODE == "queue_worker":
            rows = runtime._read_jobs_state_snapshot()[:limit]
        else:
            rows = runtime._list_jobs()[:limit]
        return {"count": len(rows), "items": rows}

    def get_job(self, job_id: str) -> dict[str, Any]:
        runtime = self._runtime()
        row = runtime._get_job_for_api(job_id)
        if not row:
            raise ServiceError(status_code=404, detail="job_id not found")
        return row

    def _queue_or_run(self, job_id: str) -> None:
        runtime = self._runtime()
        if runtime.INGEST_EXECUTION_MODE == "queue_worker":
            try:
                runtime._queue_ingest_job(job_id)
            except Exception as exc:
                runtime._set_job(
                    job_id,
                    status="failed",
                    ended_at=runtime._now_iso(),
                    error=f"queue publish failed: {exc}",
                    progress_stage="failed",
                )
                raise ServiceError(status_code=503, detail=f"failed to queue ingest job: {exc}") from exc
            return

        thread = threading.Thread(target=runtime._run_ingest_job, args=(job_id,), daemon=True)
        thread.start()

    def run_job(self, job_id: str) -> None:
        runtime = self._runtime()
        runtime._run_ingest_job(job_id)
