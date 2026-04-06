import hashlib
import os
import threading
import uuid
from typing import Any, BinaryIO

from minio.error import S3Error

from backend.app.repositories.multipart_upload_repository import MultipartUploadRepository
from backend.app.services.errors import ServiceError


class IngestService:
    @staticmethod
    def _runtime():
        from backend.app import main as runtime

        return runtime

    @staticmethod
    def _multipart_repository() -> MultipartUploadRepository:
        runtime = IngestService._runtime()
        repository = MultipartUploadRepository(
            dsn=runtime.INGEST_JOB_STATE_DSN,
            schema=runtime.INGEST_JOB_STATE_SCHEMA,
        )
        repository.ensure_table()
        return repository

    @staticmethod
    def _multipart_part_size_bytes() -> int:
        return max(5 * 1024 * 1024, int(os.getenv("INGEST_MULTIPART_PART_SIZE_BYTES", str(16 * 1024 * 1024))))

    @staticmethod
    def _multipart_presign_expires_seconds() -> int:
        return max(300, int(os.getenv("INGEST_MULTIPART_PRESIGN_EXPIRES_SECONDS", "3600")))

    @staticmethod
    def _minio_public_endpoint() -> str:
        runtime = IngestService._runtime()
        endpoint = os.getenv("MINIO_PUBLIC_ENDPOINT", "").strip() or runtime.MINIO_ENDPOINT
        secure = os.getenv("MINIO_PUBLIC_SECURE", str(runtime.MINIO_SECURE)).lower() in {"1", "true", "yes", "y"}
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        scheme = "https" if secure else "http"
        return f"{scheme}://{endpoint}"

    @classmethod
    def _get_s3_client(cls, *, public_endpoint: bool):
        runtime = cls._runtime()
        try:
            import boto3
            from botocore.client import Config
        except Exception as exc:  # pragma: no cover - dependency provided in runtime image
            raise RuntimeError("boto3 is required for multipart browser uploads") from exc

        endpoint_url = cls._minio_public_endpoint() if public_endpoint else (
            runtime.MINIO_ENDPOINT
            if runtime.MINIO_ENDPOINT.startswith("http://") or runtime.MINIO_ENDPOINT.startswith("https://")
            else f"{'https' if runtime.MINIO_SECURE else 'http'}://{runtime.MINIO_ENDPOINT}"
        )
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=runtime.MINIO_ACCESS_KEY,
            aws_secret_access_key=runtime.MINIO_SECRET_KEY,
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    @classmethod
    def _ensure_bucket_cors(cls) -> None:
        runtime = cls._runtime()
        s3_client = cls._get_s3_client(public_endpoint=False)
        allowed_origins = sorted(
            {
                *(runtime.CORS_ALLOW_ORIGINS or []),
                "http://localhost:8088",
                "http://127.0.0.1:8088",
            }
        ) or ["*"]
        try:
            s3_client.put_bucket_cors(
                Bucket=runtime.MINIO_BUCKET,
                CORSConfiguration={
                    "CORSRules": [
                        {
                            "AllowedMethods": ["GET", "PUT", "POST", "HEAD"],
                            "AllowedOrigins": allowed_origins,
                            "AllowedHeaders": ["*"],
                            "ExposeHeaders": ["ETag"],
                            "MaxAgeSeconds": 3600,
                        }
                    ]
                },
            )
        except Exception as exc:
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code in {"NotImplemented", "NotImplementedYet"} or "not implemented" in str(exc).lower():
                return
            raise

    @classmethod
    def _register_uploaded_object(
        cls,
        *,
        file_name: str,
        object_name: str,
        bucket: str,
        content_bytes: int,
        auto_start: bool,
        content_sha256: str | None,
        object_reused: bool | None,
        load_to_db: bool = True,
        resume_from_checkpoint: bool = True,
        resume_failed_only: bool = True,
    ) -> dict[str, Any]:
        runtime = cls._runtime()

        load_to_db = True
        resume_from_checkpoint = True
        resume_failed_only = True
        config_path = "config/config.json"
        source_type = runtime._infer_source_type(file_name)
        job_id = uuid.uuid4().hex
        success_path = runtime.OUTPUT_UPLOADS_DIR / job_id / "cleaned"
        failed_path = runtime.OUTPUT_UPLOADS_DIR / job_id / "failed"
        checkpoint_root = runtime.OUTPUT_UPLOADS_DIR / job_id / "checkpoints"

        runtime._set_job(
            job_id,
            status="uploaded",
            created_at=runtime._now_iso(),
            file_name=file_name,
            source_type=source_type,
            config_path=config_path,
            object_name=object_name,
            bucket=bucket,
            content_sha256=content_sha256,
            content_bytes=content_bytes,
            object_reused=object_reused,
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
            cls()._queue_or_run(job_id)

        return {
            "job_id": job_id,
            "status": "queued" if auto_start else "uploaded",
            "object_name": object_name,
            "content_sha256": content_sha256,
            "content_bytes": content_bytes,
            "object_reused": object_reused,
            "load_to_db": load_to_db,
        }

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

        content_sha256, content_bytes = self._hash_file(file_obj)
        object_name = f"sha256/{content_sha256}"
        uploaded_new_object = False

        try:
            client = runtime._get_minio_client()
            object_exists = self._object_exists(client, runtime.MINIO_BUCKET, object_name)
            if not object_exists:
                self._rewind_file(file_obj)
                client.put_object(
                    runtime.MINIO_BUCKET,
                    object_name,
                    file_obj,
                    length=content_bytes,
                    part_size=10 * 1024 * 1024,
                    content_type=content_type or "application/octet-stream",
                )
                uploaded_new_object = True
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"minio upload failed: {exc}") from exc

        return self._register_uploaded_object(
            file_name=file_name,
            object_name=object_name,
            bucket=runtime.MINIO_BUCKET,
            content_bytes=content_bytes,
            auto_start=auto_start,
            content_sha256=content_sha256,
            object_reused=not uploaded_new_object,
            load_to_db=load_to_db,
            resume_from_checkpoint=resume_from_checkpoint,
            resume_failed_only=resume_failed_only,
        )

    def initiate_multipart_upload(
        self,
        *,
        file_name: str,
        content_type: str | None,
        content_bytes: int,
        auto_start: bool,
        load_to_db: bool = True,
        resume_from_checkpoint: bool = True,
        resume_failed_only: bool = True,
    ) -> dict[str, Any]:
        runtime = self._runtime()
        part_size = self._multipart_part_size_bytes()
        part_count = (content_bytes + part_size - 1) // part_size
        if part_count > 10000:
            raise ServiceError(status_code=400, detail="file is too large for multipart upload configuration")

        session_id = uuid.uuid4().hex
        object_name = f"multipart/{session_id}/{file_name}"
        try:
            self._ensure_bucket_cors()
            s3_client = self._get_s3_client(public_endpoint=False)
            response = s3_client.create_multipart_upload(
                Bucket=runtime.MINIO_BUCKET,
                Key=object_name,
                ContentType=content_type or "application/octet-stream",
            )
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"failed to initiate multipart upload: {exc}") from exc

        upload_id = str(response["UploadId"])
        session = {
            "session_id": session_id,
            "status": "initiated",
            "bucket": runtime.MINIO_BUCKET,
            "object_name": object_name,
            "upload_id": upload_id,
            "file_name": file_name,
            "content_type": content_type,
            "content_bytes": int(content_bytes),
            "part_size": int(part_size),
            "job_id": None,
            "auto_start": bool(auto_start),
            "load_to_db": bool(load_to_db),
            "resume_from_checkpoint": bool(resume_from_checkpoint),
            "resume_failed_only": bool(resume_failed_only),
        }
        self._multipart_repository().create_session(session=session)
        return {
            "session_id": session_id,
            "bucket": runtime.MINIO_BUCKET,
            "object_name": object_name,
            "upload_id": upload_id,
            "part_size": part_size,
            "expires_in": self._multipart_presign_expires_seconds(),
        }

    def get_multipart_upload_status(self, session_id: str) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id)
        if not session:
            raise ServiceError(status_code=404, detail="upload session not found")

        uploaded_parts: list[dict[str, Any]] = []
        if session.get("status") not in {"completed", "aborted"}:
            try:
                s3_client = self._get_s3_client(public_endpoint=False)
                kwargs: dict[str, Any] = {
                    "Bucket": session["bucket"],
                    "Key": session["object_name"],
                    "UploadId": session["upload_id"],
                }
                while True:
                    response = s3_client.list_parts(**kwargs)
                    for part in response.get("Parts", []):
                        uploaded_parts.append(
                            {
                                "part_number": int(part["PartNumber"]),
                                "etag": str(part["ETag"]).strip('"'),
                            }
                        )
                    if not response.get("IsTruncated"):
                        break
                    kwargs["PartNumberMarker"] = response.get("NextPartNumberMarker")
            except Exception as exc:
                raise ServiceError(status_code=502, detail=f"failed to inspect multipart upload: {exc}") from exc

        session["uploaded_parts"] = uploaded_parts
        return session

    def get_multipart_part_url(self, *, session_id: str, part_number: int) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id)
        if not session:
            raise ServiceError(status_code=404, detail="upload session not found")
        if session.get("status") in {"completed", "aborted"}:
            raise ServiceError(status_code=409, detail=f"cannot upload parts for session status '{session['status']}'")

        total_parts = (int(session["content_bytes"]) + int(session["part_size"]) - 1) // int(session["part_size"])
        if part_number < 1 or part_number > total_parts:
            raise ServiceError(status_code=400, detail=f"part_number must be between 1 and {total_parts}")
        try:
            s3_client = self._get_s3_client(public_endpoint=True)
            url = s3_client.generate_presigned_url(
                "upload_part",
                Params={
                    "Bucket": session["bucket"],
                    "Key": session["object_name"],
                    "UploadId": session["upload_id"],
                    "PartNumber": int(part_number),
                },
                ExpiresIn=self._multipart_presign_expires_seconds(),
            )
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"failed to presign multipart part: {exc}") from exc

        self._multipart_repository().update_session(session_id=session_id, status="uploading")
        return {
            "session_id": session_id,
            "part_number": int(part_number),
            "url": url,
            "expires_in": self._multipart_presign_expires_seconds(),
        }

    def complete_multipart_upload(self, *, session_id: str, parts: list[dict[str, Any]]) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id)
        if not session:
            raise ServiceError(status_code=404, detail="upload session not found")
        if session.get("status") == "completed" and session.get("job_id"):
            job = self.get_job(str(session["job_id"]))
            return {"job_id": job["job_id"], "status": job["status"], "session_id": session_id}
        if session.get("status") == "aborted":
            raise ServiceError(status_code=409, detail="upload session has already been aborted")
        try:
            s3_client = self._get_s3_client(public_endpoint=False)
            s3_client.complete_multipart_upload(
                Bucket=session["bucket"],
                Key=session["object_name"],
                UploadId=session["upload_id"],
                MultipartUpload={
                    "Parts": [
                        {"ETag": str(item["etag"]), "PartNumber": int(item["part_number"])}
                        for item in sorted(parts, key=lambda item: int(item["part_number"]))
                    ]
                },
            )
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"failed to complete multipart upload: {exc}") from exc

        result = self._register_uploaded_object(
            file_name=str(session["file_name"]),
            object_name=str(session["object_name"]),
            bucket=str(session["bucket"]),
            content_bytes=int(session["content_bytes"]),
            auto_start=bool(session.get("auto_start", True)),
            content_sha256=None,
            object_reused=False,
            load_to_db=bool(session.get("load_to_db", True)),
            resume_from_checkpoint=bool(session.get("resume_from_checkpoint", True)),
            resume_failed_only=bool(session.get("resume_failed_only", True)),
        )
        self._multipart_repository().update_session(
            session_id=session_id,
            status="completed",
            job_id=result["job_id"],
        )
        result["session_id"] = session_id
        return result

    def abort_multipart_upload(self, *, session_id: str) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id)
        if not session:
            raise ServiceError(status_code=404, detail="upload session not found")
        if session.get("status") == "completed":
            raise ServiceError(status_code=409, detail="cannot abort a completed upload session")
        try:
            s3_client = self._get_s3_client(public_endpoint=False)
            s3_client.abort_multipart_upload(
                Bucket=session["bucket"],
                Key=session["object_name"],
                UploadId=session["upload_id"],
            )
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"failed to abort multipart upload: {exc}") from exc
        self._multipart_repository().update_session(session_id=session_id, status="aborted")
        return {"session_id": session_id, "status": "aborted"}

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

    @staticmethod
    def _rewind_file(file_obj: BinaryIO) -> None:
        if not hasattr(file_obj, "seek"):
            raise RuntimeError("uploaded file stream is not seekable")
        file_obj.seek(0)

    @classmethod
    def _hash_file(cls, file_obj: BinaryIO) -> tuple[str, int]:
        cls._rewind_file(file_obj)
        hasher = hashlib.sha256()
        total_bytes = 0
        while True:
            chunk = file_obj.read(1024 * 1024)
            if not chunk:
                break
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            hasher.update(chunk)
            total_bytes += len(chunk)
        cls._rewind_file(file_obj)
        return hasher.hexdigest(), total_bytes

    @staticmethod
    def _object_exists(client, bucket: str, object_name: str) -> bool:
        try:
            client.stat_object(bucket, object_name)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound"}:
                return False
            raise
