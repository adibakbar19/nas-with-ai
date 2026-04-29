import hashlib
import json
import os
import uuid
from typing import Any, BinaryIO

from backend.app.monitoring import record_upload_bytes
from backend.app.repositories.api_idempotency_repository import ApiIdempotencyRepository
from backend.app.repositories.multipart_upload_repository import MultipartUploadRepository
from backend.app.runtime import jobs as ingest_jobs
from backend.app.runtime import settings as runtime_settings
from backend.app.services.errors import ServiceError
from backend.app.object_store import build_s3_client, is_missing_object_error


class IngestService:
    STORAGE_LAYOUT_VERSION = "v1"

    @staticmethod
    def _safe_uploaded_file_name(file_name: str | None, *, default: str = "uploaded_file") -> str:
        raw_name = (file_name or "").strip().replace("\\", "/")
        safe_name = os.path.basename(raw_name)
        return safe_name or default

    @classmethod
    def _build_source_object_key(cls, *, agency_id: str, job_id: str, file_name: str) -> str:
        safe_file_name = cls._safe_uploaded_file_name(file_name)
        return f"agency/{agency_id}/jobs/{job_id}/source/original/{safe_file_name}"

    @classmethod
    def _build_retry_corrections_key(
        cls,
        *,
        agency_id: str,
        parent_job_id: str,
        retry_job_id: str,
        file_name: str,
    ) -> str:
        safe_file_name = cls._safe_uploaded_file_name(file_name, default="corrections.csv")
        return f"agency/{agency_id}/jobs/{parent_job_id}/retries/{retry_job_id}/corrections/{safe_file_name}"

    @classmethod
    def _build_job_manifest_key(cls, *, agency_id: str, job_id: str) -> str:
        return f"agency/{agency_id}/jobs/{job_id}/manifests/job.json"

    @classmethod
    def _build_retry_manifest_key(cls, *, agency_id: str, parent_job_id: str, retry_job_id: str) -> str:
        return f"agency/{agency_id}/jobs/{parent_job_id}/retries/{retry_job_id}/manifests/retry.json"

    @staticmethod
    def _multipart_repository() -> MultipartUploadRepository:
        repository = MultipartUploadRepository(
            dsn=runtime_settings.INGEST_JOB_STATE_DSN,
            schema=runtime_settings.INGEST_JOB_STATE_SCHEMA,
        )
        return repository

    @staticmethod
    def _idempotency_repository() -> ApiIdempotencyRepository:
        repository = ApiIdempotencyRepository(
            dsn=runtime_settings.INGEST_JOB_STATE_DSN,
            schema=runtime_settings.INGEST_JOB_STATE_SCHEMA,
        )
        return repository

    @staticmethod
    def _multipart_part_size_bytes() -> int:
        return max(5 * 1024 * 1024, int(os.getenv("INGEST_MULTIPART_PART_SIZE_BYTES", str(16 * 1024 * 1024))))

    @staticmethod
    def _multipart_presign_expires_seconds() -> int:
        return max(300, int(os.getenv("INGEST_MULTIPART_PRESIGN_EXPIRES_SECONDS", "3600")))

    @staticmethod
    def _object_store_public_endpoint() -> str:
        endpoint = os.getenv("OBJECT_STORE_PUBLIC_ENDPOINT", "").strip() or runtime_settings.OBJECT_STORE_PUBLIC_ENDPOINT
        if not endpoint:
            return ""
        secure = os.getenv("OBJECT_STORE_PUBLIC_SECURE", str(runtime_settings.OBJECT_STORE_SECURE)).lower() in {
            "1",
            "true",
            "yes",
            "y",
        }
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        scheme = "https" if secure else "http"
        return f"{scheme}://{endpoint}"

    @classmethod
    def _get_s3_client(cls, *, public_endpoint: bool):
        return build_s3_client(runtime_settings.OBJECT_STORE, public=public_endpoint)

    @classmethod
    def _ensure_bucket_cors(cls) -> None:
        manage_cors = os.getenv("OBJECT_STORE_MANAGE_CORS", "").strip().lower()
        if not manage_cors:
            manage_cors_enabled = bool(runtime_settings.OBJECT_STORE.endpoint)
        else:
            manage_cors_enabled = manage_cors in {"1", "true", "yes", "y", "on"}
        if not manage_cors_enabled:
            return
        s3_client = cls._get_s3_client(public_endpoint=False)
        allowed_origins = sorted(
            {
                *(runtime_settings.CORS_ALLOW_ORIGINS or []),
                "http://localhost:8088",
                "http://127.0.0.1:8088",
            }
        ) or ["*"]
        try:
            s3_client.put_bucket_cors(
                Bucket=runtime_settings.OBJECT_STORE_BUCKET,
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
        agency_id: str,
        job_id: str | None,
        created_by_user_id: str | None,
        created_by_username: str | None,
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
        load_to_db = True

        config_path = "config/config.json"
        source_type = ingest_jobs.infer_source_type(file_name)
        job_id = str(job_id or uuid.uuid4().hex)
        success_path = runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "cleaned"
        warning_path = runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "warnings"
        failed_path = runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "failed"
        checkpoint_root = runtime_settings.OUTPUT_UPLOADS_DIR / job_id / "checkpoints"

        ingest_jobs.set_job(
            job_id,
            agency_id=agency_id,
            status="uploaded",
            created_at=ingest_jobs.now_iso(),
            file_name=file_name,
            source_type=source_type,
            config_path=config_path,
            created_by_user_id=str(created_by_user_id or "").strip() or None,
            created_by_username=str(created_by_username or "").strip() or None,
            storage_layout_version=cls.STORAGE_LAYOUT_VERSION,
            object_name=object_name,
            source_object_name=object_name,
            manifest_object_name=cls._build_job_manifest_key(agency_id=agency_id, job_id=job_id),
            bucket=bucket,
            content_sha256=content_sha256,
            content_bytes=content_bytes,
            object_reused=object_reused,
            success_path=str(success_path),
            warning_path=str(warning_path),
            failed_path=str(failed_path),
            checkpoint_root=str(checkpoint_root),
            load_to_db=load_to_db,
            load_status="pending",
            resume_from_checkpoint=resume_from_checkpoint,
            resume_failed_only=resume_failed_only,
        )

        if auto_start:
            ingest_jobs.set_job(job_id, status="queued", persist=True)
            cls()._queue_or_run(job_id)

        return {
            "job_id": job_id,
            "agency_id": agency_id,
            "status": "queued" if auto_start else "uploaded",
            "object_name": object_name,
            "content_sha256": content_sha256,
            "content_bytes": content_bytes,
            "object_reused": object_reused,
            "load_to_db": load_to_db,
        }

    @staticmethod
    def _normalize_idempotency_key(idempotency_key: str | None) -> str | None:
        normalized = str(idempotency_key or "").strip()
        return normalized or None

    @staticmethod
    def _build_request_fingerprint(payload: dict[str, Any]) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _claim_idempotent_request(
        self,
        *,
        agency_id: str,
        operation: str,
        idempotency_key: str | None,
        request_fingerprint: str,
    ) -> tuple[str | None, dict[str, Any] | None]:
        normalized_key = self._normalize_idempotency_key(idempotency_key)
        if not normalized_key:
            return None, None
        created, record = self._idempotency_repository().claim_request(
            agency_id=agency_id,
            operation=operation,
            idempotency_key=normalized_key,
            request_fingerprint=request_fingerprint,
        )
        if str(record.get("request_fingerprint") or "") != request_fingerprint:
            raise ServiceError(status_code=409, detail="idempotency key was already used for a different request")
        if created:
            return normalized_key, None
        response = record.get("response")
        if record.get("status") == "completed" and isinstance(response, dict):
            replay = dict(response)
            replay["idempotent_replay"] = True
            return normalized_key, replay
        raise ServiceError(status_code=409, detail="request with the same idempotency key is already in progress")

    def _complete_idempotent_request(
        self,
        *,
        agency_id: str,
        operation: str,
        idempotency_key: str | None,
        resource_type: str,
        resource_id: str,
        response: dict[str, Any],
    ) -> None:
        normalized_key = self._normalize_idempotency_key(idempotency_key)
        if not normalized_key:
            return
        self._idempotency_repository().complete_request(
            agency_id=agency_id,
            operation=operation,
            idempotency_key=normalized_key,
            resource_type=resource_type,
            resource_id=resource_id,
            response=response,
        )

    def _release_idempotent_request(self, *, agency_id: str, operation: str, idempotency_key: str | None) -> None:
        normalized_key = self._normalize_idempotency_key(idempotency_key)
        if not normalized_key:
            return
        self._idempotency_repository().delete_request(
            agency_id=agency_id,
            operation=operation,
            idempotency_key=normalized_key,
        )

    def upload(
        self,
        *,
        agency_id: str,
        created_by_user_id: str | None = None,
        created_by_username: str | None = None,
        file_name: str,
        file_obj: BinaryIO,
        content_type: str | None,
        auto_start: bool,
        load_to_db: bool = True,
        resume_from_checkpoint: bool = True,
        resume_failed_only: bool = True,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        load_to_db = True

        safe_file_name = self._safe_uploaded_file_name(file_name)
        content_sha256, content_bytes = self._hash_file(file_obj)
        idempotency_request_key, replay = self._claim_idempotent_request(
            agency_id=agency_id,
            operation="direct_upload",
            idempotency_key=idempotency_key,
            request_fingerprint=self._build_request_fingerprint(
                {
                    "agency_id": agency_id,
                    "file_name": safe_file_name,
                    "content_sha256": content_sha256,
                    "content_type": content_type or "",
                    "auto_start": bool(auto_start),
                    "load_to_db": bool(load_to_db),
                    "resume_from_checkpoint": bool(resume_from_checkpoint),
                    "resume_failed_only": bool(resume_failed_only),
                }
            ),
        )
        if replay is not None:
            return replay

        job_id = uuid.uuid4().hex
        object_name = self._build_source_object_key(agency_id=agency_id, job_id=job_id, file_name=safe_file_name)

        try:
            client = self._get_s3_client(public_endpoint=False)
            self._rewind_file(file_obj)
            client.put_object(
                Bucket=runtime_settings.OBJECT_STORE_BUCKET,
                Key=object_name,
                Body=file_obj,
                ContentLength=content_bytes,
                ContentType=content_type or "application/octet-stream",
            )
        except Exception as exc:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="direct_upload",
                idempotency_key=idempotency_request_key,
            )
            raise ServiceError(status_code=502, detail=f"object storage upload failed: {exc}") from exc

        try:
            result = self._register_uploaded_object(
                agency_id=agency_id,
                job_id=job_id,
                created_by_user_id=created_by_user_id,
                created_by_username=created_by_username,
                file_name=safe_file_name,
                object_name=object_name,
                bucket=runtime_settings.OBJECT_STORE_BUCKET,
                content_bytes=content_bytes,
                auto_start=auto_start,
                content_sha256=content_sha256,
                object_reused=False,
                load_to_db=load_to_db,
                resume_from_checkpoint=resume_from_checkpoint,
                resume_failed_only=resume_failed_only,
            )
            record_upload_bytes("direct", content_bytes)
            self._complete_idempotent_request(
                agency_id=agency_id,
                operation="direct_upload",
                idempotency_key=idempotency_request_key,
                resource_type="job",
                resource_id=str(result["job_id"]),
                response=result,
            )
            return result
        except Exception:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="direct_upload",
                idempotency_key=idempotency_request_key,
            )
            raise

    def retry_failed_rows_upload(
        self,
        *,
        agency_id: str,
        created_by_user_id: str | None = None,
        created_by_username: str | None = None,
        parent_job_id: str,
        file_name: str,
        file_obj: BinaryIO,
        content_type: str | None,
        auto_start: bool,
        load_to_db: bool = True,
        require_mukim: bool = False,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        load_to_db = True
        parent_job = ingest_jobs.get_job_for_api(parent_job_id, agency_id=agency_id)
        if not parent_job:
            raise ServiceError(status_code=404, detail="parent job_id not found")

        source_failed_path = str(parent_job.get("failed_path") or "").strip()
        if not source_failed_path:
            raise ServiceError(status_code=409, detail="parent job does not have a failed output path")

        safe_file_name = self._safe_uploaded_file_name(file_name, default="corrections.csv")
        if not safe_file_name.lower().endswith(".csv"):
            raise ServiceError(status_code=400, detail="corrections upload must be a CSV file")

        content_sha256, content_bytes = self._hash_file(file_obj)
        idempotency_request_key, replay = self._claim_idempotent_request(
            agency_id=agency_id,
            operation="retry_failed_rows_upload",
            idempotency_key=idempotency_key,
            request_fingerprint=self._build_request_fingerprint(
                {
                    "agency_id": agency_id,
                    "parent_job_id": parent_job_id,
                    "file_name": safe_file_name,
                    "content_sha256": content_sha256,
                    "content_type": content_type or "",
                    "auto_start": bool(auto_start),
                    "load_to_db": bool(load_to_db),
                    "require_mukim": bool(require_mukim),
                }
            ),
        )
        if replay is not None:
            return replay
        retry_job_id = uuid.uuid4().hex
        object_name = self._build_retry_corrections_key(
            agency_id=agency_id,
            parent_job_id=parent_job_id,
            retry_job_id=retry_job_id,
            file_name=safe_file_name,
        )
        try:
            client = self._get_s3_client(public_endpoint=False)
            self._rewind_file(file_obj)
            client.put_object(
                Bucket=runtime_settings.OBJECT_STORE_BUCKET,
                Key=object_name,
                Body=file_obj,
                ContentLength=content_bytes,
                ContentType=content_type or "text/csv",
            )
        except Exception as exc:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="retry_failed_rows_upload",
                idempotency_key=idempotency_request_key,
            )
            raise ServiceError(status_code=502, detail=f"corrections upload failed: {exc}") from exc

        success_path = runtime_settings.OUTPUT_UPLOADS_DIR / parent_job_id / "retry_failed_rows" / retry_job_id / "cleaned"
        warning_path = runtime_settings.OUTPUT_UPLOADS_DIR / parent_job_id / "retry_failed_rows" / retry_job_id / "warnings"
        failed_path = runtime_settings.OUTPUT_UPLOADS_DIR / parent_job_id / "retry_failed_rows" / retry_job_id / "failed"
        config_path = str(parent_job.get("config_path") or "config/config.json")

        try:
            ingest_jobs.set_job(
                retry_job_id,
                agency_id=agency_id,
                status="uploaded",
                created_at=ingest_jobs.now_iso(),
                file_name=safe_file_name,
                source_type="csv",
                config_path=config_path,
                created_by_user_id=str(created_by_user_id or "").strip() or None,
                created_by_username=str(created_by_username or "").strip() or None,
                storage_layout_version=self.STORAGE_LAYOUT_VERSION,
                object_name=object_name,
                source_object_name=object_name,
                manifest_object_name=self._build_retry_manifest_key(
                    agency_id=agency_id,
                    parent_job_id=parent_job_id,
                    retry_job_id=retry_job_id,
                ),
                bucket=runtime_settings.OBJECT_STORE_BUCKET,
                content_sha256=content_sha256,
                content_bytes=content_bytes,
                object_reused=False,
                success_path=str(success_path),
                warning_path=str(warning_path),
                failed_path=str(failed_path),
                checkpoint_root=None,
                load_to_db=bool(load_to_db),
                load_status="pending",
                resume_from_checkpoint=False,
                resume_failed_only=False,
                job_type="retry_failed_rows",
                parent_job_id=parent_job_id,
                source_failed_path=source_failed_path,
                require_mukim=bool(require_mukim),
                last_log_line="Corrections uploaded",
            )

            if auto_start:
                ingest_jobs.set_job(
                    retry_job_id,
                    status="queued",
                    ended_at=None,
                    error=None,
                    last_log_line="Queued to retry failed rows",
                )
                self._queue_or_run(retry_job_id)

            record_upload_bytes("retry_corrections", content_bytes)
            result = {
                "job_id": retry_job_id,
                "agency_id": agency_id,
                "parent_job_id": parent_job_id,
                "status": "queued" if auto_start else "uploaded",
                "job_type": "retry_failed_rows",
                "object_name": object_name,
                "content_sha256": content_sha256,
                "content_bytes": content_bytes,
                "load_to_db": bool(load_to_db),
                "require_mukim": bool(require_mukim),
            }
            self._complete_idempotent_request(
                agency_id=agency_id,
                operation="retry_failed_rows_upload",
                idempotency_key=idempotency_request_key,
                resource_type="job",
                resource_id=retry_job_id,
                response=result,
            )
            return result
        except Exception:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="retry_failed_rows_upload",
                idempotency_key=idempotency_request_key,
            )
            raise

    def initiate_multipart_upload(
        self,
        *,
        agency_id: str,
        created_by_user_id: str | None = None,
        created_by_username: str | None = None,
        file_name: str,
        content_type: str | None,
        content_bytes: int,
        auto_start: bool,
        load_to_db: bool = True,
        resume_from_checkpoint: bool = True,
        resume_failed_only: bool = True,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        load_to_db = True
        part_size = self._multipart_part_size_bytes()
        part_count = (content_bytes + part_size - 1) // part_size
        if part_count > 10000:
            raise ServiceError(status_code=400, detail="file is too large for multipart upload configuration")

        safe_file_name = self._safe_uploaded_file_name(file_name)
        idempotency_request_key, replay = self._claim_idempotent_request(
            agency_id=agency_id,
            operation="multipart_initiate",
            idempotency_key=idempotency_key,
            request_fingerprint=self._build_request_fingerprint(
                {
                    "agency_id": agency_id,
                    "file_name": safe_file_name,
                    "content_type": content_type or "",
                    "content_bytes": int(content_bytes),
                    "auto_start": bool(auto_start),
                    "load_to_db": bool(load_to_db),
                    "resume_from_checkpoint": bool(resume_from_checkpoint),
                    "resume_failed_only": bool(resume_failed_only),
                }
            ),
        )
        if replay is not None:
            return replay
        session_id = uuid.uuid4().hex
        job_id = uuid.uuid4().hex
        object_name = self._build_source_object_key(agency_id=agency_id, job_id=job_id, file_name=safe_file_name)
        try:
            self._ensure_bucket_cors()
            s3_client = self._get_s3_client(public_endpoint=False)
            response = s3_client.create_multipart_upload(
                Bucket=runtime_settings.OBJECT_STORE_BUCKET,
                Key=object_name,
                ContentType=content_type or "application/octet-stream",
            )
        except Exception as exc:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="multipart_initiate",
                idempotency_key=idempotency_request_key,
            )
            raise ServiceError(status_code=502, detail=f"failed to initiate multipart upload: {exc}") from exc

        upload_id = str(response["UploadId"])
        session = {
            "session_id": session_id,
            "agency_id": agency_id,
            "created_by_user_id": str(created_by_user_id or "").strip() or None,
            "created_by_username": str(created_by_username or "").strip() or None,
            "status": "initiated",
            "bucket": runtime_settings.OBJECT_STORE_BUCKET,
            "object_name": object_name,
            "upload_id": upload_id,
            "file_name": safe_file_name,
            "content_type": content_type,
            "content_bytes": int(content_bytes),
            "part_size": int(part_size),
            "job_id": job_id,
            "storage_layout_version": self.STORAGE_LAYOUT_VERSION,
            "auto_start": bool(auto_start),
            "load_to_db": bool(load_to_db),
            "resume_from_checkpoint": bool(resume_from_checkpoint),
            "resume_failed_only": bool(resume_failed_only),
        }
        try:
            self._multipart_repository().create_session(session=session)
            result = {
                "session_id": session_id,
                "agency_id": agency_id,
                "job_id": job_id,
                "bucket": runtime_settings.OBJECT_STORE_BUCKET,
                "object_name": object_name,
                "upload_id": upload_id,
                "part_size": part_size,
                "expires_in": self._multipart_presign_expires_seconds(),
            }
            self._complete_idempotent_request(
                agency_id=agency_id,
                operation="multipart_initiate",
                idempotency_key=idempotency_request_key,
                resource_type="multipart_session",
                resource_id=session_id,
                response=result,
            )
            return result
        except Exception:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="multipart_initiate",
                idempotency_key=idempotency_request_key,
            )
            raise

    def get_multipart_upload_status(self, session_id: str, *, agency_id: str) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id, agency_id=agency_id)
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

    def get_multipart_part_url(self, *, session_id: str, part_number: int, agency_id: str) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id, agency_id=agency_id)
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

    def complete_multipart_upload(
        self,
        *,
        session_id: str,
        parts: list[dict[str, Any]],
        agency_id: str,
    ) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id, agency_id=agency_id)
        if not session:
            raise ServiceError(status_code=404, detail="upload session not found")
        if session.get("status") == "completed" and session.get("job_id"):
            job = self.get_job(str(session["job_id"]), agency_id=agency_id)
            return {"job_id": job["job_id"], "agency_id": agency_id, "status": job["status"], "session_id": session_id}
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
            agency_id=agency_id,
            job_id=str(session.get("job_id") or ""),
            created_by_user_id=str(session.get("created_by_user_id") or "").strip() or None,
            created_by_username=str(session.get("created_by_username") or "").strip() or None,
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
        record_upload_bytes("multipart", int(session.get("content_bytes") or 0))
        self._multipart_repository().update_session(
            session_id=session_id,
            status="completed",
            job_id=result["job_id"],
        )
        result["session_id"] = session_id
        return result

    def abort_multipart_upload(self, *, session_id: str, agency_id: str) -> dict[str, Any]:
        session = self._multipart_repository().get_session(session_id=session_id, agency_id=agency_id)
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

    def start_job(self, job_id: str, *, agency_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
        idempotency_request_key, replay = self._claim_idempotent_request(
            agency_id=agency_id,
            operation="start_job",
            idempotency_key=idempotency_key,
            request_fingerprint=self._build_request_fingerprint({"agency_id": agency_id, "job_id": job_id}),
        )
        if replay is not None:
            return replay
        job = ingest_jobs.get_job_for_api(job_id, agency_id=agency_id)
        if not job:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="start_job",
                idempotency_key=idempotency_request_key,
            )
            raise ServiceError(status_code=404, detail="job_id not found")
        job_type = str(job.get("job_type") or "bulk_ingest").strip().lower()
        ingest_jobs.set_job(job_id, persist=False, **{k: v for k, v in job.items() if k != "job_id"})
        status = str(job.get("status") or "").lower()
        if status in {"running", "queued"}:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="start_job",
                idempotency_key=idempotency_request_key,
            )
            raise ServiceError(status_code=409, detail=f"job cannot be started from status '{status}'")
        try:
            ingest_jobs.set_job(
                job_id,
                status="queued",
                ended_at=None,
                error=None,
                resume_from_checkpoint=job_type != "retry_failed_rows",
                resume_failed_only=job_type != "retry_failed_rows",
                last_log_line="Queued to retry failed rows"
                if job_type == "retry_failed_rows"
                else "Queued to resume from checkpoint",
                load_status="pending",
            )
            self._queue_or_run(job_id)
            result = {"job_id": job_id, "agency_id": agency_id, "status": "queued"}
            self._complete_idempotent_request(
                agency_id=agency_id,
                operation="start_job",
                idempotency_key=idempotency_request_key,
                resource_type="job",
                resource_id=job_id,
                response=result,
            )
            return result
        except Exception:
            self._release_idempotent_request(
                agency_id=agency_id,
                operation="start_job",
                idempotency_key=idempotency_request_key,
            )
            raise

    def list_jobs(self, *, agency_id: str, limit: int) -> dict[str, Any]:
        rows = ingest_jobs.read_jobs_state_snapshot(agency_id=agency_id)[:limit]
        return {"count": len(rows), "items": rows}

    def get_job(self, job_id: str, *, agency_id: str) -> dict[str, Any]:
        row = ingest_jobs.get_job_for_api(job_id, agency_id=agency_id)
        if not row:
            raise ServiceError(status_code=404, detail="job_id not found")
        return row

    def _queue_or_run(self, job_id: str) -> None:
        try:
            ingest_jobs.queue_ingest_job(job_id)
        except Exception as exc:
            ingest_jobs.set_job(
                job_id,
                status="failed",
                ended_at=ingest_jobs.now_iso(),
                error=f"queue publish failed: {exc}",
                progress_stage="failed",
            )
            raise ServiceError(status_code=503, detail=f"failed to queue ingest job: {exc}") from exc

    def run_job(self, job_id: str) -> None:
        ingest_jobs.run_ingest_job(job_id)

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
            client.head_object(Bucket=bucket, Key=object_name)
            return True
        except Exception as exc:
            if is_missing_object_error(exc):
                return False
            raise
