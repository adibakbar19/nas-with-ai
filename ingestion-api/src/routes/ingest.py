"""Ingest API route handlers.

All routes delegate to IngestService. ServiceError exceptions are caught
globally by the app-level exception handler — no try/except in handlers.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, File, Form, Header, Query, Request, UploadFile

from src.auth.dependencies import Agency, get_current_agency, require_permission
from src.schemas.requests import (
    MultipartUploadCompleteRequest,
    MultipartUploadInitiateRequest,
    MultipartUploadPartUrlRequest,
)
from src.schemas.responses import (
    IngestJobListResponse,
    IngestJobResponse,
    JobActionResponse,
    MultipartAbortResponse,
    MultipartCompleteResponse,
    MultipartInitiateResponse,
    MultipartPartUrlResponse,
    MultipartSessionStatusResponse,
    RetryFailedRowsResponse,
    UploadJobResponse,
)
from src.services.ingest_service import IngestService

router = APIRouter(tags=["ingest"])


def get_ingest_service(request: Request) -> IngestService:
    return request.app.state.ingest_service


@router.post("/api/v1/ingest/upload", response_model=UploadJobResponse, response_model_exclude_none=True)
async def ingest_upload(
    file: UploadFile = File(...),
    auto_start: bool = Form(True),
    resume_from_checkpoint: bool = Form(True),
    resume_failed_only: bool = Form(True),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    file_name = file.filename or "uploaded_file"
    require_permission(agency, "ingest.upload")
    return ingest_service.upload(
        agency_id=agency.agency_id,
        created_by_user_id=None,
        created_by_username=None,
        file_name=file_name,
        file_obj=file.file,
        content_type=file.content_type,
        auto_start=auto_start,
        load_to_db=True,
        resume_from_checkpoint=resume_from_checkpoint,
        resume_failed_only=resume_failed_only,
        idempotency_key=idempotency_key,
    )


@router.post(
    "/api/v1/ingest/jobs/{job_id}/retry-failed-rows",
    response_model=RetryFailedRowsResponse,
    response_model_exclude_none=True,
)
async def retry_failed_rows_upload(
    job_id: str,
    file: UploadFile = File(...),
    auto_start: bool = Form(True),
    require_mukim: bool = Form(False),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    file_name = file.filename or "corrections.csv"
    require_permission(agency, "ingest.retry")
    return ingest_service.retry_failed_rows_upload(
        agency_id=agency.agency_id,
        created_by_user_id=None,
        created_by_username=None,
        parent_job_id=job_id,
        file_name=file_name,
        file_obj=file.file,
        content_type=file.content_type,
        auto_start=auto_start,
        load_to_db=True,
        require_mukim=require_mukim,
        idempotency_key=idempotency_key,
    )


@router.post("/api/v1/ingest/jobs/{job_id}/start", response_model=JobActionResponse, response_model_exclude_none=True)
async def start_ingest_job(
    job_id: str,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "ingest.start")
    return ingest_service.start_job(job_id, agency_id=agency.agency_id, idempotency_key=idempotency_key)


@router.get("/api/v1/ingest/jobs", response_model=IngestJobListResponse, response_model_exclude_none=True)
async def list_ingest_jobs(
    limit: int = Query(20, ge=1, le=100),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "ingest.read")
    return ingest_service.list_jobs(agency_id=agency.agency_id, limit=limit)


@router.get("/api/v1/ingest/jobs/{job_id}", response_model=IngestJobResponse, response_model_exclude_none=True)
async def get_ingest_job(
    job_id: str,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "ingest.read")
    return ingest_service.get_job(job_id, agency_id=agency.agency_id)


@router.post(
    "/api/v1/ingest/uploads/multipart/initiate",
    response_model=MultipartInitiateResponse,
    response_model_exclude_none=True,
)
async def initiate_multipart_upload(
    request_body: MultipartUploadInitiateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    return ingest_service.initiate_multipart_upload(
        agency_id=agency.agency_id,
        created_by_user_id=None,
        created_by_username=None,
        file_name=request_body.file_name,
        content_type=request_body.content_type,
        content_bytes=request_body.content_bytes,
        auto_start=request_body.auto_start,
        load_to_db=True,
        resume_from_checkpoint=request_body.resume_from_checkpoint,
        resume_failed_only=request_body.resume_failed_only,
        idempotency_key=idempotency_key,
    )


@router.get(
    "/api/v1/ingest/uploads/multipart/{session_id}",
    response_model=MultipartSessionStatusResponse,
    response_model_exclude_none=True,
)
async def get_multipart_upload_status(
    session_id: str,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.read")
    return ingest_service.get_multipart_upload_status(session_id, agency_id=agency.agency_id)


@router.post(
    "/api/v1/ingest/uploads/multipart/{session_id}/part-url",
    response_model=MultipartPartUrlResponse,
    response_model_exclude_none=True,
)
async def get_multipart_part_url(
    session_id: str,
    request_body: MultipartUploadPartUrlRequest,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    return ingest_service.get_multipart_part_url(
        session_id=session_id,
        part_number=request_body.part_number,
        agency_id=agency.agency_id,
    )


@router.post(
    "/api/v1/ingest/uploads/multipart/{session_id}/complete",
    response_model=MultipartCompleteResponse,
    response_model_exclude_none=True,
)
async def complete_multipart_upload(
    session_id: str,
    request_body: MultipartUploadCompleteRequest,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    return ingest_service.complete_multipart_upload(
        session_id=session_id,
        parts=[{"part_number": item.part_number, "etag": item.etag} for item in request_body.parts],
        agency_id=agency.agency_id,
    )


@router.post(
    "/api/v1/ingest/uploads/multipart/{session_id}/abort",
    response_model=MultipartAbortResponse,
    response_model_exclude_none=True,
)
async def abort_multipart_upload(
    session_id: str,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    return ingest_service.abort_multipart_upload(session_id=session_id, agency_id=agency.agency_id)


# ── Failed rows download ─────────────────────────────────────────────────────


def _read_failed_output(
    job: dict[str, Any],
    s3_client,
    default_bucket: str,
) -> "Any":
    """Read failed parquet files from S3, return a pandas DataFrame or None."""
    import io
    import pandas as pd

    output_prefix = job.get("output_object_prefix")
    if not output_prefix:
        return None

    bucket = str(job.get("bucket") or default_bucket)
    s3_prefix = f"{output_prefix}failed/"

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
        parquet_keys = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        if not parquet_keys:
            return None

        frames = []
        for key in parquet_keys:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
        return pd.concat(frames, ignore_index=True) if frames else None
    except Exception:
        return None


@router.get("/api/v1/ingest/jobs/{job_id}/failed-rows.csv")
async def download_failed_rows(
    job_id: str,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> Any:
    from fastapi import HTTPException
    from fastapi.responses import Response

    require_permission(agency, "ingest.read")
    job = ingest_service.get_job(job_id, agency_id=agency.agency_id)

    s3_client = ingest_service._get_s3_client()
    bucket = ingest_service._store.bucket
    df = _read_failed_output(job, s3_client, bucket)

    if df is None:
        raise HTTPException(status_code=404, detail="No failed rows available for this job")

    keep_cols = [c for c in df.columns if not c.startswith("_")]
    csv_content = df[keep_cols].to_csv(index=False)
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="failed_rows_{job_id}.csv"'},
    )
