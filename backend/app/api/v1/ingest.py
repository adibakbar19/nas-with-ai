from typing import Any

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile

from backend.app.dependencies import get_ingest_service
from backend.app.schemas.ingest import (
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
from backend.app.schemas.ingest import (
    MultipartUploadCompleteRequest,
    MultipartUploadInitiateRequest,
    MultipartUploadPartUrlRequest,
)
from backend.app.security import Agency, get_current_agency, require_permission
from backend.app.services.errors import ServiceError
from backend.app.services.ingest_service import IngestService

router = APIRouter(tags=["ingest"])

@router.post("/api/v1/ingest/upload", response_model=UploadJobResponse, response_model_exclude_none=True)
def ingest_upload(
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
    try:
        return ingest_service.upload(
            agency_id=agency.agency_id,
            created_by_user_id=agency.user_id,
            created_by_username=agency.username,
            file_name=file_name,
            file_obj=file.file,
            content_type=file.content_type,
            auto_start=auto_start,
            load_to_db=True,
            resume_from_checkpoint=resume_from_checkpoint,
            resume_failed_only=resume_failed_only,
            idempotency_key=idempotency_key,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/ingest/jobs/{job_id}/retry-failed-rows",
    response_model=RetryFailedRowsResponse,
    response_model_exclude_none=True,
)
def retry_failed_rows_upload(
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
    try:
        return ingest_service.retry_failed_rows_upload(
            agency_id=agency.agency_id,
            created_by_user_id=agency.user_id,
            created_by_username=agency.username,
            parent_job_id=job_id,
            file_name=file_name,
            file_obj=file.file,
            content_type=file.content_type,
            auto_start=auto_start,
            load_to_db=True,
            require_mukim=require_mukim,
            idempotency_key=idempotency_key,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/api/v1/ingest/jobs/{job_id}/start", response_model=JobActionResponse, response_model_exclude_none=True)
def start_ingest_job(
    job_id: str,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "ingest.start")
    try:
        return ingest_service.start_job(job_id, agency_id=agency.agency_id, idempotency_key=idempotency_key)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/api/v1/ingest/jobs", response_model=IngestJobListResponse, response_model_exclude_none=True)
def list_ingest_jobs(
    limit: int = Query(20, ge=1, le=100),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "ingest.read")
    return ingest_service.list_jobs(agency_id=agency.agency_id, limit=limit)


@router.get("/api/v1/ingest/jobs/{job_id}", response_model=IngestJobResponse, response_model_exclude_none=True)
def get_ingest_job(
    job_id: str,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "ingest.read")
    try:
        return ingest_service.get_job(job_id, agency_id=agency.agency_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/ingest/uploads/multipart/initiate",
    response_model=MultipartInitiateResponse,
    response_model_exclude_none=True,
)
def initiate_multipart_upload(
    request: MultipartUploadInitiateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    try:
        return ingest_service.initiate_multipart_upload(
            agency_id=agency.agency_id,
            created_by_user_id=agency.user_id,
            created_by_username=agency.username,
            file_name=request.file_name,
            content_type=request.content_type,
            content_bytes=request.content_bytes,
            auto_start=request.auto_start,
            load_to_db=True,
            resume_from_checkpoint=request.resume_from_checkpoint,
            resume_failed_only=request.resume_failed_only,
            idempotency_key=idempotency_key,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get(
    "/api/v1/ingest/uploads/multipart/{session_id}",
    response_model=MultipartSessionStatusResponse,
    response_model_exclude_none=True,
)
def get_multipart_upload_status(
    session_id: str,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.read")
    try:
        return ingest_service.get_multipart_upload_status(session_id, agency_id=agency.agency_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/ingest/uploads/multipart/{session_id}/part-url",
    response_model=MultipartPartUrlResponse,
    response_model_exclude_none=True,
)
def get_multipart_part_url(
    session_id: str,
    request: MultipartUploadPartUrlRequest,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    try:
        return ingest_service.get_multipart_part_url(
            session_id=session_id,
            part_number=request.part_number,
            agency_id=agency.agency_id,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/ingest/uploads/multipart/{session_id}/complete",
    response_model=MultipartCompleteResponse,
    response_model_exclude_none=True,
)
def complete_multipart_upload(
    session_id: str,
    request: MultipartUploadCompleteRequest,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    try:
        return ingest_service.complete_multipart_upload(
            session_id=session_id,
            parts=[{"part_number": item.part_number, "etag": item.etag} for item in request.parts],
            agency_id=agency.agency_id,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/ingest/uploads/multipart/{session_id}/abort",
    response_model=MultipartAbortResponse,
    response_model_exclude_none=True,
)
def abort_multipart_upload(
    session_id: str,
    agency: Agency = Depends(get_current_agency),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    require_permission(agency, "multipart.write")
    try:
        return ingest_service.abort_multipart_upload(session_id=session_id, agency_id=agency.agency_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
