from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile

from backend.app.dependencies import get_ingest_service
from backend.app.schemas.ingest import (
    MultipartUploadCompleteRequest,
    MultipartUploadInitiateRequest,
    MultipartUploadPartUrlRequest,
)
from backend.app.services.errors import ServiceError
from backend.app.services.ingest_service import IngestService

router = APIRouter(tags=["ingest"])

@router.post("/api/v1/ingest/upload")
def ingest_upload(
    file: UploadFile = File(...),
    auto_start: bool = Form(True),
    load_to_db: bool = Form(True),
    resume_from_checkpoint: bool = Form(True),
    resume_failed_only: bool = Form(True),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    file_name = file.filename or "uploaded_file"
    try:
        return ingest_service.upload(
            file_name=file_name,
            file_obj=file.file,
            content_type=file.content_type,
            auto_start=auto_start,
            load_to_db=load_to_db,
            resume_from_checkpoint=resume_from_checkpoint,
            resume_failed_only=resume_failed_only,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/api/v1/ingest/jobs/{job_id}/start")
def start_ingest_job(job_id: str, ingest_service: IngestService = Depends(get_ingest_service)) -> dict[str, Any]:
    try:
        return ingest_service.start_job(job_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/api/v1/ingest/jobs/{job_id}/pause")
def pause_ingest_job(job_id: str, ingest_service: IngestService = Depends(get_ingest_service)) -> dict[str, Any]:
    try:
        return ingest_service.pause_job(job_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/api/v1/ingest/jobs")
def list_ingest_jobs(
    limit: int = Query(20, ge=1, le=100),
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    return ingest_service.list_jobs(limit=limit)


@router.get("/api/v1/ingest/jobs/{job_id}")
def get_ingest_job(job_id: str, ingest_service: IngestService = Depends(get_ingest_service)) -> dict[str, Any]:
    try:
        return ingest_service.get_job(job_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/api/v1/ingest/uploads/multipart/initiate")
def initiate_multipart_upload(
    request: MultipartUploadInitiateRequest,
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    try:
        return ingest_service.initiate_multipart_upload(
            file_name=request.file_name,
            content_type=request.content_type,
            content_bytes=request.content_bytes,
            auto_start=request.auto_start,
            load_to_db=request.load_to_db,
            resume_from_checkpoint=request.resume_from_checkpoint,
            resume_failed_only=request.resume_failed_only,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/api/v1/ingest/uploads/multipart/{session_id}")
def get_multipart_upload_status(
    session_id: str,
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    try:
        return ingest_service.get_multipart_upload_status(session_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/api/v1/ingest/uploads/multipart/{session_id}/part-url")
def get_multipart_part_url(
    session_id: str,
    request: MultipartUploadPartUrlRequest,
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    try:
        return ingest_service.get_multipart_part_url(session_id=session_id, part_number=request.part_number)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/api/v1/ingest/uploads/multipart/{session_id}/complete")
def complete_multipart_upload(
    session_id: str,
    request: MultipartUploadCompleteRequest,
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    try:
        return ingest_service.complete_multipart_upload(
            session_id=session_id,
            parts=[{"part_number": item.part_number, "etag": item.etag} for item in request.parts],
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/api/v1/ingest/uploads/multipart/{session_id}/abort")
def abort_multipart_upload(
    session_id: str,
    ingest_service: IngestService = Depends(get_ingest_service),
) -> dict[str, Any]:
    try:
        return ingest_service.abort_multipart_upload(session_id=session_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
