"""Endpoints for Jobs"""
from datetime import datetime
from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Security
from sqlalchemy.orm import Session
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND

from altimeter.qj import schemas
from altimeter.qj.api import deps
from altimeter.qj.crud.crud_job import CRUDJob
from altimeter.qj.crud.crud_result_set import CRUDResultSet
from altimeter.qj.exceptions import (
    ActiveJobVersionNotFound,
    JobInvalid,
    JobNotFound,
    JobQueryInvalid,
    JobQueryMissingAccountId,
    JobVersionNotFound,
    ResultSetNotFound,
)

JOBS_ROUTER = APIRouter()


@JOBS_ROUTER.get("", response_model=List[schemas.Job])
def get_jobs(
    *,
    db_session: Session = Depends(deps.db_session),
    job_crud: CRUDJob = Depends(deps.job_crud),
    active_only: bool = True,
) -> Any:
    """Get all Jobs"""
    return job_crud.get_multi(db_session, active_only=active_only)


@JOBS_ROUTER.post(
    "",
    response_model=schemas.Job,
    status_code=HTTP_201_CREATED,
    dependencies=[Security(deps.api_key)],
)
def create_job(
    *,
    db_session: Session = Depends(deps.db_session),
    job_crud: CRUDJob = Depends(deps.job_crud),
    job_in: schemas.JobCreate,
) -> Any:
    """Create a Job"""
    try:
        return job_crud.create(db_session, job_create_in=job_in)
    except (JobQueryInvalid, JobQueryMissingAccountId, JobInvalid) as ex:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@JOBS_ROUTER.delete("/{job_name}", dependencies=[Security(deps.api_key)])
def delete_job(
    *,
    db_session: Session = Depends(deps.db_session),
    job_crud: CRUDJob = Depends(deps.job_crud),
    job_name: str,
) -> Any:
    """Delete a Job"""
    return job_crud.delete(db_session=db_session, job_name=job_name)


@JOBS_ROUTER.get("/{job_name}", response_model=schemas.Job)
def get_job(
    *,
    db_session: Session = Depends(deps.db_session),
    job_crud: CRUDJob = Depends(deps.job_crud),
    job_name: str,
) -> Any:
    """Get the currently active version of a Job"""
    try:
        return job_crud.get_active(db_session, job_name=job_name)
    except ActiveJobVersionNotFound as ex:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=str(ex)) from ex


@JOBS_ROUTER.get("/{job_name}/latest_result_set", response_model=schemas.ResultSet)
def get_job_latest_result_set(
    *,
    db_session: Session = Depends(deps.db_session),
    result_set_crud: CRUDResultSet = Depends(deps.result_set_crud),
    job_name: str,
) -> Any:
    """Get the latest result set of a Job"""
    try:
        return result_set_crud.get_latest_for_active_job(db_session, job_name=job_name)
    except ResultSetNotFound as ex:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=str(ex)) from ex


@JOBS_ROUTER.get("/{job_name}/versions", response_model=List[schemas.Job])
def get_job_versions(
    *,
    db_session: Session = Depends(deps.db_session),
    job_crud: CRUDJob = Depends(deps.job_crud),
    job_name: str,
) -> Any:
    """Get all versions of a Job"""
    try:
        return job_crud.get_versions(db_session, job_name=job_name)
    except JobNotFound as ex:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=str(ex)) from ex


@JOBS_ROUTER.get("/{job_name}/versions/{created}", response_model=schemas.Job)
def get_job_version(
    *,
    db_session: Session = Depends(deps.db_session),
    job_crud: CRUDJob = Depends(deps.job_crud),
    job_name: str,
    created: str,
) -> Any:
    """Get a specific version of a job, addressed by created time"""
    try:
        created_datetime = datetime.fromisoformat(created)
    except ValueError as v_e:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Job {job_name} version {created} not found. "
            f"It appears this is not a valid ISO 8601 datetime: {v_e}",
        ) from v_e
    try:
        return job_crud.get_version(db_session, job_name=job_name, created=created_datetime)
    except JobVersionNotFound as ex:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=str(ex)) from ex


@JOBS_ROUTER.patch(
    "/{job_name}/versions/{created}",
    response_model=schemas.Job,
    dependencies=[Security(deps.api_key)],
)
def update_job_version(
    *,
    db_session: Session = Depends(deps.db_session),
    job_crud: CRUDJob = Depends(deps.job_crud),
    job_name: str,
    created: str,
    job_update: schemas.JobUpdate,
) -> Any:
    """Update an existing Job"""
    try:
        created_datetime = datetime.fromisoformat(created)
    except ValueError as v_e:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=f"Timestamp {created} is not a valid ISO 8601 datetime: {v_e}",
        ) from v_e
    try:
        return job_crud.update_version(
            db_session, job_name=job_name, created=created_datetime, job_update=job_update
        )
    except JobVersionNotFound as ex:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(ex)) from ex
