"""Endpoints for Jobs"""
from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Security, Response
from sqlalchemy.orm import Session
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND

from altimeter.qj import schemas
from altimeter.qj.api import deps
from altimeter.qj.crud.crud_result_set import CRUDResultSet
from altimeter.qj.exceptions import (
    JobVersionNotFound,
    ResultSetNotFound,
    ResultSetResultsLimitExceeded,
    ResultSizeExceeded,
)
from altimeter.qj.notifier import ResultSetNotifier

RESULT_SETS_ROUTER = APIRouter()


@RESULT_SETS_ROUTER.get("/result_set/{result_set_id}", response_model=schemas.ResultSet)
def get_result_set(
    *,
    db_session: Session = Depends(deps.db_session),
    result_set_crud: CRUDResultSet = Depends(deps.result_set_crud),
    result_set_id: str,
    result_format: schemas.ResultSetFormat = schemas.ResultSetFormat.json,
    response: Response,
) -> Any:
    """Get a ResultSet by id"""
    try:
        result_set = result_set_crud.get(db_session, result_set_id=result_set_id)
        response.headers["Cache-Control"] = "public, max-age=604800, immutable"
    except ResultSetNotFound as ex:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=str(ex)) from ex
    if result_format == schemas.ResultSetFormat.csv:
        return Response(content=result_set.to_api_schema().to_csv(), media_type="text/csv")
    return result_set


@RESULT_SETS_ROUTER.post(
    "/result_set",
    response_model=schemas.ResultSet,
    status_code=HTTP_201_CREATED,
    dependencies=[Security(deps.api_key)],
)
def create_result_set(
    *,
    db_session: Session = Depends(deps.db_session),
    result_set_notifier: ResultSetNotifier = Depends(deps.result_set_notifier),
    result_set_crud: CRUDResultSet = Depends(deps.result_set_crud),
    result_set_in: schemas.ResultSetCreate,
) -> Any:
    """Create a ResultSet"""
    try:
        result_set = result_set_crud.create(db_session, obj_in=result_set_in)
        if result_set.results:
            if result_set_in.job.notify_if_results:
                result_set_notification = schemas.ResultSetNotification(
                    job=result_set_in.job,
                    graph_spec=result_set_in.graph_spec,
                    created=result_set_in.created,
                    num_results=len(result_set_in.results),
                    result_set_id=str(result_set.result_set_id),
                )
                result_set_notifier.notify(notification=result_set_notification)
        return result_set
    except (JobVersionNotFound, ResultSetResultsLimitExceeded, ResultSizeExceeded) as ex:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@RESULT_SETS_ROUTER.get(
    "/expired", response_model=List[schemas.ResultSet], dependencies=[Security(deps.api_key)],
)
def get_expired_result_sets(
    *,
    db_session: Session = Depends(deps.db_session),
    result_set_crud: CRUDResultSet = Depends(deps.result_set_crud),
) -> Any:
    """Get all expired ResultSets"""
    return result_set_crud.get_expired(db_session=db_session)


@RESULT_SETS_ROUTER.delete(
    "/expired", response_model=schemas.ResultSetsPruneResult, dependencies=[Security(deps.api_key)],
)
def delete_expired_result_sets(
    *,
    db_session: Session = Depends(deps.db_session),
    result_set_crud: CRUDResultSet = Depends(deps.result_set_crud),
) -> Any:
    """Delete all expired ResultSets"""
    num_pruned = result_set_crud.delete_expired(db_session=db_session)
    return schemas.ResultSetsPruneResult(num_pruned=num_pruned)
