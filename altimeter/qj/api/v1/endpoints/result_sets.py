"""Endpoints for Jobs"""
from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Security
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

RESULT_SETS_ROUTER = APIRouter()


@RESULT_SETS_ROUTER.get("/result_set/{result_set_id}", response_model=schemas.ResultSet)
def get_result_set(
    *,
    db_session: Session = Depends(deps.db_session),
    result_set_crud: CRUDResultSet = Depends(deps.result_set_crud),
    result_set_id: str,
) -> Any:
    """Get a ResultSet by id"""
    try:
        return result_set_crud.get(db_session, result_set_id=result_set_id)
    except ResultSetNotFound as ex:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=str(ex))


@RESULT_SETS_ROUTER.post(
    "/result_set",
    response_model=schemas.ResultSet,
    status_code=HTTP_201_CREATED,
    dependencies=[Security(deps.api_key)],
)
def create_result_set(
    *,
    db_session: Session = Depends(deps.db_session),
    result_set_crud: CRUDResultSet = Depends(deps.result_set_crud),
    result_set_in: schemas.ResultSetCreate,
) -> Any:
    """Create a ResultSet"""
    try:
        return result_set_crud.create(db_session, obj_in=result_set_in)
    except (JobVersionNotFound, ResultSetResultsLimitExceeded, ResultSizeExceeded) as ex:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(ex))


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
