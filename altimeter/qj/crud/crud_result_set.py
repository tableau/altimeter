"""CRUD for ResultSets"""
from datetime import datetime
import json
from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import extract

from altimeter.core.log import Logger
from altimeter.qj import schemas
from altimeter.qj.crud.crud_job import CRUDJob
from altimeter.qj.exceptions import (
    JobVersionNotFound,
    ResultSetNotFound,
    ResultSetResultsLimitExceeded,
    ResultSizeExceeded,
)
from altimeter.qj.log import QJLogEvents
from altimeter.qj.models.job import Job
from altimeter.qj.models.result_set import ResultSet, Result

# pylint: disable=no-self-use,too-few-public-methods
class CRUDResultSet:
    """CRUD for ResultSets"""

    def __init__(self, max_result_set_results: int, max_result_size_bytes: int, job_crud: CRUDJob):
        self._max_result_set_results = max_result_set_results
        self._max_result_size_bytes = max_result_size_bytes
        self._job_crud = job_crud

    def get(self, db_session: Session, result_set_id: str,) -> ResultSet:
        """Get a ResultSet by id"""
        logger = Logger()
        logger.info(event=QJLogEvents.GetResultSet, result_set_id=result_set_id)
        query = db_session.query(ResultSet).filter(ResultSet.result_set_id == result_set_id)
        result_sets = query.all()
        num_result_sets = len(result_sets)
        if num_result_sets:
            if num_result_sets > 1:
                raise Exception(f"More than one result_set found for {result_set_id}")
            return result_sets[0]
        raise ResultSetNotFound(f"No result set {result_set_id} found")

    def get_latest_for_active_job(self, db_session: Session, job_name: str) -> ResultSet:
        """Get the latest ResultSet for the active version of a Job"""
        logger = Logger()
        logger.info(event=QJLogEvents.GetLatestResultSetForActiveJob, job_name=job_name)
        query = (
            db_session.query(ResultSet)
            .join(Job)
            .filter(Job.name == job_name)
            .filter(Job.active)
            .order_by(ResultSet.created.desc())
        )
        result = query.first()
        if result:
            return result
        raise ResultSetNotFound(f"No result set found for an active version of {job_name}")

    def create(self, db_session: Session, obj_in: schemas.ResultSetCreate) -> ResultSet:
        """Create a ResultSet"""
        logger = Logger()
        num_results = len(obj_in.results)
        logger.info(
            event=QJLogEvents.CreateResultSet,
            job=obj_in.job,
            created=obj_in.created,
            graph_spec=obj_in.graph_spec,
            num_results=num_results,
        )
        job = self._job_crud.get_version(
            db_session=db_session, job_name=obj_in.job.name, created=obj_in.job.created
        )
        if not job:
            raise JobVersionNotFound(f"Could not find job {obj_in.job.name} / {obj_in.job.created}")

        # create result_set db object
        if num_results > self._max_result_set_results:
            raise ResultSetResultsLimitExceeded(
                f"Result set has {num_results} results, limit is {self._max_result_set_results}"
            )
        result_set = ResultSet(
            job=job, created=obj_in.created, graph_spec=json.loads(obj_in.graph_spec.json())
        )
        db_session.add(result_set)

        # create result db objects
        for obj_in_result in obj_in.results:
            result_size = len(json.dumps(obj_in_result.result))
            if result_size > self._max_result_size_bytes:
                raise ResultSizeExceeded(
                    (
                        f"Result size {result_size} exceeds max {self._max_result_size_bytes}: "
                        f"{json.dumps(obj_in_result.result)[:self._max_result_size_bytes]}..."
                    )
                )
            result = Result(
                result_set=result_set,
                account_id=obj_in_result.account_id,
                result=obj_in_result.result,
            )
            db_session.add(result)

        db_session.commit()
        db_session.refresh(result_set)
        return result_set

    def get_expired(self, db_session: Session) -> List[ResultSet]:
        """Get all expired ResultSets"""
        logger = Logger()
        query = (
            db_session.query(ResultSet)
            .join(Job)
            .filter(
                (extract("epoch", datetime.utcnow()) - extract("epoch", ResultSet.created))
                > Job.result_expiration_sec
            )
        )
        results = query.all()
        logger.info(event=QJLogEvents.GetExpiredResultSets, num_results=len(results))
        return results

    def delete_expired(self, db_session: Session) -> int:
        """Delete all expired ResultSets, return the number of deleted ResultSet"""
        logger = Logger()
        result_sets_to_delete = self.get_expired(db_session=db_session)
        logger.info(event=QJLogEvents.DeleteExpiredResultSets)
        for result_set_to_delete in result_sets_to_delete:
            db_session.delete(result_set_to_delete)
        db_session.commit()
        return len(result_sets_to_delete)
