"""CRUD for Jobs"""
from datetime import datetime
from typing import List

import rdflib
from sqlalchemy.orm import Session

from altimeter.core.log import Logger
from altimeter.qj import schemas
from altimeter.qj.exceptions import (
    ActiveJobVersionNotFound,
    JobInvalid,
    JobNotFound,
    JobQueryInvalid,
    JobQueryMissingAccountId,
    JobVersionNotFound,
)
from altimeter.qj.log import QJLogEvents
from altimeter.qj.models.job import Job


# pylint: disable=no-self-use,too-few-public-methods
class CRUDJob:
    """CRUD for Jobs"""

    def __init__(
        self,
        db_ro_user: str,
        result_expiration_sec_default: int,
        result_expiration_sec_limit: int,
        max_graph_age_sec_default: int,
        max_graph_age_sec_limit: int,
        max_result_age_sec_default: int,
        max_result_age_sec_limit: int,
        account_id_key: str,
    ):
        self._db_ro_user = db_ro_user
        self._result_expiration_sec_default = result_expiration_sec_default
        self._result_expiration_sec_limit = result_expiration_sec_limit
        self._max_graph_age_sec_default = max_graph_age_sec_default
        self._max_graph_age_sec_limit = max_graph_age_sec_limit
        self._max_result_age_sec_default = max_result_age_sec_default
        self._max_result_age_sec_limit = max_result_age_sec_limit
        self._account_id_key = account_id_key

    def get_active(self, db_session: Session, job_name: str,) -> Job:
        """Get the active version of a Job"""
        logger = Logger()
        query = db_session.query(Job).filter(Job.active).filter(Job.name == job_name)
        results = query.all()
        num_results = len(results)
        logger.info(event=QJLogEvents.GetActiveJob, job_name=job_name, num_results=num_results)
        if num_results:
            assert num_results == 1, f"More than one active job found for {job_name}"
            return results[0]
        raise ActiveJobVersionNotFound(f"No active job version found for {job_name}")

    def get_multi(self, db_session: Session, active_only: bool) -> List[Job]:
        """Get all Jobs, optionally only active"""
        logger = Logger()
        query = db_session.query(Job)
        if active_only:
            query = query.filter(Job.active)
        query = query.order_by(Job.name).order_by(Job.created)
        results = query.all()
        logger.info(event=QJLogEvents.GetJobs, active_only=active_only, num_results=len(results))
        return results

    def create(self, db_session: Session, job_create_in: schemas.JobCreate) -> Job:
        """Create a Job"""
        logger = Logger()
        logger.info(event=QJLogEvents.CreateJob, job_create=job_create_in)
        try:
            query = rdflib.Graph().query(job_create_in.query)
        except Exception as ex:
            raise JobQueryInvalid(f"Invalid query {job_create_in.query}: {str(ex)}")
        query_fields = [str(query_var) for query_var in query.vars]
        if self._account_id_key not in query_fields:
            raise JobQueryMissingAccountId(
                f"Query {job_create_in.query} missing '{self._account_id_key}' field"
            )
        if job_create_in.result_expiration_sec is None:
            job_create_in.result_expiration_sec = self._result_expiration_sec_default
        if job_create_in.result_expiration_sec > self._result_expiration_sec_limit:
            raise JobInvalid(
                f"Field result_expiration_sec value {job_create_in.result_expiration_sec} "
                f"must be <= {self._result_expiration_sec_limit}"
            )
        if job_create_in.max_graph_age_sec is None:
            job_create_in.max_graph_age_sec = self._max_graph_age_sec_default
        else:
            if job_create_in.max_graph_age_sec > self._max_graph_age_sec_limit:
                raise JobInvalid(
                    f"Field max_graph_age_sec value {job_create_in.max_graph_age_sec} must be "
                    f"<= {self._max_graph_age_sec_limit}"
                )
        if job_create_in.max_result_age_sec is None:
            job_create_in.max_result_age_sec = self._max_result_age_sec_default
        else:
            if job_create_in.max_result_age_sec > self._max_result_age_sec_limit:
                raise JobInvalid(
                    f"Field max_result_age_sec value {job_create_in.max_result_age_sec} must be "
                    f"<= {self._max_result_age_sec_limit}"
                )
        obj_in_data = job_create_in.dict()
        obj_in_data["query_fields"] = query_fields
        job_create = schemas.Job(**obj_in_data)
        db_obj = Job(**job_create.dict())  # type: ignore
        db_session.add(db_obj)
        db_session.commit()
        db_session.refresh(db_obj)
        return db_obj

    def get_versions(self, db_session: Session, job_name: str) -> List[Job]:
        """Get all versions of a Job"""
        logger = Logger()
        query = db_session.query(Job).filter(Job.name == job_name).order_by(Job.created)
        results = query.all()
        logger.info(event=QJLogEvents.GetJobVersions, job_name=job_name, num_results=len(results))
        if results:
            return results
        raise JobNotFound(f"Job '{job_name}' not found")

    def get_version(self, db_session: Session, job_name: str, created: datetime) -> Job:
        """Get a specific version of a Job by created timestamp"""
        logger = Logger()
        logger.info(event=QJLogEvents.GetJobVersion, job_name=job_name)
        query = db_session.query(Job).filter(Job.name == job_name).filter(Job.created == created)
        results = query.all()
        num_results = len(results)
        if num_results:
            if num_results > 1:
                raise Exception(f"More than one job found for {job_name} with version {created}")
            return results[0]
        raise JobVersionNotFound(f"Could not find job {job_name} / {created}")

    def update_version(
        self, db_session: Session, job_name: str, created: datetime, job_update: schemas.JobUpdate,
    ) -> Job:
        """Update a Job"""
        logger = Logger()
        logger.info(
            QJLogEvents.UpdateJob, job_name=job_name, created=created, job_update=job_update
        )
        job_version = self.get_version(db_session=db_session, job_name=job_name, created=created)
        if job_update.description is not None:
            job_version.description = job_update.description
        if job_update.category is not None:
            job_version.category = job_update.category
        if job_update.result_expiration_sec is not None:
            job_version.result_expiration_sec = job_update.result_expiration_sec
        if job_update.max_graph_age_sec is not None:
            job_version.max_graph_age_sec = job_update.max_graph_age_sec
        if job_update.max_result_age_sec is not None:
            job_version.max_result_age_sec = job_update.max_result_age_sec
        if job_update.active is not None:
            if job_update.active:
                query = db_session.query(Job).filter(Job.name == job_name)
                job_versions = query.all()
                for _job_version in job_versions:
                    if _job_version != job_version:
                        _job_version.active = False
            job_version.active = job_update.active
            if job_version.active:
                self._create_views(db_session=db_session, job_version=job_version)
        db_session.commit()
        db_session.refresh(job_version)
        return job_version

    def delete(self, db_session: Session, job_name: str) -> None:
        """Delete a Job"""
        logger = Logger()
        logger.info(event=QJLogEvents.DeleteJob, job_name=job_name)
        db_session.query(Job).filter(Job.name == job_name).delete()
        latest_view_name = self._get_latest_view_name(job_name=job_name)
        self._drop_view(db_session=db_session, view_name=latest_view_name)
        all_view_name = self._get_all_view_name(job_name=job_name)
        self._drop_view(db_session=db_session, view_name=all_view_name)
        db_session.commit()

    def _get_latest_view_name(self, job_name: str) -> str:
        """Return the name of the 'latest' view"""
        return f"{job_name}_latest"

    def _get_all_view_name(self, job_name: str) -> str:
        """Return the name of the 'all' view"""
        return f"{job_name}_all"

    def _drop_view(self, db_session: Session, view_name: str) -> None:
        """Drop a view by name"""
        logger = Logger()
        logger.info(event=QJLogEvents.DropView, view_name=view_name)
        drop_sql = f"DROP VIEW IF EXISTS {view_name};"
        db_session.execute(drop_sql)

    def _create_views(self, db_session: Session, job_version: Job) -> None:
        """Create all views (_latest and _all) for a Job"""
        self._create_latest_view(db_session=db_session, job_version=job_version)
        self._create_all_view(db_session=db_session, job_version=job_version)

    def _create_latest_view(self, db_session: Session, job_version: Job) -> None:
        """Create the _latest view for a Job"""
        logger = Logger()
        latest_view_name = self._get_latest_view_name(job_name=job_version.name)
        self._drop_view(db_session=db_session, view_name=latest_view_name)
        create_sql = (
            f"CREATE VIEW {latest_view_name} AS\n"
            f"SELECT result_created, {', '.join(job_version.query_fields)}\n"
            f"FROM\n"
            "(\n"
            f"    SELECT\n"
            "        rs.created as result_created,\n"
            f"        lpad(r.account_id::text, 12, '0') as {self._account_id_key},\n"
        )
        for query_field in job_version.query_fields:
            if query_field != self._account_id_key:
                create_sql += f"        result->>'{query_field}' as {query_field},\n"
        create_sql += (
            f"    RANK () OVER (PARTITION BY r.account_id ORDER BY rs.created DESC) as rank_number\n"
            "    FROM\n"
            "        result r\n"
            "    INNER JOIN result_set rs ON r.result_set_id = rs.id\n"
            "    INNER JOIN job j ON rs.job_id = j.id\n"
            "    WHERE\n"
            f"        j.name = '{job_version.name}' AND\n"
            "        j.active = true AND\n"
            f"        rs.created > CURRENT_TIMESTAMP - INTERVAL '{job_version.max_result_age_sec} seconds'\n"
            ") ranked_query\n"
            "WHERE rank_number = 1\n"
            f"ORDER BY {self._account_id_key};\n"
        )
        logger.info(event=QJLogEvents.CreateView, view_name=latest_view_name)
        db_session.execute(create_sql)
        grant_sql = f"GRANT SELECT ON {latest_view_name} TO {self._db_ro_user};\n"
        db_session.execute(grant_sql)

    def _create_all_view(self, db_session: Session, job_version: Job) -> None:
        """Create the _all view for a Job"""
        logger = Logger()
        all_view_name = self._get_all_view_name(job_name=job_version.name)
        self._drop_view(db_session=db_session, view_name=all_view_name)
        create_sql = (
            f"CREATE VIEW {all_view_name} AS\n"
            "SELECT\n"
            "    rs.created as result_created,\n"
            f"    lpad(r.account_id::text, 12, '0') as {self._account_id_key},\n"
        )
        query_field_stmts = []
        for query_field in job_version.query_fields:
            if query_field != self._account_id_key:
                query_field_stmts.append(f"    result->>'{query_field}' as {query_field}")
        create_sql += ",\n".join(query_field_stmts) + "\n"
        create_sql += (
            "FROM\n"
            "    result r\n"
            "INNER JOIN result_set rs ON r.result_set_id = rs.id\n"
            "INNER JOIN job j ON rs.job_id = j.id\n"
            "WHERE\n"
            f"    j.name = '{job_version.name}' AND\n"
            "    j.active = true\n"
            f"ORDER BY {self._account_id_key};\n"
        )
        logger.info(event=QJLogEvents.CreateView, view_name=all_view_name)
        db_session.execute(create_sql)
        grant_sql = f"GRANT SELECT ON {all_view_name} TO {self._db_ro_user};\n"
        db_session.execute(grant_sql)
