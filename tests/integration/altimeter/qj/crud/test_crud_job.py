from datetime import datetime, timedelta
import time
import unittest

from altimeter.qj import schemas
from altimeter.qj.crud.crud_job import CRUDJob
from altimeter.qj.crud.crud_result_set import CRUDResultSet
from altimeter.qj.exceptions import (
    ActiveJobVersionNotFound,
    JobInvalid,
    JobNotFound,
    JobQueryInvalid,
    JobQueryMissingAccountId,
    JobVersionNotFound,
)
from altimeter.qj.schemas import ResultSetCreate

from tests.dbutil import temp_db_session


class TestGetActiveJob(unittest.TestCase):
    def test_get_active_job(self):
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            # job which will be activated
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["1", "2"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?s ?p ?test_account_id where {?s ?p ?test_account_id} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp = job_crud.create(
                db_session=session, job_create_in=job_create
            ).created

            # dummy inactive job
            another_job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["a", "b"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?q ?r ?test_account_id where {?q ?r ?test_account_id} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            job_crud.create(db_session=session, job_create_in=another_job_create).created

            # activate
            job_update = schemas.JobUpdate(active=True)
            _activated_job = job_crud.update_version(
                db_session=session,
                job_name="test_job",
                created=created_timestamp,
                job_update=job_update,
            )
            activated_job = schemas.Job.from_orm(_activated_job)

            _job = job_crud.get_active(session, "test_job")
            job = schemas.Job.from_orm(_job)
            self.assertEqual(activated_job, job)

    def test_get_active_job_with_no_active_job(self):
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["1", "2"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?s ?p ?test_account_id where {?s ?p ?test_account_id} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            job_crud.create(db_session=session, job_create_in=job_create)
            with self.assertRaises(ActiveJobVersionNotFound):
                job_crud.get_active(session, "test_job")


class TestGetJobs(unittest.TestCase):
    def test(self):
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create_1 = schemas.JobCreate(
                name="test_job_1",
                description="A Test Job 1",
                graph_spec=schemas.JobGraphSpec(graph_names=["1", "2"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?s ?p ?test_account_id where {?s ?p ?test_account_id} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp_1 = job_crud.create(
                db_session=session, job_create_in=job_create_1
            ).created

            job_create_2 = schemas.JobCreate(
                name="test_job_2",
                description="A Test Job 2",
                graph_spec=schemas.JobGraphSpec(graph_names=["a", "b"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?q ?r ?test_account_id where {?q ?r ?test_account_id} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp_2 = job_crud.create(
                db_session=session, job_create_in=job_create_2
            ).created

            job_create_3 = schemas.JobCreate(
                name="test_job_3",
                description="A Test Job 3",
                graph_spec=schemas.JobGraphSpec(graph_names=["a", "b"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?q ?r ?test_account_id where {?q ?r ?test_account_id} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            _job_3 = job_crud.create(db_session=session, job_create_in=job_create_3)
            job_3 = schemas.Job.from_orm(_job_3)

            # activate 1, 2
            job_update = schemas.JobUpdate(active=True)
            _activated_job_1 = job_crud.update_version(
                db_session=session,
                job_name="test_job_1",
                created=created_timestamp_1,
                job_update=job_update,
            )
            activated_job_1 = schemas.Job.from_orm(_activated_job_1)
            _activated_job_2 = job_crud.update_version(
                db_session=session,
                job_name="test_job_2",
                created=created_timestamp_2,
                job_update=job_update,
            )
            activated_job_2 = schemas.Job.from_orm(_activated_job_2)

            _active_only_jobs = job_crud.get_multi(db_session=session, active_only=True)
            active_only_jobs = [schemas.Job.from_orm(job) for job in _active_only_jobs]
            self.assertEqual(active_only_jobs, [activated_job_1, activated_job_2])

            _all_jobs = job_crud.get_multi(db_session=session, active_only=False)
            all_jobs = [schemas.Job.from_orm(job) for job in _all_jobs]
            self.assertEqual(all_jobs, [activated_job_1, activated_job_2, job_3])


class TestCreate(unittest.TestCase):
    def test_invalid_query(self):
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job With A Bad Query",
                graph_spec=schemas.JobGraphSpec(graph_names=["1", "2"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select s p test_account_id where {?s ?p ?test_account_id} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            with self.assertRaises(JobQueryInvalid):
                job_crud.create(db_session=session, job_create_in=job_create)

    def test_query_missing_account_id_field(self):
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job With A Bad Query",
                graph_spec=schemas.JobGraphSpec(graph_names=["1", "2"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?s ?p ?foo where {?s ?p ?foo} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            with self.assertRaises(JobQueryMissingAccountId):
                job_crud.create(db_session=session, job_create_in=job_create)


class TestViews(unittest.TestCase):
    def test_activate_job_creates_views(self):
        """Test that activating a job via CRUDJob.update will create sql views and validate
        some basic information schema data about them"""
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            # job which will be activated
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["1", "2"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?test_account_id ?foo ?boo where {?test_account_id ?foo ?boo} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp = job_crud.create(
                db_session=session, job_create_in=job_create
            ).created
            # activate
            job_update = schemas.JobUpdate(active=True)
            job_crud.update_version(
                db_session=session,
                job_name="test_job",
                created=created_timestamp,
                job_update=job_update,
            )
            # check latest
            latest_results = session.execute(
                "select * from information_schema.views where table_name='test_job_latest';"
            )
            self.assertEqual(latest_results.rowcount, 1)
            latest_row = next(latest_results)
            self.assertEqual(latest_row["table_catalog"], "qj")
            self.assertEqual(latest_row["table_schema"], "public")
            self.assertEqual(latest_row["table_name"], "test_job_latest")
            self.assertEqual(latest_row["check_option"], "NONE")
            self.assertEqual("NO", latest_row["is_updatable"])
            self.assertEqual("NO", latest_row["is_insertable_into"])
            self.assertEqual("NO", latest_row["is_trigger_updatable"])
            self.assertEqual("NO", latest_row["is_trigger_deletable"])
            self.assertEqual("NO", latest_row["is_trigger_insertable_into"])
            # check all
            all_results = session.execute(
                "select * from information_schema.views where table_name='test_job_all';"
            )
            self.assertEqual(all_results.rowcount, 1)
            all_row = next(all_results)
            self.assertEqual(all_row["table_catalog"], "qj")
            self.assertEqual(all_row["table_schema"], "public")
            self.assertEqual(all_row["table_name"], "test_job_all")
            self.assertEqual(all_row["check_option"], "NONE")
            self.assertEqual("NO", all_row["is_updatable"])
            self.assertEqual("NO", all_row["is_insertable_into"])
            self.assertEqual("NO", all_row["is_trigger_updatable"])
            self.assertEqual("NO", all_row["is_trigger_deletable"])
            self.assertEqual("NO", all_row["is_trigger_insertable_into"])

    def test_views_with_two_fresh_result_sets_both_complete(self):
        """Add two result sets for data from two accounts, both of which are within
        max_result_age_sec and have the same accounts. Run a query against the latest view to
        validate we get data for both accounts from the correct result sets (using created time).
        Test that we get everything in the all view."""
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        result_set_crud = CRUDResultSet(
            max_result_set_results=int(1e6), max_result_size_bytes=int(1e6), job_crud=job_crud,
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["test"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?test_account_id ?foo ?boo where {?test_account_id ?foo ?boo} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp = job_crud.create(
                db_session=session, job_create_in=job_create
            ).created
            # activate
            job_update = schemas.JobUpdate(active=True)
            _job = job_crud.update_version(
                db_session=session,
                job_name="test_job",
                created=created_timestamp,
                job_update=job_update,
            )
            job = schemas.Job.from_orm(_job)

            account_id_a = "012345678901"
            account_id_b = "567890123456"

            result_set_1_time = datetime.now() - timedelta(hours=2)
            result_set_1_graph_spec = schemas.ResultSetGraphSpec(
                graph_uris_load_times={"test": result_set_1_time.timestamp()}
            )
            results_1 = [
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldhello_a", "boo": "oldthere_a"},
                ),
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldboo_a", "boo": "oldfoo_a"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldhello_b", "boo": "oldthere_b"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldboo_b", "boo": "oldfoo_b"},
                ),
            ]
            result_set_1_create = ResultSetCreate(
                job=job,
                graph_spec=result_set_1_graph_spec,
                results=results_1,
                created=result_set_1_time,
            )
            result_set_crud.create(db_session=session, obj_in=result_set_1_create)

            result_set_2_time = datetime.now()
            result_set_2_graph_spec = schemas.ResultSetGraphSpec(
                graph_uris_load_times={"test": result_set_2_time.timestamp()}
            )
            results_2 = [
                schemas.Result(
                    account_id=account_id_a, result={"foo": "newhello_a", "boo": "newthere_a"},
                ),
                schemas.Result(
                    account_id=account_id_a, result={"foo": "newboo_a", "boo": "newfoo_a"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "newhello_b", "boo": "newthere_b"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "newboo_b", "boo": "newfoo_b"},
                ),
            ]
            result_set_2_create = ResultSetCreate(
                job=job,
                graph_spec=result_set_2_graph_spec,
                results=results_2,
                created=result_set_2_time,
            )
            result_set_crud.create(db_session=session, obj_in=result_set_2_create)

            # check latest results
            latest_results = session.execute("select * from test_job_latest")
            self.assertEqual(latest_results.rowcount, 4)
            latest_rows = latest_results.fetchall()
            self.assertSequenceEqual(
                sorted(latest_rows),
                sorted(
                    [
                        (result_set_2_time, account_id_a, "newhello_a", "newthere_a"),
                        (result_set_2_time, account_id_a, "newboo_a", "newfoo_a"),
                        (result_set_2_time, account_id_b, "newhello_b", "newthere_b"),
                        (result_set_2_time, account_id_b, "newboo_b", "newfoo_b"),
                    ]
                ),
            )

            # check all results
            all_results = session.execute("select * from test_job_all")
            self.assertEqual(all_results.rowcount, 8)
            all_rows = all_results.fetchall()
            self.assertSequenceEqual(
                sorted(all_rows),
                sorted(
                    [
                        (result_set_1_time, account_id_a, "oldhello_a", "oldthere_a"),
                        (result_set_1_time, account_id_a, "oldboo_a", "oldfoo_a"),
                        (result_set_1_time, account_id_b, "oldhello_b", "oldthere_b"),
                        (result_set_1_time, account_id_b, "oldboo_b", "oldfoo_b"),
                        (result_set_2_time, account_id_a, "newhello_a", "newthere_a"),
                        (result_set_2_time, account_id_a, "newboo_a", "newfoo_a"),
                        (result_set_2_time, account_id_b, "newhello_b", "newthere_b"),
                        (result_set_2_time, account_id_b, "newboo_b", "newfoo_b"),
                    ]
                ),
            )

    def test_views_with_two_fresh_result_sets_one_missing_an_account(self):
        """Add two result sets, both of which are within max_result_age_sec, the newer of which is
        missing one account.  Run a query against the latest view to validate we get data for
        both rows from the correct result sets (using created time)"""
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        result_set_crud = CRUDResultSet(
            max_result_set_results=int(1e6), max_result_size_bytes=int(1e6), job_crud=job_crud,
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["test"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?test_account_id ?foo ?boo where {?test_account_id ?foo ?boo} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp = job_crud.create(
                db_session=session, job_create_in=job_create
            ).created
            # activate
            job_update = schemas.JobUpdate(active=True)
            _job = job_crud.update_version(
                db_session=session,
                job_name="test_job",
                created=created_timestamp,
                job_update=job_update,
            )
            job = schemas.Job.from_orm(_job)

            account_id_a = "012345678901"
            account_id_b = "567890123456"

            result_set_1_time = datetime.now() - timedelta(hours=2)
            result_set_1_graph_spec = schemas.ResultSetGraphSpec(
                graph_uris_load_times={"test": result_set_1_time.timestamp()}
            )
            results_1 = [
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldhello_a", "boo": "oldthere_a"},
                ),
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldboo_a", "boo": "oldfoo_a"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldhello_b", "boo": "oldthere_b"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldboo_b", "boo": "oldfoo_b"},
                ),
            ]
            result_set_1_create = ResultSetCreate(
                job=job,
                graph_spec=result_set_1_graph_spec,
                results=results_1,
                created=result_set_1_time,
            )
            result_set_crud.create(db_session=session, obj_in=result_set_1_create)

            result_set_2_time = datetime.now()
            result_set_2_graph_spec = schemas.ResultSetGraphSpec(
                graph_uris_load_times={"test": result_set_2_time.timestamp()}
            )
            results_2 = [
                schemas.Result(
                    account_id=account_id_a, result={"foo": "newhello_a", "boo": "newthere_a"},
                ),
                schemas.Result(
                    account_id=account_id_a, result={"foo": "newboo_a", "boo": "newfoo_a"},
                ),
            ]
            result_set_2_create = ResultSetCreate(
                job=job,
                graph_spec=result_set_2_graph_spec,
                results=results_2,
                created=result_set_2_time,
            )
            result_set_crud.create(db_session=session, obj_in=result_set_2_create)

            # check latest results
            latest_results = session.execute("select * from test_job_latest")
            latest_rows = latest_results.fetchall()
            self.assertSequenceEqual(
                sorted(latest_rows),
                sorted(
                    [
                        (result_set_2_time, account_id_a, "newhello_a", "newthere_a"),
                        (result_set_2_time, account_id_a, "newboo_a", "newfoo_a"),
                        (result_set_1_time, account_id_b, "oldhello_b", "oldthere_b"),
                        (result_set_1_time, account_id_b, "oldboo_b", "oldfoo_b"),
                    ]
                ),
            )
            # check all results
            all_results = session.execute("select * from test_job_all")
            all_rows = all_results.fetchall()
            self.assertSequenceEqual(
                sorted(all_rows),
                sorted(
                    [
                        (result_set_1_time, account_id_a, "oldhello_a", "oldthere_a"),
                        (result_set_1_time, account_id_a, "oldboo_a", "oldfoo_a"),
                        (result_set_1_time, account_id_b, "oldhello_b", "oldthere_b"),
                        (result_set_1_time, account_id_b, "oldboo_b", "oldfoo_b"),
                        (result_set_2_time, account_id_a, "newhello_a", "newthere_a"),
                        (result_set_2_time, account_id_a, "newboo_a", "newfoo_a"),
                    ]
                ),
            )

    def test_views_with_expired_result_set(self):
        """Add a single result set which is older than max_result_age_sec. Validate the latest
        view returns no results, also validate all_view"""
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        result_set_crud = CRUDResultSet(
            max_result_set_results=int(1e6), max_result_size_bytes=int(1e6), job_crud=job_crud,
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["test"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?test_account_id ?foo ?boo where {?test_account_id ?foo ?boo} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp = job_crud.create(
                db_session=session, job_create_in=job_create
            ).created
            # activate
            job_update = schemas.JobUpdate(active=True)
            _job = job_crud.update_version(
                db_session=session,
                job_name="test_job",
                created=created_timestamp,
                job_update=job_update,
            )
            job = schemas.Job.from_orm(_job)

            account_id_a = "012345678901"
            account_id_b = "567890123456"

            result_set_1_time = datetime.now() - timedelta(
                seconds=job_create.max_result_age_sec + 1
            )
            result_set_1_graph_spec = schemas.ResultSetGraphSpec(
                graph_uris_load_times={"test": result_set_1_time.timestamp()}
            )
            results_1 = [
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldhello_a", "boo": "oldthere_a"},
                ),
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldboo_a", "boo": "oldfoo_a"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldhello_b", "boo": "oldthere_b"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldboo_b", "boo": "oldfoo_b"},
                ),
            ]
            result_set_1_create = ResultSetCreate(
                job=job,
                graph_spec=result_set_1_graph_spec,
                results=results_1,
                created=result_set_1_time,
            )
            result_set_crud.create(db_session=session, obj_in=result_set_1_create)

            # check latest results
            latest_results = session.execute("select * from test_job_latest")
            self.assertEqual(latest_results.rowcount, 0)
            # check all results
            all_results = session.execute("select * from test_job_all")
            all_rows = all_results.fetchall()
            self.assertSequenceEqual(
                sorted(all_rows),
                sorted(
                    [
                        (result_set_1_time, account_id_a, "oldhello_a", "oldthere_a"),
                        (result_set_1_time, account_id_a, "oldboo_a", "oldfoo_a"),
                        (result_set_1_time, account_id_b, "oldhello_b", "oldthere_b"),
                        (result_set_1_time, account_id_b, "oldboo_b", "oldfoo_b"),
                    ]
                ),
            )

    def test_views_with_two_fresh_result_sets_one_missing_an_account_other_expired(self):
        """Add two result sets, one of which are within max_result_age_sec and the other is expired.
        The newer is missing one account.  Run a query against the latest view to validate we get
        data for only one account (from the unexpired set)"""
        db_ro_user = "test_ro"
        job_crud = CRUDJob(
            db_ro_user=db_ro_user,
            result_expiration_sec_default=int(1e6),
            result_expiration_sec_limit=int(1e6),
            max_graph_age_sec_default=int(1e6),
            max_graph_age_sec_limit=int(1e6),
            max_result_age_sec_default=int(1e6),
            max_result_age_sec_limit=int(1e6),
            account_id_key="test_account_id",
        )
        result_set_crud = CRUDResultSet(
            max_result_set_results=int(1e6), max_result_size_bytes=int(1e6), job_crud=job_crud,
        )
        with temp_db_session() as session:
            session.execute(f"CREATE ROLE {db_ro_user}")
            job_create = schemas.JobCreate(
                name="test_job",
                description="A Test Job",
                graph_spec=schemas.JobGraphSpec(graph_names=["test"]),
                category=schemas.Category.gov,
                severity=schemas.Severity.info,
                query="select ?test_account_id ?foo ?boo where {?test_account_id ?foo ?boo} limit 10",
                max_graph_age_sec=int(1e6),
                result_expiration_sec=int(1e6),
                max_result_age_sec=int(1e6),
            )
            created_timestamp = job_crud.create(
                db_session=session, job_create_in=job_create
            ).created
            # activate
            job_update = schemas.JobUpdate(active=True)
            _job = job_crud.update_version(
                db_session=session,
                job_name="test_job",
                created=created_timestamp,
                job_update=job_update,
            )
            job = schemas.Job.from_orm(_job)

            account_id_a = "012345678901"
            account_id_b = "567890123456"

            result_set_1_time = datetime.now() - timedelta(
                seconds=job_create.max_result_age_sec + 1
            )
            result_set_1_graph_spec = schemas.ResultSetGraphSpec(
                graph_uris_load_times={"test": result_set_1_time.timestamp()}
            )
            results_1 = [
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldhello_a", "boo": "oldthere_a"},
                ),
                schemas.Result(
                    account_id=account_id_a, result={"foo": "oldboo_a", "boo": "oldfoo_a"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldhello_b", "boo": "oldthere_b"},
                ),
                schemas.Result(
                    account_id=account_id_b, result={"foo": "oldboo_b", "boo": "oldfoo_b"},
                ),
            ]
            result_set_1_create = ResultSetCreate(
                job=job,
                graph_spec=result_set_1_graph_spec,
                results=results_1,
                created=result_set_1_time,
            )
            result_set_crud.create(db_session=session, obj_in=result_set_1_create)

            result_set_2_time = datetime.now()
            result_set_2_graph_spec = schemas.ResultSetGraphSpec(
                graph_uris_load_times={"test": result_set_2_time.timestamp()}
            )
            results_2 = [
                schemas.Result(
                    account_id=account_id_a, result={"foo": "newhello_a", "boo": "newthere_a"},
                ),
                schemas.Result(
                    account_id=account_id_a, result={"foo": "newboo_a", "boo": "newfoo_a"},
                ),
            ]
            result_set_2_create = ResultSetCreate(
                job=job,
                graph_spec=result_set_2_graph_spec,
                results=results_2,
                created=result_set_2_time,
            )
            result_set_crud.create(db_session=session, obj_in=result_set_2_create)

            # check latest results
            latest_results = session.execute("select * from test_job_latest")
            latest_rows = latest_results.fetchall()
            self.assertSequenceEqual(
                sorted(latest_rows),
                sorted(
                    [
                        (result_set_2_time, account_id_a, "newhello_a", "newthere_a"),
                        (result_set_2_time, account_id_a, "newboo_a", "newfoo_a"),
                    ]
                ),
            )
            # check all results
            all_results = session.execute("select * from test_job_all")
            all_rows = all_results.fetchall()
            self.assertSequenceEqual(
                sorted(all_rows),
                sorted(
                    [
                        (result_set_1_time, account_id_a, "oldhello_a", "oldthere_a"),
                        (result_set_1_time, account_id_a, "oldboo_a", "oldfoo_a"),
                        (result_set_1_time, account_id_b, "oldhello_b", "oldthere_b"),
                        (result_set_1_time, account_id_b, "oldboo_b", "oldfoo_b"),
                        (result_set_2_time, account_id_a, "newhello_a", "newthere_a"),
                        (result_set_2_time, account_id_a, "newboo_a", "newfoo_a"),
                    ]
                ),
            )
