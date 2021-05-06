import os
from unittest import mock

from altimeter.qj.schemas.job import Category, JobCreate, JobGraphSpec, JobUpdate, Severity


@mock.patch.dict(os.environ, {"REGION": "us-west-2"})
def test_job_update_from_create():
    job_create = JobCreate(
        name="test",
        description="test",
        graph_spec=JobGraphSpec(graph_names=["test"]),
        category=Category.gov,
        severity=Severity.debug,
        query="select ?account_id ?account_name where { ?account a <alti:aws:account> ; <alti:account_id> ?account_id ; <alti:name> ?account_name } order by ?account_name",
        notify_if_results=False,
    )
    job_update = JobUpdate.from_job_create(job_create)
    expected_job_update = JobUpdate(
        description="test", category=Category.gov, severity=Severity.debug, notify_if_results=False,
    )
    print(job_update)  # False
    print(expected_job_update)  # None

    assert job_update == expected_job_update
