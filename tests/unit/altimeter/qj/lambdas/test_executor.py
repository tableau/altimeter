from datetime import datetime
from typing import List

import boto3
import moto

from altimeter.qj.schemas.job import Job, Category, Severity, JobGraphSpec
from altimeter.qj.lambdas.executor import enqueue_queries


@moto.mock_sqs
def test_enqueue_queries():
    sqs_client = boto3.client("sqs", region_name="us-west-2")
    queue_url = sqs_client.create_queue(QueueName="test")["QueueUrl"]
    job_1 = Job(
        name="fooboo",
        description="FooBoo",
        graph_spec=JobGraphSpec(graph_names=["test1"]),
        category=Category.gov,
        severity=Severity.debug,
        query="select ?account_id ?account_name where { ?account a <alti:aws:account> ; <alti:account_id> ?account_id ; <alti:name> ?account_name } order by ?account_name",
        active=True,
        created=datetime(2020, 1, 1),
        query_fields=["account_id", "account_name"],
        max_graph_age_sec=10000,
        result_expiration_sec=100000,
        max_result_age_sec=100000,
        notify_if_results=False,
    )
    job_2 = Job(
        name="boo",
        description="Boo",
        graph_spec=JobGraphSpec(graph_names=["test2"]),
        category=Category.gov,
        severity=Severity.debug,
        query="select ?account_id ?account_name where { ?account a <alti:aws:account> ; <alti:account_id> ?account_id ; <alti:name> ?account_name } order by ?account_name",
        active=True,
        created=datetime(2020, 1, 1),
        query_fields=["account_id", "account_name"],
        max_graph_age_sec=10000,
        result_expiration_sec=100000,
        max_result_age_sec=100000,
        notify_if_results=False,
    )
    jobs: List[Job] = [job_1, job_2]
    enqueue_queries(jobs=jobs, queue_url=queue_url, execution_hash="1234", region="us-west-2")
    msgs = []
    resp = sqs_client.receive_message(QueueUrl=queue_url)
    for raw_msg in resp["Messages"]:
        msgs.append(Job.parse_raw(raw_msg["Body"]))
    resp = sqs_client.receive_message(QueueUrl=queue_url)
    for raw_msg in resp["Messages"]:
        msgs.append(Job.parse_raw(raw_msg["Body"]))
    assert msgs == [job_1, job_2]
