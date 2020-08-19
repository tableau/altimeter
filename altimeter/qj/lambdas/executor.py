#!/usr/bin/env python3
"""Execute all known QJs"""
import hashlib
import json
from typing import Any, Dict, List
import uuid

import boto3

from altimeter.core.log import Logger
from altimeter.qj import schemas
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import ExecutorConfig
from altimeter.qj.log import QJLogEvents


def lambda_handler(event: Dict[str, Any], _: Any) -> None:
    """Lambda entrypoint"""
    # if this was triggered by an sns message, use that message as part of the
    # deduplication id for each sqs message.  Otherwise generate a unique so that
    # repeated manual runs of executor will not be dedupe'd
    sns_message = event.get("Records", [{}])[0].get("Sns", {}).get("Message")
    if sns_message:
        execution_hash = hashlib.sha256(sns_message.encode()).hexdigest()
    else:
        execution_hash = hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()
    config = ExecutorConfig()
    logger = Logger()
    logger.info(
        event=QJLogEvents.InitConfig,
        sns_triggered=bool(sns_message),
        execution_hash=execution_hash,
    )
    qj_client = QJAPIClient(host=config.api_host, port=config.api_port)
    jobs = qj_client.get_jobs(active_only=True)
    logger.info(event=QJLogEvents.GetJobs, num_jobs=len(jobs))
    enqueue_queries(
        jobs=jobs,
        queue_url=config.query_queue_url,
        execution_hash=execution_hash,
        region=config.region,
    )


def enqueue_queries(
    jobs: List[schemas.Job], queue_url: str, execution_hash: str, region: str
) -> None:
    """Enqueue querys by sending a message for each job key to queue_url"""
    sqs_client = boto3.client("sqs", region_name=region)
    logger = Logger()
    with logger.bind(queue_url=queue_url, execution_hash=execution_hash):
        for job in jobs:
            job_hash = hashlib.sha256()
            job_hash.update(json.dumps(job.json()).encode())
            message_group_id = job_hash.hexdigest()
            job_hash.update(execution_hash.encode())
            message_dedupe_id = job_hash.hexdigest()
            logger.info(
                QJLogEvents.ScheduleJob,
                job=job,
                message_group_id=message_group_id,
                message_dedupe_id=message_dedupe_id,
            )
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=job.json(),
                MessageGroupId=message_group_id,
                MessageDeduplicationId=message_dedupe_id,
            )
