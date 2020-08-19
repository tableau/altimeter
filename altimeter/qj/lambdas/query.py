#!/usr/bin/env python3
"""Run the query portion of a QJ"""
import json
from typing import Any, Dict, List

from altimeter.core.log import Logger
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint
from altimeter.core.neptune.results import QueryResult
from altimeter.qj import schemas
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import QueryConfig
from altimeter.qj.log import QJLogEvents
from altimeter.qj.security import get_api_key


def lambda_handler(event: Dict[str, Any], _: Any) -> None:
    """ entrypoint"""
    config = QueryConfig()
    logger = Logger()
    logger.info(event=QJLogEvents.InitConfig, config=config)

    records = event.get("Records", [])
    if not records:
        raise Exception("No records found")
    if len(records) > 1:
        raise Exception(f"More than one record. BatchSize is probably not 1. event: {event}")
    body = records[0].get("body")
    if body is None:
        raise Exception(f"No record body found. BatchSize is probably not 1. event: {event}")
    body = json.loads(body)
    job = schemas.Job(**body)
    logger.info(event=QJLogEvents.InitJob, job=job)

    logger.info(event=QJLogEvents.RunQueryStart)
    query_result = run_query(job=job, config=config)
    logger.info(event=QJLogEvents.RunQueryEnd, num_results=query_result.get_length())

    results: List[schemas.Result] = []
    if config.account_id_key not in query_result.query_result_set.fields:
        raise Exception(f"Query results must contain field '{config.account_id_key}'")
    for q_r in query_result.to_list():
        account_id = q_r[config.account_id_key]
        result = schemas.Result(
            account_id=account_id,
            result={key: val for key, val in q_r.items() if key != config.account_id_key},
        )
        results.append(result)

    graph_spec = schemas.ResultSetGraphSpec(
        graph_uris_load_times=query_result.graph_uris_load_times
    )
    result_set = schemas.ResultSetCreate(job=job, graph_spec=graph_spec, results=results)

    api_key = get_api_key(region_name=config.region)
    qj_client = QJAPIClient(host=config.api_host, port=config.api_port, api_key=api_key)
    logger.info(event=QJLogEvents.CreateResultSetStart)
    qj_client.create_result_set(result_set=result_set)
    logger.info(event=QJLogEvents.CreateResultSetEnd)


def run_query(job: schemas.Job, config: QueryConfig) -> QueryResult:
    """Run a query and return a QueryResult object"""
    endpoint = NeptuneEndpoint(
        host=config.neptune_host, port=config.neptune_port, region=config.neptune_region
    )
    neptune_client = AltimeterNeptuneClient(
        max_age_min=int(job.max_graph_age_sec / 60.0), neptune_endpoint=endpoint
    )
    query_result = neptune_client.run_query(
        graph_names=set(job.graph_spec.graph_names), query=job.query
    )
    return query_result
