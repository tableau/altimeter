"""Run the query portion of a QJ"""
import time
from typing import Any, Dict, List

from altimeter.core.log import Logger
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint
from altimeter.core.neptune.exceptions import NeptuneQueryException
from altimeter.core.neptune.results import QueryResult
from altimeter.qj import schemas
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import QueryConfig
from altimeter.qj.log import QJLogEvents
from altimeter.qj.security import get_api_key


def query(event: Dict[str, Any]) -> None:
    """Run the query portion of a QJ"""
    query_config = QueryConfig()
    logger = Logger()
    logger.info(event=QJLogEvents.InitConfig, config=query_config)
    job_name = event.get("job_name")
    if not job_name:
        raise Exception("Missing expected input parameter 'job_name'")
    qj_client = QJAPIClient(host=query_config.api_host, port=query_config.api_port)
    job = qj_client.get_job(job_name=job_name)
    if not job:
        raise Exception(f"ERROR: unknown job {job_name}")
    logger.info(event=QJLogEvents.InitJob, job=job)
    logger.info(event=QJLogEvents.RunQueryStart)
    max_tries = 5
    current_try = 0
    while True:
        current_try += 1
        try:
            query_result = run_query(job=job, config=query_config)
            break
        except NeptuneQueryException as nq_ex:
            logger.info(event=QJLogEvents.RunQueryError, detail=str(nq_ex))
        if current_try >= max_tries:
            logger.info(event=QJLogEvents.RunQueryError, detail="Max tries exceeded")
            raise NeptuneQueryException("Max tries exceeded")
        time.sleep(5)
    logger.info(event=QJLogEvents.RunQueryEnd, num_results=query_result.get_length())

    results: List[schemas.Result] = []
    if query_config.account_id_key not in query_result.query_result_set.fields:
        raise Exception(f"Query results must contain field '{query_config.account_id_key}'")
    for q_r in query_result.to_list():
        account_id = q_r[query_config.account_id_key]
        result = schemas.Result(
            account_id=account_id,
            result={key: val for key, val in q_r.items() if key != query_config.account_id_key},
        )
        results.append(result)

    graph_spec = schemas.ResultSetGraphSpec(
        graph_uris_load_times=query_result.graph_uris_load_times
    )
    result_set = schemas.ResultSetCreate(job=job, graph_spec=graph_spec, results=results)

    api_key = get_api_key(region_name=query_config.region)
    qj_client = QJAPIClient(host=query_config.api_host, port=query_config.api_port, api_key=api_key)
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
    if job.raw_query:
        query_result = neptune_client.run_historic_query(
            graph_names=set(job.graph_spec.graph_names),
            query=job.query,
        )
    else:
        query_result = neptune_client.run_query(
            graph_names=set(job.graph_spec.graph_names),
            query=job.query,
        )
    return query_result
