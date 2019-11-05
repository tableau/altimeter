#!/usr/bin/env python3
"""Remove graphs matching a given name which are older than n minutes."""
from datetime import datetime


from altimeter.core.log import LogEvent, Logger
from altimeter.core.awslambda import get_required_lambda_env_var
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint, META_GRAPH_NAME


def lambda_handler(event, context):
    host = get_required_lambda_env_var("NEPTUNE_HOST")
    port = get_required_lambda_env_var("NEPTUNE_PORT")
    region = get_required_lambda_env_var("NEPTUNE_REGION")
    max_age_min = get_required_lambda_env_var("MAX_AGE_MIN")
    graph_name = get_required_lambda_env_var("GRAPH_NAME")
    try:
        max_age_min = int(max_age_min)
    except ValueError as ve:
        raise Exception(f"env var MAX_AGE_MIN must be an int: {ve}")
    now = int(datetime.now().timestamp())
    oldest_acceptable_graph_epoch = now - max_age_min * 60

    endpoint = NeptuneEndpoint(host=host, port=port, region=region)
    client = AltimeterNeptuneClient(max_age_min=max_age_min, neptune_endpoint=endpoint)
    logger = Logger()

    uncleared = []

    # first prune metadata - if clears below are partial we want to make sure no clients
    # consider this a valid graph still.
    logger.info(event=LogEvent.PruneNeptuneMetadataGraphStart)
    client.clear_old_graph_metadata(name=graph_name, max_age_min=max_age_min)
    logger.info(event=LogEvent.PruneNeptuneMetadataGraphEnd)
    # now clear actual graphs
    with logger.bind(neptune_endpoint=endpoint):
        logger.info(event=LogEvent.PruneNeptuneGraphsStart)
        for graph_metadata in client.get_graph_metadatas(name=graph_name):
            assert graph_metadata.name == graph_name
            graph_epoch = graph_metadata.end_time
            with logger.bind(graph_uri=graph_metadata.uri, graph_epoch=graph_epoch):
                if graph_epoch < oldest_acceptable_graph_epoch:
                    logger.info(event=LogEvent.PruneNeptuneGraphStart)
                    try:
                        client.clear_graph(graph_uri=graph_metadata.uri)
                        logger.info(event=LogEvent.PruneNeptuneGraphEnd)
                    except Exception as ex:
                        logger.error(
                            event=LogEvent.PruneNeptuneGraphError,
                            msg=f"Error pruning graph {graph_metadata.uri}: {ex}",
                        )
                        uncleared.append(graph_metadata.uri)
                        continue
                else:
                    logger.info(event=LogEvent.PruneNeptuneGraphSkip)
        logger.info(event=LogEvent.PruneNeptuneGraphsEnd)
        if uncleared:
            msg = f"Errors were found pruning {uncleared}."
            logger.error(event=LogEvent.PruneNeptuneGraphsError, msg=msg)
            raise Exception(msg)
