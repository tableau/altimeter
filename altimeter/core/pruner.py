from datetime import datetime
from typing import List

from pydantic import BaseModel

from altimeter.core.config import Config, InvalidConfigException, GraphPrunerConfig
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.neptune.client import NeptuneEndpoint, AltimeterNeptuneClient


class GraphPrunerResults(BaseModel):
    """Results of a GraphPrune run"""

    pruned_graph_uris: List[str]
    skipped_graph_uris: List[str]


def prune_graph(graph_pruner_config: GraphPrunerConfig) -> GraphPrunerResults:
    config = Config.from_path(path=graph_pruner_config.config_path)
    return prune_graph_from_config(config)


def prune_graph_from_config(config: Config) -> GraphPrunerResults:
    if config.neptune is None:
        raise InvalidConfigException("Configuration missing neptune section.")
    now = int(datetime.now().timestamp())
    oldest_acceptable_graph_epoch = now - config.pruner_max_age_min * 60
    endpoint = NeptuneEndpoint(
        host=config.neptune.host, port=config.neptune.port, region=config.neptune.region
    )
    client = AltimeterNeptuneClient(
        max_age_min=config.pruner_max_age_min, neptune_endpoint=endpoint
    )
    logger = Logger()
    uncleared = []
    pruned_graph_uris = []
    skipped_graph_uris = []
    logger.info(event=LogEvent.PruneNeptuneGraphsStart)
    all_graph_metadatas = client.get_graph_metadatas(name=config.graph_name)
    with logger.bind(neptune_endpoint=str(endpoint)):
        for graph_metadata in all_graph_metadatas:
            assert graph_metadata.name == config.graph_name
            graph_epoch = graph_metadata.end_time
            with logger.bind(graph_uri=graph_metadata.uri, graph_epoch=graph_epoch):
                if graph_epoch < oldest_acceptable_graph_epoch:
                    logger.info(event=LogEvent.PruneNeptuneGraphStart)
                    try:
                        client.clear_registered_graph(
                            name=config.graph_name, uri=graph_metadata.uri
                        )
                        logger.info(event=LogEvent.PruneNeptuneGraphEnd)
                        pruned_graph_uris.append(graph_metadata.uri)
                    except Exception as ex:
                        logger.error(
                            event=LogEvent.PruneNeptuneGraphError,
                            msg=f"Error pruning graph {graph_metadata.uri}: {ex}",
                        )
                        uncleared.append(graph_metadata.uri)
                        continue
                else:
                    logger.info(event=LogEvent.PruneNeptuneGraphSkip)
                    skipped_graph_uris.append(graph_metadata.uri)
        # now find orphaned graphs - these are in neptune but have no metadata
        registered_graph_uris = [g_m.uri for g_m in all_graph_metadatas]
        all_graph_uris = client.get_graph_uris(name=config.graph_name)
        orphaned_graphs = set(all_graph_uris) - set(registered_graph_uris)
        if orphaned_graphs:
            for orphaned_graph_uri in orphaned_graphs:
                with logger.bind(graph_uri=orphaned_graph_uri):
                    logger.info(event=LogEvent.PruneOrphanedNeptuneGraphStart)
                    try:
                        client.clear_graph_data(uri=orphaned_graph_uri)
                        logger.info(event=LogEvent.PruneOrphanedNeptuneGraphEnd)
                        pruned_graph_uris.append(orphaned_graph_uri)
                    except Exception as ex:
                        logger.error(
                            event=LogEvent.PruneNeptuneGraphError,
                            msg=f"Error pruning graph {orphaned_graph_uri}: {ex}",
                        )
                        uncleared.append(orphaned_graph_uri)
                        continue
        logger.info(event=LogEvent.PruneNeptuneGraphsEnd)
        if uncleared:
            msg = f"Errors were found pruning {uncleared}."
            logger.error(event=LogEvent.PruneNeptuneGraphsError, msg=msg)
            raise Exception(msg)
    return GraphPrunerResults(
        pruned_graph_uris=pruned_graph_uris, skipped_graph_uris=skipped_graph_uris,
    )
