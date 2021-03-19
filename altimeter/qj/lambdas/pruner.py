"""Prune results according to Job config settings"""
from altimeter.core.log import Logger
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import PrunerConfig
from altimeter.qj.log import QJLogEvents
from altimeter.qj.security import get_api_key


def pruner() -> None:
    """Prune results according to Job config settings"""
    logger = Logger()
    pruner_config = PrunerConfig()
    logger.info(event=QJLogEvents.InitConfig, config=pruner_config)
    api_key = get_api_key(region_name=pruner_config.region)
    qj_client = QJAPIClient(
        host=pruner_config.api_host, port=pruner_config.api_port, api_key=api_key
    )
    logger.info(event=QJLogEvents.DeleteStart)
    result = qj_client.delete_expired_result_sets()
    logger.info(event=QJLogEvents.DeleteEnd, result=result)
