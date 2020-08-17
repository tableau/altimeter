#!/usr/bin/env python3
"""Prune results according to Job config settings"""
from typing import Any, Dict

from altimeter.core.log import Logger
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import PrunerConfig
from altimeter.qj.log import QJLogEvents
from altimeter.qj.security import get_api_key


def lambda_handler(_: Dict[str, Any], __: Any) -> None:
    """Lambda entrypoint"""
    logger = Logger()
    config = PrunerConfig()
    logger.info(event=QJLogEvents.InitConfig, config=config)
    api_key = get_api_key(region_name=config.region)
    qj_client = QJAPIClient(host=config.api_host, port=config.api_port, api_key=api_key)
    logger.info(event=QJLogEvents.DeleteStart)
    result = qj_client.delete_expired_result_sets()
    logger.info(event=QJLogEvents.DeleteEnd, result=result)
