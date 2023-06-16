"""Execute all known QJs"""
import json
from typing import Any, Dict, List

from altimeter.core.log import Logger
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import ExecutorConfig
from altimeter.qj.log import QJLogEvents


def executor(_: Dict[str, Any]) -> List[str]:
    """Return the name of active QJ names"""
    exec_config = ExecutorConfig()
    logger = Logger()
    logger.info(event=QJLogEvents.InitConfig)
    qj_client = QJAPIClient(host=exec_config.api_host, port=exec_config.api_port)
    jobs = qj_client.get_jobs(active_only=True)
    logger.info(event=QJLogEvents.GetJobs, num_jobs=len(jobs))
    return [job.name for job in jobs]
