"""Execute all known QJs, run the query portion of a QJ, and prune results according to Job config settings"""
import os.path
from typing import Any, Dict

from altimeter.qj.config import QJHandlerConfig
from altimeter.qj.lambdas.executor import executor
from altimeter.qj.lambdas.query import query
from altimeter.qj.lambdas.pruner import pruner


class InvalidLambdaModeException(Exception):
    """Indicates the mode associated with the queryjob lambda is invalid"""


def lambda_handler(event: Dict[str, Any], _: Any) -> None:
    """Lambda entrypoint"""
    handler = QJHandlerConfig()
    if handler.mode == "executor":
        executor(event)
    elif handler.mode == "query":
        query(event)
    elif handler.mode == "pruner":
        pruner()
    else:
        raise InvalidLambdaModeException(
            f"Invalid lambda MODE value.\nENV: {os.environ}\nEvent: {event}"
        )
