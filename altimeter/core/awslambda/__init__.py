"""Contains various utility functions useful in Lambdas."""
import os
from typing import Any, Dict

from altimeter.core.awslambda.exceptions import (
    RequiredEnvironmentVariableNotPresentException,
    RequiredEventVariableNotPresentException,
)


def get_required_lambda_env_var(key: str) -> str:
    """Get a variable from os.environ.

    Args:
        key: Key to look up in the os environment.

    Returns:
        String value for the given key

    Raises:
        RequiredEnvironmentVariableNotPresentException if key is not present.
    """
    value = os.environ.get(key)
    if value is None:
        raise RequiredEnvironmentVariableNotPresentException(f"Missing required env var {key}")
    return value


def get_required_lambda_event_var(event: Dict[str, Any], key: str) -> Any:
    """Get a variable from a lambda event dict.

    Args:
        event: Lambda event dict
        key: Key to look up in the event

    Returns:
        String value for the given key

    Raises:
        RequiredEventVariableNotPresentException if key is not in event
    """
    value = event.get(key)
    if value is None:
        raise RequiredEventVariableNotPresentException(
            f"Missing required event var {key} in {event}"
        )
    return value
