"""Contains functions for pulling runtime parameters from the environment, lambda events, etc"""
import os
from typing import Dict, Any

from altimeter.core.parameters.exceptions import (
    RequiredVariableNotPresentException,
    RequiredEnvironmentVariableNotPresentException,
    RequiredEventVariableNotPresentException,
)


def _get_required_var(key: str, data: Dict[str, Any]) -> str:
    """Get a value from a dict coerced to str.
    raise RequiredVariableNotPresentException if it does not exist"""
    value = data.get(key)
    if value is None:
        raise RequiredVariableNotPresentException(f"Missing required var {key}")
    return str(value)


def _get_required_env_var(key: str) -> str:
    """Get a required env var coerced to a string.
    raise RequiredEnvironmentVariableNotPresentException if it does not exist"""
    try:
        return _get_required_var(key, dict(os.environ))
    except RequiredVariableNotPresentException as rvnpe:
        raise RequiredEnvironmentVariableNotPresentException(str(rvnpe))


def get_required_str_env_var(key: str) -> str:
    """Get a required string env variable"""
    return _get_required_env_var(key)


def get_required_int_env_var(key: str) -> int:
    """Get a required integer env variable.
    Raise ValueError if it is not an integer"""
    value = _get_required_env_var(key)
    try:
        return int(value)
    except ValueError as ve:
        raise ValueError(f"env var {key} must contain an integer value, not {value}: {ve}")


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
