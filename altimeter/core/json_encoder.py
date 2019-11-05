"""Function for encoding JSON with datetimes."""
from datetime import datetime
from typing import Any


def json_encoder(obj: Any) -> Any:
    """json encoder function supporting datetime serialization.

    Args:
        obj: object to encode to JSON

    Returns:
        json encoded data
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type {} not serializable".format(type(obj)))
