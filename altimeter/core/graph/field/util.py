"""Grab-bag functions for Field parsing"""
import re


FIRST_CAP_RE = re.compile("(.)([A-Z][a-z]+)")
ALL_CAP_RE = re.compile("([a-z0-9])([A-Z])")


def camel_case_to_snake_case(name: str) -> str:
    """Convert a string from CamelCase into snake_case.
    Args:
        name: string to convert

    Returns:
         snake cased string
     """
    first_capped_str = FIRST_CAP_RE.sub(r"\1_\2", name)
    return ALL_CAP_RE.sub(r"\1_\2", first_capped_str).lower()
