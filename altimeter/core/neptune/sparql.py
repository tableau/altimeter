"""SPARQL related functions."""

import re
from typing import List

from altimeter.core.neptune.exceptions import InvalidQueryException


def finalize_query(query: str, graph_uris: List[str]) -> str:
    """Finalize a generic sparql query - specifically add a FROM clause
    containing graph uris for this query.

    Args:
        query: query string
        graph_uris: list of graph uris

    Returns:
        finalized query string
    """
    # find the where clause. once found, insert a from clause before it with the
    # graph_uris.
    found_where = False
    output_lines = []
    for line in query.split("\n"):
        if found_where:
            output_lines.append(line)
        else:
            where_regex = r"^([^#]|)+(\s|^)(where)(\s+{|{|\s*$|\s*#).*$"
            where_match = re.search(where_regex, line, re.IGNORECASE)
            if where_match:
                found_where = True
                before = line[: where_match.start(2)].rstrip()
                after = line[where_match.start(2) :].lstrip()
                final_query_lines = []
                if before:
                    final_query_lines.append(before)
                for graph_uri in graph_uris:
                    final_query_lines.append(f"FROM <{graph_uri}>")
                final_query_lines.append(after)
                output_lines.append(" ".join(final_query_lines))
            else:
                output_lines.append(line)
    if found_where:
        return "\n".join(output_lines)
    raise InvalidQueryException(f"Unable to find where clause in {query}")
