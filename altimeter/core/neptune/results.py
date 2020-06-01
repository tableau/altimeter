"""Classes for representing query results"""
from collections import Counter
import csv
import io
import json
from typing import Any, Dict, List, Type, Union


class QueryResultSet:
    """Represents the results of a SPARQL query.

    Args:
         fields: List of field names
         values: list of value dicts as returned from neptune's query api.
    """

    def __init__(self, fields: List[str], values: List[Dict[str, Any]]):
        self.fields = fields
        self.values = values
        self.length = len(self.values)

    @classmethod
    def from_sparql_endpoint_json(
        cls: Type["QueryResultSet"], resp: Dict[str, Any]
    ) -> "QueryResultSet":
        """Build a QueryResultSet object from the returned data
        of a sparql endpoint json query (has top level field 'head' and
        results')

        Args:
            resp: response dict from neptune's query api

        Returns:
            QueryResultSet object
        """
        fields = resp.get("head", {}).get("vars", [])
        values = resp.get("results", {}).get("bindings", [])
        return cls(fields, values)

    def to_list(self) -> List[Dict[str, Any]]:
        """Create a list of dicts representing these results, each dict
        is an individual result row.

        Returns:
             List of dicts representing this QueryResultSet.
        """
        result_list = []
        for value in self.values:
            result_list.append({field: value.get(field, {}).get("value") for field in self.fields})
        return result_list

    def to_csv(self) -> str:
        """Create a CSV representation of this QueryResultSet.

        Returns:
            csv as a str
        """
        with io.StringIO() as csv_buf:
            result_list = self.to_list()
            writer = csv.DictWriter(csv_buf, fieldnames=self.fields, lineterminator="\n")
            writer.writeheader()
            for result in result_list:
                writer.writerow(result)
            csv_buf.seek(0)
            return csv_buf.read()

    def to_ndjson(self) -> str:
        """Create an NDJSON representation of this QueryResult.

        Returns:
            NDJSON as a str
        """
        with io.StringIO() as ndjson_buf:
            result_list = self.to_list()
            for result in result_list:
                ndjson_buf.write(json.dumps(result) + "\n")
            ndjson_buf.seek(0)
            return ndjson_buf.read()

    def get_stats(self, field_keys: List[str]) -> Counter:
        """Return a Counter representing statistics about this result set keyed by a user
        specified list of field keys (e.g. account_id and account_name)

        Args:
            field_keys: list of field names to use as stat keys

        Returns:
            Counter containing result stats.
        """
        stats: Counter = Counter()
        results = self.to_list()
        for result in results:
            stat_key_parts = []
            for field_key in field_keys:
                stat_key_parts.append(result[field_key])
            stat_key = "/".join(stat_key_parts)
            stats[stat_key] += 1
        return stats

    @classmethod
    def from_dict(cls: Type["QueryResultSet"], data: Dict[str, Any]) -> "QueryResultSet":
        fields = data.get("fields")
        if fields is None:
            raise ValueError(f"{cls.__name__} missing key 'fields': {data}")
        values = data.get("values")
        if values is None:
            raise ValueError(f"{cls.__name__} missing key 'values': {data}")
        return cls(fields=fields, values=values)


class QueryResult:
    """Represents the results of a SPARQL query and includes the
    graph uris from which results were pulled.

    Args:
         graph_uris_load_times: Dict with keys which are the graph uris which were used in this
                                query and values which are the load end times for the graph.
         query_result_set: QueryResultSet containing results
    """

    def __init__(self, graph_uris_load_times: Dict[str, int], query_result_set: QueryResultSet):
        self.graph_uris_load_times = graph_uris_load_times
        self.query_result_set = query_result_set

    def get_length(self) -> int:
        """Get the length of this result.

        Returns:
            int length
        """
        return self.query_result_set.length

    def to_dict(self) -> Dict[str, Union[List[Any], Dict[str, int]]]:
        """Generate a dict representing this QueryResult

        Returns:
            dict representation of this QueryResult
        """
        return {
            "graph-uris-load-times": self.graph_uris_load_times,
            "results": self.query_result_set.to_list(),
        }

    def to_list(self) -> List[Dict[str, Any]]:
        """Generate a list representing this QueryResult

        Returns:
            List of dicts representing this QueryResult
        """
        return self.query_result_set.to_list()

    def to_csv(self) -> str:
        """Create a CSV representation of this QueryResult.

        Returns:
            csv as a str
        """
        return self.query_result_set.to_csv()

    def to_ndjson(self) -> str:
        """Create an NDJSON representation of this QueryResult.

        Returns:
            NDJSON as a str
        """
        return self.query_result_set.to_ndjson()

    def get_stats(self, field_keys: List[str]) -> Counter:
        """Return a Counter representing statistics about this result set keyed by a user
        specified list of field keys (e.g. account_id and account_name)

        Args:
            field_keys: list of field names to use as stat keys

        Returns:
            Counter containing result stats.
        """
        return self.query_result_set.get_stats(field_keys)
