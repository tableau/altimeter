"""Client for loading and accessing Altimeter generated data in Neptune"""
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Set

from aws_requests_auth.aws_auth import AWSRequestsAuth
import boto3
import requests

from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.neptune.exceptions import (
    NeptuneClearGraphException,
    NeptuneLoadGraphException,
    NeptuneNoFreshGraphFoundException,
    NeptuneNoGraphsFoundException,
    NeptuneQueryException,
    NeptuneUpdateGraphException,
)
from altimeter.core.neptune.results import QueryResult, QueryResultSet
from altimeter.core.neptune.sparql import finalize_query

GRAPH_BASE_URI = "https://alti"
META_GRAPH_NAME = f"{GRAPH_BASE_URI}/__meta__"
SESSION_LIFETIME_MINUTES = 5


def get_required_tag_value(tag_set: List[Dict[str, str]], key: str) -> str:
    """Get a tag value from a TagSet. Raise ValueError if the key is not present.

    Args:
        tag_set: list of dicts, each of which contains keys 'Key' and 'Value'.
        key: tag key string

    Returns:
        tag value string

    Raises:
        ValueError if key is not present in tag_set
    """
    for tag in tag_set:
        tag_key, tag_value = tag["Key"], tag["Value"]
        if tag_key == key:
            return tag_value
    raise ValueError(f"Required tag key {key} not found in {tag_set}")


@dataclass(frozen=True)
class NeptuneEndpoint:
    """Represents an AWS Neptune endpoint.

    Args:
        host: neptune host
        port: neptune port
        region: neptune region
    """

    host: str
    port: int
    region: str

    def get_endpoint_str(self) -> str:
        """Get the endpoint as a string in host:port format

        Returns:
            endpoint string
        """
        return f"{self.host}:{self.port}"

    def get_sparql_endpoint(self) -> str:
        """Get the sparql query url for this Neptune endpoint

        Returns:
            sparql query endpoint url
        """
        return f"http://{self.get_endpoint_str()}/sparql"

    def get_loader_endpoint(self) -> str:
        """Get the loader url for this Neptune endpoint

        Returns:
            loader endpoint url
        """
        return f"http://{self.get_endpoint_str()}/loader"


@dataclass(frozen=True)
class GraphMetadata:
    """A GraphMetadata represents the details of a graph.
    These details are stored in a metadata graph in Neptune and used by clients
    to find the latest available successfuly loaded graph.

    Args:
        uri: graph uri
        name: graph name
        version: graph version
        start_time: epoch timestamp of graph start time
        end_time: epoch timestamp of graph end time
    """

    uri: str
    name: str
    version: str
    start_time: int
    end_time: int


class AltimeterNeptuneClient:
    """Client to run sparql queries against a neptune instance using graph name conventions to
    determine most recent graph.

    Args:
        max_age_min: maximum acceptable age in minutes of graphs.  Only graphs which are found
                     that meet this critera will be queried.
        neptune_endpoint: NeptuneEndpoint object for this client
    """

    def __init__(self, max_age_min: int, neptune_endpoint: NeptuneEndpoint):
        self._neptune_endpoint = neptune_endpoint
        self._max_age_min = max_age_min
        self._auth = None
        # initially set this to a time in the past such that _get_auth's logic is simpler
        # regarding first run.
        self._auth_expiration = datetime.now() - timedelta(hours=24)

    def run_query(self, graph_names: Set[str], query: str) -> QueryResult:
        """Runs a SPARQL query against the latest available graphs given a list of
        graph names.

        Args:
            graph_names: list of graph names to query
            query: query string. This query string should not include any 'from' clause;
                   the graph_names param will be used to inject the correct graph uris
                   by locating the latest acceptable (based on `max_age_min`) graph.

        Returns:
            QueryResult object
        """
        graph_uris_load_times: Dict[str, int] = {}
        for graph_name in graph_names:
            graph_metadata = self._get_latest_graph_metadata(name=graph_name)
            graph_uris_load_times[graph_metadata.uri] = graph_metadata.end_time
        finalized_query = finalize_query(query, graph_uris=list(graph_uris_load_times.keys()))
        query_result_set = self.run_raw_query(finalized_query)
        return QueryResult(graph_uris_load_times, query_result_set)

    def load_graph(self, bucket: str, key: str, load_iam_role_arn: str) -> GraphMetadata:
        """Load a graph into Neptune.
        Args:
             bucket: s3 bucket of graph rdf
             key: s3 key of graph rdf
             load_iam_role_arn: arn of iam role used to load the graph

        Returns:
            GraphMetadata object describing loaded graph

        Raises:
            NeptuneLoadGraphException if errors occur during graph load
        """
        session = boto3.Session(region_name=self._neptune_endpoint.region)
        s3_client = session.client("s3")
        rdf_object_tagging = s3_client.get_object_tagging(Bucket=bucket, Key=key)
        tag_set = rdf_object_tagging["TagSet"]
        graph_name = get_required_tag_value(tag_set, "name")
        graph_version = get_required_tag_value(tag_set, "version")
        graph_start_time = int(get_required_tag_value(tag_set, "start_time"))
        graph_end_time = int(get_required_tag_value(tag_set, "end_time"))
        graph_metadata = GraphMetadata(
            uri=f"{GRAPH_BASE_URI}/{graph_name}/{graph_version}/{graph_end_time}",
            name=graph_name,
            version=graph_version,
            start_time=graph_start_time,
            end_time=graph_end_time,
        )
        logger = Logger()
        with logger.bind(
            rdf_bucket=bucket,
            rdf_key=key,
            graph_uri=graph_metadata.uri,
            neptune_endpoint=self._neptune_endpoint.get_endpoint_str(),
        ):
            session = boto3.Session(region_name=self._neptune_endpoint.region)
            credentials = session.get_credentials()
            auth = AWSRequestsAuth(
                aws_access_key=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_token=credentials.token,
                aws_host=self._neptune_endpoint.get_endpoint_str(),
                aws_region=self._neptune_endpoint.region,
                aws_service="neptune-db",
            )
            post_body = {
                "source": f"s3://{bucket}/{key}",
                "format": "rdfxml",
                "iamRoleArn": load_iam_role_arn,
                "region": self._neptune_endpoint.region,
                "failOnError": "TRUE",
                "parallelism": "MEDIUM",
                "parserConfiguration": {
                    "baseUri": GRAPH_BASE_URI,
                    "namedGraphUri": graph_metadata.uri,
                },
            }
            logger.info(event=LogEvent.NeptuneLoadStart, post_body=post_body)
            submit_resp = requests.post(
                self._neptune_endpoint.get_loader_endpoint(), json=post_body, auth=auth
            )
            if submit_resp.status_code != 200:
                raise NeptuneLoadGraphException(
                    f"Non 200 from Neptune: {submit_resp.status_code} : {submit_resp.text}"
                )
            submit_resp_json = submit_resp.json()
            load_id = submit_resp_json["payload"]["loadId"]
            with logger.bind(load_id=load_id):
                logger.info(event=LogEvent.NeptuneLoadPolling)
                while True:
                    time.sleep(10)
                    status_resp = requests.get(
                        f"{self._neptune_endpoint.get_loader_endpoint()}/{load_id}",
                        params={"details": "true", "errors": "true"},
                        auth=auth,
                    )
                    if status_resp.status_code != 200:
                        raise NeptuneLoadGraphException(
                            f"Non 200 from Neptune: {status_resp.status_code} : {status_resp.text}"
                        )
                    status_resp_json = status_resp.json()
                    status = status_resp_json["payload"]["overallStatus"]["status"]
                    logger.info(event=LogEvent.NeptuneLoadPolling, status=status)
                    if status == "LOAD_COMPLETED":
                        break
                    if status not in ("LOAD_NOT_STARTED", "LOAD_IN_PROGRESS"):
                        logger.error(event=LogEvent.NeptuneLoadError, status=status)
                        raise NeptuneLoadGraphException(f"Error loading graph: {status_resp_json}")
                logger.info(event=LogEvent.NeptuneLoadEnd)

                logger.info(event=LogEvent.MetadataGraphUpdateStart)
                self._register_graph(graph_metadata=graph_metadata)
                logger.info(event=LogEvent.MetadataGraphUpdateEnd)

                return graph_metadata

    def _get_auth(self) -> AWSRequestsAuth:
        """Generate an AWSRequestsAuth object using a boto session for the current/local account.

        Returns:
             AWSRequestsAuth object
        """
        if datetime.now() >= self._auth_expiration:
            session = boto3.Session()
            credentials = session.get_credentials()
            region = (
                session.region_name
                if self._neptune_endpoint.region is None
                else self._neptune_endpoint.region
            )
            auth = AWSRequestsAuth(
                aws_access_key=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_token=credentials.token,
                aws_host=f"{self._neptune_endpoint.host}:{self._neptune_endpoint.port}",
                aws_region=region,
                aws_service="neptune-db",
            )
            self._auth = auth
            self._auth_expiration = datetime.now() + timedelta(minutes=SESSION_LIFETIME_MINUTES)
        return self._auth

    def _register_graph(self, graph_metadata: GraphMetadata) -> None:
        """Registers a GraphMetadata object into the metadata graph
        The meta graph keeps track of graph uris and metadata.
        Run this after a graph is completely loaded and then use
        _get_latest_graph_metadata to query this graph to find the latest graph.

        Args:
            graph_metadata: GraphMetadata to load into the metadata graph.

        Raises:
            NeptuneUpdateGraphException if an error occurred during metadata graph update
        """

        auth = self._get_auth()
        neptune_sparql_url = self._neptune_endpoint.get_sparql_endpoint()
        update_stmt = (
            f"INSERT DATA {{\n"
            f"    GRAPH <{META_GRAPH_NAME}>\n"
            f"        {{ <alti:graph:{graph_metadata.uri}> "
            f'            <alti:uri>         "{graph_metadata.uri}" ;\n'
            f'            <alti:name>        "{graph_metadata.name}" ;\n'
            f'            <alti:version>     "{graph_metadata.version}" ;\n'
            f"            <alti:start_time>  {graph_metadata.start_time} ;\n"
            f"            <alti:end_time>    {graph_metadata.end_time} ;\n"
            f"}}\n"
            "}\n"
        )
        resp = requests.post(neptune_sparql_url, data={"update": update_stmt}, auth=auth)
        if resp.status_code != 200:
            raise NeptuneUpdateGraphException(
                (f"Error updating graph {META_GRAPH_NAME} " f"with {update_stmt} : {resp.text}")
            )

    def get_graph_uris(self, name: str) -> List[str]:
        """Return all graph uris regardless of whether they have corresponding metadata entries

        Args:
            name: graph name

        Returns:
            list of graph uris
        """
        query = "SELECT ?graph_uri WHERE { GRAPH ?graph_uri { } }"
        results = self.run_raw_query(query=query)
        results_list = results.to_list()
        all_graph_uris = [result["graph_uri"] for result in results_list]
        graph_prefix = f"{GRAPH_BASE_URI}/{name}/"
        graph_uris = [uri for uri in all_graph_uris if uri.startswith(graph_prefix)]
        return graph_uris

    def get_graph_metadatas(self, name: str, version: Optional[str] = None) -> List[GraphMetadata]:
        """Return all graph metadatas for a given name/version. These represent fully loaded
        graphs in the Neptune database.

        Args:
            name: graph name
            version: graph version

        Returns:
            list of GraphMetadata objects for the given graph name/version
        """
        if version is None:
            get_graph_metadatas_query = (
                "SELECT ?uri ?name ?version ?start_time ?end_time\n"
                f"FROM <{META_GRAPH_NAME}>\n"
                f"WHERE {{ ?graph_metadata <alti:uri> ?uri ;\n"
                f'                         <alti:name> "{name}" ;\n'
                f"                         <alti:name> ?name ;\n"
                f"                         <alti:version> ?version ;\n"
                f"                         <alti:start_time> ?start_time ;\n"
                f"                         <alti:end_time> ?end_time }}\n"
                f"ORDER BY DESC(?end_time)\n"
            )
        else:
            get_graph_metadatas_query = (
                "SELECT ?uri ?name ?version ?start_time ?end_time\n"
                f"FROM <{META_GRAPH_NAME}>\n"
                f"WHERE {{ ?graph_metadata <alti:uri> ?uri ;\n"
                f'                         <alti:name> "{name}" ;\n'
                f"                         <alti:name> ?name ;\n"
                f'                         <alti:version> "{version}" ;\n'
                f"                         <alti:version> ?version ;\n"
                f"                         <alti:start_time> ?start_time ;\n"
                f"                         <alti:end_time> ?end_time }}\n"
                f"ORDER BY DESC(?end_time)\n"
            )
        results = self.run_raw_query(query=get_graph_metadatas_query)
        results_list = results.to_list()
        graph_metadatas: List[GraphMetadata] = []
        for result in results_list:
            graph_metadata = GraphMetadata(
                uri=result["uri"],
                name=result["name"],
                version=result["version"],
                start_time=int(result["start_time"]),
                end_time=int(result["end_time"]),
            )
            graph_metadatas.append(graph_metadata)
        return graph_metadatas

    def _get_latest_graph_metadata(self, name: str, version: Optional[str] = None) -> GraphMetadata:
        """Return a GraphMetadata object representing the most recently successfully loaded graph
        for a given name / version.

        Args:
            name: graph name
            version: graph version

        Returns:
            GraphMetadata of the latest graph for the given name/version

        Raises:
            NeptuneNoGraphsFoundException if no matching graphs were found
            NeptuneNoFreshGraphFoundException if no graphs could be found within ax_age_min
        """
        if version is None:
            get_graph_metadatas_query = (
                f"SELECT ?uri ?version ?start_time ?end_time\n"
                f"FROM <{META_GRAPH_NAME}>\n"
                f"WHERE {{\n"
                f"    ?graph_metadata <alti:uri> ?uri ;\n"
                f'                    <alti:name> "{name}" ;\n'
                f"                    <alti:version> ?version ;\n"
                f"                    <alti:start_time> ?start_time ;\n"
                f"                    <alti:end_time> ?end_time }}\n"
                f"ORDER BY DESC(?version) DESC(?end_time)\n"
                f"LIMIT 1"
            )
        else:
            get_graph_metadatas_query = (
                f"SELECT ?uri ?version ?start_time ?end_time\n"
                f"FROM <{META_GRAPH_NAME}>\n"
                f"WHERE {{\n"
                f"    ?graph_metadata <alti:uri> ?uri ;\n"
                f'                    <alti:name> "{name}" ;\n'
                f"                    <alti:version> {version} ;\n"
                f"                    <alti:version> ?version ;\n"
                f"                    <alti:start_time> ?start_time ;\n"
                f"                    <alti:end_time> ?end_time }}\n"
                f"ORDER BY DESC(?end_time)\n"
                f"LIMIT 1"
            )
        results = self.run_raw_query(query=get_graph_metadatas_query)
        results_list = results.to_list()
        if not results_list:
            raise NeptuneNoGraphsFoundException(f"No graphs found for graph name '{name}'")
        if len(results_list) != 1:
            raise RuntimeError("Logic error - more than one graph returned.")
        result = results_list[0]
        latest_uri = result["uri"]
        latest_version = result["version"]
        latest_start_time = int(result["start_time"])
        latest_end_time = int(result["end_time"])
        now = int(datetime.now().timestamp())
        oldest_acceptable_graph_end_time = now - self._max_age_min * 60
        if latest_end_time < oldest_acceptable_graph_end_time:
            raise NeptuneNoFreshGraphFoundException(
                (
                    f"Could not find a graph named '{name}' younger "
                    f"than {self._max_age_min} "
                    f"minutes old.  Found: {results_list}"
                )
            )
        return GraphMetadata(
            uri=latest_uri,
            name=name,
            version=latest_version,
            start_time=latest_start_time,
            end_time=latest_end_time,
        )

    def clear_registered_graph(self, name: str, uri: str) -> None:
        """Remove data and metadata for a graph by uri

        Args:
            name: graph name
            uri: graph uri

        Raises:
            NeptuneUpdateGraphException if an error occurred during clearing
        """
        # clear metadata first such that clients will not use this graph if
        # data clear fails
        self.clear_graph_metadata(name=name, uri=uri)
        # then clear data
        self.clear_graph_data(uri=uri)

    def clear_graph_metadata(self, name: str, uri: str) -> None:
        """Clear a graph metadata entry"""
        auth = self._get_auth()
        neptune_sparql_url = self._neptune_endpoint.get_sparql_endpoint()
        delete_stmt = (
            f"WITH <{META_GRAPH_NAME}>\n"
            f"DELETE\n"
            f'  {{ ?graph <alti:uri>         "{uri}" ;\n'
            f'            <alti:name>        "{name}" ;\n'
            f"            <alti:version>     ?version ;\n"
            f"            <alti:start_time>  ?start_time ;\n"
            f"            <alti:end_time>    ?end_time }}\n"
            f"WHERE\n"
            f'  {{ ?graph <alti:uri>         "{uri}" ;\n'
            f'            <alti:name>        "{name}" ;\n'
            f"            <alti:version>     ?version ;\n"
            f"            <alti:start_time>  ?start_time ;\n"
            f"            <alti:end_time>    ?end_time }}\n"
        )
        resp = requests.post(neptune_sparql_url, data={"update": delete_stmt}, auth=auth)
        if resp.status_code != 200:
            raise NeptuneUpdateGraphException(
                (f"Error updating graph {META_GRAPH_NAME} " f"with {delete_stmt} : {resp.text}")
            )

    def clear_graph_data(self, uri: str) -> None:
        """Clear a graph in Neptune"""
        auth = self._get_auth()
        neptune_sparql_url = self._neptune_endpoint.get_sparql_endpoint()
        update_stmt = f"clear graph <{uri}>"
        resp = requests.post(neptune_sparql_url, data={"update": update_stmt}, auth=auth)
        if resp.status_code != 200:
            raise NeptuneClearGraphException(
                (f"Error clearing graph {uri} " f"with {update_stmt} : {resp.text}")
            )

    def run_raw_query(self, query: str) -> QueryResultSet:
        """Run a query against a neptune instance, return a dict of results. Generally this
        should be called from `run_query`

        Args:
            query: complete query to run

        Returns:
            QueryResultSet object

        Raises:
            NeptuneQueryException if an error occurred running the query
        """
        neptune_sparql_url = self._neptune_endpoint.get_sparql_endpoint()
        auth = self._get_auth()
        resp = requests.post(neptune_sparql_url, data={"query": query}, auth=auth)
        if resp.status_code != 200:
            raise NeptuneQueryException(f"Error running query {query}: {resp.text}")
        return QueryResultSet.from_sparql_endpoint_json(resp.json())
