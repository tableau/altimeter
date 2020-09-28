"""Client for loading and accessing Altimeter generated data in Neptune"""
import hmac
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
import requests
import hashlib
from typing import Dict, List, Optional, Set, Tuple

from aws_requests_auth.aws_auth import AWSRequestsAuth
import boto3

from gremlin_python.process.graph_traversal import __
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from tornado import httpclient
from urllib import parse

from altimeter.core.exceptions import AltimeterException
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
    ssl: bool = True
    auth_mode: str = "DEFAULT"

    def get_endpoint_str(self) -> str:
        """Get the endpoint as a string in host:port format

        Returns:
            endpoint string
        """
        return f"{self.host}:{self.port}"

    def get_sparql_endpoint(self, ssl: bool = True) -> str:
        """Get the sparql query url for this Neptune endpoint

        Returns:
            sparql query endpoint url
        """
        if ssl:
            return f"https://{self.get_endpoint_str()}/sparql"
        else:
            return f"http://{self.get_endpoint_str()}/sparql"

    def get_loader_endpoint(self, ssl: bool = True) -> str:
        """Get the loader url for this Neptune endpoint

        Returns:
            loader endpoint url
        """
        if ssl:
            return f"https://{self.get_endpoint_str()}/loader"
        else:
            return f"http://{self.get_endpoint_str()}/loader"

    def get_gremlin_endpoint(self, ssl: bool = True) -> str:
        """Get the loader url for this Neptune endpoint

        Returns:
            loader endpoint url
        """
        if ssl:
            return f"wss://{self.get_endpoint_str()}/gremlin"
        else:
            return f"ws://{self.get_endpoint_str()}/gremlin"


def discover_neptune_endpoint() -> NeptuneEndpoint:
    """Find a Neptune"""
    instance_id_prefix = "alti-"
    neptune_client = boto3.client("neptune")
    paginator = neptune_client.get_paginator("describe_db_instances")
    for resp in paginator.paginate():
        for instance in resp.get("DBInstances", []):
            instance_id = instance.get("DBInstanceIdentifier")
            if instance_id:
                if instance_id.startswith(instance_id_prefix):
                    endpoint = instance["Endpoint"]
                    host = endpoint["Address"]
                    port = endpoint["Port"]
                    region = boto3.session.Session().region_name
                    return NeptuneEndpoint(host=host, port=port, region=region)
    raise AltimeterException(f"No Neptune instance found matching {instance_id_prefix}*")


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


class RequestParameters:
    """
    Holds the request parameters for Sigv4Signing
    """

    def __init__(self, uri: str, querystring: str, headers: Dict):
        self.uri = uri
        self.querystring = querystring
        self.headers = headers


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
        self.logger = Logger()

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
        logger = self.logger
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

    @staticmethod
    def __normalize_query_string(query: str) -> str:
        """Normalize the query string"""
        kv = (list(map(str.strip, s.split("="))) for s in query.split("&") if len(s) > 0)

        normalized = "&".join("%s=%s" % (p[0], p[1] if len(p) > 1 else "") for p in sorted(kv))
        return normalized

    def __get_signature_key(
        self, key: str, datestamp: str, regionname: str, servicename: str
    ) -> bytes:
        """Get the signed signature key
        :return: The signed key
        """
        key_date = self.__sign(("AWS4" + key).encode("utf-8"), datestamp)
        key_region = self.__sign(key_date, regionname)
        key_service = self.__sign(key_region, servicename)
        key_signing = self.__sign(key_service, "aws4_request")
        return key_signing

    @staticmethod
    def __sign(key: bytes, msg: str) -> bytes:
        """ Sign the msg with the key """
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def prepare_request(
        self, method: str = "GET", payload: str = "", querystring: Dict = {}
    ) -> RequestParameters:
        """
        This prepares the request for sigv4signing.  This is heavily influenced by the code here:
        https://github.com/awslabs/amazon-neptune-tools/tree/master/neptune-python-utils
        :param method: The method name
        :param payload: The request payload
        :param querystring: The request querystring
        :return: The request parameters
        """
        session = boto3.Session()
        credentials = session.get_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key
        session_token = credentials.token

        service = "neptune-db"
        algorithm = "AWS4-HMAC-SHA256"

        request_parameters = parse.urlencode(querystring).replace("%27", "%22")
        canonical_querystring = self.__normalize_query_string(request_parameters)

        t = datetime.utcnow()
        amz_date = t.strftime("%Y%m%dT%H%M%SZ")
        datestamp = t.strftime("%Y%m%d")
        canonical_headers = "host:{}:{}\nx-amz-date:{}\n".format(
            self._neptune_endpoint.host, self._neptune_endpoint.port, amz_date
        )
        signed_headers = "host;x-amz-date"
        payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        canonical_request = "{}\n/{}\n{}\n{}\n{}\n{}".format(
            method,
            "gremlin",
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        )
        credential_scope = "{}/{}/{}/aws4_request".format(
            datestamp, self._neptune_endpoint.region, service
        )
        string_to_sign = "{}\n{}\n{}\n{}".format(
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        )
        signing_key = self.__get_signature_key(
            secret_key, datestamp, self._neptune_endpoint.region, service
        )
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        authorization_header = "{} Credential={}/{}, SignedHeaders={}, Signature={}".format(
            algorithm, access_key, credential_scope, signed_headers, signature
        )
        headers = {"x-amz-date": amz_date, "Authorization": authorization_header}
        if session_token:
            headers["x-amz-security-token"] = session_token
        return RequestParameters(
            "{}?{}".format(
                self._neptune_endpoint.get_gremlin_endpoint(self._neptune_endpoint.ssl),
                canonical_querystring,
            )
            if canonical_querystring
            else self._neptune_endpoint.get_gremlin_endpoint(),
            canonical_querystring,
            headers,
        )

    def connect_to_gremlin(self) -> Tuple[traversal, DriverRemoteConnection]:
        """
        Get the Gremlin traversal and connection for the Neptune endpoint
        :return: The Traversal object
        """
        if self._neptune_endpoint.auth_mode.lower() == "default":
            gremlin_connection = DriverRemoteConnection(
                self._neptune_endpoint.get_gremlin_endpoint(self._neptune_endpoint.ssl), "g"
            )
        else:
            request_parameters = self.prepare_request()
            signed_ws_request = httpclient.HTTPRequest(
                request_parameters.uri, headers=request_parameters.headers
            )
            gremlin_connection = DriverRemoteConnection(signed_ws_request, "g")
        graph_traversal_source = traversal().withRemote(gremlin_connection)

        return graph_traversal_source, gremlin_connection

    def __write_vertices(self, g: traversal, vertices: List[Dict], scan_id: str) -> None:
        """
        Writes the vertices to the labeled property graph
        :param g: The graph traversal source
        :param vertices: A list of dictionaries for each vertex
        :return: None
        """
        cnt = 0
        t = g
        for r in vertices:
            vertex_id = f'{r["~id"]}_{scan_id}'
            t = (
                t.V(vertex_id)
                .fold()
                .coalesce(
                    __.unfold(),
                    __.addV(self.parse_arn(r["~label"])["resource"]).property(T.id, vertex_id),
                )
            )
            for k in r.keys():
                # Need to handle numbers that are bigger than a Long in Java, for now we stringify it
                if isinstance(r[k], int) and (
                    r[k] > 9223372036854775807 or r[k] < -9223372036854775807
                ):
                    r[k] = str(r[k])
                if k not in ["~id", "~label"]:
                    t = t.property(k, r[k])
            cnt += 1
            if cnt % 100 == 0 or cnt == len(vertices):
                try:
                    self.logger.info(
                        event=LogEvent.NeptunePeriodicWrite,
                        msg=f"Writing vertices {cnt} of {len(vertices)}",
                    )
                    t.next()
                    t = g
                except Exception as err:
                    print(str(err))
                    raise NeptuneLoadGraphException(
                        f"Error loading vertex {r} " f"with {str(t.bytecode)}"
                    )

    def __write_edges(self, g: traversal, edges: List[Dict], scan_id: str) -> None:
        """
        Writes the edges to the labeled property graph
        :param g: The graph traversal source
        :param edges: A list of dictionaries for each edge
        :return: None
        """
        cnt = 0
        t = g
        for r in edges:
            to_id = f'{r["~to"]}_{scan_id}'
            from_id = f'{r["~from"]}_{scan_id}'
            t = (
                t.addE(r["~label"])
                .property(T.id, str(r["~id"]))
                .from_(
                    __.V(from_id)
                    .fold()
                    .coalesce(
                        __.unfold(),
                        __.addV(self.parse_arn(r["~from"])["resource"])
                        .property(T.id, from_id)
                        .property("scan_id", scan_id)
                        .property("arn", r["~from"]),
                    )
                )
                .to(
                    __.V(to_id)
                    .fold()
                    .coalesce(
                        __.unfold(),
                        __.addV(self.parse_arn(r["~to"])["resource"])
                        .property(T.id, to_id)
                        .property("scan_id", scan_id)
                        .property("arn", r["~to"]),
                    )
                )
            )
            cnt += 1
            if cnt % 100 == 0 or cnt == len(edges):
                try:
                    self.logger.info(
                        event=LogEvent.NeptunePeriodicWrite,
                        msg=f"Writing edges {cnt} of {len(edges)}",
                    )
                    t.next()
                    t = g
                except Exception as err:
                    self.logger.error(event=LogEvent.NeptuneLoadError, msg=str(err))
                    raise NeptuneLoadGraphException(
                        f"Error loading edge {r} " f"with {str(t.bytecode)}"
                    )

    @staticmethod
    def parse_arn(arn: str) -> Dict:
        """
        Parses an ARN into the component pieces
        :param arn: The arn to parse
        :return: A dictionary of the arn pieces
        """
        # http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
        elements = str(arn).split(":", 5)
        result = {}
        if len(elements) == 6:
            result = {
                "arn": elements[0],
                "partition": elements[1],
                "service": elements[2],
                "region": elements[3],
                "account": elements[4],
                "resource": elements[5],
                "resource_type": None,
            }
        else:
            result["resource"] = str(arn)
        if "/" in str(result["resource"]):
            result["resource_type"], result["resource"] = str(result["resource"]).split("/", 1)
        elif ":" in str(result["resource"]):
            result["resource_type"], result["resource"] = str(result["resource"]).split(":", 1)

        if str(result["resource"]).startswith("ami-"):
            result["resource"] = result["resource_type"]

        return result

    def write_to_neptune_lpg(self, graph: Dict, scan_id: str) -> None:
        """
        Writes the graph to a labeled property graph
        :param scan_id: The unique string representing the scan
        :param graph: The graph to write
        :return: None
        """
        if "vertices" in graph and "edges" in graph and len(graph["vertices"]) > 0:
            g, conn = self.connect_to_gremlin()
            self.__write_vertices(g, graph["vertices"], scan_id)
            self.__write_edges(g, graph["edges"], scan_id)
            conn.close()
        else:
            raise NeptuneNoGraphsFoundException

    def write_to_neptune_rdf(self, graph: Dict) -> None:
        """
        Writes the graph to an RDF graph
        :param graph: The graph to write
        :return: None
        """

        auth = self._get_auth()
        neptune_sparql_url = self._neptune_endpoint.get_sparql_endpoint()
        triples = ""
        for subject, predicate, obj in graph:
            triples = triples + subject.n3() + " " + predicate.n3() + " " + obj.n3() + " . \n"

        insert_stmt = (
            "INSERT DATA {\n" + f"    GRAPH <{META_GRAPH_NAME}>\n" "{\n" f"{triples}" "}\n" "}\n"
        )

        resp = requests.post(neptune_sparql_url, data={"update": insert_stmt}, auth=auth)
        if resp.status_code != 200:
            raise NeptuneUpdateGraphException(
                f"Error updating graph {META_GRAPH_NAME} " f"with {insert_stmt} : {resp.text}"
            )
