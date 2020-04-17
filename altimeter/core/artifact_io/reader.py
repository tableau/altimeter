"""Classes for ArtifactReaders. An ArtifactReader reads a scan artifact dict
from something - e.g. a file, s3 key, etc."""
import abc
import io
import json
from typing import Any, Dict, Type

import boto3

from altimeter.core.artifact_io import is_s3_uri, parse_s3_uri
from altimeter.core.config import Config
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.rdf import GraphPackage


class ArtifactReader(abc.ABC):
    """ArtifactReaders read JSON artifacts from locations - e.g. s3, filesystem, etc."""

    def read_json(self, path: str) -> Dict[str, Any]:
        """Read a json artifact

        Args:
            path: path to artifact to read

        Returns:
            artifact content
        """

    def read_graph_pkg(self, path: str) -> GraphPackage:
        """Read a graph and return a GraphPackage

        Args:
            path: path to artifact to read

        Returns:
            GraphPackage object
        """

    @classmethod
    def from_config(cls: Type["ArtifactReader"], config: Config) -> "ArtifactReader":
        """Create an ArtifactReader based on a config. This either returns a FileArtifactReader
        or an S3ArtifactReader depending on the value of Config.artifact_path"""
        if is_s3_uri(config.artifact_path):
            return S3ArtifactReader()
        return FileArtifactReader()


class FileArtifactReader(ArtifactReader):
    """ArtifactReader to read from the filesystem"""

    def read_json(self, path: str) -> Dict[str, Any]:
        """Read a json artifact

        Args:
            path: filesystem path to artifact

        Returns:
            artifact content
        """
        logger = Logger()
        with logger.bind(artifact_path=path):
            logger.info(event=LogEvent.ReadFromFSStart)
            with open(path, "r") as artifact_fp:
                data = json.load(artifact_fp)
            logger.info(event=LogEvent.ReadFromFSEnd)
            return data

    def read_graph_pkg(self, path: str) -> GraphPackage:
        """Read a graph and return a GraphPackage

        Args:
            path: path to artifact to read

        Returns:
            GraphPackage object
        """
        with open(path, "r") as fp:
            graph_set_dict = json.load(fp)
        graph_set = GraphSet.from_dict(data=graph_set_dict)
        return GraphPackage(
            graph=graph_set.to_rdf(),
            name=graph_set.name,
            version=graph_set.version,
            start_time=graph_set.start_time,
            end_time=graph_set.end_time,
        )


class S3ArtifactReader(ArtifactReader):
    """ArtifactReader to read from S3"""

    def read_json(self, path: str) -> Dict[str, Any]:
        """Read a json artifact

        Args:
            path: s3 uri to artifact. s3://bucket/key/path

        Returns:
            artifact content
        """
        bucket, key = parse_s3_uri(path)
        session = boto3.Session()
        s3_client = session.client("s3")
        logger = Logger()
        with logger.bind(bucket=bucket, key=key):
            with io.BytesIO() as artifact_bytes_buf:
                logger.info(event=LogEvent.ReadFromS3Start)
                s3_client.download_fileobj(bucket, key, artifact_bytes_buf)
                artifact_bytes_buf.flush()
                artifact_bytes_buf.seek(0)
                artifact_bytes = artifact_bytes_buf.read()
                logger.info(event=LogEvent.ReadFromS3End)
                artifact_str = artifact_bytes.decode("utf-8")
                artifact_dict = json.loads(artifact_str)
                return artifact_dict

    def read_graph_pkg(self, path: str) -> GraphPackage:
        """Read a graph and return a GraphPackage

        Args:
            path: s3 uri to artifact. s3://bucket/key/path

        Returns:
            GraphPackage object
        """
        bucket, key = parse_s3_uri(path)
        session = boto3.Session()
        s3_client = session.client("s3")
        logger = Logger()
        with logger.bind(bucket=bucket, key=key):
            logger.info(event=LogEvent.ReadFromS3Start)
            with io.BytesIO() as bytes_buf:
                s3_client.download_fileobj(bucket, key, bytes_buf)
                bytes_buf.flush()
                bytes_buf.seek(0)
                graph_set_bytes = bytes_buf.read()
                logger.info(event=LogEvent.ReadFromS3End)
            graph_set_str = graph_set_bytes.decode("utf-8")
            graph_set_dict = json.loads(graph_set_str)
            graph_set = GraphSet.from_dict(graph_set_dict)
            return GraphPackage(
                graph=graph_set.to_rdf(),
                name=graph_set.name,
                version=graph_set.version,
                start_time=graph_set.start_time,
                end_time=graph_set.end_time,
            )
