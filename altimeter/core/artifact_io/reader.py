"""Classes for ArtifactReaders. An ArtifactReader reads a scan artifact dict
from something - e.g. a file, s3 key, etc."""
import abc
import io
import json
from typing import Any, Dict, Tuple

import boto3

from altimeter.core.log import LogEvent, Logger
from altimeter.core.artifact_io.exceptions import InvalidS3URIException


class ArtifactReader(abc.ABC):
    """ArtifactReaders read JSON artifacts from locations - e.g. s3, filesystem, etc."""

    @abc.abstractmethod
    def read_artifact(self, artifact_path: str) -> Dict[str, Any]:
        """Read an artifact

        Args:
            artifact_path: path to artifact to read

        Returns:
            artifact content
        """


class FileArtifactReader(ArtifactReader):
    """ArtifactReader to read from the filesystem"""

    def read_artifact(self, artifact_path: str) -> Dict[str, Any]:
        """Read an artifact

        Args:
            artifact_path: filesystem path to artifact

        Returns:
            artifact content
        """
        logger = Logger()
        with logger.bind(artifact_path=artifact_path):
            logger.info(event=LogEvent.ReadFromFSStart)
            with open(artifact_path, "r") as artifact_fp:
                data = json.load(artifact_fp)
            logger.info(event=LogEvent.ReadFromFSEnd)
            return data


class S3ArtifactReader(ArtifactReader):
    """ArtifactReader to read from S3"""

    def read_artifact(self, artifact_path: str) -> Dict[str, Any]:
        """Read an artifact

        Args:
            artifact_path: s3 uri to artifact. s3://bucket/key/path

        Returns:
            artifact content
        """
        bucket, key = parse_s3_uri(artifact_path)
        session = boto3.Session()
        s3_client = session.client("s3")
        logger = Logger()
        with io.BytesIO() as artifact_bytes_buf:
            with logger.bind(bucket=bucket, key=key):
                logger.info(event=LogEvent.ReadFromS3Start)
                s3_client.download_fileobj(bucket, key, artifact_bytes_buf)
                artifact_bytes_buf.flush()
                artifact_bytes_buf.seek(0)
                artifact_bytes = artifact_bytes_buf.read()
                logger.info(event=LogEvent.ReadFromS3End)
                artifact_str = artifact_bytes.decode("utf-8")
                artifact_dict = json.loads(artifact_str)
                return artifact_dict


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse an s3 uri (s3://bucket/key/path) into bucket and key parts

    Args:
        uri: s3 uri (s3://bucket/key/path)

    Returns:
        Tuple of (bucket, key)

    Raises:
        :class:`InvalidS3URIException` if the uri
        argument is not a valid S3 URI.
    """
    s3_uri_prefix = "s3://"
    if not uri.startswith(s3_uri_prefix):
        raise InvalidS3URIException("S3 URIs should begin with 's3://")
    uri = uri.rstrip("/")
    parts = uri[len(s3_uri_prefix) :].split("/")
    if len(parts) < 2:
        raise InvalidS3URIException(f"{uri} missing key portion")
    bucket = parts[0]
    if not bucket:
        raise InvalidS3URIException(f"Bad bucket portion in uri {uri}")
    key_parts = [part.rstrip("/ ") for part in parts[1:]]
    if not all(key_parts):
        raise InvalidS3URIException(f"Bad key portion in uri {uri}")
    key = "/".join(key_parts)
    return bucket, key
