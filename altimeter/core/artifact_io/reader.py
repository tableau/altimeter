"""Classes for ArtifactReaders. An ArtifactReader reads a scan artifact dict
from something - e.g. a file, s3 key, etc."""
import abc
import io
import json
from typing import Any, Dict, Type

import boto3

from altimeter.core.artifact_io import is_s3_uri, parse_s3_uri
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent


class ArtifactReader(abc.ABC):
    """ArtifactReaders read JSON artifacts from locations - e.g. s3, filesystem, etc."""

    def read_json(self, path: str) -> Dict[str, Any]:
        """Read a json artifact

        Args:
            path: path to artifact to read

        Returns:
            artifact content
        """

    @classmethod
    def from_artifact_path(cls: Type["ArtifactReader"], artifact_path: str) -> "ArtifactReader":
        """Create an ArtifactReader based on an artifact path. This either returns a
        FileArtifactReader or an S3ArtifactReader depending on the value of artifact_path"""
        if is_s3_uri(artifact_path):
            _, key_prefix = parse_s3_uri(artifact_path)
            if key_prefix is not None:
                raise ValueError(
                    f"S3 artifact path should be s3://<bucket>, no key - got {artifact_path}"
                )
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
        if key is None:
            raise ValueError(f"Unable to read from s3 uri missing key: {path}")
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
