"""Classes for ArtifactWriters. An ArtifactWriter writes a scan artifact dict
to something - e.g. a file, s3 key, etc."""
import abc
import io
import json
import os
from pathlib import Path
from typing import Any, Dict

import boto3

from altimeter.core.json_encoder import json_encoder
from altimeter.core.log import LogEvent, Logger


class ArtifactWriter(abc.ABC):
    """ArtifactReaders write JSON artifacts to locations - e.g. s3, filesystem, etc."""

    @abc.abstractmethod
    def write_artifact(self, name: str, data: Dict[str, Any]) -> str:
        """Read an artifact

        Args:
            name: artifact name
            data: artifact data

        Returns:
            path to written artifact
        """


class FileArtifactWriter(ArtifactWriter):
    """ArtifactWriter which writes to a file.

    Args:
         output_dir: output filesystem dir
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir

    def write_artifact(self, name: str, data: Dict[str, Any]) -> str:
        """Write artifact data to self.output_dir/name.json

        Args:
            name: filename
            data: artifact data

        Returns:
            Full filesystem path of artifact file
        """
        logger = Logger()
        os.makedirs(self.output_dir, exist_ok=True)
        artifact_path = os.path.join(self.output_dir, f"{name}.json")
        with logger.bind(artifact_path=artifact_path):
            logger.info(event=LogEvent.WriteToFSStart)
            with open(artifact_path, "w") as artifact_fp:
                json.dump(data, artifact_fp, default=json_encoder)
            logger.info(event=LogEvent.WriteToFSEnd)
        return artifact_path


class S3ArtifactWriter(ArtifactWriter):
    """ArtifactWriter which writes to S3.

    Args:
        bucket: s3 bucket
        key_prefix: s3 key prefix
    """

    def __init__(self, bucket: str, key_prefix: str):
        self.bucket = bucket
        self.key_prefix = key_prefix

    def write_artifact(self, name: str, data: Dict[str, Any]) -> str:
        """Write artifact data to s3://self.bucket/self.key_prefix/name.json

        Args:
            name: s3 key name
            data: artifact data

        Returns:
            S3 uri (s3://bucket/key/path) to artifact
        """

        output_key = "/".join((self.key_prefix, f"{name}.json"))
        logger = Logger()
        with logger.bind(bucket=self.bucket, key=output_key):
            logger.info(event=LogEvent.WriteToS3Start)
            s3_client = boto3.Session().client("s3")
            results_str = json.dumps(data, default=json_encoder)
            results_bytes = results_str.encode("utf-8")
            with io.BytesIO(results_bytes) as results_bytes_stream:
                s3_client.upload_fileobj(results_bytes_stream, self.bucket, output_key)
            logger.info(event=LogEvent.WriteToS3End)
        return f"s3://{self.bucket}/{output_key}"
