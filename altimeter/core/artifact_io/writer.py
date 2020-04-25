"""Classes for ArtifactWriters. An ArtifactWriter writes a scan artifact dict
to something - e.g. a file, s3 key, etc."""
import abc
import io
import gzip
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Type

import boto3

from altimeter.core.artifact_io import is_s3_uri, parse_s3_uri
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.json_encoder import json_encoder
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent

GZIP = "gz"


class ArtifactWriter(abc.ABC):
    """ArtifactWriters write JSON artifacts to locations - e.g. s3, filesystem, etc."""

    @abc.abstractmethod
    def write_json(self, name: str, data: Dict[str, Any]) -> str:
        """Write a json artifact

        Args:
            name: name
            data: data

        Returns:
            path to written artifact
        """

    @abc.abstractmethod
    def write_graph_set(
        self, name: str, graph_set: GraphSet, compression: Optional[str] = None
    ) -> str:
        """Write a graph artifact

        Args:
            name: name
            graph_set: GraphSet object to write

        Returns:
            path to written artifact
        """

    @classmethod
    def from_artifact_path(
        cls: Type["ArtifactWriter"], artifact_path: str, scan_id: str
    ) -> "ArtifactWriter":
        """Create an ArtifactWriter based on an artifact path. This either returns a FileArtifactWriter
        or an S3ArtifactWriter depending on the value of artifact_path"""
        if is_s3_uri(artifact_path):
            bucket, key_prefix = parse_s3_uri(artifact_path)
            if key_prefix is not None:
                raise ValueError(
                    f"S3 artifact path should be s3://<bucket>, no key - got {artifact_path}"
                )
            return S3ArtifactWriter(bucket=bucket, key_prefix=scan_id)
        return FileArtifactWriter(scan_id=scan_id, output_dir=Path(artifact_path))


class FileArtifactWriter(ArtifactWriter):
    """ArtifactWriter which writes to a file.

    Args:
         output_dir: output filesystem dir
    """

    def __init__(self, scan_id: str, output_dir: Path):
        self.output_dir = output_dir.joinpath(scan_id)

    def write_json(self, name: str, data: Dict[str, Any]) -> str:
        """Write artifact data to self.output_dir/name.json

        Args:
            name: filename
            data: data

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

    def write_graph_set(
        self, name: str, graph_set: GraphSet, compression: Optional[str] = None
    ) -> str:
        """Write a graph artifact

        Args:
            name: name
            graph_set: GraphSet object to write

        Returns:
            path to written artifact
        """
        logger = Logger()
        os.makedirs(self.output_dir, exist_ok=True)
        if compression is None:
            artifact_path = os.path.join(self.output_dir, f"{name}.rdf")
        elif compression == GZIP:
            artifact_path = os.path.join(self.output_dir, f"{name}.rdf.gz")
        else:
            raise ValueError(f"Unknown compression arg {compression}")
        graph = graph_set.to_rdf()
        with logger.bind(artifact_path=artifact_path):
            logger.info(event=LogEvent.WriteToFSStart)
            with open(artifact_path, "wb") as fp:
                if compression is None:
                    graph.serialize(fp)
                elif compression == GZIP:
                    with gzip.GzipFile(fileobj=fp, mode="wb") as gz:
                        graph.serialize(gz)
                else:
                    raise ValueError(f"Unknown compression arg {compression}")
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

    def write_json(self, name: str, data: Dict[str, Any]) -> str:
        """Write artifact data to s3://self.bucket/self.key_prefix/name.json

        Args:
            name: s3 key name
            data: data

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

    def write_graph_set(
        self, name: str, graph_set: GraphSet, compression: Optional[str] = None
    ) -> str:
        """Write a graph artifact

        Args:
            name: name
            graph_set: GraphSet to write

        Returns:
            path to written artifact
        """
        logger = Logger()
        if compression is None:
            key = f"{name}.rdf"
        elif compression == GZIP:
            key = f"{name}.rdf.gz"
        else:
            raise ValueError(f"Unknown compression arg {compression}")
        output_key = "/".join((self.key_prefix, key))
        graph = graph_set.to_rdf()
        with logger.bind(bucket=self.bucket, key_prefix=self.key_prefix, key=key):
            logger.info(event=LogEvent.WriteToS3Start)
            with io.BytesIO() as rdf_bytes_buf:
                if compression is None:
                    graph.serialize(rdf_bytes_buf)
                elif compression == GZIP:
                    with gzip.GzipFile(fileobj=rdf_bytes_buf, mode="wb") as gz:
                        graph.serialize(gz)
                else:
                    raise ValueError(f"Unknown compression arg {compression}")
                rdf_bytes_buf.flush()
                rdf_bytes_buf.seek(0)
                session = boto3.Session()
                s3_client = session.client("s3")
                s3_client.upload_fileobj(rdf_bytes_buf, self.bucket, output_key)
            s3_client.put_object_tagging(
                Bucket=self.bucket,
                Key=output_key,
                Tagging={
                    "TagSet": [
                        {"Key": "name", "Value": graph_set.name},
                        {"Key": "version", "Value": graph_set.version},
                        {"Key": "start_time", "Value": str(graph_set.start_time)},
                        {"Key": "end_time", "Value": str(graph_set.end_time)},
                    ]
                },
            )
            logger.info(event=LogEvent.WriteToS3End)
        return f"s3://{self.bucket}/{output_key}"
