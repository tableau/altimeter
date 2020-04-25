import json
import os
from pathlib import Path
import tempfile
import unittest

import boto3
import moto

from altimeter.core.artifact_io.writer import ArtifactWriter, FileArtifactWriter, S3ArtifactWriter

class TestArtifactWriter(unittest.TestCase):
    def test_from_artifact_path_s3(self):
        writer = ArtifactWriter.from_artifact_path(artifact_path="s3://bucket",
                                                   scan_id="test-scan-id")
        self.assertIsInstance(writer, S3ArtifactWriter)

    def test_from_artifact_path_filepath(self):
        writer = ArtifactWriter.from_artifact_path(artifact_path="/file/path",
                                                   scan_id="test-scan-id")
        self.assertIsInstance(writer, FileArtifactWriter)


class TestFileArtifactReader(unittest.TestCase):
    def test_with_valid_data(self):
        data = {"foo": "boo"}
        scan_id = 'test-scan-id'
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_writer = FileArtifactWriter(scan_id=scan_id, output_dir=Path(temp_dir))
            artifact_writer.write_json("test_name", data)
            path = os.path.join(temp_dir, 'test-scan-id', "test_name.json")
            with open(path, "r") as fp:
                written_data = json.load(fp)
        self.assertDictEqual(written_data, data)


class TestS3ArtifactWriter(unittest.TestCase):
    @moto.mock_s3
    def test_with_valid_object(self):
        data = {"foo": "boo"}
        scan_id = 'test-scan-id'
        s3_client = boto3.Session().client("s3")
        s3_client.create_bucket(Bucket="test_bucket")
        artifact_writer = S3ArtifactWriter(bucket="test_bucket", key_prefix=scan_id)
        artifact_writer.write_json("test_name", data)
        resp = s3_client.get_object(Bucket="test_bucket", Key="test-scan-id/test_name.json")
        written_data = json.load(resp["Body"])
        self.assertDictEqual(data, written_data)
