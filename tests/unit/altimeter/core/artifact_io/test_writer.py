import json
import os
import tempfile
import unittest

import boto3
import moto

from altimeter.core.artifact_io.writer import FileArtifactWriter, S3ArtifactWriter


class TestFileArtifactReader(unittest.TestCase):
    def test_with_valid_data(self):
        data = {"foo": "boo"}
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_writer = FileArtifactWriter(output_dir=temp_dir)
            artifact_writer.write_artifact("test_name", data)
            path = os.path.join(temp_dir, "test_name.json")
            with open(path, "r") as fp:
                written_data = json.load(fp)
        self.assertDictEqual(written_data, data)


class TestS3ArtifactWriter(unittest.TestCase):
    @moto.mock_s3
    def test_with_valid_object(self):
        data = {"foo": "boo"}
        s3_client = boto3.Session().client("s3")
        s3_client.create_bucket(Bucket="test_bucket")
        artifact_writer = S3ArtifactWriter(bucket="test_bucket", key_prefix="foo/boo")
        artifact_writer.write_artifact("test_name", data)
        resp = s3_client.get_object(Bucket="test_bucket", Key="foo/boo/test_name.json")
        written_data = json.load(resp["Body"])
        self.assertDictEqual(data, written_data)
