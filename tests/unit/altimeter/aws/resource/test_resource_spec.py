import inspect
from typing import Any, Type
from unittest import TestCase

from altimeter.aws.resource.resource_spec import ListFromAWSResult, AWSResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestAWSResourceSpecSubClassing(TestCase):
    def test_valid_concrete(self):
        class C(AWSResourceSpec):
            type_name = "t"
            service_name = "c"

            def list_from_aws(
                cls: Type["C"], client, account_id: str, region: str
            ) -> ListFromAWSResult:
                pass

        self.assertFalse(inspect.isabstract(C))

    def test_invalid_concrete(self):
        with self.assertRaises(TypeError):

            class C(AWSResourceSpec):
                def list_from_aws(
                    cls: Type["C"], client, account_id: str, region: str
                ) -> ListFromAWSResult:
                    pass

    def test_valid_abstract(self):
        class C(AWSResourceSpec):
            pass

        self.assertTrue(inspect.isabstract(C))


class TestSkipResourceScanFlag(TestCase):
    class TestAWSAccessor(AWSAccessor):
        def client(self, service_name: str) -> Any:
            return None

    def test_true(self):
        class TestResource(AWSResourceSpec):
            type_name = "t"
            service_name = "fakesvc"

            @classmethod
            def skip_resource_scan(
                cls: Type["TestResource"], client, account_id: str, region: str
            ) -> bool:
                return True

        accessor = TestSkipResourceScanFlag.TestAWSAccessor(None, None, None)
        TestResource.scan(scan_accessor=accessor)

    def test_false(self):
        class TestResource(AWSResourceSpec):
            type_name = "t"
            service_name = "fakesvc"

        accessor = TestSkipResourceScanFlag.TestAWSAccessor(None, None, None)
        with self.assertRaises(AttributeError):
            TestResource.scan(scan_accessor=accessor)
