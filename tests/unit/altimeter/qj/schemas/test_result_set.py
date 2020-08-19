import os
from unittest import mock, TestCase

from altimeter.qj.schemas.result_set import Result


@mock.patch.dict(os.environ, {"REGION": "us-west-2"})
class TestResultAccountIdValidators(TestCase):
    def test_result_account_id_zero_fill(self):
        result = Result(account_id="1234", result={"foo": "boo"})
        self.assertEqual(result.account_id, "000000001234")

    def test_result_account_id_is_int(self):
        with self.assertRaises(ValueError):
            Result(account_id="abcd", result={"foo": "boo"})
