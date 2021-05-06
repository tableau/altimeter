import os
from unittest import mock, TestCase
from datetime import datetime

from altimeter.qj.schemas.result_set import Result, ResultSet, ResultSetGraphSpec
from altimeter.qj.schemas.job import Job, Category, Severity, JobGraphSpec


@mock.patch.dict(os.environ, {"REGION": "us-west-2"})
class TestResultAccountIdValidators(TestCase):
    def test_result_account_id_zero_fill(self):
        result = Result(account_id="1234", result={"foo": "boo"})
        self.assertEqual(result.account_id, "000000001234")

    def test_result_account_id_is_int(self):
        with self.assertRaises(ValueError):
            Result(account_id="abcd", result={"foo": "boo"})


@mock.patch.dict(os.environ, {"REGION": "us-west-2"})
class TestResultSet(TestCase):
    def test_to_csv_returns_csv_str(self):
        result_set = ResultSet(
            job=Job(
                name="foobizz",
                description="FooBizz",
                graph_spec=JobGraphSpec(graph_names=["test"]),
                category=Category.gov,
                severity=Severity.debug,
                query="select ?foo ?fizz where { ?foo ?fizz } order by ?foo",
                active=True,
                created=datetime(2020, 1, 1),
                query_fields=["account_id", "account_name"],
                max_graph_age_sec=10000,
                result_expiration_sec=100000,
                max_result_age_sec=100000,
                notify_if_results=False,
            ),
            graph_spec=ResultSetGraphSpec(
                graph_uris_load_times={"https://alti/alti/1/1234": 1612974818}
            ),
            results=[
                Result(account_id="123456789101", result={"foo": "boo", "fizz": "bizz"}),
                Result(account_id="123456789101", result={"foo": "boo2", "fizz": "bizz2"}),
            ],
        )
        expected_csv = "account_id,foo,fizz\n123456789101,boo,bizz\n123456789101,boo2,bizz2\n"
        self.assertEqual(expected_csv, result_set.to_csv())

    def test_empty_results_to_csv_returns_csv_str(self):
        result_set = ResultSet(
            job=Job(
                name="foobizz",
                description="FooBizz",
                graph_spec=JobGraphSpec(graph_names=["test"]),
                category=Category.gov,
                severity=Severity.debug,
                query="select ?foo ?fizz where { ?foo ?fizz } order by ?foo",
                active=True,
                created=datetime(2020, 1, 1),
                query_fields=["account_id", "account_name"],
                max_graph_age_sec=10000,
                result_expiration_sec=100000,
                max_result_age_sec=100000,
                notify_if_results=False,
            ),
            graph_spec=ResultSetGraphSpec(
                graph_uris_load_times={"https://alti/alti/1/1234": 1612974818}
            ),
            results=[],
        )
        expected_csv = ""
        self.assertEqual(expected_csv, result_set.to_csv())
