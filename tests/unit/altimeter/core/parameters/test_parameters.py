import mock
import os
import unittest

from altimeter.core.parameters import get_required_str_env_var, get_required_int_env_var
from altimeter.core.parameters import get_required_lambda_event_var
from altimeter.core.parameters.exceptions import (
    RequiredEnvironmentVariableNotPresentException,
    RequiredEventVariableNotPresentException,
)


class TestGetRequiredStrEnvVar(unittest.TestCase):
    @mock.patch.dict(os.environ, {"TEST_KEY_123": "foo"})
    def test_with_value_present(self):
        value = get_required_str_env_var("TEST_KEY_123")
        self.assertEqual(value, "foo")

    @mock.patch.dict(os.environ, {})
    def test_with_value_absent(self):
        with self.assertRaises(RequiredEnvironmentVariableNotPresentException):
            get_required_str_env_var("TEST_KEY_123")

class TestGetRequiredIntEnvVar(unittest.TestCase):
    @mock.patch.dict(os.environ, {"TEST_KEY_123": "1"})
    def test_with_int_value_present(self):
        value = get_required_int_env_var("TEST_KEY_123")
        self.assertEqual(value, 1)

    @mock.patch.dict(os.environ, {})
    def test_with_value_absent(self):
        with self.assertRaises(RequiredEnvironmentVariableNotPresentException):
            get_required_int_env_var("TEST_KEY_123")

    @mock.patch.dict(os.environ, {"TEST_KEY_123": "abcd"})
    def test_with_nonint_value_present(self):
        with self.assertRaises(ValueError):
            get_required_int_env_var("TEST_KEY_123")

class TestGetRequiredLambdaEventVar(unittest.TestCase):
    def test_with_value_present(self):
        event = {"TEST_KEY_123": "foo"}
        value = get_required_lambda_event_var(event, "TEST_KEY_123")
        self.assertEqual(value, "foo")

    def test_with_value_absent(self):
        event = {"TEST_KEY_456": "foo"}
        with self.assertRaises(RequiredEventVariableNotPresentException):
            get_required_lambda_event_var(event, "TEST_KEY_123")
