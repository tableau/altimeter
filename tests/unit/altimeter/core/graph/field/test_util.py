from unittest import TestCase

from altimeter.core.graph.field.util import camel_case_to_snake_case


class TestCamelCaseToSnakeCase(TestCase):
    def test_snake_case_input(self):
        test_str = "snake_case_input"
        self.assertEqual(test_str, camel_case_to_snake_case(test_str))

    def test_camel_case_input(self):
        test_str = "CamelCaseInput"
        expected_out = "camel_case_input"
        self.assertEqual(expected_out, camel_case_to_snake_case(test_str))
