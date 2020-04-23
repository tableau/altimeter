from unittest import TestCase

from altimeter.core.config import (
    InvalidConfigException,
    get_optional_section,
    get_required_list_param,
    get_required_bool_param,
    get_required_int_param,
    get_required_section,
    get_required_str_param,
)

class TestGetRequiredListParam(TestCase):
    def test_present(self):
        config_dict = {"foo": ["ab", "cd"]}
        value = get_required_list_param("foo", config_dict)
        self.assertEqual(value, ("ab", "cd"))

    def test_absent(self):
        config_dict = {"abcd": "foo"}
        with self.assertRaises(InvalidConfigException):
            get_required_list_param("foo", config_dict)

    def test_nonlist(self):
        config_dict = {"foo": "abcd"}
        with self.assertRaises(InvalidConfigException):
            get_required_list_param("foo", config_dict)

class TestGetRequiredBoolParam(TestCase):
    def test_present(self):
        config_dict = {"foo": True}
        value = get_required_bool_param("foo", config_dict)
        self.assertEqual(value, True)

    def test_absent(self):
        config_dict = {"abcd": True}
        with self.assertRaises(InvalidConfigException):
            get_required_bool_param("foo", config_dict)

    def test_nonbool(self):
        config_dict = {"foo": "abcd"}
        with self.assertRaises(InvalidConfigException):
            get_required_bool_param("foo", config_dict)

class TestGetRequiredIntParam(TestCase):
    def test_present(self):
        config_dict = {"foo": 1}
        value = get_required_int_param("foo", config_dict)
        self.assertEqual(value, 1)

    def test_absent(self):
        config_dict = {"abcd": 1}
        with self.assertRaises(InvalidConfigException):
            get_required_int_param("foo", config_dict)

    def test_nonint(self):
        config_dict = {"foo": "abcd"}
        with self.assertRaises(InvalidConfigException):
            get_required_int_param("foo", config_dict)

class TestGetRequiredStrParam(TestCase):
    def test_present(self):
        config_dict = {"foo": "abcd"}
        value = get_required_str_param("foo", config_dict)
        self.assertEqual(value, "abcd")

    def test_absent(self):
        config_dict = {"abcd": "foo"}
        with self.assertRaises(InvalidConfigException):
            get_required_str_param("foo", config_dict)

    def test_nonstr(self):
        config_dict = {"foo": 1}
        with self.assertRaises(InvalidConfigException):
            get_required_str_param("foo", config_dict)

class TestGetRequiredSection(TestCase):
    def test_present(self):
        config_dict = {"SectionA": {"foo": "boo"}}
        section = get_required_section("SectionA", config_dict)
        self.assertDictEqual(section, {"foo": "boo"})

    def test_absent(self):
        config_dict = {"SectionA": {"foo": "boo"}}
        with self.assertRaises(InvalidConfigException):
            get_required_section("SectionB", config_dict)

    def test_nondict(self):
        config_dict = {"SectionA": "foo"}
        with self.assertRaises(InvalidConfigException):
            get_required_section("SectionA", config_dict)

class TestGetRequiredSection(TestCase):
    def test_present(self):
        config_dict = {"SectionA": {"foo": "boo"}}
        section = get_required_section("SectionA", config_dict)
        self.assertDictEqual(section, {"foo": "boo"})

    def test_absent(self):
        config_dict = {"SectionA": {"foo": "boo"}}
        with self.assertRaises(InvalidConfigException):
            get_required_section("SectionB", config_dict)

    def test_nondict(self):
        config_dict = {"SectionA": "foo"}
        with self.assertRaises(InvalidConfigException):
            get_required_section("SectionA", config_dict)

class TestGetOptionalSection(TestCase):
    def test_present(self):
        config_dict = {"SectionA": {"foo": "boo"}}
        section = get_optional_section("SectionA", config_dict)
        self.assertDictEqual(section, {"foo": "boo"})

    def test_absent(self):
        config_dict = {"SectionA": {"foo": "boo"}}
        section = get_optional_section("SectionB", config_dict)
        self.assertIsNone(section)

    def test_nondict(self):
        config_dict = {"SectionA": "foo"}
        with self.assertRaises(InvalidConfigException):
            get_optional_section("SectionA", config_dict)
