from typing import Any, Dict, List
from unittest import TestCase

from altimeter.core.graph.field.base import SubField
from altimeter.core.graph.field.exceptions import (
    ParentKeyMissingException,
    InvalidParentKeyException,
)
from altimeter.core.graph.links import BaseLink


class TestField(SubField):
    def parse(self, data: Any, context: Dict[str, Any]) -> List[BaseLink]:
        raise NotImplementedError()


class TestSubField(TestCase):
    def test_missing_parent_alti_key(self):
        test_field = TestField()
        with self.assertRaises(ParentKeyMissingException):
            test_field.get_parent_alti_key(data={}, context={})

    def test_nonstr_parent_alti_key(self):
        test_field = TestField()
        with self.assertRaises(InvalidParentKeyException):
            test_field.get_parent_alti_key(data={}, context={"parent_alti_key": TestField()})
