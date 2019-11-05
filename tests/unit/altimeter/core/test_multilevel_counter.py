from unittest import TestCase

from altimeter.core.multilevel_counter import MultilevelCounter


class TestMultiLevelCounter(TestCase):
    def test_increment(self):
        ml_counter = MultilevelCounter()
        ml_counter.increment("foo", "boo", "goo")
        expected_data = {"count": 1, "foo": {"count": 1, "boo": {"count": 1, "goo": {"count": 1}}}}
        self.assertDictEqual(expected_data, ml_counter.to_dict())

    def test_merge_updates_self(self):
        ml_counter_self = MultilevelCounter()
        ml_counter_self.increment("foo", "boo", "goo")

        ml_counter_other = MultilevelCounter()
        ml_counter_other.increment("boo", "goo", "moo")

        ml_counter_self.merge(ml_counter_other)

        expected_data = {
            "count": 2,
            "foo": {"count": 1, "boo": {"count": 1, "goo": {"count": 1}}},
            "boo": {"count": 1, "goo": {"count": 1, "moo": {"count": 1}}},
        }

        self.assertDictEqual(expected_data, ml_counter_self.to_dict())

    def test_merge_does_not_update_other(self):
        ml_counter_self = MultilevelCounter()
        ml_counter_self.increment("foo", "boo", "goo")

        ml_counter_other = MultilevelCounter()
        ml_counter_other.increment("boo", "goo", "moo")

        ml_counter_self.merge(ml_counter_other)

        expected_data = {"count": 1, "boo": {"count": 1, "goo": {"count": 1, "moo": {"count": 1}}}}

        self.assertDictEqual(expected_data, ml_counter_other.to_dict())

    def test_from_dict(self):
        data = {
            "count": 2,
            "foo": {"count": 1, "boo": {"count": 1, "goo": {"count": 1}}},
            "boo": {"count": 1, "goo": {"count": 1, "moo": {"count": 1}}},
        }
        ml_counter = MultilevelCounter.from_dict(data)
        self.assertDictEqual(ml_counter.to_dict(), data)
