from unittest import TestCase

from altimeter.core.multilevel_counter import MultilevelCounter


class TestMultiLevelCounter(TestCase):
    def test_increment(self):
        ml_counter = MultilevelCounter()
        ml_counter.increment("foo", "boo", "goo")
        expected_ml_counter = MultilevelCounter(
            count=1,
            multilevel_counters={
                "foo": MultilevelCounter(
                    count=1,
                    multilevel_counters={
                        "boo": MultilevelCounter(
                            count=1,
                            multilevel_counters={
                                "goo": MultilevelCounter(count=1, multilevel_counters={})
                            },
                        )
                    },
                )
            },
        )
        self.assertEqual(ml_counter, expected_ml_counter)

    def test_merge_updates_self(self):
        ml_counter_self = MultilevelCounter()
        ml_counter_self.increment("foo", "boo", "goo")

        ml_counter_other = MultilevelCounter()
        ml_counter_other.increment("boo", "goo", "moo")

        ml_counter_self.merge(ml_counter_other)

        expected_ml_counter_self = MultilevelCounter(
            count=2,
            multilevel_counters={
                "foo": MultilevelCounter(
                    count=1,
                    multilevel_counters={
                        "boo": MultilevelCounter(
                            count=1,
                            multilevel_counters={
                                "goo": MultilevelCounter(count=1, multilevel_counters={})
                            },
                        )
                    },
                ),
                "boo": MultilevelCounter(
                    count=1,
                    multilevel_counters={
                        "goo": MultilevelCounter(
                            count=1,
                            multilevel_counters={
                                "moo": MultilevelCounter(count=1, multilevel_counters={})
                            },
                        )
                    },
                ),
            },
        )
        self.assertEqual(ml_counter_self, expected_ml_counter_self)

    def test_merge_does_not_update_other(self):
        ml_counter_self = MultilevelCounter()
        ml_counter_self.increment("foo", "boo", "goo")

        ml_counter_other = MultilevelCounter()
        ml_counter_other.increment("boo", "goo", "moo")

        ml_counter_self.merge(ml_counter_other)

        expected_ml_counter_other = MultilevelCounter(
            count=1,
            multilevel_counters={
                "boo": MultilevelCounter(
                    count=1,
                    multilevel_counters={
                        "goo": MultilevelCounter(
                            count=1,
                            multilevel_counters={
                                "moo": MultilevelCounter(count=1, multilevel_counters={})
                            },
                        )
                    },
                )
            },
        )

        self.assertEqual(ml_counter_other, expected_ml_counter_other)
