from unittest import TestCase

from altimeter.core.graph.node_cache import NodeCache


class TestNodeCache(TestCase):
    def test_setitem_new_key(self):
        node_cache = NodeCache()
        node_cache["foo"] = "boo"
        self.assertEqual(node_cache["foo"], "boo")

    def test_setitem_existing_key(self):
        node_cache = NodeCache()
        node_cache["foo"] = "boo"
        with self.assertRaises(KeyError):
            node_cache["foo"] = "goo"
