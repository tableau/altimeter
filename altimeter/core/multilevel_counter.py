"""Multilevel Counter is a counter which allows counting categorized items in a hierarchical
fashion."""

from collections import defaultdict
from typing import Any, DefaultDict, Dict, Type, Union


class MultilevelCounter:
    """Multilevel Counter is a counter which allows counting categorized items in a hierarchical
    fashion.

    Examples:
        For example if you are counting API calls to made to AWS accounts/regions/services,
        the following could be used to count:

            >>> mc = MultilevelCounter()
            >>> # call to account 123, region us-east-1, service ec2
            >>> mc.increment('123', 'us-east-1', 'ec2')
            >>> # another call to account 123, region us-east-1, service ec2
            >>> mc.increment('123', 'us-east-1', 'ec2')
            >>> # call to account 123, region us-east-1 service s3
            >>> mc.increment('123', 'us-east-1', 's3')
            >>> # call to account 456, region eu-west-1, service ecs
            >>> mc.increment('456', 'eu-west-1', 'ecs')

            >>> # The output below reflects that we made a total of 4 calls,
            >>> # 3 to account 123, 1 to account 456, and so on.
            >>> import json
            >>> print(json.dumps(mc.to_dict(), indent=2))
            {
              "count": 4,
              "123": {
                "count": 3,
                "us-east-1": {
                  "count": 3,
                  "ec2": {
                    "count": 2
                  },
                  "s3": {
                    "count": 1
                  }
                }
              },
              "456": {
                "count": 1,
                "eu-west-1": {
                  "count": 1,
                  "ecs": {
                    "count": 1
                  }
                }
              }
            }
    """

    def __init__(self) -> None:
        self.count = 0
        self.multilevel_counters: DefaultDict[str, MultilevelCounter] = defaultdict(
            MultilevelCounter
        )

    def increment(self, *categories: str) -> None:
        """Increment a given category tuple.

        Args:
             categories: ordered categories to increment.
        """
        self.count += 1
        if categories:
            category = categories[0]
            self.multilevel_counters[category].increment(*categories[1:])

    def merge(self, other: "MultilevelCounter") -> None:
        """Merge another MultilevelCounter into this counter.

        Args:
            other: MultilevelCounter to merge into self.
        """
        self.count += other.count
        for other_cat, other_stat in other.multilevel_counters.items():
            self.multilevel_counters[other_cat].merge(other_stat)

    def to_dict(self) -> Dict[str, Union[int, Dict[str, Any]]]:
        """Convert the contents of this MultilevelCounter to a dict.

        Returns:
            dict representation of this counter.
        """
        data: Dict[str, Union[int, Dict[str, Any]]] = {"count": self.count}
        for cat, stat in self.multilevel_counters.items():
            data[cat] = stat.to_dict()
        return data

    @classmethod
    def from_dict(
        cls: Type["MultilevelCounter"], stats_data: Dict[str, Any]
    ) -> "MultilevelCounter":
        """Create a MultilevelCounter from a dict.

        Args:
            stats_data: dict representing MultilevelCounter data

        Returns:
            MultilevelCounter
        """
        count = 0
        multilevel_counters: DefaultDict[str, MultilevelCounter]
        multilevel_counters = defaultdict(MultilevelCounter)
        for key, val in stats_data.items():
            if key == "count":
                count = val
            else:
                multilevel_counters[key] = cls.from_dict(val)
        obj = cls()
        obj.count = count
        obj.multilevel_counters = multilevel_counters
        return obj
