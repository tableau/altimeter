"""Multilevel Counter is a counter which allows counting categorized items in a hierarchical
fashion."""

from typing import Dict

from altimeter.core.base_model import BaseMutableModel


class MultilevelCounter(BaseMutableModel):
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
            >>> print(json.dumps(mc.dict(), indent=2))
            {
              "count": 4,
              "multilevel_counters": {
                "123": {
                  "count": 3,
                  "multilevel_counters": {
                    "us-east-1": {
                      "count": 3,
                      "multilevel_counters": {
                        "ec2": {
                          "count": 2,
                          "multilevel_counters": {}
                        },
                        "s3": {
                          "count": 1,
                          "multilevel_counters": {}
                        }
                      }
                    }
                  }
                },
                "456": {
                  "count": 1,
                  "multilevel_counters": {
                    "eu-west-1": {
                      "count": 1,
                      "multilevel_counters": {
                        "ecs": {
                          "count": 1,
                          "multilevel_counters": {}
                        }
                      }
                    }
                  }
                }
              }
            }
    """

    count: int = 0
    multilevel_counters: Dict[str, "MultilevelCounter"] = {}

    def increment(self, *categories: str) -> None:
        """Increment a given category tuple.

        Args:
             categories: ordered categories to increment.
        """
        self.count += 1
        if categories:
            category = categories[0]
            self.multilevel_counters.setdefault(category, MultilevelCounter()).increment(
                *categories[1:]
            )

    def merge(self, other: "MultilevelCounter") -> None:
        """Merge another MultilevelCounter into this counter.

        Args:
            other: MultilevelCounter to merge into self.
        """
        self.count += other.count
        for other_cat, other_stat in other.multilevel_counters.items():
            self.multilevel_counters.setdefault(other_cat, MultilevelCounter()).merge(other_stat)


MultilevelCounter.update_forward_refs()
