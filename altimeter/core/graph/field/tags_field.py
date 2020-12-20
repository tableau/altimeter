"""Tags Fields represent AWS Tags."""
from typing import Dict, Any, List

from altimeter.core.graph.field.base import Field
from altimeter.core.graph.field.exceptions import TagsFieldMissingTagsKeyException
from altimeter.core.graph.links import LinkCollection, TagLink


class TagsField(Field):
    """A TagsField is a field used to parse data in the form

        {'Tags': [{'Key': 'tag_key', 'Value': 'tag_value'}, ...]}

    Examples:
        >>> input = {"Tags": [{"Key": "Name", "Value": "Jerry"}, \
                              {"Key": "DOB", "Value": "1942-08-01"}]}
        >>> field = TagsField()
        >>> link_collection = field.parse(data=input, context={})
        >>> print(link_collection.dict(exclude_unset=True))
        {'tag_links': ({'pred': 'Name', 'obj': 'Jerry'}, {'pred': 'DOB', 'obj': '1942-08-01'})}

    Args:
        optional: Whether this key is optional. Defaults to False.
    """

    def __init__(self, optional: bool = True):
        self.optional = optional

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a list of Links.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            List of TagLink objects, one for each tag.
        """
        links: List[TagLink] = []
        tag_dicts = data.get("Tags")
        if tag_dicts:
            for tag_dict in tag_dicts:
                links.append(TagLink(pred=tag_dict["Key"], obj=tag_dict["Value"]))
            return LinkCollection(tag_links=links)
        if self.optional:
            return LinkCollection()
        raise TagsFieldMissingTagsKeyException(f"Expected key 'Tags' in {data}")
