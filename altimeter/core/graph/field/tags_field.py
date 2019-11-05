"""Tags Fields represent AWS Tags."""
from typing import Dict, Any, List

from altimeter.core.graph.field.base import Field
from altimeter.core.graph.field.exceptions import TagsFieldMissingTagsKeyException
from altimeter.core.graph.link.links import TagLink
from altimeter.core.graph.link.base import Link


class TagsField(Field):
    """A TagsField is a field used to parse data in the form

        {'Tags': [{'Key': 'tag_key', 'Value': 'tag_value'}, ...]}

    Examples:
        >>> input = {"Tags": [{"Key": "Name", "Value": "Jerry"}, \
                              {"Key": "DOB", "Value": "1942-08-01"}]}
        >>> field = TagsField()
        >>> links = field.parse(data=input, context={})
        >>> print(links[0].to_dict())
        {'pred': 'Name', 'obj': 'Jerry', 'type': 'tag'}
        >>> print(links[1].to_dict())
        {'pred': 'DOB', 'obj': '1942-08-01', 'type': 'tag'}

    Args:
        optional: Whether this key is optional. Defaults to False.
    """

    def __init__(self, optional: bool = True):
        self.optional = optional

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> List[Link]:
        """Parse this field and return a list of Links.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            List of TagLink objects, one for each tag.
        """
        fields: List[Link] = []
        tag_dicts = data.get("Tags")
        if tag_dicts:
            for tag_dict in tag_dicts:
                field = TagLink(pred=tag_dict["Key"], obj=tag_dict["Value"])
                fields.append(field)
            return fields
        if self.optional:
            return []
        raise TagsFieldMissingTagsKeyException(f"Expected key 'Tags' in {data}")
