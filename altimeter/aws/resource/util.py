"""Utilty grab-bag"""
import json
from typing import Dict, Any, List


def policy_doc_dict_to_sorted_str(policy_doc: Dict[str, Any]) -> str:
    """Generate a string representation of an IAM Policy document which is recursively sorted such
    that policies can be compared without order diffs.

    Args:
        policy_doc: policy document

    Returns:
        Recursively sorted string representation of the policy document.
    """
    sorted_policy_dict = deep_sort_dict(policy_doc)
    return json.dumps(sorted_policy_dict)


def deep_sort_dict(dct: Dict) -> Dict:
    """Recursively sort a dictionary and additionally sort any embedded lists.

    Args:
        dct: dict to sort

    Returns:
        Recursively sorted dict, with any embedded lists also sorted.
    """
    output_dict: Dict = {}
    for key, value in sorted(dct.items()):
        if isinstance(value, dict):
            output_dict[key] = deep_sort_dict(value)
        elif isinstance(value, list):
            output_dict[key] = deep_sort_list(value)
        elif isinstance(value, (str, int, float)):
            output_dict[key] = value
        else:
            raise NotImplementedError(f"Type {type(value)} not implemented in deep_sort_dict.")
    return output_dict


def deep_sort_list(lst: List) -> List:
    """Recursively sort a list and additionally sort any embedded dicts.

    Args:
        lst: list to sort

    Returns:
        Recursively sorted list, with any embedded dicts also sorted.
    """
    output_list: List = []
    for value in lst:
        if isinstance(value, dict):
            output_list.append(deep_sort_dict(value))
        elif isinstance(value, list):
            output_list.append(deep_sort_list(value))
        elif isinstance(value, (str, int, float)):
            output_list.append(value)
        else:
            raise NotImplementedError(f"Type {type(value)} not implemented in deep_sort_list.")
    return sorted(output_list, key=json.dumps)
