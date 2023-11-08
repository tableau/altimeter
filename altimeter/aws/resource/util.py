"""Utilty grab-bag"""
import json
from typing import Dict, Any, Callable, List, Optional

from botocore.exceptions import ClientError


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


def binary_aws_list_op(
    aws_op: Callable,
    resource_ids: List[str],
    resource_id_kwarg_field: str,
    aws_op_kwargs: Optional[Dict[str, List[str]]] = None,
) -> List[Dict[str, Any]]:
    responses = []
    if aws_op_kwargs:
        operation_kwargs = aws_op_kwargs.copy()
    else:
        operation_kwargs = {}

    if resource_ids:
        try:
            operation_kwargs[resource_id_kwarg_field] = resource_ids
            responses.append(aws_op(**operation_kwargs))
        except ClientError as c_ex:
            # if any resource ids appear in the error string, remove them from the
            # list to recurse upon
            filtered_resource_ids = [i_id for i_id in resource_ids if i_id not in str(c_ex)]

            if filtered_resource_ids:
                pivot = len(filtered_resource_ids) // 2
                top_ids, bottom_ids = filtered_resource_ids[:pivot], filtered_resource_ids[pivot:]

                responses += binary_aws_list_op(
                    aws_op=aws_op,
                    resource_ids=top_ids,
                    resource_id_kwarg_field=resource_id_kwarg_field,
                    aws_op_kwargs=aws_op_kwargs,
                )

                responses += binary_aws_list_op(
                    aws_op=aws_op,
                    resource_ids=bottom_ids,
                    resource_id_kwarg_field=resource_id_kwarg_field,
                    aws_op_kwargs=aws_op_kwargs,
                )
        except Exception:
            pass
    return responses
