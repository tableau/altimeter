"""A ResourceSpec defines how an individual resource (e.g. EC2 Instances) JSON
is converted into graph data.  It contains a Schema which contains a set of
Fields defining the transformation."""
import abc
from collections import defaultdict
import inspect
from typing import Any, DefaultDict, Dict, List, Set, Type

from altimeter.core.resource.exceptions import (
    MultipleResourceSpecClassesFoundException,
    ResourceSpecClassNotFoundException,
)
from altimeter.core.graph.exceptions import UnmergableDuplicateResourceIdsFoundException
from altimeter.core.graph.link.base import Link
from altimeter.core.graph.schema import Schema
from altimeter.core.resource.resource import Resource


class ResourceSpec(abc.ABC):
    """A ResourceSpec defines how an individual resource (e.g. EC2 Instances) JSON
    is converted into graph data.  It contains a Schema which contains a set of
    Fields defining the transformation."""

    schema: Schema = Schema()
    type_name: str = ""
    # A list of ResourceSpec classes which are allowed to clobber/overwrite this one if
    # duplicate resource_ids are found.  The primary use case for this is to allow
    # UnscannedAccountResourceSpec generated resources to overwrite AccountResourceSpecs.
    allow_clobber: List[Type] = []

    def __init_subclass__(cls: Type["ResourceSpec"], **kwargs: Any) -> None:
        if not inspect.isabstract(cls):
            for required in ("schema", "type_name"):
                if not getattr(cls, required):
                    raise TypeError(
                        f"Can not instantiate {cls.__name__} without {required} attribute."
                    )
        return super().__init_subclass__()

    @classmethod
    @abc.abstractmethod
    def scan(cls: Type["ResourceSpec"], scan_accessor: Any) -> List[Resource]:
        """Scan for this ResourceSpec using scan_accessor and return a list of Resource objects

        Args:
            scan_accessor: scan accessor object for accessing required APIs

        Returns:
            List of Resource objects
        """

    @classmethod
    def get_full_type_name(cls: Type["ResourceSpec"]) -> str:
        """Get the fully qualified type name for this class.  Generally this is something like
        aws:ec2:type_name.

        Returns:
            full resource type name string
        """
        return cls.type_name

    @classmethod
    def generate_id(
        cls: Type["ResourceSpec"], short_resource_id: str, context: Dict[str, Any]
    ) -> str:
        """Generate a full id for this type given a short_resource_id.

        Args:
            short_resource_id: short resource id for this resource
            context: contains auxiliary information which can be passed through the parse process.

        Returns:
            full resource id string
        """
        return f"{cls.type_name}:{short_resource_id}"

    @staticmethod
    def get_by_class_name(class_name: str) -> Type["ResourceSpec"]:
        """Get a ResourceSpec class by class name.

        Args:
            class_name: class name to match

        Returns:
            ResourceSpec subclass

        Raises:
            MultipleResourceSpecClassesFoundException if more than one match was found.
                this indicates a code bug.
            ResourceSpecClassNotFoundException if no ResourceSpec class was found.
        """
        subclasses = get_concrete_subclasses(ResourceSpec)
        candidates = []
        for subclass in subclasses:
            if subclass.__name__ == class_name:
                candidates.append(subclass)
        if len(candidates) == 1:
            return candidates.pop()
        if len(candidates) > 1:
            raise MultipleResourceSpecClassesFoundException(
                f"Multiple ResourceSpec subclasses found named {class_name}: {candidates}"
            )
        raise ResourceSpecClassNotFoundException(
            f"No ResourceSpec subclass found named {class_name}"
        )

    @staticmethod
    def get_by_full_type_name(type_name: str) -> List[Type["ResourceSpec"]]:
        """Get a ResourceSpec classes by full_type name.

        Args:
           type_name: type name of ResourceSpec to find

        Returns:
            list of ResourceSpec classes matching type_name

        Raises:
            ResourceSpecClassNotFoundException if no ResourceSpec classes could be
            found matching type_name.
        """
        subclasses: List[Type[ResourceSpec]] = get_concrete_subclasses(ResourceSpec)
        candidates = []
        for subclass in subclasses:
            if subclass.get_full_type_name() == type_name:
                candidates.append(subclass)
        if candidates:
            return candidates
        raise ResourceSpecClassNotFoundException(
            f"No ResourceSpec subclass found with full_type_name {type_name}"
        )

    @staticmethod
    def merge_resources(resource_id: str, resources: List[Resource]) -> Resource:
        """Merge multiple resources with the same id into one.  This is permissible
        in two cases:

            1) If all resources have the same value for 'get_full_type' and no key/values
            conflict they will be merged into a single resource by combining all key/values
            into a single resource

            2) If all resources do not have the same value for 'get_full_type', if classes
            have the allow_clobber attribute set depending on the values a resource may be
            created.

        Args:
             resource_id: common resource id
             resources: list of Resources to merge

        Returns:
            merged Resource object

        Raises:
            UnmergableDuplicateResourceIdsFoundException if resources could not be merged.
        """
        full_type_names = {resource.type_name for resource in resources}
        if len(full_type_names) > 1:
            # in this case conflicting resources have different full_type_names, this
            # can be a permissible if allow_clobber is used. Here we determine this by
            # building a dict where the keys are resource spec classes which have
            # appeared in the duplicate resources allow_clobber list and the
            # values are the classes where they appeared.  If at the end in
            # this dict there are any values which match the complete list of
            # spec classes, we use the resource of that type
            spec_classes_resources: DefaultDict[Type[ResourceSpec], List[Resource]] = defaultdict(
                list
            )
            allow_clobber_classes_classes: DefaultDict[
                Type[ResourceSpec], Set[Type[ResourceSpec]]
            ] = defaultdict(set)
            for resource in resources:
                full_type_name = resource.type_name
                spec_classes: List[Type[ResourceSpec]] = ResourceSpec.get_by_full_type_name(
                    full_type_name
                )
                for spec_class in spec_classes:
                    spec_classes_resources[spec_class].append(resource)
                    for allow_clobber_class in spec_class.allow_clobber:
                        allow_clobber_classes_classes[allow_clobber_class].add(spec_class)
            all_spec_classes = set(spec_classes_resources.keys())
            winning_class = None
            for allow_clobber_class, classes in allow_clobber_classes_classes.items():
                if len(classes) == len(all_spec_classes) - 1:
                    winning_class = (all_spec_classes - classes).pop()
            if not winning_class:
                raise UnmergableDuplicateResourceIdsFoundException(
                    (
                        f"Multiple resources for {resource_id} with "
                        f"different types that aren't clobberable: "
                        f"{[resource.to_dict() for resource in resources]}"
                    )
                )
            resources = spec_classes_resources[winning_class]
        full_type_names = {resource.type_name for resource in resources}
        merged_resource_type_name = full_type_names.pop()
        merged_link_keys_links: Dict[str, Link] = {}
        for duplicate_resource in resources:
            for link in duplicate_resource.links:
                duplicate_link = merged_link_keys_links.get(link.pred)
                if duplicate_link:
                    if duplicate_link.field_type != link.field_type:
                        raise UnmergableDuplicateResourceIdsFoundException(
                            f"Conflicting link types {link.field_type}, {duplicate_link.field_type} found in duplicate #s {resources}"
                        )
                    if duplicate_link.obj != link.obj:
                        raise UnmergableDuplicateResourceIdsFoundException(
                            f"Conflicting link values {link.obj}, {duplicate_link.obj} found in duplicate #s {resources}"
                        )
                else:
                    merged_link_keys_links[link.pred] = link
        merged_links = list(merged_link_keys_links.values())
        return Resource(
            resource_id=resource_id, type_name=merged_resource_type_name, links=merged_links
        )


def get_concrete_subclasses(cls: Type[Any]) -> List[Type]:
    """Get all concrete subclasses of a class.

    Args:
        cls: class to find subclasses of

    Returns:
        list of classes which are concrete subclasses of cls
    """
    all_subclasses = cls.__subclasses__()
    concrete_subclasses = [sc for sc in all_subclasses if not inspect.isabstract(sc)]
    for subclass in all_subclasses:
        concrete_subclasses += get_concrete_subclasses(subclass)
    return concrete_subclasses
