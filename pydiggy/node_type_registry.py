import inspect

from collections import ChainMap
from copy import deepcopy
from functools import partial
from itertools import count as _count
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    _GenericAlias,
    get_type_hints,
)

from pydiggy.node import Node, Facets, is_facets, PropType
from pydiggy._types import (
    ACCEPTABLE_GENERIC_ALIASES,
    ACCEPTABLE_TRANSLATIONS,
    DGRAPH_TYPES,
    SELF_INSERTING_DIRECTIVE_ARGS,
    Directive,
    geo,
    reverse,
    uid,
    reverse,
    count,
    upsert,
    lang,
)

def get_node_type(name: str) -> Node:
    """
    Retrieve a registered node class.

    Example: Region = get_node("Region")

    This is a safe method to make sure that any models used have been
    declared as a Node.
    """
    registered = {x.__name__: x for x in NodeTypeRegistry._node_types}
    return registered.get(name, None)

class NodeTypeRegistry:
    _i = _count()
    _node_types = []

    @classmethod
    def _generate_uid(cls) -> str:
        i = next(cls._i)
        yield f"unsaved.{i}"

    @classmethod
    def _reset(cls) -> None:
        cls._i = _count()
        for node_type in cls._node_types:
            node_type._reset()

    @classmethod
    def _register_node_type(cls, node_type: type) -> None:
        cls._node_types.append(node_type)

    @classmethod
    def _generate_schema(cls) -> str:
        nodes = cls._node_types
        edges = {}
        schema = []
        type_schema = []
        edge_schema = []
        unknown_schema = []

        type_schema.append(f"_type: string .")

        # TODO:
        # - Prime candidate for some refactoring to reduce complexity
        for node in nodes:
            name = node._get_name()
            type_schema.append(f"{name}: bool @index(bool) .")

            annotations = get_type_hints(node)
            for prop_name, prop_type in annotations.items():
                # TODO:
                # - Probably need to be a little more careful for the origins
                # - Currently, it is assuming Union[something, None],
                #   but if you had two legit items inside Union (or anything else)
                #   then the second would be ignored. Which, is probably correct
                #   unless Dgraph schema would not allow multiple types for a single
                #   predicate. If it is possible, then the solution may simply
                #   be to loop over a list of deepcopy(annotations.items()),
                #   and append all the __args__ to that list to extend the iteration
                # - is_list_type should probably become its own function
                is_list_type = (
                    True
                    if isinstance(prop_type, _GenericAlias)
                    and prop_type.__origin__ in (list, tuple)
                    else False
                )

                if (
                    isinstance(prop_type, _GenericAlias)
                    and prop_type.__origin__ in ACCEPTABLE_GENERIC_ALIASES
                ):
                    prop_type = prop_type.__args__[0]

                prop_type = PropType(
                    prop_type,
                    is_list_type,
                    node._directives.get(prop_name, []),
                )

                if prop_name in edges:
                    if prop_type != edges.get(
                        prop_name
                    ) and not cls._is_node_type(prop_type[0]):

                        # Check if there is a type conflict
                        if (
                            edges.get(prop_name).directives
                            != prop_type.directives
                            and all(
                                (
                                    inspect.isclass(x)
                                    and issubclass(x, Directive)
                                )
                                or issubclass(x.__class__, Directive)
                                for x in edges.get(prop_name).directives
                            )
                            and all(
                                (
                                    inspect.isclass(x)
                                    and issubclass(x, Directive)
                                )
                                or issubclass(x.__class__, Directive)
                                for x in prop_type.directives
                            )
                        ):
                            pass
                        else:
                            raise ConflictingType(
                                prop_name, prop_type, edges.get(prop_name)
                            )

                # Set the type for translating the value
                if prop_type[0] in ACCEPTABLE_TRANSLATIONS:
                    edges[prop_name] = prop_type
                elif cls._is_node_type(prop_type[0]):
                    edges[prop_name] = PropType(
                        "uid",
                        is_list_type,
                        node._directives.get(prop_name, []),
                    )
                else:
                    if prop_name != "uid":
                        origin = getattr(prop_type[0], "__origin__", None)
                        # if origin and origin
                        unknown_schema.append(
                            f"{prop_name}: {prop_type[0]} || {origin}"
                        )

        # TODO:
        # - When the v1.1 comes out, will need to address this:
        #
        #         for edge_name, (edge_type, is_list_type) in edges.items():
        #             type_name = cls._get_type_name(edge_type)
        #             # Currently, Dgraph does not support [uid] schema.
        #             # See https://github.com/dgraph-io/dgraph/issues/2511
        #             if is_list_type and type_name != 'uid':
        for edge_name, edge in edges.items():
            type_name = cls._get_type_name(edge.prop_type)
            if edge.is_list_type and type_name != "uid":
                type_name = f"[{type_name}]"

            directives = edge.directives
            directives = " ".join([str(d) for d in directives] + [""])

            edge_schema.append(f"{edge_name}: {type_name} {directives}.")

        type_schema.sort()
        edge_schema.sort()

        schema = type_schema + edge_schema

        return "\n".join(schema), "\n".join(unknown_schema)

    @classmethod
    def _get_type_name(cls, schema_type):
        if isinstance(schema_type, str):
            return schema_type

        name = schema_type.__name__
        if cls._is_node_type(schema_type):
            return name
        else:
            if name not in DGRAPH_TYPES:
                raise Exception(f"Could not find type: {name}")
            return DGRAPH_TYPES.get(name)

    @classmethod
    def _get_staged(cls):
        all_staged = [node_type._get_staged() for node_type in cls._node_types]
        return dict(ChainMap(*all_staged))

    @classmethod
    def _clear_staged(cls):
        for node_type in cls._node_types:
            node_type._clear_staged()
        cls._i = _count()

    @classmethod
    def _hydrate(
        cls, raw: Dict[str, Any], types: Dict[str, Node] = None
    ) -> Node: # -> Dict[str: List[Node]]
        # TODO:
        # - Accept types that are passed. Loop thru them and register if needed
        #   and raising an exception if they are not valid.
        # - This method is another candidate for some refactoring to reduce
        #   complexity.
        # - Should create a Facets type so that the type annotation of this function
        #   is _hydrate(cls, raw: str, types: Dict[str, Node] = None) -> Union[Node, Facets]
        registered = {x.__name__: x for x in NodeTypeRegistry._node_types}
        localns = {x.__name__: x for x in NodeTypeRegistry._node_types}
        localns.update({"List": List, "Union": Union, "Tuple": Tuple})

        if "_type" in raw and raw.get("_type") in registered:
            if "uid" not in raw:
                raise InvalidData("Missing uid.")

            k = registered[raw.get("_type")]

            keys = deepcopy(list(raw.keys()))
            facet_data = [
                (key.split("|")[1], raw.pop(key)) for key in keys if "|" in key
            ]

            kwargs = {"uid": int(raw.pop("uid"), 16)}
            delay = []
            computed = {}

            pred_items = [
                (pred, value)
                for pred, value in raw.items()
                if not pred.startswith("_")
            ]

            annotations = get_type_hints(
                k, globalns=globals(), localns=localns
            )
            for pred, value in pred_items:
                """
                The pred falls into one of three categories:
                1. predicates that have already been defined
                2. predicates that are a reverse of a relationship
                3. predicates that are used for some computed value
                """
                if pred in annotations:
                    if isinstance(value, list):
                        prop_type = annotations[pred]
                        is_list_type = (
                            True
                            if isinstance(prop_type, _GenericAlias)
                            and prop_type.__origin__ in (list, tuple)
                            else False
                        )
                        if is_list_type:
                            value = [cls._hydrate(x) for x in value]
                        else:
                            if len(value) > 1:
                                # This should NOT happen. Because uid
                                # in dgraph is not forced 1:1 with a uid predicate
                                # it should return as a [<Node>] with only
                                # a single item in it. If the developer wants
                                # multiple: then the Node definition should
                                # be List[MyNode]
                                # Will probably need to be revisited when
                                # Dgraph v. 1.1 is released
                                raise Exception("Unknown data")
                            node_type = get_node_type(value[0].get("_type"))
                            value = node_type._hydrate(value[0])
                    elif isinstance(value, dict):
                        value = cls._hydrate(value)

                    if value is not None:
                        kwargs.update({pred: value})
                elif pred.startswith("~"):
                    p = pred[1:]
                    if isinstance(value, list):
                        for x in value:
                            keys = deepcopy(list(x.keys()))
                            value_facet_data = [
                                (k.split("|")[1], x.pop(k))
                                for k in keys
                                if "|" in k
                            ]
                            item = NodeTypeRegistry._hydrate(x)

                            if value_facet_data:
                                item = Facets(item, **dict(value_facet_data))
                            delay.append((item, p, value_facet_data))
                    elif isinstance(value, dict):
                        delay.append(
                            (
                                get_node(value.get("_type"))._hydrate(value),
                                p,
                                None,
                            )
                        )
                else:
                    if pred.endswith("_uid"):
                        value = int(value, 16)
                    computed.update({pred: value})

            instance = k(**kwargs)
            for d, p, v in delay:
                if is_facets(d):
                    f = Facets(instance, **dict(v))
                    setattr(d.obj, p, f)
                else:
                    setattr(d, p, instance)

            if computed:
                instance.computed = Computed(**computed)

            if facet_data:
                facets = Facets(instance, **dict(facet_data))
                return facets
            else:
                return instance
        return None

    @classmethod
    def json(cls) -> Dict[str, List[Node]]:
        """
        Return mapping of Node names to a list of node instances.

        Can be used as a way to dump what node instances are in memory.
        """
        # TODO:
        # - Probably should be renamed
        # - Instead of being a List[Node], it should probably be a Set
        return {
            x.__name__: list(
                map(partial(x._explode, max_depth=0), x._instances.values())
            )
            for x in cls._node_types
            if len(x._instances) > 0
        }


    @classmethod
    def create(cls, **kwargs) -> Node:
        """
        Constructor for creating a node.
        """
        instance = cls()
        for k, v in kwargs.items():
            setattr(instance, k, v)
        return instance

    @staticmethod
    def _is_node_type(cls) -> bool:
        """Check if a class is a <class 'Node'>"""
        return inspect.isclass(cls) and issubclass(cls, Node)