from __future__ import annotations

import inspect
import copy
import json

from .types import ACCEPTABLE_GENERIC_ALIASES
from .types import ACCEPTABLE_TRANSLATIONS
from .types import DGRAPH_TYPES
from .types import SELF_INSERTING_DIRECTIVE_ARGS
from .types import geo
from .types import uid
from .types import Directive
from .types import reverse
from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from decimal import Decimal
from itertools import count
from functools import partial
from pydiggy.exceptions import ConflictingType
from pydiggy.exceptions import InvalidData
from pydiggy.exceptions import MissingAttribute
from pydiggy.connection import get_client
from pydiggy.utils import _parse_subject
from pydiggy.utils import _rdf_value
from pydiggy.utils import _raw_value
from typing import Dict
from typing import List
from typing import Union
from typing import Tuple
from typing import Optional
from typing import _GenericAlias
from typing import get_type_hints
from enum import Enum


PropType = namedtuple("PropType", ("prop_type", "is_list_type", "directives"))


def Facets(obj, **kwargs):
    f = namedtuple("Facets", ["obj"] + list(kwargs.keys()))
    return f(obj, *kwargs.values())


def Computed(**kwargs):
    f = namedtuple("Computed", list(kwargs.keys()))
    return f(*kwargs.values())


def is_facets(node):
    if (
        isinstance(node, tuple)
        and hasattr(node, "obj")
        and node.__class__.__name__ == "Facets"
    ):
        return True
    return False


def is_computed(node):
    if isinstance(node, tuple) and node.__class__.__name__ == "Computed":
        return True
    return False


def _force_instance(directive, prop_type=None):
    if isinstance(directive, Directive):
        return directive

    args = []
    key = (directive, prop_type)
    if key in SELF_INSERTING_DIRECTIVE_ARGS:
        arg = SELF_INSERTING_DIRECTIVE_ARGS.get(key)
        args.append(arg)

    return directive(*args)


def get_node(name):
    registered = {x.__name__: x for x in Node._nodes}
    return registered.get(name, None)


class NodeMeta(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        directives = [
            x for x in attrs if x in attrs.get("__annotations__", {}).keys()
        ]
        attrs["_directives"] = dict()
        attrs["_instances"] = dict()

        for base in bases:
            attrs["_directives"].update(base._directives)

        for directive in directives:
            d = copy.deepcopy(attrs.get(directive))
            attrs.pop(directive)

            if not isinstance(d, (list, tuple, set)):
                d = (d,)

            if not all(
                (inspect.isclass(x) and issubclass(x, Directive))
                or issubclass(x.__class__, Directive)
                for x in d
            ):
                continue

            prop_type = attrs.get("__annotations__").get(directive)
            d = map(lambda x: _force_instance(x, prop_type), d)

            attrs["_directives"][directive] = tuple(d)

        return super().__new__(cls, name, bases, attrs, **kwargs)


# @dataclass
class Node(metaclass=NodeMeta):
    # uid: Union[uuid.UUID, int]
    uid: int

    _i = count()
    _nodes = []
    _staged = {}

    def __init_subclass__(cls, is_abstract: bool = False) -> None:
        if not is_abstract:
            cls._register_node(cls)

    def __init__(self, uid=None, **kwargs):
        if uid is None:
            uid = next(self._generate_uid())

        self.uid = uid
        self._dirty = set()

        self._annotations = get_type_hints(self.__class__)

        for arg, val in kwargs.items():
            if arg in self._annotations:
                setattr(self, arg, val)

        self.__class__._instances.update({self.uid: self})
        self._init = True
        # The following code looks to see if there are any typing.List
        # annotations. If yes, it auto creates an empty list.localns
        # This is probably not a feature worth including. Developer
        # should take responsibility for creating at run time.
        # localns = {x.__name__: x for x in Node._nodes}
        # localns.update({
        #     'List': List
        # })
        # annotations = get_type_hints(self, localns=localns)
        # for pred, pred_type in annotations.items():
        #     if isinstance(pred_type, _GenericAlias) and \
        #             pred_type.__origin__ == list:
        #         setattr(self, pred, [])

    def __repr__(self):
        return f"<{self.__class__.__name__}:{self.uid}>"

    def __hash__(self):
        return self.uid

    def __getattr__(self, attribute):
        if attribute in self.__annotations__:
            raise MissingAttribute(self, attribute)
        super().__getattribute__(attribute)

    def __eq__(self, other):
        if not issubclass(other.__class__, Node):
            return False
        return self.uid == other.uid

    def __setattr__(self, name, value):
        orig = self.__dict__.get(name, None)
        self.__dict__[name] = value
        if hasattr(self, "_init") and self._init and not name.startswith("_"):
            self._dirty.add(name)
        if name in self._directives and any(
            isinstance(d, reverse) for d in self._directives[name]
        ):
            directive = list(
                filter(
                    lambda d: isinstance(d, reverse), self._directives[name]
                )
            )[0]
            reverse_name = directive.name if directive.name else f"_{name}"

            def _assign(obj, key, value, do_many, remove=False):
                o = obj.obj if is_facets(obj) else obj

                if is_facets(obj):
                    props = obj._asdict()
                    props["obj"] = value
                    value = Facets(**props)

                if do_many:
                    if not hasattr(o, key):
                        setattr(o, key, list())
                    if remove:
                        o.__dict__[key].remove(value)
                    else:
                        o.__dict__[key].append(value)
                else:
                    setattr(o, key, value)

            # TODO:
            # - Add tuple and set support
            if isinstance(value, (list,)):
                for item in value:
                    if is_facets(item) and directive.with_facets is False:
                        item = item.obj
                    _assign(item, reverse_name, self, directive.many)
            else:
                # TODO:
                # Also, run a check that value is of type directive
                if value is not None:
                    _assign(value, reverse_name, self, directive.many)
                elif orig:
                    _assign(
                        orig, reverse_name, self, directive.many, remove=True
                    )

    @classmethod
    def _reset(cls):
        cls._i = count()
        cls._instances = dict()

    @classmethod
    def _register_node(cls, node):
        cls._nodes.append(node)

    @classmethod
    def _get_name(cls):
        return cls.__name__

    @classmethod
    def _generate_schema(cls):
        nodes = cls._nodes
        edges = {}
        schema = []
        type_schema = []
        edge_schema = []
        unknown_schema = []

        type_schema.append(f"_type: string .")

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

        # <<<<<<< HEAD
        #         for edge_name, (edge_type, is_list_type) in edges.items():
        #             type_name = cls._get_type_name(edge_type)
        #             # Currently, Dgraph does not support [uid] schema.
        #             # See https://github.com/dgraph-io/dgraph/issues/2511
        #             if is_list_type and type_name != 'uid':
        # =======
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
        return cls._staged

    @classmethod
    def _clear_staged(cls):
        cls._staged = {}
        cls._i = count()

    @classmethod
    def _hydrate(cls, raw):
        registered = {x.__name__: x for x in Node._nodes}

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

            annotations = get_type_hints(k)
            for pred, value in pred_items:
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
                                raise Exception("Unknown data")
                            node = get_node(value[0].get("_type"))
                            value = node._hydrate(value[0])
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
                            item = get_node(x.get("_type"))._hydrate(x)

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
    def json(cls):
        return {
            x.__name__: list(
                map(partial(cls._explode, max_depth=0), x._instances.values())
            )
            for x in cls._nodes
            if len(x._instances) > 0
        }

    @classmethod
    def _explode(cls, instance, max_depth=None, depth=0, include=None):
        obj = {"_type": instance.__class__.__name__}

        if not isinstance(instance, Node) and not is_facets(instance):
            if is_facets(instance):
                return instance._asdict()
            raise Exception("Cannot explode a non-Node object")

        if is_facets(instance):
            data = list(instance._asdict().items())
        else:
            data = list(instance.__dict__.items())
        if include:
            for prop in include:
                data.append((prop, getattr(instance, prop, None)))

        data = filter(lambda x: x[1] is not None, data)

        annotations = get_type_hints(instance.__class__)
        for key, value in data:
            if (
                is_facets(instance)
                or key in annotations.keys()
                or key == "uid"
                or (include and key in include)
            ):
                if isinstance(value, (str, int, float, bool)):
                    obj[key] = value
                elif issubclass(value.__class__, Node):
                    explode = (
                        depth < max_depth if max_depth is not None else True
                    )
                    if explode:
                        obj[key] = cls._explode(
                            value, depth=(depth + 1), max_depth=max_depth
                        )
                    else:
                        obj[key] = str(value)
                elif isinstance(value, (list,)):
                    obj[key] = [
                        cls._explode(x, depth=(depth + 1), max_depth=max_depth)
                        for x in value
                    ]
                elif is_computed(value):
                    obj.update({key: value._asdict()})
                elif is_facets(value):
                    prop_type = annotations[key]
                    is_list_type = (
                        True
                        if isinstance(prop_type, _GenericAlias)
                        and prop_type.__origin__ in (list, tuple)
                        else False
                    )
                    if is_list_type:
                        if key not in obj:
                            obj[key] = []
                        obj[key].append(value._asdict())
                    else:
                        item = value._asdict()
                        item.update({"is_facets": True})
                        obj[key] = value._asdict()
        return obj

    @classmethod
    def create(cls, **kwargs):
        instance = cls()
        for k, v in kwargs.items():
            setattr(instance, k, v)
        return instance

    @staticmethod
    def _is_node_type(cls):
        """Check if a class is a <class 'Node'>"""
        return inspect.isclass(cls) and issubclass(cls, Node)

    def _generate_uid(self):
        i = next(self._i)
        yield f"unsaved.{i}"

    def to_json(self, include=None):
        return self.__class__._explode(self, include=include)

    def stage(self):
        self.edges = {}

        for arg, _ in self._annotations.items():
            if not arg.startswith("_") and arg != "uid":
                val = getattr(self, arg, None)
                if val is not None:
                    self.edges[arg] = val
        self._staged[self.uid] = self

    def save(self, client=None, host=None, port=None):
        localns = {x.__name__: x for x in Node._nodes}
        localns.update({"List": List, "Union": Union, "Tuple": Tuple})
        annotations = get_type_hints(self, globalns=globals(), localns=localns)

        if client is None:
            client = get_client(host=host, port=9080)

        def _make_obj(node, pred, obj):
            annotation = annotations.get(pred, "")
            if (
                hasattr(annotation, "__origin__")
                and annotation.__origin__ == list
            ):
                annotation = annotation.__args__[0]

            try:
                if annotation == str:
                    obj = f'"{obj}"'
                elif annotation == bool:
                    obj = f'"{str(obj).lower()}"'
                elif annotation in (int,):
                    obj = f'"{int(obj)}"^^<xs:int>'
                elif annotation in (float,) or isinstance(obj, float):
                    obj = f'"{obj}"^^<xs:float>'
                elif Node._is_node_type(obj.__class__):
                    obj, passed = _parse_subject(obj.uid)
                    staged = Node._get_staged()

                    if (
                        obj not in staged
                        and passed not in staged
                        and not isinstance(passed, int)
                    ):
                        raise NotStaged(
                            f"<{node.__class__.__name__} {pred}={obj}>"
                        )
            except ValueError:
                raise ValueError(
                    f"Incorrect value type. Received <{node.__class__.__name__} {pred}={obj}>. Expecting <{node.__class__.__name__} {pred}={annotation.__name__}>"
                )

            if isinstance(obj, (tuple, set)):
                obj = list(obj)

            return obj

        setters = []
        deleters = []

        saveable = (
            x for x in self._dirty if x != "computed" and x in annotations
        )

        for pred in saveable:
            obj = getattr(self, pred)
            subject, passed = _parse_subject(self.uid)
            if not isinstance(obj, list):
                obj = [obj]

            for o in obj:
                if issubclass(o.__class__, Enum):
                    o = o.value

                facets = []
                if is_facets(o):
                    for facet in o.__class__._fields[1:]:
                        val = _raw_value(getattr(o, facet))
                        facets.append(f"{facet}={val}")
                    o = o.obj

                if not isinstance(o, (list, tuple, set)):
                    out = [o]
                else:
                    out = o

                for output in out:
                    if output is None:
                        line = f"{subject} <{pred}> * ."
                        deleters.append(line)
                        continue

                    is_node_type = self._is_node_type(output.__class__)
                    output = _make_obj(self, pred, output)

                    # Temporary measure until dgraph 1.1 with 1:1 uid
                    if is_node_type:
                        prop_type = annotations[pred]
                        is_list_type = (
                            True
                            if isinstance(prop_type, _GenericAlias)
                            and prop_type.__origin__ in (list, tuple)
                            else False
                        )
                        if not is_list_type:
                            line = f"{subject} <{pred}> * ."
                            transaction = client.txn()
                            try:
                                transaction.mutate(del_nquads=line)
                                transaction.commit()
                            finally:
                                transaction.discard()

                    if facets:
                        facets = ", ".join(facets)
                        line = f"{subject} <{pred}> {output} ({facets}) ."
                    else:
                        line = f"{subject} <{pred}> {output} ."
                    setters.append(line)

        set_mutations = "\n".join(setters)
        delete_mutations = "\n".join(deleters)
        transaction = client.txn()
        # print('set', set_mutations)
        # print('delete', delete_mutations)

        try:
            if set_mutations or delete_mutations:
                o = transaction.mutate(
                    set_nquads=set_mutations, del_nquads=delete_mutations
                )
                transaction.commit()
        finally:
            transaction.discard()
