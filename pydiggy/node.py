from __future__ import annotations

import copy
import json
import inspect
from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache, partial
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

from pydiggy.connection import get_client, PyDiggyClient
from pydiggy.exceptions import ConflictingType, InvalidData, MissingAttribute
from pydiggy.utils import _parse_subject, _raw_value, _rdf_value
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

PropType = namedtuple("PropType", ("prop_type", "is_list_type", "directives"))


def Facets(obj, **kwargs):
    f = namedtuple("Facets", ["obj"] + list(kwargs.keys()))
    return f(obj, *kwargs.values())


def Computed(**kwargs):
    f = namedtuple("Computed", list(kwargs.keys()))
    return f(*kwargs.values())


def is_facets(node: Node) -> bool:
    if (
        isinstance(node, tuple)
        and hasattr(node, "obj")
        and node.__class__.__name__ == "Facets"
    ):
        return True
    return False


def is_computed(node: Node) -> bool:
    if isinstance(node, tuple) and node.__class__.__name__ == "Computed":
        return True
    return False


def _force_instance(
    directive: Union[Directive, reverse, count, upsert, lang],
    prop_type: str = None,
) -> Directive:
    # TODO:
    # - Make sure directive is an instance of, or a class defined as a directive
    #   Or, raise an exception

    if isinstance(directive, Directive):
        return directive

    args = []
    key = (directive, prop_type)
    if key in SELF_INSERTING_DIRECTIVE_ARGS:
        arg = SELF_INSERTING_DIRECTIVE_ARGS.get(key)
        args.append(arg)

    return directive(*args)


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


class Node(metaclass=NodeMeta):
    _instances = dict()
    _staged = dict()


    @classmethod
    def _get_staged(cls):
        return cls._staged

    @classmethod
    def _clear_staged(cls):
        cls._staged = {}
        # NodeTypeRegistry._i needs to be updated when clearing staged nodes

    @classmethod
    def _reset(cls) -> None:
        cls._instances = dict()

    @classmethod
    def _get_name(cls) -> str:
        return cls.__name__

    def __init_subclass__(cls, is_abstract: bool = False) -> None:
        if not is_abstract:
            from pydiggy.node_type_registry import NodeTypeRegistry
            NodeTypeRegistry._register_node_type(cls)

    def __init__(self, uid=None, **kwargs):

        from pydiggy.node_type_registry import NodeTypeRegistry
        if uid is None:
            # TODO:
            # - There probably should be another property that is set here
            #   so that it is possible to identify with a boolean if the instance
            #   is brand new (and has never been committed to the DB) or if it
            #   is being freshly generated
            uid = next(NodeTypeRegistry._generate_uid())

        self.uid = uid
        self._dirty = set()

        # TODO:
        # - perhaps this code to generate self._annotations belongs somewhere
        #   else. Regardless, there is a lot of cleanup in this module that
        #   could probably use this property instead of running get_type_hints
        localns = {x.__name__: x for x in NodeTypeRegistry._node_types}
        localns.update({"List": List, "Union": Union, "Tuple": Tuple})
        self._annotations = get_type_hints(
            self.__class__, globalns=globals(), localns=localns
        )

        for arg, val in kwargs.items():
            if arg in self._annotations:
                setattr(self, arg, val)

        self.__class__._instances.update({self.uid: self})
        self._init = True

        # The following code looks to see if there are any typing.List
        # annotations. If yes, it auto creates an empty list.localns
        # This is probably not a feature worth including. Developer
        # should take responsibility for creating at run time. But, then again...
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
        return hash(self.uid)

    def __getattr__(self, attribute):
        if (not attribute == "__annotations__")\
            and (attribute in self.__annotations__):
            raise MissingAttribute(self, attribute)
        super().__getattribute__(attribute)

    def __eq__(self, other):
        if not issubclass(other.__class__, Node):
            return False
        return self.uid == other.uid

    def __setattr__(self, name: str, value: Any):
        # TODO:
        # - Make sure name is not a protected keyword being manually set (like _type)

        orig = self.__dict__.get(name, None)
        self.__dict__[name] = value
        # TODO: This causes issues with Node types that want private variables
        if hasattr(self, "_init")\
            and self._init\
            and not name.startswith("_"):
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
    def _explode(
        cls,
        instance: Node,
        max_depth: Optional[int] = None,
        depth: int = 0,
        include: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Explode a Node object into a mapping
        """
        # TODO:
        # - Candidate for refactoring
        # - Should the default max_depth be None?
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

        annotations = (
            instance.obj._annotations
            if is_facets(instance)
            else instance._annotations
        )
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
                    explode = (
                        depth < max_depth if max_depth is not None else True
                    )
                    if explode:
                        obj[key] = [
                            cls._explode(
                                x, depth=(depth + 1), max_depth=max_depth
                            )
                            for x in value
                        ]
                    else:
                        obj[key] = str(value)
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

    def to_json(self, include: List[str] = None) -> Dict[str, Any]:
        # TODO:
        # - Should this be renamed? It is a little misleading. Perhaps to_dict()
        #   would make more sense.
        return self.__class__._explode(self, include=include)

    def stage(self) -> None:
        """
        Identify a node instance that it is primed and ready to be migrated
        """
        self.edges = {}

        for arg, _ in self._annotations.items():
            if not arg.startswith("_") and arg != "uid":
                val = getattr(self, arg, None)
                if val is not None:
                    self.edges[arg] = val
        self._staged[self.uid] = self

    def save(
        self, client: PyDiggyClient = None, host: str = None, port: int = None
    ) -> None:
        # TODO:
        # - User self._annotations
        localns = {x.__name__: x for x in Node._nodes}
        localns.update({"List": List, "Union": Union, "Tuple": Tuple})
        annotations = get_type_hints(self, globalns=globals(), localns=localns)

        if client is None:
            client = get_client(host=host, port=9080)

        def _make_obj(node, pred, obj):
            #TODO: Remove this in favor of the _make_obj in operations
            annotation = annotations.get(pred, "")
            if (
                hasattr(annotation, "__origin__")
                and annotation.__origin__ == list
            ):
                annotation = annotation.__args__[0]

            try:
                if annotation == str:
                    obj = re.sub('"','\\"', obj.rstrip())
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

        try:
            if set_mutations or delete_mutations:
                o = transaction.mutate(
                    set_nquads=set_mutations, del_nquads=delete_mutations
                )
                transaction.commit()
        finally:
            transaction.discard()
