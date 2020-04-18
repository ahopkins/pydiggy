from __future__ import annotations

import copy
import inspect
import re
from collections import namedtuple
from copy import deepcopy
from datetime import datetime
from enum import Enum
from functools import partial
from itertools import count as _count
from typing import (Any, Dict, List, Optional, Tuple, Union, _GenericAlias,
                    get_type_hints)

from pydiggy._types import ACCEPTABLE_GENERIC_ALIASES  # uid,
from pydiggy._types import (ACCEPTABLE_TRANSLATIONS, DGRAPH_TYPES,
                            SELF_INSERTING_DIRECTIVE_ARGS, Directive, count,
                            geo, lang, reverse, upsert)
from pydiggy.connection import PyDiggyClient, get_client
from pydiggy.exceptions import (ConflictingType, InvalidData, MissingAttribute,
                                NotStaged)
from pydiggy.utils import _parse_subject, _raw_value

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


def extract_type(p_type):
    if "[" in p_type:
        regex = r"\[(\w+)\]"
        matches = re.search(regex, p_type)
        return matches.group(1)
    else:
        return p_type


def get_node(name: str) -> Node:
    """
    Retrieve a registered node class.

    Example: Region = get_node("Region")

    This is a safe method to make sure that any models used have been
    declared as a Node.
    """
    registered = {x.__name__: x for x in Node._nodes}
    return registered.get(name, None)


class ReverseRegistry(dict):
    _singleton = None

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls, *args, **kwargs)
        return cls._singleton

    def __setattr__(self, key, value):
        if key in self:
            raise Exception(">>>>")
        else:
            super().__setattr__(key, value)


class NodeMeta(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        directives = [
            x for x in attrs if x in attrs.get("__annotations__", {}).keys()
        ]
        attrs["_directives"] = dict()
        attrs["_instances"] = dict()
        attrs["_reverses"] = set()

        for base in bases:
            attrs["_directives"].update(base._directives)

        for directive in directives:
            d_key = copy.deepcopy(directive)
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

            for d_obj in attrs["_directives"][directive]:
                if isinstance(d_obj, reverse):
                    reverse_with = extract_type(
                        attrs.get("__annotations__", {}).get(d_key)
                    )
                    query_name = f"~{d_key}"
                    reverse_name = d_obj.name if d_obj.name else f"_{d_key}"
                    ReverseRegistry(
                        {f"{reverse_with}.{query_name}.{name}": reverse_name}
                    )

        return super().__new__(cls, name, bases, attrs, **kwargs)


class Node(metaclass=NodeMeta):
    uid: int

    _i = _count()
    _nodes = []
    _staged = {}

    def __init_subclass__(cls, is_abstract: bool = False) -> None:
        if not is_abstract:
            cls._register_node(cls)

    def __init__(self, uid=None, **kwargs):
        if uid is None:
            # TODO:
            # - There probably should be another property that is set here
            #   so that it is possible to identify with a boolean if the instance
            #   is brand new (and has never been committed to the DB) or if it
            #   is being freshly generated
            uid = next(self._generate_uid())
            self._fresh = True
        else:
            self._fresh = False

        self.uid = uid
        self._dirty = set()
        self._pending_delete = set()

        # TODO:
        # - perhaps this code to generate self._annotations belongs somewhere
        #   else. Regardless, there is a lot of cleanup in this module that
        #   could probably use this property instead of running get_type_hints
        localns = {x.__name__: x for x in Node._nodes}
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
        if attribute in self.__annotations__:
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
            if reverse_name not in self.__class__._reverses:
                use_class = (
                    value[0].__class__
                    if isinstance(value, list)
                    else value.__class__
                )
                use_class._reverses.add(reverse_name)

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
    def _reset(cls) -> None:
        cls._i = _count()
        cls._instances = dict()

    @classmethod
    def _register_node(cls, node: Node) -> None:
        cls._nodes.append(node)

    @classmethod
    def _get_name(cls) -> str:
        return cls.__name__

    @classmethod
    def _generate_schema(cls) -> str:
        nodes = cls._nodes
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
            if edge.is_list_type:
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
        cls._i = _count()

    @classmethod
    def _hydrate(
        cls, raw: Dict[str, Any], types: Dict[str, Node] = None
    ) -> Node:
        # TODO:
        # - Accept types that are passed. Loop thru them and register if needed
        #   and raising an exception if they are not valid.
        # - This method is another candidate for some refactoring to reduce
        #   complexity.
        # - Should create a Facets type so that the type annotation of this function
        #   is _hydrate(cls, raw: str, types: Dict[str, Node] = None) -> Union[Node, Facets]
        registered = {x.__name__: x for x in Node._nodes}
        localns = {x.__name__: x for x in Node._nodes}
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
                            node = get_node(value[0].get("_type"))
                            value = node._hydrate(value[0])
                    elif isinstance(value, dict):
                        prop_type = annotations[pred]
                        if prop_type != geo:
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
    def json(cls) -> Dict[str, List[Node]]:
        """
        Return mapping of Node names to a list of node instances.

        Can be used as a way to dump what node instances are in memory.
        """
        # TODO:
        # - Probably should be renamed
        # - Instrad of being a List[Node], it should probably be a Set
        return {
            x.__name__: list(
                map(partial(cls._explode, max_depth=1), x._instances.values())
            )
            for x in cls._nodes
            if len(x._instances) > 0
        }

    @classmethod
    def _explode(
        cls,
        instance: Node,
        max_depth: int = 1,
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
            raise TypeError("Cannot explode a non-Node object")

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
            do_explosion = (
                is_facets(instance)
                or key in annotations.keys()
                or key == "uid"
                or (include and key in include)
                or key in instance.__class__._reverses
            )
            if do_explosion:
                if isinstance(value, (str, int, float, bool)):
                    obj[key] = value
                elif isinstance(value, (datetime,)):
                    obj[key] = value.astimezone().isoformat()
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
                elif isinstance(value, (dict,)):
                    obj[key] = value
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

    @property
    def _type(self):
        return self.__class__.__name__

    def _generate_uid(self) -> str:
        i = next(self._i)
        yield f"unsaved.{i}"

    def to_json(self, include: List[str] = None, **kwargs) -> Dict[str, Any]:
        # TODO:
        # - Should this be renamed? It is a little misleading. Perhaps to_dict()
        #   would make more sense.
        return self.__class__._explode(self, include=include, **kwargs)

    def stage(self, *args) -> None:
        """
        Identify a node instance that it is primed and ready to be migrated
        """
        self.edges = {}

        for arg, _ in self._annotations.items():
            if not arg.startswith("_") and arg != "uid":
                val = getattr(self, arg, None)
                if val is not None and (not args or arg in args):
                    self.edges[arg] = val
        self._staged[self.uid] = self

    def delete(self, node=None, pred: str = None) -> None:
        if not pred:
            pred = "*"
        else:
            pred = f"<{pred}>"

        if not node:
            node = "*"
        else:
            node = f"<{node.uid}>"

        self._pending_delete.add(f"<{self.uid}> {pred} {node} .")

    def save(
        self,
        client: PyDiggyClient = None,
        host: str = None,
        port: int = None,
        commit: bool = True,
    ) -> None:
        # TODO:
        # - User self._annotations
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
                # if annotation == str or pred == "_type":
                if annotation == str:
                    obj = f'"{obj}"'
                elif annotation == bool:
                    obj = f'"{str(obj).lower()}"'
                elif annotation in (int,):
                    obj = f'"{int(obj)}"^^<xs:int>'
                elif annotation in (float,) or isinstance(obj, float):
                    obj = f'"{obj}"^^<xs:float>'
                elif annotation in (geo,):
                    if hasattr(obj, "__geojson__"):
                        obj = obj.__geojson__()
                    obj = f'"{obj}"^^<geo:geojson>'
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
        deleters = list(self._pending_delete)

        saveable = [
            x for x in self._dirty if x != "computed" and x in annotations
        ]

        subject, passed = _parse_subject(self.uid)
        if self._fresh:
            line = f'{subject} <{self._type}> "true" .'
            setters.append(line)
            line = f'{subject} <_type> "{self._type}" .'
            setters.append(line)

        for pred in saveable:
            obj = getattr(self, pred)
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
                    if is_node_type and commit and not self._fresh:
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

        set_mutations = "\n\t".join(setters)
        delete_mutations = "\n\t".join(deleters)
        transaction = client.txn()

        print(
            f"Ready for operation: {len(setters)} setters, {len(deleters)} deleters"
        )

        try:
            if set_mutations or delete_mutations:
                print("~ set_mutations")
                print("\t" + (set_mutations if set_mutations else "NONE"))
                print("~ delete_mutations")
                print(
                    "\t" + (delete_mutations if delete_mutations else "NONE")
                )
                o = transaction.mutate(
                    set_nquads=set_mutations, del_nquads=delete_mutations
                )

                if commit:
                    transaction.commit()

                    if hasattr(o, "uids"):
                        if str(self.uid) in o.uids:
                            self.uid = o.uids.get(self.uid)
        finally:
            if commit:
                transaction.discard()
