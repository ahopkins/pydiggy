import json as _json
import re
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Union, get_type_hints, Dict, Any

from pydiggy.connection import get_client, PyDiggyClient
from pydiggy.exceptions import NotStaged
from pydiggy.node import Node
from pydiggy.node_type_registry import NodeTypeRegistry
from pydiggy._types import *  # noqa
from pydiggy.utils import _parse_subject, _raw_value


def _make_obj(node, pred, obj):
    annotation = node._annotations.get(pred, "")
    if (
        hasattr(annotation, "__origin__")
        and annotation.__origin__ == list
    ):
        annotation = annotation.__args__[0]

    if issubclass(obj.__class__, Enum):
        obj = obj.value

    # TODO:
    # - integreate utils._rdf_value
    try:
        if NodeTypeRegistry._is_node_type(obj.__class__):
            uid, passed = _parse_subject(obj.uid)
            staged = NodeTypeRegistry._get_staged()

            if (
                uid not in staged
                and passed not in staged
                and not isinstance(passed, int)
            ):
                raise NotStaged(
                    f"<{node.__class__.__name__} {pred}={uid}|{obj.__class__.__name__}>"
                )
            else:
                try:
                    uid = hex(int(obj.uid))
                    obj = f"<{uid}>"
                except ValueError:
                    obj = f"_:{obj.uid}"
        elif annotation == bool:
            obj = f'"{str(obj).lower()}"'
        elif annotation in (int,):
            obj = f'"{int(obj)}"^^<xs:int>'
        elif annotation in (float,) or isinstance(obj, float):
            obj = f'"{obj}"^^<xs:float>'
        elif isinstance(obj, datetime):
            obj = f'"{obj.isoformat()}"'
        else:
            obj = re.sub('"','\\"', obj.rstrip())
            obj = f'"{obj}"'
    except ValueError:
        raise ValueError(
            f"Incorrect value type. Received <{node.__class__.__name__} {pred}={obj}>. Expecting <{node.__class__.__name__} {pred}={annotation.__name__}>"
        )

    if isinstance(obj, (tuple, set)):
        obj = list(obj)

    return obj


def generate_mutation() -> str:
    """
    Retrieve staged instances and generate the mutation query
    """
    staged = Node._get_staged()
    # localns = {x.__name__: x for x in Node._nodes}
    # localns.update({"List": List, "Union": Union, "Tuple": Tuple})
    # annotations = get_type_hints(Node, globalns=globals(), localns=localns)

    # query = ['{', '\tset {']
    query = list()

    for uid, node in staged.items():
        subject, passed = _parse_subject(uid)

        edges = node.edges

        line = f'{subject} <{node.__class__.__name__}> "true" .'
        query.append(line)
        line = f'{subject} <_type> "{node.__class__.__name__}" .'
        query.append(line)

        for pred, obj in edges.items():
            # annotation = annotations.get(pred, "")
            if not isinstance(obj, list):
                obj = [obj]

            for o in obj:
                facets = []
                if isinstance(o, tuple) and hasattr(o, "obj"):
                    for facet in o.__class__._fields[1:]:
                        val = _raw_value(getattr(o, facet))
                        facets.append(f"{facet}={val}")
                    o = o.obj

                if not isinstance(o, (list, tuple, set)):
                    out = [o]
                else:
                    out = o

                for output in out:
                    output = _make_obj(node, pred, output)

                    if facets:
                        facets = ", ".join(facets)
                        line = f"{subject} <{pred}> {output} ({facets}) ."
                    else:
                        line = f"{subject} <{pred}> {output} ."
                    query.append(line)

    query = "\n".join(query)
    Node._clear_staged()

    return query


def hydrate(data: str, types: List[Node] = None) -> Dict[str, List[Node]]:
    """
    Given data retrieved from dgraph, return Python Node instances
    """
    # if not isinstance(data, dict) or data_set not in data:
    #     raise InvalidData
    types = {x.__name__: x for x in types} if types else None

    output = {}
    # data = data.get(data_set)
    registered = {x.__name__: x for x in NodeTypeRegistry._node_types}

    for func_name, raw_data in data.items():
        hydrated = []
        for raw in raw_data:
            if "_type" in raw and raw.get("_type") in registered:
                cls = registered.get(raw.get("_type"))
                hydrated.append(NodeTypeRegistry._hydrate(raw, types=types))

        output[func_name] = hydrated

    return output


def query(
    qry: str,
    client: PyDiggyClient = None,
    raw: bool = False,
    json: bool = False,
    *args,
    **kwargs,
) -> Dict[str, Any]:
    """
    Perform a pydgraph query and return hydrated Python Node objects.

    :param raw: Should the raw return of the query be returned
    :param json: Should the raw python objects of the query be returned
    """
    if client is None:
        client = get_client(**kwargs)
        if "host" in kwargs:
            kwargs.pop("host")
        if "port" in kwargs:
            kwargs.pop("port")
    raw_data = client.query(qry, *args, **kwargs)
    json_data = _json.loads(raw_data.json)
    output = hydrate(json_data)

    if raw:
        output["raw"] = raw_data

    if json:
        output["json"] = json_data

    return output


def run_mutation(mutation: str, client=None, *args, **kwargs):
    # TODO:
    # - Should come up with some sort of way to split mutations and run them
    #   consexutively instead of all at once. The following is the general idea,
    #   but it will fail because not all references will be properly defined
    #   from one mutation to the next
    # MAX = 1_000
    # mutations = mutation.split("\n")
    # mutations = [mutations[i:i + MAX] for i in range(0, len(mutations), MAX)]
    # o = []
    # if client is None:
    #     client = get_client()
    # for m in mutations:
    #     transaction = client.txn()
    #     try:
    #         print(f'Running {len(m)}')
    #         o.append(transaction.mutate(set_nquads="\n".join(m)))
    #         transaction.commit()
    #     finally:
    #         transaction.discard()
    transaction = client.txn()
    try:
        o = transaction.mutate(set_nquads=mutation)
        transaction.commit()
    finally:
        transaction.discard()
    # else:
    return o
