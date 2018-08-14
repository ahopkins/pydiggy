from pydiggy.node import Node
from pydiggy.exceptions import NotStaged, InvalidData
from typing import get_type_hints, List, Union, Tuple
from pydiggy.types import *  # noqa
from pydiggy.connection import get_client
import json


def _parse_subject(uid):
    if isinstance(uid, int):
        return f"<{hex(uid)}>", uid
    else:
        return f"_:{uid}", uid


def _make_obj(node, pred, obj):
    localns = {x.__name__: x for x in Node._nodes}
    localns.update({"List": List, "Union": Union, "Tuple": Tuple})
    annotations = get_type_hints(node, globalns=globals(), localns=localns)
    annotation = annotations.get(pred, "")
    if hasattr(annotation, "__origin__") and annotation.__origin__ == list:
        annotation = annotation.__args__[0]
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

        if obj not in staged and passed not in staged:
            raise NotStaged(obj)

    if isinstance(obj, (tuple, set)):
        obj = list(obj)

    return obj


def generate_mutation():
    staged = Node._get_staged()
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
            if not isinstance(obj, list):
                obj = [obj]

            for o in obj:
                facets = []
                if isinstance(o, tuple) and hasattr(o, "obj"):
                    for facet in o.__class__._fields[1:]:
                        val = getattr(o, facet)
                        facets.append(f'{facet}="{val}"')
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

    return query


def hydrate(data):
    print(not isinstance(data, dict))
    print("data" not in data)
    print(data)
    if not isinstance(data, dict) or "data" not in data.keys():
        raise InvalidData

    output = {}
    data = data.get("data")
    registered = {x.__name__: x for x in Node._nodes}

    iter = data.items() if isinstance(data, dict) else data

    for func_name, raw_data in iter:
        hydrated = []
        for raw in raw_data:
            if "_type" in raw and raw.get("_type") in registered:
                cls = registered.get(raw.get("_type"))
                hydrated.append(cls._hydrate(raw))

        output[func_name] = hydrated

    return output


def query(qry: str, client=None, *args, **kwargs):
    if client is None:
        client = get_client()
    result = client.query(qry, *args, **kwargs)
    result = json.loads(result.json)
    return hydrate(result)


def run_mutation(mutation: str, client=None, *args, **kwargs):
    MAX = 1_000
    mutations = mutation.split("\n")
    mutations = [mutations[i:i + MAX] for i in range(0, len(mutations), MAX)]
    o = []
    if client is None:
        client = get_client()
    for m in mutations:
        transaction = client.txn()
        try:
            o.append(transaction.mutate(set_nquads="\n".join(m)))
            transaction.commit()
        finally:
            transaction.discard()
    return o
