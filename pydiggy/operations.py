from pydiggy.node import Node
from pydiggy.exceptions import NotStaged, InvalidData
from typing import get_type_hints, List, Union, Tuple
from pydiggy.types import *  # noqa


def _parse_subject(uid):
    if isinstance(uid, int):
        return f'<{hex(uid)}>', uid
    else:
        return f'_:{uid}', uid


def _make_obj(node, pred, obj):
    localns = {x.__name__: x for x in Node._nodes}
    localns.update({
        'List': List,
        'Union': Union,
        'Tuple': Tuple,
    })
    annotations = get_type_hints(node, globalns=globals(), localns=localns)
    annotation = annotations.get(pred, '')
    if annotation == str:
        obj = f'"{obj}"'
    elif annotation == bool:
        obj = f'"{str(obj).lower()}"'
    elif annotation in (int, float, ):
        obj = obj
    elif Node._is_node_type(obj.__class__):
        obj, passed = _parse_subject(obj.uid)
        staged = Node._get_staged()

        if obj not in staged and passed not in staged:
            raise NotStaged(obj)

    return obj


def generate_mutation():
    staged = Node._get_staged()
    query = ['{', '\tset {']

    for uid, node in staged.items():
        subject, passed = _parse_subject(uid)

        edges = node.edges

        line = f'\t\t{subject} <{node.__class__.__name__}> "true" .'
        query.append(line)
        line = f'\t\t{subject} <_type> "{node.__class__.__name__}" .'
        query.append(line)

        for pred, obj in edges.items():
            if not isinstance(obj, list):
                obj = [obj]

            for o in obj:
                facets = []
                if isinstance(o, tuple) and hasattr(o, 'obj'):
                    for facet in o.__class__._fields[1:]:
                        val = getattr(o, facet)
                        facets.append(f'{facet}="{val}"')
                    o = o.obj

                o = _make_obj(node, pred, o)

                if facets:
                    facets = ', '.join(facets)
                    line = f'\t\t{subject} <{pred}> {o} ({facets}) .'
                else:
                    line = f'\t\t{subject} <{pred}> {o} .'
                query.append(line)

    query.append('\t}')
    query.append('}')

    query = '\n'.join(query)

    return query


def hydrate(data):
    if not isinstance(data, dict) or 'data' not in data:
        raise InvalidData

    output = {}
    data = data.get('data')
    registered = {x.__name__: x for x in Node._nodes}

    for func_name, raw_data in data.items():
        hydrated = []
        for raw in raw_data:
            if '_type' in raw and raw.get('_type') in registered:
                cls = registered.get(raw.get('_type'))
                hydrated.append(cls._hydrate(raw))

        output[func_name] = hydrated

    return output
