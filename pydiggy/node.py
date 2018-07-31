from .exceptions import ConflictingType
import inspect

from typing import get_type_hints


class Node:
    _nodes = []

    def __init_subclass__(cls, is_abstract: bool = False):
        if not is_abstract:
            cls._register_node(cls)

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

        for node in nodes:
            name = node._get_name()
            type_schema.append(f'{name}: bool .')

            try:
                annotations = get_type_hints(node)
            except NameError as e:
                raise e
            else:
                for prop_name, prop_type in annotations.items():
                    if prop_name in edges:
                        if prop_type != edges.get(prop_name) and \
                                not cls._is_node_type(prop_type):
                            raise ConflictingType(
                                prop_name, prop_type, edges.get(prop_name))

                    # if isinstance(prop_type, ForwardRef):
                    #     # deferred.append((prop_name, prop_type))
                    if prop_type in (str, int, bool):
                        edges[prop_name] = prop_type
                    elif cls._is_node_type(prop_type):
                        edges[prop_name] = 'uid'
                    else:
                        unknown_schema.append(f'{prop_name}: {prop_type}')

        for edge_name, edge_type in edges.items():
            edge_schema.append(f'{edge_name}: {edge_type} .')

        type_schema.sort()
        edge_schema.sort()

        schema = ["# Types"] + type_schema + ["\n# Edges"] + edge_schema

        if unknown_schema:
            schema = schema + ["\n# Unknown shema"] + unknown_schema
        return "\n".join(schema)

    @staticmethod
    def _is_node_type(cls):
        """Check if a class is a <class 'Node'>"""
        return inspect.isclass(cls) and issubclass(cls, Node)
