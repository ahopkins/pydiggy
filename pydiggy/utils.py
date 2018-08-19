from pydiggy.node import Node


def get_node(name):
    registered = {x.__name__: x for x in Node._nodes}
    return registered.get(name, None)
