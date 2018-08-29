# -*- coding: utf-8 -*-

"""Top-level package for PyDiggy."""

__author__ = """Adam Hopkins"""
__email__ = "admhpkns@gmail.com"
__version__ = "0.1.0"

from pydiggy.node import Node
from pydiggy.node import Facets
from pydiggy.node import is_facets
from pydiggy.node import get_node
from pydiggy.operations import generate_mutation, hydrate, run_mutation, query
from .types import uid
from .types import geo
from .types import count
from .types import exact
from .types import index
from .types import lang
from .types import reverse
from .types import upsert


__all__ = (
    "count",
    "exact",
    "Facets",
    "generate_mutation",
    "geo",
    "get_node",
    "hydrate",
    "is_facets",
    "index",
    "lang",
    "Node",
    "query",
    "reverse",
    "run_mutation",
    "uid",
    "upsert",
)
