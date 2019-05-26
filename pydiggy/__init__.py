# -*- coding: utf-8 -*-

"""Top-level package for PyDiggy."""

__author__ = """Adam Hopkins"""
__email__ = "admhpkns@gmail.com"
__version__ = "0.1.0"

from pydiggy.node import Facets, Node, get_node, is_facets
from pydiggy.operations import generate_mutation, hydrate, query, run_mutation

from .types import count, exact, geo, index, lang, reverse, uid, upsert

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
