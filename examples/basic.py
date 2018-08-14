from __future__ import annotations


from pydiggy import Node, generate_mutation, Facets
from pydiggy import geo
from pydiggy import index
from pydiggy import lang
from pydiggy import count
from pydiggy import reverse
from pydiggy import upsert
from pydiggy import types
from typing import List, Union


class Region(Node):
    area: int = index(types._int)
    population: int = index
    description: str = lang
    short_description: str = lang()
    name: str = index(types.fulltext)
    abbr: str = (index(types.exact), count, upsert)
    coord: geo
    borders: List[Region] = reverse

    # __upsert__ = area
    # __reverse__ = borders


if __name__ == "__main__":
    por = Region(name="Portugal")
    spa = Region(name="Spain")
    gas = Region(name="Gascony")
    mar = Region(name="Marseilles")

    por.borders = [spa]
    spa.borders = [por, gas, mar]
    gas.borders = [Facets(spa, foo="bar", hello="world"), mar]
    mar.borders = [spa, gas]

    por.stage()
    spa.stage()
    gas.stage()
    mar.stage()

    print(generate_mutation())
    schema, unknown = Node._generate_schema()
    print(schema)
