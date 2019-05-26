from __future__ import annotations

from typing import List, Union

from pydiggy import (
    Facets,
    Node,
    count,
    generate_mutation,
    geo,
    index,
    lang,
    reverse,
    _types as types,
    upsert,
)


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
