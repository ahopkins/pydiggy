from __future__ import annotations


from pydiggy import Node, generate_mutation, Facets
from typing import List


class Region(Node):
    area: int
    population: int
    name: str
    borders: List[Region]


if __name__ == "__main__":
    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")
    gas = Region(uid=0x13, name="Gascony")
    mar = Region(uid=0x14, name="Marseilles")

    por.borders = [spa]
    spa.borders = [por, gas, mar]
    gas.borders = [Facets(spa, foo="bar", hello="world"), mar]
    mar.borders = [spa, gas]

    por.stage()
    spa.stage()
    gas.stage()
    mar.stage()

    print(generate_mutation())
