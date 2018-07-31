from __future__ import annotations


from pydiggy import Node, generate_mutation
from typing import List


class Region(Node):
    area: int
    population: int
    name: str
    borders: List[Region]


if __name__ == '__main__':
    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")
    gas = Region(uid=0x13, name="Gascony")

    por.borders = [spa]
    spa.borders = [por, gas]
    gas.borders = [spa]

    por.stage()
    spa.stage()
    gas.stage()

    generate_mutation()
