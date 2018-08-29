"""
Create a test to make sure that no __annotations__ startswith('_')
"""
from pydiggy import Facets, Node

# import pytest


def test__node__to__json(RegionClass):
    Region = RegionClass

    Region._Node__reset()

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")
    gas = Region(name="Gascony")
    mar = Region(name="Marseilles")

    por.borders = [spa]
    spa.borders = [por, gas, mar]
    gas.borders = [Facets(spa, foo="bar", hello="world"), mar]
    mar.borders = [spa, gas]

    print(Node.json())
    assert False
