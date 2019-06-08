"""
Create a test to make sure that no __annotations__ startswith('_')
"""
# import pytest
from pprint import pprint as print

from pydiggy import Facets, Node, NodeTypeRegistry


def test__node__to__json(RegionClass):
    Region = RegionClass

    NodeTypeRegistry._reset()

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")
    gas = Region(name="Gascony")
    mar = Region(name="Marseilles")

    por.borders = [spa]
    spa.borders = [por, gas, mar]
    gas.borders = [Facets(spa, foo="bar", hello="world"), mar]
    mar.borders = [spa, gas]

    regions = NodeTypeRegistry.json().get("Region")

    control = [
        {"_type": "Region", "borders": "[<Region:18>]", "name": "Portugal", "uid": 17},
        {
            "_type": "Region",
            "borders": "[<Region:17>, <Region:unsaved.0>, <Region:unsaved.1>]",
            "name": "Spain",
            "uid": 18,
        },
        {
            "_type": "Region",
            "borders": "[Facets(obj=<Region:18>, foo='bar', hello='world'), "
            "<Region:unsaved.1>]",
            "name": "Gascony",
            "uid": "unsaved.0",
        },
        {
            "_type": "Region",
            "borders": "[<Region:18>, <Region:unsaved.0>]",
            "name": "Marseilles",
            "uid": "unsaved.1",
        },
    ]

    assert regions == control


def test__node__with__quotes(RegionClass):
    Region = RegionClass

    NodeTypeRegistry._reset()

    florida = Region(name="Florida \'The \"Sunshine\" State\'")

    regions = NodeTypeRegistry.json().get("Region")

    control = [
    {'_type': 'Region',
    'name': 'Florida \'The "Sunshine" State\'',
    'uid': 'unsaved.0'}
    ]

    assert regions == control