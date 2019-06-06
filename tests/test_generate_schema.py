from __future__ import annotations

from typing import List

from pydiggy import Node, reverse

def test_generate_schema():
    class Map(Node):
        pass

    class Region(Node):
        map: 'Map' = reverse(name="territories", many=True)
        name: str
        borders: List['Region']

    schema, unknown = Node._generate_schema()

    expected = """Map: bool @index(bool) .
Region: bool @index(bool) .
_type: string .
name: string .
uid: int ."""

    assert schema == expected