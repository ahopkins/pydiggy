from __future__ import annotations

from typing import List
from pydiggy import Node
from pydiggy import reverse


def test_reverse_normal():
    class Region(Node):
        borders: Region = reverse

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")

    por.borders = spa

    assert spa == por.borders
    assert por == spa._borders


def test_reverse_list_many():
    class Region(Node):
        borders: List[Region] = reverse(many=True)

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")

    por.borders = [spa]

    assert spa in por.borders
    assert por in spa._borders


def test_reverse_list_single():
    class Region(Node):
        borders: List[Region] = reverse

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")

    por.borders = [spa]

    assert spa in por.borders
    assert por == spa._borders


def test_reverse_name():
    class Person(Node):
        parent: Person = reverse(name='child')

    p = Person()
    c = Person()

    c.parent = p

    assert p.child == c


def test_reverse_name_many():
    class Person(Node):
        parent: Person = reverse(name='children', many=True)

    p = Person()
    c1 = Person()
    c2 = Person()

    c1.parent = p
    c2.parent = p

    assert c1 in p.children
    assert c2 in p.children
