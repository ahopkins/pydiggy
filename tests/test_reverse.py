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


def test_reverse_list():
    class Region(Node):
        borders: List[Region] = reverse

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")

    por.borders = [spa]

    assert spa in por.borders
    assert por in spa._borders
