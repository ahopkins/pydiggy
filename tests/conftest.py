from __future__ import annotations

from typing import List

import pytest

from pydiggy import Node


@pytest.fixture
def RegionClass():
    class Region(Node):
        area: int
        population: int
        name: str
        borders: List[Region]

    return Region


@pytest.fixture
def TypeTestClass():
    class TypeClass(Node):
        int_type: int
        float_type: float
        str_type: str
        bool_type: bool
        node_type: TypeClass

    return TypeClass


@pytest.fixture
def commands():
    return ["generate", "flush"]
