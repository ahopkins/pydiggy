from __future__ import annotations

import pytest
from pydiggy import Node
from typing import List


@pytest.fixture
def RegionClass():
    class Region(Node):
        area: int
        population: int
        name: str
        borders: List[Region]
    return Region
