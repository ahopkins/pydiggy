from __future__ import annotations


from pydiggy import Node, generate_mutation, Facets, hydrate, is_facets
from typing import List
import pytest


@pytest.fixture
def retrieved_data():
    return {
        "data": {
            "allRegions": [
                {
                    "uid": "0x11",
                    "_type": "Region",
                    "name": "Portugal",
                    "borders": [
                        {"uid": "0x12", "_type": "Region", "name": "Spain"}
                    ],
                },
                {
                    "uid": "0x12",
                    "_type": "Region",
                    "name": "Spain",
                    "borders": [
                        {"uid": "0x11", "_type": "Region", "name": "Portugal"},
                        {"uid": "0x13", "_type": "Region", "name": "Gascony"},
                        {
                            "uid": "0x14",
                            "_type": "Region",
                            "name": "Marseilles",
                        },
                    ],
                },
                {
                    "uid": "0x13",
                    "_type": "Region",
                    "name": "Gascony",
                    "borders": [
                        {
                            "uid": "0x12",
                            "_type": "Region",
                            "name": "Spain",
                            "borders|foo": "bar",
                            "borders|hello": "world",
                        },
                        {
                            "uid": "0x14",
                            "_type": "Region",
                            "name": "Marseilles",
                        },
                    ],
                },
                {
                    "uid": "0x14",
                    "_type": "Region",
                    "name": "Marseilles",
                    "borders": [
                        {"uid": "0x12", "_type": "Region", "name": "Spain"},
                        {"uid": "0x13", "_type": "Region", "name": "Gascony"},
                    ],
                },
            ]
        },
        "extensions": {
            "server_latency": {
                "parsing_ns": 23727,
                "processing_ns": 2000535,
                "encoding_ns": 7803450,
            },
            "txn": {"start_ts": 117, "lin_read": {"ids": {"1": 49}}},
        },
    }


def test_hydration(retrieved_data):
    class Region(Node):
        area: int
        population: int
        name: str
        borders: List[Region]

    data = hydrate(retrieved_data)

    assert "allRegions" in data
    assert len(data.get("allRegions")) == 4

    h = data.get("allRegions")[0]
    assert isinstance(h, Region)
    assert h.uid == 0x11
    assert h.name == "Portugal"
    assert isinstance(h.borders[0], Region)
    assert h.borders[0].uid == 0x12
    assert h.borders[0].name == "Spain"
    assert not is_facets(h.borders[0])

    h = data.get("allRegions")[1]
    assert isinstance(h, Region)
    assert h.uid == 0x12
    assert h.name == "Spain"
    assert len(h.borders) == 3

    h = data.get("allRegions")[2]
    assert h.uid == 0x13
    assert isinstance(h.borders[0], tuple)
    assert h.borders[0].__class__.__name__ == "Facets"
    assert h.borders[0].foo == "bar"
    assert h.borders[0].hello == "world"
    assert is_facets(h.borders[0])
