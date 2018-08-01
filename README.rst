=======
PyDiggy
=======


.. .. image:: https://img.shields.io/pypi/v/pydiggy.svg
..         :target: https://pypi.python.org/pypi/pydiggy

.. .. image:: https://img.shields.io/travis/ahopkins/pydiggy.svg
..         :target: https://travis-ci.org/ahopkins/pydiggy

.. .. image:: https://readthedocs.org/projects/pydiggy/badge/?version=latest
..         :target: https://pydiggy.readthedocs.io/en/latest/?badge=latest
..         :alt: Documentation Status


.. .. image:: https://pyup.io/repos/github/ahopkins/pydiggy/shield.svg
..      :target: https://pyup.io/repos/github/ahopkins/pydiggy/
..      :alt: Updates



Dgraph to Python object mapper


* Free software: MIT license

.. * Documentation: https://pydiggy.readthedocs.io.

.. note::

    This project is still under active development, and is not ready for a release. Of cours thoughts and ideas are welcome.

.. note::

    Python 3.7 only. Sorry.


EXAMPLE
-------

.. code-block:: python

    # ./examples/__init__

    from .basic import *  # noqa


    # ./examples/basic.py

    from __future__ import annotations


    from pydiggy import Node
    from typing import List


    class Region(Node):
        area: int
        population: int
        name: str
        borders: List[Region]



CLI
---

Point the CLI utility at an existing module to generate a Dgraph schema.

.. code-block::

    $ python3 -m pydiggy generate examples

    Generating schema for: examples

    Nodes found: (1)
        - Region

    Your schema:
    ~~~~~~~~

    Region: bool @index(bool) .
    _type: string .
    area: int .
    borders: uid .
    name: string .
    population: int .

    ~~~~~~~~

GENERATE MUTATIONS
------------------

.. code-block:: python

    from pydiggy import generate_mutation, Facets

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")
    gas = Region(uid=0x13, name="Gascony")
    mar = Region(uid=0x14, name="Marseilles")

    por.borders = [spa]
    spa.borders = [por, gas, mar]
    gas.borders = [Facets(spa, foo='bar', hello='world'), mar]
    mar.borders = [spa, gas]

    por.stage()
    spa.stage()
    gas.stage()
    mar.stage()

    print(generate_mutation())

The result:

.. code-block::

    {
        set {
            <0x11> <Region> "true" .
            <0x11> <_type> "Region" .
            <0x11> <name> "Portugal" .
            <0x11> <borders> <0x12> .
            <0x12> <Region> "true" .
            <0x12> <_type> "Region" .
            <0x12> <name> "Spain" .
            <0x12> <borders> <0x11> .
            <0x12> <borders> <0x13> .
            <0x12> <borders> <0x14> .
            <0x13> <Region> "true" .
            <0x13> <_type> "Region" .
            <0x13> <name> "Gascony" .
            <0x13> <borders> <0x12> (foo="bar", hello="world") .
            <0x13> <borders> <0x14> .
            <0x14> <Region> "true" .
            <0x14> <_type> "Region" .
            <0x14> <name> "Marseilles" .
            <0x14> <borders> <0x12> .
            <0x14> <borders> <0x13> .
        }
    }

HYDATE FROM JSON TO PYTHON OBJECTS
----------------------------------

Given some response from Dgraph:

.. code-block:: JSON

    {
        "data": {
            "allRegions": [
                {
                    "uid": "0x11",
                    "_type": "Region",
                    "name": "Portugal",
                    "borders": [
                        {
                            "uid": "0x12",
                            "_type": "Region",
                            "name": "Spain"
                        }
                    ]
                },
                {
                    "uid": "0x12",
                    "_type": "Region",
                    "name": "Spain",
                    "borders": [
                        {
                            "uid": "0x11",
                            "_type": "Region",
                            "name": "Portugal"
                        },
                        {
                            "uid": "0x13",
                            "_type": "Region",
                            "name": "Gascony"
                        },
                        {
                            "uid": "0x14",
                            "_type": "Region",
                            "name": "Marseilles"
                        }
                    ]
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
                            "borders|hello": "world"
                        },
                        {
                            "uid": "0x14",
                            "_type": "Region",
                            "name": "Marseilles"
                        }
                    ]
                },
                {
                    "uid": "0x14",
                    "_type": "Region",
                    "name": "Marseilles",
                    "borders": [
                        {
                            "uid": "0x12",
                            "_type": "Region",
                            "name": "Spain"
                        },
                        {
                            "uid": "0x13",
                            "_type": "Region",
                            "name": "Gascony"
                        }
                    ]
                }
            ]
        },
        "extensions": {
            "server_latency": {
                "parsing_ns": 23727,
                "processing_ns": 2000535,
                "encoding_ns": 7803450
            },
            "txn": {
                "start_ts": 117,
                "lin_read": {
                    "ids": {
                        "1": 49
                    }
                }
            }
        }
    }

We can turn it into some Python objects:

.. code-block:: python

    >>> data = hydrate(retrieved_data)

    {'allRegions': [<Region:17>, <Region:18>, <Region:19>, <Region:20>]}
