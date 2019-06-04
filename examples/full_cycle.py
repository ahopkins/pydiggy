# This example requires a running instance of DGraph (see instructions: https://docs.dgraph.io/get-started/#step-1-install-dgraph)
# and pydgraph (see instructions in the readme: https://github.com/dgraph-io/pydgraph#readme)

from __future__ import annotations

from typing import List, Union
import json
import traceback

from pydiggy import (
    Facets,
    Node,
    count,
    generate_mutation,
    geo,
    index,
    lang,
    reverse,
    _types as types,
    upsert,
    hydrate,
)
import pydgraph


class Region(Node):
    area: int = index(types._int)
    population: int = index
    description: str = lang
    short_description: str = lang()
    name: str = index(types.fulltext)
    abbr: str = (index(types.exact), count, upsert)
    coord: geo
    borders: List[Region] = reverse

    # __upsert__ = area
    # __reverse__ = borders

def sample_mutation():
    por = Region(name="Portugal")
    spa = Region(name="Spain")
    gas = Region(name="Gascony")
    mar = Region(name="Marseilles")

    por.borders = [spa]
    spa.borders = [por, gas, mar]
    gas.borders = [Facets(spa, foo="bar", hello="world"), mar]
    mar.borders = [spa, gas]

    por.stage()
    spa.stage()
    gas.stage()
    mar.stage()

    return generate_mutation()

def sample_schema():
    schema, unknown = Node._generate_schema()
    return schema


if __name__ == "__main__":
    client_stub = pydgraph.DgraphClientStub('localhost:9080')
    client = pydgraph.DgraphClient(client_stub)

    op = pydgraph.Operation(drop_all=True)
    client.alter(op)

    try:
        schema = sample_schema()
        print(schema)
        op = pydgraph.Operation(schema=schema)
        client.alter(op)
    except Exception as e:
        print("failed schema upload")
        print(e)

    txn = client.txn()
    try:
        mutation = sample_mutation()
        print(mutation)
        txn.mutate(set_nquads = mutation)
        txn.commit()
    except Exception as e:
        print("failed to upload sample data")
        print(e)
    finally:
        txn.discard()

    # Note that the query must include the UID and _type in order for hydrate
    # to work.
    query = """
            {
              my_region(func: eq(name, "Portugal")) {
                uid
                name
                _type
                borders {
                    uid
                    _type
                    name
                }
              }
            }
            """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        res = json.loads(res.json)
        print(res)
        res = hydrate(res)
        print(res)
    except Exception as e:
        traceback.print_exc()
        # print(e)
    finally:
      txn.discard()