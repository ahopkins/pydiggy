import pydgraph


def get_stub(host="localhost", port=9080):  # noqa
    return pydgraph.DgraphClientStub(f"{host}:{port}")


def get_client(host="localhost", port=9080):  # noqa
    stub = get_stub(host=host, port=port)
    return pydgraph.DgraphClient(stub)
