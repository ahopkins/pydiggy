import pydgraph
from os import environ


DEFAULT_DGRAPH_HOST = environ.get('DEFAULT_DGRAPH_HOST', 'localhost')
DEFAULT_DGRAPH_PORT = int(environ.get('DEFAULT_DGRAPH_PORT', 9080))


class PyDiggyClient(pydgraph.DgraphClient):

    def __repr__(self):
        stubs = "|".join([x.addr for x in self._clients])
        return f"<PyDiggyClient {stubs}>"


class PyDiggyTestTransaction:

    def __init__(self, **kwargs):
        pass

    def mutate(self, **kwargs):
        pass

    def commit(self, **kwargs):
        pass

    def discard(self, **kwargs):
        pass


class PyDiggyTestClient(pydgraph.DgraphClient):

    def query(self, query, variables=None, timeout=None, metadata=None,
              credentials=None):
        # print(f"Test client:\n{query[:20]}...")
        class Result:
            @property
            def json(self):
                return b"{}"

        return Result()

    def txn(self, read_only=False):
        return PyDiggyTestTransaction()


def get_stub(host=DEFAULT_DGRAPH_HOST, port=DEFAULT_DGRAPH_PORT):  # noqa
    addr = f"{host}:{port}"
    stub = pydgraph.DgraphClientStub(addr)
    stub.addr = addr
    return stub


def get_client(host=DEFAULT_DGRAPH_HOST, port=DEFAULT_DGRAPH_PORT, test=False):  # noqa
    if test:
        return PyDiggyTestClient(get_stub())
    stub = get_stub(host=host, port=port)
    return PyDiggyClient(stub)
