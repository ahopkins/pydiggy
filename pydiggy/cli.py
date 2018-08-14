# -*- coding: utf-8 -*-

"""Console script for pydiggy."""
import click
import importlib

from pydiggy.node import Node
from pydiggy.connection import get_client
from pydgraph import Operation


@click.group()
def main():
    """Console script for pydiggy."""
    pass  # noqa


@main.command()
@click.confirmation_option(
    prompt="Are you sure you want to flush all data in the db?"
)
@click.option(
    "-h", "--host", default="localhost", type=str, help="Dgraph host address"
)
@click.option("-p", "--port", default=9080, type=int, help="Dgraph port")
def flush(host, port):
    click.echo(f"Connecting to {host}:{port}")
    client = get_client(host=host, port=port)
    op = Operation(drop_all=True)
    client.alter(op)
    click.echo("Done.")


@main.command()
@click.argument("module")
@click.option(
    "--run/--no-run",
    default=False,
    help="Whether to run the schema changes or not",
)
@click.option(
    "-h", "--host", default="localhost", type=str, help="Dgraph host address"
)
@click.option("-p", "--port", default=9080, type=int, help="Dgraph port")
def generate(module, run, host, port):
    """Generate a Dgraph schema"""
    click.echo(f"Generating schema for: {module}")
    importlib.import_module(module)

    num_nodes = len(Node._nodes)
    click.echo(f"\nNodes found: ({num_nodes})")
    for node in Node._nodes:
        click.echo(f"    - {node._get_name()}")

    schema, unknown = Node._generate_schema()

    if not run:
        click.echo("\nYour schema:\n~~~~~~~~\n")
        click.echo(schema)
        click.echo("\n~~~~~~~~\n")

        if unknown:
            click.echo("\nUnknown schema:\n~~~~~~~~\n")
            click.echo(unknown)
            click.echo("\n~~~~~~~~\n")
    else:
        click.echo(f"\nConnecting to {host}:{port}")
        client = get_client(host=host, port=port)

        op = Operation(schema=schema)
        client.alter(op)
        click.echo("Done.")
