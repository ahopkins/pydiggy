# -*- coding: utf-8 -*-

"""Console script for pydiggy."""
import click
import importlib

from .node import Node


@click.group()
def main():
    """Console script for pydiggy."""
    pass


@main.command()
@click.option('--module', help='name of the module to generate')
@click.option('--run/--no-run', default=False, help='Whether to run the schema changes or not')
def generate(module, run):
    """Generate a Dgraph schema"""
    click.echo(f'Generating schema for: {module}')
    importlib.import_module(module)

    num_nodes = len(Node._nodes)
    click.echo(f'\nNodes found: ({num_nodes})')
    for node in Node._nodes:
        click.echo(f'    - {node._get_name()}')

    if not run:
        click.echo('\nYour schema:')
        click.echo(Node._generate_schema())
    else:
        click.echo('\nShould be running.')
