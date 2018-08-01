#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydiggy` package."""

import pytest

from click.testing import CliRunner

from pydiggy import cli, Node


@pytest.fixture
def runner():
    runner = CliRunner()
    return runner


def test_command_line_interface_has_commands(runner, commands):
    result = runner.invoke(cli.main)
    assert result.exit_code == 0

    for command in commands:
        assert command in result.output


def test_dry_run_generate_schema(runner):
    Node._nodes = []
    result = runner.invoke(cli.main, ["generate", "tests.fakeapp", "--no-run"])
    assert result.exit_code == 0
    assert "Nodes found: (1)" in result.output
    assert "Region: bool @index(bool) ." in result.output
    assert "_type: string ." in result.output
    assert "area: int ." in result.output
    assert "borders: uid ." in result.output
    assert "name: string ." in result.output
    assert "population: int ." in result.output
