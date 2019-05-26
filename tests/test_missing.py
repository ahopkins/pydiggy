import pytest

from pydiggy.exceptions import MissingAttribute


def test_missing(RegionClass):
    Region = RegionClass

    por = Region(name="Portugal")

    with pytest.raises(AttributeError):
        print(por.abbreviation)

    with pytest.raises(MissingAttribute):
        print(por.population)
