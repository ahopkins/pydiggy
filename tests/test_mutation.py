from pydiggy import Facets, generate_mutation, NodeTypeRegistry


def test_mutations(RegionClass):
    Region = RegionClass

    NodeTypeRegistry._reset()

    por = Region(uid=0x11, name="Portugal")
    spa = Region(uid=0x12, name="Spain")
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

    mutation = generate_mutation()

    control = """<0x11> <Region> "true" .
<0x11> <_type> "Region" .
<0x11> <name> "Portugal" .
<0x11> <borders> <0x12> .
<0x12> <Region> "true" .
<0x12> <_type> "Region" .
<0x12> <name> "Spain" .
<0x12> <borders> <0x11> .
<0x12> <borders> _:unsaved.0 .
<0x12> <borders> _:unsaved.1 .
_:unsaved.0 <Region> "true" .
_:unsaved.0 <_type> "Region" .
_:unsaved.0 <name> "Gascony" .
_:unsaved.0 <borders> <0x12> (foo="bar", hello="world") .
_:unsaved.0 <borders> _:unsaved.1 .
_:unsaved.1 <Region> "true" .
_:unsaved.1 <_type> "Region" .
_:unsaved.1 <name> "Marseilles" .
_:unsaved.1 <borders> <0x12> .
_:unsaved.1 <borders> _:unsaved.0 ."""  # noqa

    control = [x.strip() for x in control.split("\n")]
    mutation = [x.strip() for x in mutation.split("\n")]

    assert len(control) == len(mutation)
    import pprint

    pprint.pprint(mutation)
    assert control == mutation


def test__mutation__with__quotes(RegionClass):
    Region = RegionClass

    NodeTypeRegistry._reset()

    florida = Region(name="Florida 'The \"Sunshine\" State'")

    florida.stage()

    mutation = generate_mutation()

    control = """_:unsaved.0 <Region> "true" .
_:unsaved.0 <_type> "Region" .
_:unsaved.0 <name> "Florida 'The \\"Sunshine\\" State'" ."""

    assert mutation == control
