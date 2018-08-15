from pydiggy import operations


def test__parse_subject():
    subject = operations._parse_subject("abc")
    assert subject == ("_:abc", "abc")

    subject = operations._parse_subject(123)
    assert subject == ("<0x7b>", 123)

    subject = operations._parse_subject(0x7b)
    assert subject == ("<0x7b>", 123)


def test__make_obj(TypeTestClass):
    TypeTestClass._Node__reset()
    node = TypeTestClass()

    o = operations._make_obj(node, "str_type", "FooBar")
    assert o == '"FooBar"'

    o = operations._make_obj(node, "int_type", 123)
    assert o == '"123"^^<xs:int>'

    o = operations._make_obj(node, "float_type", 9.9)
    assert o == '"9.9"^^<xs:float>'

    o = operations._make_obj(node, "bool_type", True)
    assert o == '"true"'

    o = operations._make_obj(node, "node_type", node)
    assert o == "_:unsaved.0"
