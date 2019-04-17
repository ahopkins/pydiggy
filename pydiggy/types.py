from datetime import datetime
from decimal import Decimal
from typing import Union
from typing import Optional
from dataclasses import dataclass
from inspect import isclass


class uid:
    pass


class geo:
    pass


class DirectiveArgument:
    @staticmethod
    def _clean_name(name):
        if name.startswith("_"):
            return name[1:]
        return name


class Tokenizer(DirectiveArgument):
    pass


class Directive:
    def __str__(self):
        args = []
        if "__annotations__" in self.__class__.__dict__:
            for ann in self.__class__.__annotations__:
                arg = getattr(self, ann, None)
                if arg is None or not isclass(arg):
                    continue
                # if arg is None or not issubclass(arg, DirectiveArgument):
                #     raise Exception(arg)
                args.append(arg)

        if args:
            arglist = ", ".join([a._clean_name(a.__name__) for a in args])
            args = f"({arglist})"
        else:
            args = ""

        return f"@{self.__class__.__name__}{args}"


_hash = type("_hash", (Tokenizer,), {})
exact = type("exact", (Tokenizer,), {})
term = type("term", (Tokenizer,), {})
fulltext = type("fulltext", (Tokenizer,), {})
trigram = type("trigram", (Tokenizer,), {})
_int = type("_int", (Tokenizer,), {})
_float = type("_float", (Tokenizer,), {})
_bool = type("_bool", (Tokenizer,), {})


class index(Directive):
    tokenizer: Union[
        _hash, exact, term, fulltext, trigram, _int, _float, _bool
    ]

    def __init__(self, tokenizer=None):
        self.tokenizer = tokenizer


class reverse(Directive):
    name: Optional[str]
    many: bool
    with_facets: bool

    def __init__(self, name=None, many=False, with_facets=False):
        self.name = name
        self.many = many
        self.with_facets = with_facets


count = type("count", (Directive,), {})
upsert = type("upsert", (Directive,), {})
lang = type("lang", (Directive,), {})

DGRAPH_TYPES = {  # Unsupported dgraph type: password, geo
    "uid": "uid",
    "geo": "geo",
    "str": "string",
    "int": "int",
    "float": "float",
    "bool": "bool",
    "datetime": "dateTime",
    "Decimal": "float",
}

ACCEPTABLE_TRANSLATIONS = (str, int, bool, float, datetime, Decimal, uid, geo)
ACCEPTABLE_GENERIC_ALIASES = (list, tuple, Union)
SELF_INSERTING_DIRECTIVE_ARGS = {
    (index, "int"): _int,
    (index, "float"): _float,
    (index, "bool"): _bool,
    (index, "geo"): geo,
}
