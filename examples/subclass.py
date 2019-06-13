"""
Here is an example of you you may be able to get more ORM-like capabilities.
You still need to predefine your queries, but you might be able to do something
like this to give you more programmatic freedom with your models.

person = Person.get(1)
hopkins = Person.filter(family=99)

"""

from __future__ import annotations


import os
from copy import deepcopy
from functools import partial

from enum import Enum, auto
from pydiggy import Node, index, exact, reverse
from pydiggy import query as do_query
from pydiggy.connection import get_client


client = get_client(test=True)


class Gender(Enum):
    MALE = auto()
    FEMALE = auto()
    OTHER = auto()


REGISTERED_ENUM = {"Gender": Gender}


class NoneNode:
    def __bool__(self):
        return False

    def __eq__(self, other):
        if other is None:
            return True
        else:
            return False

    @staticmethod
    def to_json():
        return None


class BaseAbstract(Node, is_abstract=True):
    __defaults__ = {}

    def __init__(self, *args, **kwargs):
        for key, value in self.__defaults__.items():
            setattr(self, key, deepcopy(value))

        super().__init__(*args, **kwargs)

    def __getattr__(self, key):
        if key[0].isupper():
            if key in REGISTERED_ENUM:
                cls = REGISTERED_ENUM.get(key)
                attribute = key.lower()
                return cls(getattr(self, attribute))
            else:
                return None
        else:
            if key.startswith("check_"):
                return partial(self.__checker, key[6:])
            return super().__getattr__(key)

    def __checker(self, key, default=None):
        return getattr(self, key, default)

    @classmethod
    def get(
        cls,
        uid=None,
        query=None,
        query_name="_get_query",
        single=True,
        first=None,
        offset=None,
        filters=None,
        subqueries=None,
    ):
        if os.environ.get("TEST_MODE"):
            return cls._instances.get(uid, None)
        if not hasattr(cls, query_name) and query is None:
            raise Exception(f"Cannot query {query_name} for {cls.__name__}")

        params = {}
        for injectable in ("uid", "first", "offset", "filters", "subqueries"):
            if injectable in locals():
                i = locals()[injectable]
                params.update({injectable: i})
        raw_query = query if query else getattr(cls, query_name)
        q = raw_query.format(**params)

        result = do_query(q, client=client, json=True)

        item = result.get("q")

        if item:
            if single:
                if len(item) > 1:
                    raise Exception("Found more than one")
                item = item[0]
        else:
            return NoneNode() if single else [NoneNode()]

        if single and item.__class__.__name__ != cls.__name__:
            raise Exception(f"Found {item.__class__.__name__}, and not {cls.__name__}.")

        return item

    @classmethod
    def filter(cls, query=None, query_name="_get_query", **kwargs):
        subqueries = []

        def make_filter(key, value):
            fltr = cls._filters[key]
            if isinstance(fltr, dict):
                if value in fltr:
                    if isinstance(fltr[value], tuple):
                        subqueries.append(cls._subqueries.get(fltr[value][0]))
                        return fltr[value][1]
                    return fltr[value]
                else:
                    return ""
            elif isinstance(fltr, tuple):
                subqueries.append(cls._subqueries.get(fltr[0]).format(value))
                return fltr[1]
            else:
                return fltr.format(value)

        filters = [make_filter(k, v) for k, v in kwargs.items() if k in cls._filters]
        if hasattr(cls, "_required_filters"):
            filters += list(cls._required_filters)
        filters = " and ".join((filter(lambda x: x, filters)))

        return cls.get(
            query=query,
            query_name=query_name,
            single=False,
            filters=filters,
            subqueries=subqueries,
        )

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_json(self, include=None):
        if hasattr(self, "_computed"):
            include = [] if include is None else include
            include += list(self._computed) + ["computed"]
        json = super().to_json(include=include)
        json = self.clean_computed(json)
        return json

    @staticmethod
    def clean_computed(json):
        if "computed" in json:
            kwargs = {k: v for k, v in json.get("computed").items()}
            json.update(**kwargs)
            del json["computed"]
        return json


class Family(BaseAbstract):
    name: str = index(exact)


class Person(BaseAbstract):
    gender: int = index  # ENUM
    family: Family = reverse(name="members", many=True)
    name: str = index(exact)
    birth_year: int = index
    death_year: int = index
    father: Person = reverse(name="children", many=True)
    mother: Person = reverse(name="children", many=True)

    _computed = ("age", "num_children")
    _filters = {
        "gender": {
            "male": f'eq(gender, "{Gender.MALE.value}")',
            "female": f'eq(gender, "{Gender.FEMALE.value}")',
            "other": f'eq(gender, "{Gender.OTHER.value}")',
        },
        "family": ("family", "uid(family)"),
        "living": {"true": "(not has(death_year))", "false": "has(death_year)"},
    }
    _subqueries = {
        "family": """
            var(func: has(Family)) @filter(uid({})) {{
              family as ~family
            }}
        """
    }
    _required_filters = ("(uid(all))",)
    _primitive_list = """
        {{
            all as var(func: has(Person)) {{
                year as birth_year
                age as math({year} - year)
                asFather as count(~father)
                asMother as count(~mother)
                num_children as math(asFather + asMother)
            }}

            {subqueries}

            q(func: has(Person), first: {first}, offset: {offset})
            @filter({filters})
            @normalize
            {{
                _type: _type
                uid
                name: name
                gender: gender
                age: val(age)
                family {{
                    _type
                    family_uid: uid
                    family_name: name
                }}
                num_subjects: val(num_subjects)
                num_children: val(num_children)
            }}
        }}
    """
    _get_query = """
        {{
            q(func: uid({uid}))
            {{
                _type
                uid
                name
                family {{
                    _type
                    uid
                    name
                }}
                gender
                father {{
                    _type
                    uid
                    name
                }}
                mother {{
                    _type
                    uid
                    name
                }}
            }}
        }}
    """
    _parent_query = """
        {{
            q(func: uid({uid}))
            {{
                _type
                uid
                father {{
                    _type
                    uid
                }}
                mother {{
                    _type
                    uid
                }}
            }}
        }}
    """

    @classmethod
    def _get_parents(cls, person, step=1):
        father = Person.get(person.father.uid) if hasattr(person, "father") else None
        mother = Person.get(person.mother.uid) if hasattr(person, "mother") else None
        generation = [
            {"person": father, "step": step},
            {"person": mother, "step": step},
        ]
        parents = list(filter(lambda x: x.get("person"), generation))

        parents = sum(
            [
                parents,
                sum(
                    [
                        cls._get_parents(parent.get("person"), step + 1)
                        for parent in parents
                    ],
                    [],
                ),
            ],
            [],
        )
        return parents

    @property
    def ancestors(self):
        return [{"person": self, "step": 0}] + self.__class__._get_parents(self)
