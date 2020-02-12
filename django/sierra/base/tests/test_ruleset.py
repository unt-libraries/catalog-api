"""
Tests the blacklight.localsierrarules classes and functions.
"""

from __future__ import unicode_literals
import pytest

from base import ruleset as r

# FIXTURES / TEST DATA

@pytest.fixture
def ruleset_test_obj_class():
    class RulesetTestObjClass(object):
        def __init__(self, **kwargs):
            for kwarg, val in kwargs.items():
                setattr(self, kwarg, val)
    return RulesetTestObjClass


@pytest.fixture
def ex_nondict_map_class():
    class StartswithMapClass(object):
        def __init__(self, startswith_map):
            self.startswith_map = startswith_map

        def get(self, test_val, default=None):
            for k, v in self.startswith_map.items():
                if test_val is not None and test_val.startswith(k):
                    return v
            return default
    return StartswithMapClass


@pytest.fixture
def ruleset_class():
    return r.Ruleset


@pytest.fixture
def dict_ruleset(ruleset_class):
    return ruleset_class([
        ('location_id', {'czm': False, 'law': False, 'w': False}),
        ('itype_id', {7: False, 8: False, 22: False}),
        (('itype_id', 'location_id'), {(7, 'xmus'): True}),
    ], default=True)


@pytest.fixture
def nondict_ruleset(ruleset_class, ex_nondict_map_class):
    startswith = ex_nondict_map_class
    return ruleset_class([
        ('location_id', startswith({
            'czm': 'Chilton Hall Media Library',
            'r': 'Discovery Park Library',
            's': 'Eagle Commons Library',
            'w': 'Willis Library'
        }))
    ], default=None)


# TESTS

@pytest.mark.parametrize('obj_attrs, expected', [
    ({'location_id': 'czm', 'itype_id': 1}, False),
    ({'location_id': 'lwww', 'itype_id': 1}, True),
    ({'location_id': 'lwww', 'itype_id': 7}, False),
    ({'location_id': 'xmus', 'itype_id': 1}, True),
    ({'location_id': 'xmus', 'itype_id': 7}, True),
    ({'location_id': 'xmus', 'itype_id': 8}, False),
    ({'location_id': 'xmus'}, True),
])
def test_ruleset_evaluate_works_for_dict_ruleset(obj_attrs, expected,
                                                 dict_ruleset,
                                                 ruleset_test_obj_class):
    """
    The `Ruleset.evaluate` method should return the expected value, for
    Ruleset objects configured using standard dictionaries (i.e. the
    `dict_ruleset` fixture).
    """
    obj = ruleset_test_obj_class(**obj_attrs)
    assert dict_ruleset.evaluate(obj) == expected


@pytest.mark.parametrize('obj_attrs, expected', [
    ({'location_id': 'czm'}, 'Chilton Hall Media Library'),
    ({'location_id': 'czmrf'}, 'Chilton Hall Media Library'),
    ({'location_id': 'c'}, None),
    ({'location_id': 'w1'}, 'Willis Library'),
    ({}, None),
])
def test_ruleset_evaluate_works_for_nondict_ruleset(obj_attrs, expected,
                                                    nondict_ruleset,
                                                    ruleset_test_obj_class):
    """
    The `Ruleset.evaluate` method should return the expected value, for
    Ruleset objects configured using custom, non-dict map objects, if
    the custom object implements an appropriate `get` method (i.e.
    the `nondict_ruleset` fixture).
    """
    obj = ruleset_test_obj_class(**obj_attrs)
    assert nondict_ruleset.evaluate(obj) == expected
