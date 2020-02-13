"""
Tests the base.ruleset classes and functions.
"""

from __future__ import unicode_literals
import pytest

from base import ruleset as r

# FIXTURES / TEST DATA
# Note that, in addition to the fixtures below, we're using these
# fixtures from base.tests.conftest:
#   - ruleset_test_obj_class

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


@pytest.fixture
def str_pattern_map_class():
    return r.StrPatternMap


@pytest.fixture
def example_str_pattern_map_obj(str_pattern_map_class):
    return str_pattern_map_class({
        r'(^a|^b)': 'Label A/B',
        r'^c': 'Label C',
    }, exclude=('azzzz', 'bzzzz', 'czzzz'))


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


@pytest.mark.parametrize('forward_mapping, multi, expected', [
    ({'a1': ['b1', 'b2'], 'a2': ['b3', 'b4']}, True,
     {'b1': set(['a1']), 'b2': set(['a1']), 'b3': set(['a2']),
      'b4': set(['a2'])}),
    ({'a1': set(['b1', 'b2']), 'a2': set(['b3', 'b4'])}, True,
     {'b1': set(['a1']), 'b2': set(['a1']), 'b3': set(['a2']),
      'b4': set(['a2'])}),
    ({'a1': ['b1', 'b2', 'b3'], 'a2': ['b1', 'b3', 'b4']}, True,
     {'b1': set(['a1', 'a2']), 'b2': set(['a1']), 'b3': set(['a1', 'a2']),
      'b4': set(['a2'])}),
    ({'a1': ['b1', 'b2'], 'a2': ['b3', 'b4']}, False,
     {'b1': 'a1', 'b2': 'a1', 'b3': 'a2', 'b4': 'a2'}),
])
def test_reversemapping(forward_mapping, multi, expected):
    """
    The reverse_set_mapping helper function should take the given
    `forward_mapping` dict and `multi` value, and return the `expected`
    dict.
    """
    assert r.reverse_mapping(forward_mapping, multi=multi) == expected


@pytest.mark.parametrize('args, expected', [
    (['a'], 'Label A/B'),
    (['b'], 'Label A/B'),
    (['ax'], 'Label A/B'),
    (['c'], 'Label C'),
    (['cabc'], 'Label C'),
    (['bzzzz'], None),
    (['z'], None),
    (['z', 'Error'], 'Error'),
])
def test_strpatternmap_get(args, expected, example_str_pattern_map_obj):
    """
    A call to StrPatternMap's `get` method with the given `args` should
    return the `expected` value.
    """
    assert example_str_pattern_map_obj.get(*args) == expected


def test_strpatternmap_init_blank_object(str_pattern_map_class):
    """
    Initializing a StrPatternMap object with empty parameters (and no
    optional `exclude` parameter) should not raise errors.
    """
    str_pattern_map_class({})
    assert True
