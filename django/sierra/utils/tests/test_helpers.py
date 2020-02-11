"""
Tests the utils.helpers classes and functions.
"""

from __future__ import unicode_literals
import pytest
from utils import helpers as h

# FIXTURES AND TEST DATA
@pytest.fixture
def example_str_pattern_map_obj():
    return h.StrPatternMap({
        r'(^a|^b)': 'Label A/B',
        r'^c': 'Label C',
    }, exclude=('azzzz', 'bzzzz', 'czzzz'))


# TESTS

@pytest.mark.parametrize('forward_mapping, expected', [
    ({'a1': ['b1', 'b2'], 'a2': ['b3', 'b4']},
     {'b1': set(['a1']), 'b2': set(['a1']), 'b3': set(['a2']),
      'b4': set(['a2'])}),
    ({'a1': set(['b1', 'b2']), 'a2': set(['b3', 'b4'])},
     {'b1': set(['a1']), 'b2': set(['a1']), 'b3': set(['a2']),
      'b4': set(['a2'])}),
    ({'a1': ['b1', 'b2', 'b3'], 'a2': ['b1', 'b3', 'b4']},
     {'b1': set(['a1', 'a2']), 'b2': set(['a1']), 'b3': set(['a1', 'a2']),
      'b4': set(['a2'])}),
])
def test_reversesetmapping(forward_mapping, expected):
    """
    The reverse_set_mapping helper function should take the given
    `forward_mapping` dict and return the `expected` dict.
    """
    assert h.reverse_set_mapping(forward_mapping) == expected


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
