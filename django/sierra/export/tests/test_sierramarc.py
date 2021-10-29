# -*- coding: utf-8 -*-

"""
Tests the export.sierramarc classes/functions.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import pytest
from export import sierramarc as sm

# FIXTURES AND TEST DATA
pytestmark = pytest.mark.django_db


# TESTS

@pytest.mark.parametrize('kwargs', [
    {'data': 'abcdefg'},
    {'data': 'abcdefg', 'indicators': '12'},
    {'data': 'abcdefg', 'subfields': ['a', 'Test']},
    {'data': 'abcdefg', 'indicators': '12', 'subfields': ['a', 'Test']}
])
def test_sierramarcfield_init_creates_control_field(kwargs):
    """
    Initializing a SierraMarcField using a `data` parameter should make
    a control field, even if a `subfields` and/or `indicators` value is
    also passed.
    """
    field = sm.SierraMarcField('008', **kwargs)
    assert field.tag == '008'
    assert field.data == kwargs['data']
    assert not hasattr(field, 'indicators')
    assert not hasattr(field, 'subfields')


@pytest.mark.parametrize('kwargs', [
    {},
    {'indicators': '12'},
    {'subfields': ['a', 'Test1', 'b', 'Test2']},
    {'subfields': ['a', 'Test'], 'group_tag': 'a'}
])
def test_sierramarcfield_init_creates_varfield(kwargs):
    """
    Initializing a SierraMarcField without a `data` parameter should
    make a variable-length field. If indicators are not provided,
    defaults should be blank ([' ', ' ']). If subfields are not
    provided, default should be an empty list.
    """
    field = sm.SierraMarcField('100', **kwargs)
    expected_ind = kwargs.get('indicators', '  ')
    expected_sf = kwargs.get('subfields', [])
    expected_gt = kwargs.get('group_tag', ' ')
    assert field.tag == '100'
    assert field.indicator1 == expected_ind[0]
    assert field.indicator2 == expected_ind[1]
    assert field.subfields == expected_sf
    assert field.group_tag == expected_gt


@pytest.mark.parametrize('grouptag, fieldtag, matchtag, expected', [
    ('', '100', '100', True),
    ('', '100', '200', False),
    ('', '100', 'a', False),
    ('', '100', 'a100', False),
    ('a', '100', '100', True),
    ('a', '100', 'a100', True),
    ('a', '100', 'a', True),
    ('a', '100', 'b', False),
    ('a', '100', 'b100', False)
])
def test_sierramarcfield_matchestag(grouptag, fieldtag, matchtag, expected):
    """
    The SierraMarcField `matches_tag` method should return True if the
    provided `matchtag` matches a field with the given `grouptag` and
    `fieldtag`, otherwise False.
    """
    f = sm.SierraMarcField(fieldtag, subfields=['a', 'Test'],
                           group_tag=grouptag)
    assert f.matches_tag(matchtag) == expected


@pytest.mark.parametrize('subfields, incl, excl, expected', [
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'abc', None,
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'a', None,
     [('a', 'a1'), ('a', 'a2')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], '', None,
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], None, '',
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], None, 'd',
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'd', None,
     []),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], None, 'a',
     [('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], None, 'bc',
     [('a', 'a1'), ('a', 'a2')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'abc', 'bc',
     [('a', 'a1'), ('a', 'a2')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'bc', 'bc',
     []),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'a', 'bc',
     [('a', 'a1'), ('a', 'a2')]),
])
def test_sierramarcfield_filtersubfields(subfields, incl, excl, expected):
    """
    The SierraMarcField `filter_subfields` method should return the
    expected tuples, given a field built using the given `subfields`
    and the provided `tags` and `excl` args.
    """
    field = sm.SierraMarcField('100', subfields=subfields)
    filtered = list(field.filter_subfields(incl, excl))
    assert len(filtered) == len(expected)
    for i, tup in enumerate(filtered):
        assert tup == expected[i]


@pytest.mark.parametrize('fparams, args, expected', [
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], (), ['a100_1', 'a100_2', 'b100_1', 'a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('c', '110'), []),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a',), ['a100_1', 'a100_2', 'a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a', '110'), ['a100_1', 'a100_2', 'a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('100',), ['a100_1', 'a100_2', 'b100_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('245',), ['a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a100',), ['a100_1', 'a100_2']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('100', 'a', '245'), ['a100_1', 'a100_2',
                                                        'b100_1', 'a245_1']),
])
def test_sierramarcrecord_getfields(fparams, args, expected, params_to_fields,
                                    add_marc_fields):
    """
    SierraMarcRecord `get_fields` method should return the expected
    fields, given the provided `fields` definitions, when passed the
    given `args`.
    """
    fields = params_to_fields(fparams)
    r = add_marc_fields(sm.SierraMarcRecord(), fields)
    filtered = r.get_fields(*args)
    assert len(filtered) == len(expected)
    for f in filtered:
        assert f.format_field() in expected


@pytest.mark.parametrize('fparams, include, exclude, expected', [
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a',), ('100',), ['a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a',), None, ['a100_1', 'a100_2', 'a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], None, ('100',), ['a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], None, None, ['a100_1', 'a100_2', 'b100_1',
                                               'a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a',), ('100', '245'), []),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('100',), ('100', '245'), []),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a', 'c',), ('100',), ['a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], ('a',), ('100', '300'), ['a245_1']),
])
def test_sierramarcrecord_filterfields(fparams, include, exclude, expected,
                                       params_to_fields, add_marc_fields):
    """
    SierraMarcRecord `filter_fields` method should return the expected
    fields, given the provided `fields` definitions, when passed the
    given `include` and `exclude` args.
    """
    fields = params_to_fields(fparams)
    r = add_marc_fields(sm.SierraMarcRecord(), fields)
    filtered = list(r.filter_fields(include, exclude))
    assert len(filtered) == len(expected)
    for f in filtered:
        assert f.format_field() in expected


def test_s2mconverter_compileoriginalmarc_vf_order(sierra_test_record,
                                                   add_varfields_to_record):
    """
    The SierraToMarcConverter `compile_original_marc` method should
    put variable-length fields into the correct order, based on field
    tag groupings and the vf.occ_num values. This should mirror the
    order catalogers put record fields into in Sierra.
    """
    b = sierra_test_record('bib_no_items')
    add_varfields_to_record(b, 'y', '036', ['|a1'], '  ', 0, True)
    add_varfields_to_record(b, 'y', '036', ['|a2'], '  ', 1, False)
    add_varfields_to_record(b, 'a', '100', ['|a3'], '  ', 0, True)
    add_varfields_to_record(b, 'n', '520', ['|a4'], '  ', 0, True)
    add_varfields_to_record(b, 'n', '520', ['|a5'], '  ', 1, False)
    add_varfields_to_record(b, 'n', '500', ['|a6'], '  ', 2, True)
    add_varfields_to_record(b, 'n', '530', ['|a7'], '  ', 3, True)
    add_varfields_to_record(b, 'y', '856', ['|a8'], '  ', 2, True)
    add_varfields_to_record(b, 'y', '856', ['|a9'], '  ', 3, False)
    rec = sm.SierraToMarcConverter().compile_original_marc(b)
    fields = rec.get_fields('036', '100', '500', '520', '530', '856')
    assert fields == sorted(fields, key=lambda l: l.value())

