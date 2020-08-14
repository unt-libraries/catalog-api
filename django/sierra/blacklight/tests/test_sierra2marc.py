# -*- coding: utf-8 -*-

"""
Tests the blacklight.parsers functions.
"""

from __future__ import unicode_literals
import pytest
import ujson
import datetime
import pytz

from blacklight import sierra2marc as s2m


# FIXTURES AND TEST DATA
pytestmark = pytest.mark.django_db


@pytest.fixture
def s2mbatch_class():
    """
    Pytest fixture; returns the s2m.S2MarcBatchBlacklightSolrMarc
    class.
    """
    return s2m.S2MarcBatchBlacklightSolrMarc


@pytest.fixture
def bibrecord_to_pymarc(s2mbatch_class):
    """
    Pytest fixture for converting a `bib` from the Sierra DB (i.e. a
    base.models.BibRecord instance) to a pymarc MARC record object.
    """
    def _bibrecord_to_pymarc(bib):
        s2m_obj = s2mbatch_class(bib)
        return s2m_obj.compile_original_marc(bib)
    return _bibrecord_to_pymarc


@pytest.fixture
def add_marc_fields():
    """
    Pytest fixture for adding fields to the given `bib` (pymarc Record
    object). If `overwrite_existing` is True, which is the default,
    then all new MARC fields will overwrite existing fields with the
    same tag.

    One or more `fields` may be passed. Each field is a tuple of:
        (tag, contents, indicators)

    `tag` can be a 3-digit numeric MARC tag ('245') or a 4-digit tag,
    where the III field group tag is prepended ('t245' is a t-tagged
    245 field).

    Indicators is optional. If the MARC tag is 001 to 009, then a data
    field is created from `contents`. Otherwise `contents` is treated
    as a list of subfields, and `indicators` defaults to blank, blank.
    """
    def _add_marc_fields(bib, fields, overwrite_existing=True):
        fieldobjs = []
        for f in fields:
            tag, contents = f[0:2]
            group_tag = ''
            if len(tag) == 4:
                group_tag, tag = tag[0], tag[1:]
            if overwrite_existing:
                bib.remove_fields(tag)
            if int(tag) < 10:
                fieldobjs.append(s2m.make_mfield(tag, data=contents))
            else:
                ind = tuple(f[2]) if len(f) > 2 else tuple('  ')
                fieldobjs.append(s2m.make_mfield(tag, subfields=contents,
                                                 indicators=ind,
                                                 group_tag=group_tag))
        bib.add_field(*fieldobjs)
        return bib
    return _add_marc_fields


@pytest.fixture
def blasm_pipeline_class():
    """
    Pytest fixture; returns the BlacklightASMPipeline class.
    """
    return s2m.BlacklightASMPipeline


@pytest.fixture
def plbundleconverter_class():
    """
    Pytest fixture; returns the PipelineBundleConverter class.
    """
    return s2m.PipelineBundleConverter


@pytest.fixture
def assert_json_matches_expected():
    """
    Pytest fixture for asserting that a list of `json_strs` and
    `exp_dicts` are equivalent. Tests to make sure each key/val pair
    in each of exp_dicts is found in the corresponding `json_strs`
    obj.
    """
    def _assert_json_matches_expected(json_strs, exp_dicts, exact=False):
        if not isinstance(json_strs, (list, tuple)):
            json_strs = [json_strs]
        if not isinstance(exp_dicts, (list, tuple)):
            exp_dicts = [exp_dicts]
        assert len(json_strs) == len(exp_dicts)
        for json, exp_dict in zip(json_strs, exp_dicts):
            cmp_dict = ujson.loads(json)
            for key in exp_dict.keys():
                assert cmp_dict[key] == exp_dict[key]
            if exact:
                assert set(cmp_dict.keys()) == set(exp_dict.keys())
    return _assert_json_matches_expected


@pytest.fixture
def update_test_bib_inst(add_varfields_to_record, add_items_to_bib,
                         add_locations_to_bib):
    """
    Pytest fixture. Update the given `bib` (base.models.BibRecord)
    instance with given `varfields`, `items_info`, and/or `locations`.
    Returns the updated bib instance. Underneath, fixture factories are
    used that ensure the changes are reverted after the test runs.
    """
    def _update_test_bib_inst(bib, varfields=[], items=[], locations=None):
        if locations is not None:
            bib = add_locations_to_bib(bib, locations, overwrite_existing=True)

        for field_tag, marc_tag, vals in varfields:
            bib = add_varfields_to_record(bib, field_tag, marc_tag, vals,
                                          overwrite_existing=True)
        items_to_add = []
        for item in items:
            try:
                attrs, item_vfs = item
            except ValueError:
                attrs, item_vfs = item, []
            items_to_add.append({'attrs': attrs, 'varfields': item_vfs})
        return add_items_to_bib(bib, items_to_add)
    return _update_test_bib_inst


@pytest.fixture
def make_name_structs():
    """
    Pytest fixture. Returns a function to use for generating
    name_structs (dictionary structures) from the given X00, X10, or
    X11 field. Data should be passed as a `tag`, `subfields`,
    `indicators` tuple (used in many of the test parameters).
    """
    def _make_name_structs(marcfield):
        tag, subfields, indicators = marcfield
        f = s2m.make_mfield(tag, subfields=subfields, indicators=indicators)
        return s2m.extract_name_structs_from_field(f)
    return _make_name_structs


@pytest.fixture
def marcfield_strings_to_params():
    """
    Pytest fixture. Returns a list you can pass directly to the
    `add_marc_fields` fixture for the `fields` parameter, given a list
    of MARC field strings copied/pasted from the LOC or OCLC website.
    """
    def _marcfield_strings_to_params(field_strings):
        utils = s2m.MarcParseUtils()
        return [utils.parse_marc_display_field(s)[0:3] for s in field_strings]
    return _marcfield_strings_to_params


# TESTS

@pytest.mark.parametrize('kwargs', [
    {'data': 'abcdefg'},
    {'data': 'abcdefg', 'indicators': '12'},
    {'data': 'abcdefg', 'subfields': ['a', 'Test']},
    {'data': 'abcdefg', 'indicators': '12', 'subfields': ['a', 'Test']}
])
def test_makemfield_creates_control_field(kwargs):
    """
    When passed a `data` parameter, `make_mfield` should create a
    pymarc control field, even if a `subfields` and/or `indicators`
    value is also passed.
    """
    field = s2m.make_mfield('008', **kwargs)
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
def test_makemfield_creates_varfield(kwargs):
    """
    When NOT passed a `data` parameters, `make_mfield` should create a
    pymarc variable-length field. If indicators are not provided,
    defaults should be blank ([' ', ' ']). If subfields are not
    provided, default should be an empty list.
    """
    field = s2m.make_mfield('100', **kwargs)
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
    SierraMarcField `matches_tag` method should return True if the
    provided `matchtag` matches a field with the given `grouptag` and
    `fieldtag`, otherwise False.
    """
    f = s2m.SierraMarcField(fieldtag, subfields=['a', 'Test'],
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
    SierraMarcField `filter_subfields` method should return the
    expected tuples, given a field built using the given `subfields`
    and the provided `tags` and `excl` args.
    """
    field = s2m.SierraMarcField('100', subfields=subfields)
    filtered = list(field.filter_subfields(incl, excl))
    assert len(filtered) == len(expected)
    for i, tup in enumerate(filtered):
        assert tup == expected[i]


@pytest.mark.parametrize('fields, args, expected', [
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
def test_sierramarcrecord_getfields(fields, args, expected, add_marc_fields):
    """
    SierraMarcRecord `get_fields` method should return the expected
    fields, given the provided `fields` definitions, when passed the
    given `args`.
    """
    r = add_marc_fields(s2m.SierraMarcRecord(), fields)
    filtered = r.get_fields(*args)
    assert len(filtered) == len(expected)
    for f in filtered:
        assert f.format_field() in expected


@pytest.mark.parametrize('fields, include, exclude, expected', [
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
def test_sierramarcrecord_filterfields(fields, include, exclude, expected,
                                       add_marc_fields):
    """
    SierraMarcRecord `filter_fields` method should return the expected
    fields, given the provided `fields` definitions, when passed the
    given `include` and `exclude` args.
    """
    r = add_marc_fields(s2m.SierraMarcRecord(), fields)
    filtered = list(r.filter_fields(include, exclude))
    assert len(filtered) == len(expected)
    for f in filtered:
        assert f.format_field() in expected


def test_explodesubfields_returns_expected_results():
    """
    `explode_subfields` should return lists of subfield values for a
    pymarc Field object based on the provided sftags string.
    """
    field = s2m.make_mfield('260', subfields=['a', 'Place :',
                                               'b', 'Publisher,',
                                               'c', '1960;',
                                               'a', 'Another place :',
                                               'b', 'Another Publisher,',
                                               'c', '1992.'])
    places, pubs, dates = s2m.explode_subfields(field, 'abc')
    assert places == ['Place :', 'Another place :']
    assert pubs == ['Publisher,', 'Another Publisher,']
    assert dates == ['1960;', '1992.']


@pytest.mark.parametrize('field, inc, exc, unq, start, end, limit, expected', [
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', None,
     ('a1 b1 c1', 'a2 b2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'ac', '', 'ac', '', '', None,
     ('a1 c1', 'a2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'acd', '', 'acd', '', '', None,
     ('a1 c1', 'a2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'cba', '', 'cba', '', '', None,
     ('a1 b1 c1', 'a2 b2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1']),
     'abc', '', 'abc', '', '', None,
     ('a1 b1 c1',)),
    (('260', ['a', 'a1', 'b', 'b1',
              'a', 'a2', 'c', 'c2']),
     'abc', '', 'abc', '', '', None,
     ('a1 b1', 'a2 c2')),
    (('260', ['b', 'b1',
              'b', 'b2', 'a', 'a1', 'c', 'c1']),
     'abc', '', 'abc', '', '', None,
     ('b1', 'b2 a1 c1')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', 'abc', 'abc', '', '', None,
     ('')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', 'ac', 'abc', '', '', None,
     ('b1', 'b2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', 1,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', 2,
     ('a1 b1 c1', 'a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', 3,
     ('a1 b1 c1', 'a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', '', None,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', None,
     ('', 'a1 b1', 'c1', 'a2 b2', 'c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', 1,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', 2,
     ('', 'a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', 3,
     ('', 'a1 b1', 'c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', None,
     ('a1', 'b1 c1', 'a2', 'b2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', 1,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', 2,
     ('a1', 'b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', 3,
     ('a1', 'b1 c1', 'a2 b2 c2',)),
    (('260', ['a', 'a1.1', 'a', 'a1.2', 'b', 'b1.1',
              'a', 'a2.1', 'b', 'b2.1',
              'b', 'b3.1']),
     'ab', '', '', '', 'b', None,
     ('a1.1 a1.2 b1.1', 'a2.1 b2.1', 'b3.1')),
    (('700', ['a', 'Name', 'd', 'Dates', 't', 'Title', 'p', 'Part']),
     '', '', '', 'tp', '', 2,
     ('Name Dates', 'Title Part')),
])
def test_groupsubfields_groups_correctly(field, inc, exc, unq, start, end,
                                         limit, expected):
    """
    `group_subfields` should put subfields from a pymarc Field object
    into groupings based on the provided parameters.
    """
    mfield = s2m.make_mfield(field[0], subfields=field[1])
    result = s2m.group_subfields(mfield, inc, exc, unq, start, end, limit)
    assert len(result) == len(expected)
    for group, exp in zip(result, expected):
        assert group.value() == exp


@pytest.mark.parametrize('field_info, sftags, expected', [
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'a',
     (['a1', 'a2'])),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc',
     (['a1', 'b1', 'c1', 'a2', 'b2', 'c2'])),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     None,
     (['a1', 'b1', 'c1', 'a2', 'b2', 'c2'])),
])
def test_pullfromsubfields_and_no_pullfunc(field_info, sftags, expected):
    """
    Calling `pull_from_subfields` with no `pull_func` specified should
    return values from the given pymarc Field object and the specified
    sftags, as a list.
    """
    field = s2m.make_mfield(field_info[0], subfields=field_info[1])
    for val, exp in zip(s2m.pull_from_subfields(field, sftags), expected):
        assert  val == exp


def test_pullfromsubfields_with_pullfunc():
    """
    Calling `pull_from_subfields` with a custom `pull_func` specified
    should return values from the given pymarc Field object and the
    specified sftags, run through pull_func, as a flat list.
    """
    subfields = ['a', 'a1.1 a1.2', 'b', 'b1.1 b1.2', 'c', 'c1',
                 'a', 'a2', 'b', 'b2', 'c', 'c2.1 c2.2']
    field = s2m.make_mfield('260', subfields=subfields)

    def pf(val):
        return val.split(' ')

    expected = ['a1.1', 'a1.2', 'b1.1', 'b1.2', 'c1', 'a2', 'b2', 'c2.1',
                'c2.2']
    pulled = s2m.pull_from_subfields(field, sftags='abc', pull_func=pf)
    for val, exp in zip(pulled, expected):
        assert val == exp


@pytest.mark.parametrize('subfields, sep, sff, expected', [
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], '; ', None,
      '(case files) aperture cards; 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], '; ', {'exclude': '3'},
      'aperture cards; 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      '3', 'microfilm', 'f', '48x'], '; ', None,
      '(case files) aperture cards; 9 x 19 cm.; (microfilm) 48x'),
    (['a', 'aperture cards', 'b', '9 x 19 cm.', 'd', 'microfilm',
      'f', '48x', '3', 'case files'], '; ', None,
      'aperture cards; 9 x 19 cm.; microfilm; 48x (case files)'),
    (['3', 'case files', '3', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], '; ', None,
      '(case files, aperture cards) 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], '. ', None,
      '(case files) aperture cards. 9 x 19 cm. microfilm. 48x'),
    (['a', 'Register at https://libproxy.library.unt.edu/login?url=https://what'
           'ever.com'], ' ', None,
      'Register at https://libproxy.library.unt.edu/login?url=https://whatever.'
      'com'),
])
def test_genericdisplayfieldparser_parse(subfields, sep, sff, expected):
    """
    The GenericDisplayFieldParser `parse` method should return the
    expected result when parsing a MARC field with the given
    `subfields`, given the provided `sep` (separator) and `sff`
    (subfield filter).
    """
    field = s2m.make_mfield('300', subfields=subfields)
    assert s2m.GenericDisplayFieldParser(field, sep, sff).parse() == expected


@pytest.mark.parametrize('subfields, expected', [
    (['a', 'soprano voice', 'n', '2', 'a', 'mezzo-soprano voice', 'n', '1',
      'a', 'tenor saxophone', 'n', '1', 'd', 'bass clarinet', 'n', '1',
      'a', 'trumpet', 'n', '1', 'a', 'piano', 'n', '1', 'a', 'violin',
      'n', '1', 'd', 'viola', 'n', '1', 'a', 'double bass', 'n', '1', 's', '8',
      '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '8',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('soprano voice', '2')]}],
        [{'primary': [('mezzo-soprano voice', '1')]}],
        [{'primary': [('tenor saxophone', '1')]},
         {'doubling': [('bass clarinet', '1')]}],
        [{'primary': [('trumpet', '1')]}],
        [{'primary': [('piano', '1')]}],
        [{'primary': [('violin', '1')]}, {'doubling': [('viola', '1')]}],
        [{'primary': [('double bass', '1')]}],
     ]}),
    (['b', 'flute', 'n', '1', 'a', 'orchestra', 'e', '1', 'r', '1', 't', '1',
      '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
        [{'solo': [('flute', '1')]}],
        [{'primary': [('orchestra', '1')]}],
     ]}),
    (['a', 'flute', 'n', '1', 'd', 'piccolo', 'n', '1', 'd', 'alto flute',
      'n', '1', 'd', 'bass flute', 'n', '1', 's', '1', '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('flute', '1')]},
         {'doubling': [('piccolo', '1'), ('alto flute', '1'),
                       ('bass flute', '1')]}],
     ]}),
    (['a', 'violin', 'n', '1', 'd', 'flute', 'n', '1', 'p', 'piccolo', 'n', '1',
      'a', 'cello', 'n', '1', 'a', 'piano', 'n', '1', 's', '3', '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '3',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]}, {'doubling': [('flute', '1')]},
         {'alt': [('piccolo', '1')]}],
        [{'primary': [('cello', '1')]}],
        [{'primary': [('piano', '1')]}],
     ]}),
    (['b', 'soprano voice', 'n', '3', 'b', 'alto voice', 'n', '2',
      'b', 'tenor voice', 'n', '1', 'b', 'baritone voice', 'n', '1',
      'b', 'bass voice', 'n', '1', 'a', 'mixed chorus', 'e', '2',
      'v', 'SATB, SATB', 'a', 'children\'s chorus', 'e', '1', 'a',
      'orchestra', 'e', '1', 'r', '8', 't', '4', '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '8',
      'total_ensembles': '4',
      'parts': [
        [{'solo': [('soprano voice', '3')]}],
        [{'solo': [('alto voice', '2')]}],
        [{'solo': [('tenor voice', '1')]}],
        [{'solo': [('baritone voice', '1')]}],
        [{'solo': [('bass voice', '1')]}],
        [{'primary': [('mixed chorus', '2', ['SATB, SATB'])]}],
        [{'primary': [('children\'s chorus', '1')]}],
        [{'primary': [('orchestra', '1')]}],
     ]}),
    (['a', 'violin', 'p', 'flute', 'd', 'viola', 'p', 'alto flute',
      'd', 'cello', 'p', 'saxophone', 'd', 'double bass'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]}, {'alt': [('flute', '1')]},
         {'doubling': [('viola', '1')]}, {'alt': [('alto flute', '1')]},
         {'doubling': [('cello', '1')]}, {'alt': [('saxophone', '1')]},
         {'doubling': [('double bass', '1')]}],
     ]}),
    (['a', 'violin', 'd', 'viola', 'd', 'cello', 'd', 'double bass', 
      'p', 'flute', 'd', 'alto flute', 'd', 'saxophone'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
         {'alt': [('flute', '1')]},
         {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
     ]}),
    (['a', 'violin', 'v', 'Note1', 'v', 'Note2', 'd', 'viola', 'v', 'Note3',
      'd', 'cello', 'n', '2', 'v', 'Note4', 'v', 'Note5'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1', ['Note1', 'Note2'])]},
         {'doubling': [('viola', '1', ['Note3']),
                       ('cello', '2', ['Note4', 'Note5'])]}]
    ]}),
    (['a', 'violin', 'd', 'viola', 'd', 'cello', 'd', 'double bass',
      'p', 'flute', 'p', 'clarinet', 'd', 'alto flute', 'd', 'saxophone'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
         {'alt': [('flute', '1'), ('clarinet', '1')]},
         {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
     ]}),
    (['a', 'violin', 'p', 'flute', 'p', 'trumpet', 'p', 'clarinet',
      'd', 'viola', 'p', 'alto flute', 'd', 'cello', 'p', 'saxophone',
      'd', 'double bass'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'alt': [('flute', '1'), ('trumpet', '1'), ('clarinet', '1')]},
         {'doubling': [('viola', '1')]}, {'alt': [('alto flute', '1')]},
         {'doubling': [('cello', '1')]}, {'alt': [('saxophone', '1')]},
         {'doubling': [('double bass', '1')]}],
     ]}),
    (['3', 'Piece One', 'b', 'flute', 'n', '1', 'a', 'orchestra', 'e', '1',
      'r', '1', 't', '1', '2', 'lcmpt'],
     {'materials_specified': ['Piece One'],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
        [{'solo': [('flute', '1')]}],
        [{'primary': [('orchestra', '1')]}],
     ]}),
])
def test_performancemedparser_parse(subfields, expected):
    """
    PerformanceMedParser `parse` method should return a dict with the
    expected structure, given the provided MARC 382 field.
    """
    field = s2m.make_mfield('382', subfields=subfields)
    assert s2m.PerformanceMedParser(field).parse() == expected


@pytest.mark.parametrize('subfields, expected', [
    (['b', 'Ph.D', 'c', 'University of Louisville', 'd', '1997.'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': '1997',
      'note_parts': ['Ph.D ― University of Louisville, 1997']
     }),
    (['b', 'Ph.D', 'c', 'University of Louisville.'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': None,
      'note_parts': ['Ph.D ― University of Louisville']
     }),
    (['b', 'Ph.D', 'd', '1997.'],
     {'degree': 'Ph.D',
      'institution': None,
      'date': '1997',
      'note_parts': ['Ph.D ― 1997']
     }),
    (['b', 'Ph.D'],
     {'degree': 'Ph.D',
      'institution': None,
      'date': None,
      'note_parts': ['Ph.D']
     }),
    (['g', 'Some thesis', 'b', 'Ph.D', 'c', 'University of Louisville',
      'd', '1997.'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': '1997',
      'note_parts': ['Some thesis', 'Ph.D ― University of Louisville, 1997']
     }),
    (['g', 'Some thesis', 'b', 'Ph.D', 'c', 'University of Louisville',
      'd', '1997.', 'g', 'Other info', 'o', 'identifier'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': '1997',
      'note_parts': ['Some thesis', 'Ph.D ― University of Louisville, 1997',
                     'Other info', 'identifier']
     }),
])
def test_dissertationnotesfieldparser_parse(subfields, expected):
    """
    DissertationNotesFieldParser `parse` method should return a dict
    with the expected structure, given the provided MARC 502 subfields.
    """
    field = s2m.make_mfield('502', subfields=subfields)
    assert s2m.DissertationNotesFieldParser(field).parse() == expected


def test_blasmpipeline_do_creates_compiled_dict(blasm_pipeline_class):
    """
    The `do` method of BlacklightASMPipeline should return a dict
    compiled from the return value of each of the `get` methods--each
    key/value pair from each return value added to the finished value.
    If the same dict key is returned by multiple methods and the vals
    are lists, the lists are concatenated.
    """
    class DummyPipeline(blasm_pipeline_class):
        fields = ['dummy1', 'dummy2', 'dummy3', 'dummy4']
        prefix = 'get_'

        def get_dummy1(self, r, marc_record):
            return {'d1': 'd1v'}

        def get_dummy2(self, r, marc_record):
            return { 'd2a': 'd2av', 'd2b': 'd2bv' }

        def get_dummy3(self, r, marc_record):
            return { 'stuff': ['thing'] }

        def get_dummy4(self, r, marc_record):
            return { 'stuff': ['other thing']}

    dummy_pipeline = DummyPipeline()
    bundle = dummy_pipeline.do('test', 'test')
    assert bundle == { 'd1': 'd1v', 'd2a': 'd2av', 'd2b': 'd2bv',
                       'stuff': ['thing', 'other thing'] }


def test_blasmpipeline_getid(bl_sierra_test_record, blasm_pipeline_class):
    """
    BlacklightASMPipeline.get_id should return the bib Record ID
    formatted according to III's specs.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('b6029459')
    val = pipeline.get_id(bib, None)
    assert val == {'id': '.b6029459'}


@pytest.mark.parametrize('in_val, expected', [
    (True, 'true'),
    (False, 'false')
])
def test_blasmpipeline_getsuppressed(in_val, expected, bl_sierra_test_record,
                                     blasm_pipeline_class,
                                     setattr_model_instance):
    """
    BlacklightASMPipeline.get_suppressed should return 'false' if the
    record is not suppressed.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('b6029459')
    setattr_model_instance(bib, 'is_suppressed', in_val)
    val = pipeline.get_suppressed(bib, None)
    assert val == {'suppressed': expected}


@pytest.mark.parametrize('bib_locs, created_date, cat_date, expected', [
    ([], None, None, None),
    (['czwww'], datetime.datetime(2018, 1, 1, tzinfo=pytz.utc),
     datetime.datetime(2019, 12, 31, tzinfo=pytz.utc),
     '2018-01-01T00:00:00Z'),
    (['czwww', 'czm'], datetime.datetime(2018, 1, 1, tzinfo=pytz.utc),
     datetime.datetime(2019, 12, 31, tzinfo=pytz.utc),
     '2019-12-31T00:00:00Z'),
    (['czwww', 'mwww'], datetime.datetime(2018, 1, 1, tzinfo=pytz.utc),
     datetime.datetime(2019, 12, 31, tzinfo=pytz.utc),
     '2018-01-01T00:00:00Z'),
    (['czm'], datetime.datetime(2018, 1, 1, tzinfo=pytz.utc),
     datetime.datetime(2019, 12, 31, tzinfo=pytz.utc),
     '2019-12-31T00:00:00Z'),
])
def test_blasmpipeline_getdateadded(bib_locs, created_date, cat_date, expected,
                                    bl_sierra_test_record, blasm_pipeline_class,
                                    get_or_make_location_instances,
                                    update_test_bib_inst,
                                    setattr_model_instance):
    """
    BlacklightASMPipeline.get_date_added should return the correct date
    in the datetime format Solr requires.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    loc_info = [{'code': code} for code in bib_locs]
    locations = get_or_make_location_instances(loc_info)
    if locations:
        bib = update_test_bib_inst(bib, locations=locations)
    setattr_model_instance(bib, 'cataloging_date_gmt', cat_date)
    setattr_model_instance(bib.record_metadata, 'creation_date_gmt',
                           created_date)
    val = pipeline.get_date_added(bib, None)
    assert val == {'date_added': expected}


def test_blasmpipeline_getiteminfo_ids(bl_sierra_test_record,
                                       blasm_pipeline_class,
                                       update_test_bib_inst,
                                       assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BlacklightASMPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'i' key for each
    JSON object should match the numeric portion of the III rec num for
    that item.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=[{}, {'is_suppressed': True}, {}])
    val = pipeline.get_item_info(bib, None)
    
    items = [l.item_record for l in bib.bibrecorditemrecordlink_set.all()]
    expected = [{'i': str(item.record_metadata.record_num)} for item in items
                if not item.is_suppressed]
    assert_json_matches_expected(val['items_json'], expected)


@pytest.mark.parametrize('bib_cn_info, items_info, expected', [
    ([('c', '050', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [])],
     [('TEST BIB CN', None)]),
    ([('c', '090', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [])],
     [('TEST BIB CN', None)]),
    ([('c', '092', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [])],
     [('TEST BIB CN', None)]),
    ([('c', '099', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [])],
     [('TEST BIB CN', None)]),
    ([],
     [({'copy_num': 1}, [('c', '050', ['|aTEST ITEM CN'])])],
     [('TEST ITEM CN', None)]),
    ([],
     [({'copy_num': 1}, [('c', '090', ['|aTEST ITEM CN'])])],
     [('TEST ITEM CN', None)]),
    ([],
     [({'copy_num': 1}, [('c', '092', ['|aTEST ITEM CN'])])],
     [('TEST ITEM CN', None)]),
    ([],
     [({'copy_num': 1}, [('c', '099', ['|aTEST ITEM CN'])])],
     [('TEST ITEM CN', None)]),
    ([],
     [({'copy_num': 1}, [('c', None, ['TEST ITEM CN'])])],
     [('TEST ITEM CN', None)]),
    ([('c', '050', ['TEST BIB CN'])],
     [({'copy_num': 1}, [('c', None, ['TEST ITEM CN'])]),
      ({'copy_num': 1}, [])],
     [('TEST ITEM CN', None),
      ('TEST BIB CN', None)]),
    ([('c', '050', ['TEST BIB CN'])],
     [({'copy_num': 1}, [('c', '999', ['TEST ITEM CN'])]),
      ({'copy_num': 1}, [])],
     [('TEST ITEM CN', None),
      ('TEST BIB CN', None)]),
    ([('c', '050', ['TEST BIB CN'])],
     [({'copy_num': 2}, [('c', None, ['TEST ITEM CN'])]),
      ({'copy_num': 3}, [])],
     [('TEST ITEM CN c.2', None),
      ('TEST BIB CN c.3', None)]),
    ([('c', '050', ['TEST BIB CN'])],
     [({'copy_num': 1}, [('v', None, ['volume 1'])])],
     [('TEST BIB CN', 'volume 1')]),
    ([('c', '050', ['TEST BIB CN'])],
     [({'copy_num': 1}, [('v', None, ['volume 2', 'volume 1'])])],
     [('TEST BIB CN', 'volume 2')]),
    ([('c', '050', ['TEST BIB CN'])],
     [({'copy_num': 2}, [('v', None, ['volume 1'])])],
     [('TEST BIB CN', 'volume 1 c.2')]),
    ([],
     [({'copy_num': 1}, [])],
     [(None, None)]),
], ids=[
    'bib cn (c050), no item cn => bib cn',
    'bib cn (c090), no item cn => bib cn',
    'bib cn (c092), no item cn => bib cn',
    'bib cn (c099), no item cn => bib cn',
    'no bib cn, item cn (c050) => item cn',
    'no bib cn, item cn (c090) => item cn',
    'no bib cn, item cn (c092) => item cn',
    'no bib cn, item cn (c099) => item cn',
    'no bib cn, item cn (non-marc c-tagged field) => item cn',
    'item cn, if present, overrides bib cn',
    'item cn w/MARC tag 999 counts as valid cn',
    'copy_num > 1 is appended to cn',
    'volume is appended to cn',
    'if >1 volumes, only the first is used',
    'both copy_num AND volume may appear (volume first, then copy)',
    'if NO cn, copy, or volume, cn defaults to None/null'
])
def test_blasmpipeline_getiteminfo_callnum_vol(bib_cn_info, items_info,
                                               expected, bl_sierra_test_record,
                                               blasm_pipeline_class,
                                               update_test_bib_inst,
                                               assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BlacklightASMPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'c' key for each
    JSON object contains the call number, and the 'v' key contains the
    volume. Various parameters test how the item call numbers and
    volumes are generated.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, varfields=bib_cn_info, items=items_info)
    val = pipeline.get_item_info(bib, None)
    expected = [{'c': cn, 'v': vol} for cn, vol in expected]
    assert_json_matches_expected(val['items_json'], expected)


@pytest.mark.parametrize('items_info, expected', [
    ([({'copy_num': 1}, [('b', None, ['1234567890'])])],
     [{'b': '1234567890'}]),
    ([({'copy_num': 1}, [('b', None, ['2', '1'])])],
     [{'b': '2'}]),
    ([({'copy_num': 1}, [('p', None, ['Note1', 'Note2'])])],
     [{'n': ['Note1', 'Note2']}]),
    ([({'copy_num': 1}, [])],
     [{'b': None, 'n': None}]),
], ids=[
    'one barcode',
    'if the item has >1 barcode, just the first is used',
    'if the item has >1 note, then all are included',
    'if no barcodes/notes, barcode/notes is None/null',
])
def test_blasmpipeline_getiteminfo_bcodes_notes(items_info, expected,
                                                bl_sierra_test_record,
                                                blasm_pipeline_class,
                                                update_test_bib_inst,
                                                assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BlacklightASMPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'b' and 'n' keys
    for each JSON object contain the barcode and public notes,
    respectively. Various parameters test how those are generated.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    val = pipeline.get_item_info(bib, None)
    assert_json_matches_expected(val['items_json'], expected)


@pytest.mark.parametrize('items_info, exp_items, exp_more_items', [
    ([({}, [('b', None, ['1'])]),
      ({}, [('b', None, ['2'])])],
     [{'b': '1'},
      {'b': '2'}],
     None),
    ([({}, [('b', None, ['1'])]),
      ({}, [('b', None, ['2'])]),
      ({}, [('b', None, ['3'])])],
     [{'b': '1'},
      {'b': '2'},
      {'b': '3'}],
     None),
    ([({}, [('b', None, ['1'])]),
      ({}, [('b', None, ['2'])]),
      ({}, [('b', None, ['3'])]),
      ({}, [('b', None, ['4'])]),
      ({}, [('b', None, ['5'])])],
     [{'b': '1'},
      {'b': '2'},
      {'b': '3'}],
     [{'b': '4'},
      {'b': '5'}]),
    ([({}, [('b', None, ['7'])]),
      ({}, [('b', None, ['3'])]),
      ({}, [('b', None, ['5'])]),
      ({}, [('b', None, ['2'])]),
      ({}, [('b', None, ['4'])]),
      ({}, [('b', None, ['6'])]),
      ({}, [('b', None, ['1'])])],
     [{'b': '7'},
      {'b': '3'},
      {'b': '5'}],
     [{'b': '2'},
      {'b': '4'},
      {'b': '6'},
      {'b': '1'},]),
], ids=[
    'fewer than three items => expect <3 items, no more_items',
    'three items => expect 3 items, no more_items',
    'more than three items => expect >3 items, plus more_items',
    'multiple items in bizarre order stay in order'
])
def test_blasmpipeline_getiteminfo_num_items(items_info, exp_items,
                                             exp_more_items,
                                             bl_sierra_test_record,
                                             blasm_pipeline_class,
                                             update_test_bib_inst,
                                             assert_json_matches_expected):
    """
    BlacklightASMPipeline.get_item_info return value should be a dict
    with keys `items_json`, `more_items_json`, and `has_more_items`
    that are based on the total number of items on the record. The
    first three attached items are in items_json; others are in
    more_items_json. has_more_items is 'true' if more_items_json is
    not empty. Additionally, items should remain in the order they
    appear on the record.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    val = pipeline.get_item_info(bib, None)
    assert_json_matches_expected(val['items_json'], exp_items)
    if exp_more_items:
        assert val['has_more_items'] == 'true'
        assert_json_matches_expected(val['more_items_json'], exp_more_items)
    else:
        assert val['has_more_items'] == 'false'
        assert val['more_items_json'] is None


@pytest.mark.parametrize('items_info, expected_r', [
    # Note: tests that are commented out represent "normal" policies;
    # currently due to COVID-19 a lot of requesting is restricted. We
    # will update these further as policies change.
    ([({'location_id': 'x'}, {}),
      # ({'location_id': 'w3'}, {}),
      ({'location_id': 'xmus', 'itype_id': 7}, {})],
     'catalog'),
    ([({'location_id': 'czwww'}, {}),
      ({'location_id': 'w3', 'item_status_id': 'o'}, {}),
      ({'location_id': 'w3', 'itype_id': 7}, {}),
      ({'location_id': 'w3', 'itype_id': 20}, {}),
      ({'location_id': 'w3', 'itype_id': 29}, {}),
      ({'location_id': 'w3', 'itype_id': 69}, {}),
      ({'location_id': 'w3', 'itype_id': 74}, {}),
      ({'location_id': 'w3', 'itype_id': 112}, {}),
      ],
     None),
    ([({'location_id': 'w4spe'}, {}),
      # ({'location_id': 'w4mr1'}, {}),
      # ({'location_id': 'w4mr2'}, {}),
      # ({'location_id': 'w4mr3'}, {}),
      # ({'location_id': 'w4mrb'}, {}),
      # ({'location_id': 'w4mrx'}, {})
     ], 'aeon'),
    ([({'location_id': 'jlf'}, {})],
     None),
], ids=[
    'items that are requestable through the catalog (Sierra)',
    'items that are not requestable',
    'items that are requestable through Aeon',
    'items that are at JLF -- temporarily unavailable'
])
def test_blasmpipeline_getiteminfo_requesting(items_info, expected_r,
                                              bl_sierra_test_record,
                                              blasm_pipeline_class,
                                              update_test_bib_inst,
                                              assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BlacklightASMPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'r' key for each
    JSON object contains a string describing how end users request the
    item. (See parameters for details.) Note that this hits the
    highlights but isn't exhaustive.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    val = pipeline.get_item_info(bib, None)
    exp_items = [{'r': expected_r} for i in range(0, len(items_info))]
    assert_json_matches_expected(val['items_json'], exp_items[0:3])
    if val['more_items_json'] is not None:
        assert_json_matches_expected(val['more_items_json'], exp_items[3:])


@pytest.mark.parametrize('bib_locations, bib_cn_info, expected', [
    ([('w', 'Willis Library'), ('czm', 'Chilton Media Library')],
     [('c', '090', ['|aTEST BIB CN'])],
     [{'i': None, 'c': 'TEST BIB CN', 'l': 'w'},
      {'i': None, 'c': 'TEST BIB CN', 'l': 'czm'}]),
    ([],
     [('c', '090', ['|aTEST BIB CN'])],
     [{'i': None, 'c': 'TEST BIB CN', 'l': 'none'}]),
])
def test_blasmpipeline_getiteminfo_pseudo_items(bib_locations, bib_cn_info,
                                                expected,
                                                bl_sierra_test_record,
                                                blasm_pipeline_class,
                                                get_or_make_location_instances,
                                                update_test_bib_inst,
                                                assert_json_matches_expected):
    """
    When a bib record has no attached items, the `items_json` key of
    the value returned by BlacklightASMPipeline.get_item_info should
    contain "pseudo-item" entries generated based off the bib locations
    and bib call numbers.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    loc_info = [{'code': code, 'name': name} for code, name in bib_locations]
    locations = get_or_make_location_instances(loc_info)
    bib = update_test_bib_inst(bib, varfields=bib_cn_info, locations=locations)
    val = pipeline.get_item_info(bib, None)
    assert_json_matches_expected(val['items_json'], expected)


@pytest.mark.parametrize('marcfields, items_info, expected', [
    ([('856', ['u', 'http://example.com', 'y', 'The Resource',
               'z', 'connect to electronic resource'])],
     [],
     [{'u': 'http://example.com', 'n': 'connect to electronic resource',
       'l': 'The Resource', 't': 'fulltext' }]),
    ([('856', ['u', 'http://example.com" target="_blank"', 'y', 'The Resource',
               'z', 'connect to electronic resource'])],
     [],
     [{'u': 'http://example.com', 'n': 'connect to electronic resource',
       'l': 'The Resource', 't': 'fulltext' }]),
    ([('856', ['u', 'http://example.com', 'u', 'incorrect',
               'z', 'connect to electronic resource', 'z', 'some version'])],
     [],
     [{'u': 'http://example.com',
       'n': 'connect to electronic resource some version',
       't': 'fulltext'}]),
    ([('856', ['u', 'http://example.com'])],
     [],
     [{'u': 'http://example.com', 't': 'link' }]),
    ([('856', ['z', 'Some label, no URL'])],
     [],
     []),
    ([('856', ['u', 'http://example.com', 'z', 'connect to e-resource'])],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'connect to online version'])],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access Journal Online'])],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'get full-text access'])],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access Full Text'])],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access thing here'], ' 0')],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access thing here'], ' 1')],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access thing here'], ' 2')],
     [],
     [{'t': 'link'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access online copy'], ' 2')],
     [],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access Bookplate here'])],
     [],
     [{'t': 'link'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access thing here'])],
     [({'item_status_id': 'w'}, [])],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access thing here'])],
     [({'item_status_id': '-'}, []),
      ({'item_status_id': 'w'}, [])],
     [{'t': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'z', 'access contents here']),
      ('856', ['u', 'http://example.com/2', 'z', 'access thing here'])],
     [({'item_status_id': 'w'}, [])],
     [{'t': 'link'}, {'t': 'link'}]),
    ([('962', ['t', 'Media Thing', 'u', 'http://example.com'])],
     [],
     [{'u': 'http://example.com', 'n': 'Media Thing', 't': 'media' }]),
    ([('962', ['t', 'Media Thing',
               'u', 'http://www.library.unt.edu/media/covers/cover.jpg',
               'e', 'http://www.library.unt.edu/media/thumb/thumb.jpg'])],
     [],
     []),
    ([('962', ['t', 'Media Thing'])],
     [],
     []),
    ([('962', ['t', 'Access Online Version', 'u', 'http://example.com'])],
     [],
     [{'t': 'fulltext' }]),
], ids=[
    '856: simple full text URL',
    '856: strip text from end of URL: " target=_blank',
    '856 w/repeated subfields',
    '856 w/out |y or |z is okay',
    '856 w/out |u is ignored (NO urls_json entry)',
    '856, type fulltext ("e-resource")',
    '856, type fulltext ("online version")',
    '856, type fulltext ("X Online")',
    '856, type fulltext ("full-text")',
    '856, type fulltext ("Full Text")',
    '856, type fulltext, ind2 == 0',
    '856, type fulltext, ind2 == 1',
    '856, type link, ind2 == 2',
    '856, type fulltext, ind2 == 2 BUT note says e.g. "online copy"',
    '856, type link ("bookplate")',
    '856, type fulltext: item with online status and 1 URL',
    '856, type fulltext: >1 items, 1 with online status, and 1 URL',
    '856, type link: item with online status but >1 URLs',
    '962 (media manager) URL, no media cover => urls_json entry',
    '962 (media manager) URL, w/media cover => NO urls_json entry',
    '962 (media manager) field, no URL => NO urls_json entry',
    '962 (media manager) field, type fulltext based on title',
])
def test_blasmpipeline_geturlsjson(marcfields, items_info, expected,
                                   bl_sierra_test_record, blasm_pipeline_class,
                                   bibrecord_to_pymarc, update_test_bib_inst,
                                   add_marc_fields,
                                   assert_json_matches_expected):
    """
    The `urls_json` key of the value returned by
    BlacklightASMPipeline.get_urls_json should be a list of JSON
    objects, each one corresponding to a URL.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('856', '962')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_urls_json(bib, bibmarc)
    assert_json_matches_expected(val['urls_json'], expected)


@pytest.mark.parametrize('marcfields, expected_url', [
    ([('962', ['t', 'Cover Image',
               'u', 'http://www.library.unt.edu/media/covers/cover.jpg',
               'e', 'http://www.library.unt.edu/media/covers/thumb.jpg'])],
     'https://library.unt.edu/media/covers/cover.jpg'),
    ([('962', ['t', 'Cover Image',
               'u', 'http://www.library.unt.edu/media/covers/cover.jpg" '
                    'target="_blank',
               'e', 'http://www.library.unt.edu/media/covers/thumb.jpg" '
                    'target="_blank'])],
     'https://library.unt.edu/media/covers/cover.jpg'),
    ([('962', ['t', 'Cover Image',
               'u', 'http://example.com/media/covers/cover.jpg',
               'e', 'http://example.com/media/covers/thumb.jpg'])],
     None),
    ([('856', ['u', 'http://digital.library.unt.edu/ark:/67531/metadc130771',
               'z', 'Connect to online resource'])],
     'https://digital.library.unt.edu/ark:/67531/metadc130771/small/'),
    ([('856', ['u', 'http://texashistory.unt.edu/ark:/67531/metadc130771',
               'z', 'Connect to online resource'])],
     'https://texashistory.unt.edu/ark:/67531/metadc130771/small/'),
    ([('856', ['u', 'http://digital.library.unt.edu/ark:/1/md/?utm_source=cat',
               'z', 'Connect to online resource'])],
     'https://digital.library.unt.edu/ark:/1/md/small/'),
    ([('856', ['u', 'http://digital.library.unt.edu/ark:/1/md/" target="_blank',
               'z', 'Connect to online resource'])],
     'https://digital.library.unt.edu/ark:/1/md/small/'),
    ([('856', ['u', 'http://digital.library.unt.edu/ark:/1/md/?utm_source=cat"'
                    ' target="_blank',
               'z', 'Connect to online resource'])],
     'https://digital.library.unt.edu/ark:/1/md/small/'),
    ([('856', ['u', 'http://example.com/whatever', 'z', 'Resource'])],
     None)
], ids=[
    'standard media library cover',
    'media cover with hacked attribute additions on URLs',
    'other 962 image(s): ignore non-UNTL media images',
    'standard Digital Library cover',
    'standard Portal cover',
    'strip querystrings when formulating DL/Portal URLs',
    'DL/Portal cover with hacked attribute additions on URLs',
    'DL/Portal cover with hacked attribute additions on URLs AND querystring',
    'other 856 link(s): ignore non-DL/Portal URLs'
])
def test_blasmpipeline_getthumbnailurl(marcfields, expected_url,
                                       bl_sierra_test_record,
                                       blasm_pipeline_class,
                                       bibrecord_to_pymarc, add_marc_fields):
    """
    BlacklightASMPipeline.get_thumbnail_url should return a URL for
    a local thumbnail image, if one exists. (Either Digital Library or
    a Media Library thumbnail.)
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('856', '962')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_thumbnail_url(bib, bibmarc)
    assert val['thumbnail_url'] == expected_url


@pytest.mark.parametrize('marcfields, exp_pub_sort, exp_pub_year_display, '
                         'exp_pub_year_facet, exp_pub_decade_facet, '
                         'exp_pub_dates_search', [
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004'])],
     '2004', '2004', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004'])],
     '2004', '2004', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 's2004    '),
      ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
      ('260', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
     '2004', '2004', ['2004', '2005'], ['2000-2009'], ['2004', '2005',
                                                       '2000s']),
    ([('008', 's2004    '),
      ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
      ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
     '2004', '2004', ['2004', '2005'], ['2000-2009'], ['2004', '2005',
                                                       '2000s']),
    ([('008', 's2004    '),
      ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2005, c2004'])],
     '2004', '2004', ['2004', '2005'], ['2000-2009'], ['2004', '2005',
                                                       '2000s']),
    ([('008', 's2004    '),
      ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2005'], ' 2'),
      ('264', ['c', 'copyright 2004 by XYZ'], ' 4')],
     '2004', '2004', ['2004', '2005'], ['2000-2009'], ['2004', '2005',
                                                       '2000s']),
    ([('008', 's2004    '),
      ('046', ['a', 's', 'c', '2019']),
      ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
      ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
     '2004', '2004', ['2004', '2005', '2019'], ['2000-2009', '2010-2019'],
     ['2004', '2005', '2019', '2000s', '2010s']),
    ([('008', 's2004    '),
      ('046', ['k', '2019']),
      ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
      ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
     '2004', '2004', ['2004', '2005', '2019'], ['2000-2009', '2010-2019'],
     ['2004', '2005', '2019', '2000s', '2010s']),
    ([('008', 's2004    '),
      ('046', ['a', 's', 'c', '2018', 'k', '2019'])],
     '2004', '2004', ['2004', '2018', '2019'], ['2000-2009', '2010-2019'],
     ['2004', '2018', '2019', '2000s', '2010s']),
    ([('008', 's2004    ')],
     '2004', '2004', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('046', ['a', 's', 'c', '2019']),],
     '2019', '2019', ['2019'], ['2010-2019'], ['2019', '2010s']),
    ([('046', ['k', '2019']),],
     '2019', '2019', ['2019'], ['2010-2019'], ['2019', '2010s']),
    ([('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),],
     '2004', '2004', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 's2004    '),
      ('046', ['k', '2004']),
      ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
      ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
     '2004', '2004', ['2004', '2005'], ['2000-2009'], ['2004', '2005',
                                                       '2000s']),
    ([('008', 's2004    '),
      ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004, c2003',
               'e', 'Place2 :', 'f', 'Printer', 'g', '2005'])],
     '2004', '2004', ['2003', '2004', '2005'], ['2000-2009'],
     ['2003', '2004', '2005', '2000s']),
    ([('008', 'b2004    ')],
     '2004', '2004', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'c20049999')],
     '2004', '2004 to present', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'd20042016')],
     '2004', '2004 to 2016', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'd20042016'),
     ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004-2016'])],
     '2004', '2004 to 2016', ['2004', '2016'], ['2000-2009', '2010-2019'],
     ['2004', '2016', '2000s', '2010s']),
    ([('008', 'e20041126')],
     '2004', '2004', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'i20042016')],
     '2004', '2004 to 2016', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'k20042016')],
     '2004', '2004 to 2016', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'm20042016')],
     '2004', '2004 to 2016', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'nuuuuuuuu')],
     '----', 'dates unknown', [], [], []),
    ([('008', 'p20162004')],
     '2016', '2016', ['2004', '2016'], ['2000-2009', '2010-2019'],
     ['2004', '2016', '2000s', '2010s']),
    ([('008', 'q20042005')],
     '2004', '2004 to 2005', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 'r20042016')],
     '2004', '2004', ['2004', '2016'], ['2000-2009', '2010-2019'],
     ['2004', '2016', '2000s', '2010s']),
    ([('008', 't20042016')],
     '2004', '2004', ['2004', '2016'], ['2000-2009', '2010-2019'],
     ['2004', '2016', '2000s', '2010s']),
    ([('008', 'u2004uuuu')],
     '2004', '2004 to ?', ['2004'], ['2000-2009'], ['2004', '2000s']),
    ([('008', 's199u    ')],
     '199-', '1990s', ['1990', '1991', '1992', '1993', '1994', '1995', '1996',
                       '1997', '1998', '1999'], ['1990-1999'],
     ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998',
      '1999', '1990s']),
    ([('008', 's19uu    ')],
     '19--', '20th century', [], ['1900-1909', '1910-1919', '1920-1929',
                                  '1930-1939', '1940-1949', '1950-1959',
                                  '1960-1969', '1970-1979', '1980-1989',
                                  '1990-1999'],
     ['20th century', '1900s', '1910s', '1920s', '1930s', '1940s', '1950s',
      '1960s', '1970s', '1980s', '1990s']),
    ([('008', 's1uuu    ')],
     '1---', 'dates unknown', [], [], []),
    ([('008', 'q198u1990')],
     '198-', '1980s to 1990', ['1980', '1981', '1982', '1983', '1984', '1985',
                               '1986', '1987', '1988', '1989'], ['1980-1989'],
     ['1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988',
      '1989', '1980s']),
    ([('008', 'q15uu17uu')],
     '15--', '16th to 18th century', [], ['1500-1509', '1510-1519',
                                          '1520-1529', '1530-1539',
                                          '1540-1549', '1550-1559',
                                          '1560-1569', '1570-1579',
                                          '1580-1589', '1590-1599'],
     ['16th century', '1500s', '1510s', '1520s', '1530s', '1540s', '1550s',
      '1560s', '1570s', '1580s', '1590s']),
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '[199-?]'])],
     '2004', '2004', ['2004', '1990', '1991', '1992', '1993', '1994', '1995',
                      '1996', '1997', '1998', '1999'],
     ['1990-1999', '2000-2009'],
     ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998',
      '1999', '1990s', '2000s', '2004']),
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '[1990s?]'])],
     '2004', '2004', ['2004', '1990', '1991', '1992', '1993', '1994', '1995',
                      '1996', '1997', '1998', '1999'],
     ['1990-1999', '2000-2009'],
     ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998',
      '1999', '1990s', '2000s', '2004']),
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '[19--?]'])],
     '2004', '2004', ['2004'], ['1900-1909', '1910-1919', '1920-1929',
                                '1930-1939', '1940-1949', '1950-1959',
                                '1960-1969', '1970-1979', '1980-1989',
                                '1990-1999', '2000-2009'],
     ['2004', '20th century', '1900s', '1910s', '1920s', '1930s', '1940s',
      '1950s', '1960s', '1970s', '1980s', '1990s', '2000s']),
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '[20th century]'])],
     '2004', '2004', ['2004'], ['1900-1909', '1910-1919', '1920-1929',
                                '1930-1939', '1940-1949', '1950-1959',
                                '1960-1969', '1970-1979', '1980-1989',
                                '1990-1999', '2000-2009'],
     ['2004', '20th century', '1900s', '1910s', '1920s', '1930s', '1940s',
      '1950s', '1960s', '1970s', '1980s', '1990s', '2000s']),
    ([('008', 's2014    '),
      ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '201[4]'])],
     '2014', '2014', ['2014'], ['2010-2019'], ['2014', '2010s']),
    ([('008', 's0300    '),
      ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '[ca. 300?]'])],
     '0300', '300', ['0300'], ['0300-0309'], ['300', '300s']),
    ([('008', 's0301    '),
      ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '[ca. 300?]'])],
     '0301', '301', ['0300', '0301'], ['0300-0309'], ['300', '301', '300s']),
    ([('008', 's2014    '),
      ('362', ['a', 'Vol. 1, no. 1 (Apr. 1981)-'], '0 ')],
     '2014', '2014', ['2014', '1981'], ['2010-2019', '1980-1989'],
     ['2014', '2010s', '1981', '1980s']),
    ([('008', 's2014    '),
      ('362', ['a', 'Began with vol. 4, published in 1947.'], '1 ')],
     '2014', '2014', ['2014', '1947'], ['2010-2019', '1940-1949'],
     ['2014', '2010s', '1947', '1940s']),
    ([('008', 's2014    '),
      ('362', ['a', 'Published in 1st century.'], '1 ')],
     '2014', '2014', ['2014'], ['2010-2019', '0000-0009', '0010-0019',
                                '0020-0029', '0030-0039', '0040-0049',
                                '0050-0059', '0060-0069', '0070-0079',
                                '0080-0089', '0090-0099'],
     ['2014', '2010s', '0s', '10s', '20s', '30s', '40s', '50s', '60s', '70s',
      '80s', '90s', '1st century']),
], ids=[
    'standard, single date in 008 repeated in 260',
    'standard, single date in 008 repeated in 264',
    'single date in 008, multiple dates in 260s',
    'single date in 008, multiple dates in 264s',
    'single date in 008, pubdate and copyright date in 260',
    'single date in 008, pubdate and copyright dates in 264s',
    'single date in 008, dates in 264s and coded part of 046',
    'single date in 008, dates in 264s and non-coded part of 046',
    'single date in 008, multiple dates in 046',
    'single date in 008, no other dates',
    'date in coded 046, no other pubdate fields',
    'date in non-coded 046, no other pubdate fields',
    'date in 260/264, no other pubdate fields',
    'various repeated dates should be deduplicated',
    'various dates in one 260 field should all be captured',
    '008 code b: interpreted as single pub date',
    '008 code c: continuing resource date range to present',
    '008 code d: continuing resource, past date range',
    '008 code d: continuing resource, past date range, w/264',
    '008 code e: detailed date',
    '008 code i: inclusive dates of collection',
    '008 code k: range of years of bulk of collection',
    '008 code m: multiple dates',
    '008 code n: dates unknown',
    '008 code p: date of distribution, date of production',
    '008 code q: questionable date',
    '008 code r: reprint date, original date',
    '008 code t: publication date, copyright date',
    '008 code u: continuing resource, status unknown',
    'decade (199u) in single-date 008',
    'century (19uu) in single-date 008',
    'unknown date (1uuu) in single-date 008',
    'decade (199u) in date-range 008',
    'centuries (19uu) in date-range 008',
    'decade (199-) in 26X but NOT in 008',
    'decade (spelled out) in 26X but NOT in 008',
    'century (19--) in 26X but NOT in 008',
    'century (spelled out) in 26X but NOT in 008',
    'partial date in square brackets is recognizable',
    'three-digit year in 008 and 260c work',
    'three-digit year only 260c works',
    'formatted date in 362 works',
    'non-formatted date in 362 works',
    'century (1st) in 362 works'
])
def test_blasmpipeline_getpubinfo_dates(marcfields, exp_pub_sort,
                                        exp_pub_year_display,
                                        exp_pub_year_facet,
                                        exp_pub_decade_facet,
                                        exp_pub_dates_search,
                                        bl_sierra_test_record,
                                        blasm_pipeline_class,
                                        bibrecord_to_pymarc, add_marc_fields):
    """
    BlacklightASMPipeline.get_pub_info should return date-string fields
    matching the expected parameters.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('260', '264', '362')
    if len(marcfields) and marcfields[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], marcfields[0][1], data[15:])
        marcfields[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_pub_info(bib, bibmarc)
    assert val['publication_sort'] == exp_pub_sort
    assert val['publication_year_display'] == exp_pub_year_display
    assert len(val['publication_year_facet']) == len(exp_pub_year_facet)
    assert len(val['publication_decade_facet']) == len(exp_pub_decade_facet)
    assert len(val['publication_dates_search']) == len(exp_pub_dates_search)
    for v in val['publication_year_facet']:
        assert v in exp_pub_year_facet
    for v in val['publication_decade_facet']:
        assert v in exp_pub_decade_facet
    for v in val['publication_dates_search']:
        assert v in exp_pub_dates_search


@pytest.mark.parametrize('marcfields, expected', [
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'])],
     {'publication_display': ['Place : Publisher, 2004']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'], ' 1')],
     {'publication_display': ['Place : Publisher, 2004']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0')],
     {'creation_display': ['Place : Producer, 2004']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Distributor,', 'c', '2004.'], ' 2')],
     {'distribution_display': ['Place : Distributor, 2004']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'manufacture_display': ['Place : Manufacturer, 2004']}),
    ([('008', 's2004    '),
      ('264', ['c', '2004'], ' 4')],
     {'copyright_display': ['2004']}),
    ([('008', 's2004    '),
      ('264', ['c', 'c2004'], ' 4')],
     {'copyright_display': ['©2004']}),
    ([('008', 'b2004    ')],
     {'publication_display': ['2004']}),
    ([('008', 'c20049999')],
     {'publication_display': ['2004 to present']}),
    ([('008', 'd20042012')],
     {'publication_display': ['2004 to 2012']}),
    ([('008', 'e20040101')],
     {'publication_display': ['2004']}),
    ([('008', 'i20042012')],
     {'creation_display': ['Collection created in 2004 to 2012']}),
    ([('008', 'k20042012')],
     {'creation_display': ['Collection created in 2004 to 2012']}),
    ([('008', 'm20042012')],
     {'publication_display': ['2004 to 2012']}),
    ([('008', 'm20049999')],
     {'publication_display': ['2004 to present']}),
    ([('008', 'm2004    ')],
     {'publication_display': ['2004']}),
    ([('008', 'muuuu2012')],
     {'publication_display': ['? to 2012']}),
    ([('008', 'nuuuuuuuu')], {}),
    ([('008', 'p20122004')],
     {'distribution_display': ['Released in 2012'],
      'creation_display': ['Created or produced in 2004']}),
    ([('008', 'q20042012')],
     {'publication_display': ['2004 to 2012']}),
    ([('008', 'r20122004')],
     {'distribution_display': ['Reproduced or reissued in 2012'],
      'publication_display': ['Originally published in 2004']}),
    ([('008', 'ruuuu2004')],
     {'publication_display': ['Originally published in 2004']}),
    ([('008', 'r2012uuuu')],
     {'distribution_display': ['Reproduced or reissued in 2012']}),
    ([('008', 's2004    ')],
     {'publication_display': ['2004']}),
    ([('008', 't20042012')],
     {'publication_display': ['2004'],
      'copyright_display': ['2012']}),
    ([('008', 'u2004uuuu')],
     {'publication_display': ['2004 to ?']}),
    ([('008', 's201u    ')],
     {'publication_display': ['the 2010s']}),
    ([('008', 's20uu    ')],
     {'publication_display': ['the 21st century']}),
    ([('008', 'm200u201u')],
     {'publication_display': ['the 2000s to the 2010s']}),
    ([('008', 'm19uu20uu')],
     {'publication_display': ['the 20th to the 21st century']}),
    ([('008', 'm19uu201u')],
     {'publication_display': ['the 20th century to the 2010s']}),
    ([('008', 's2012    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2012, c2004.'])],
     {'publication_display': ['Place : Publisher, 2012'],
      'copyright_display': ['©2004']}),
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,',
               'c', '2004, c2012 by Some Publisher.'])],
     {'publication_display': ['Place : Publisher, 2004'],
      'copyright_display': ['©2012 by Some Publisher']}),
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,',
               'c', '2012 printing, copyright 2004.'])],
     {'publication_display': ['Place : Publisher, 2012 printing'],
      'copyright_display': ['©2004']}),
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,',
               'c', '2004-2012.'])],
     {'publication_display': ['Place : Publisher, 2004-2012']}),
    ([('008', 's2004    '),
      ('260', ['a', 'First Place :', 'b', 'First Publisher;', 
               'a', 'Second Place :', 'b', 'Second Publisher,',
               'c', '2004.'])],
     {'publication_display': ['First Place : First Publisher; '
                              'Second Place : Second Publisher, 2004']}),
    ([('008', 's2004    '),
      ('260', ['a', 'First Place;', 'a', 'Second Place :', 'b', 'Publisher,',
               'c', '2004.'])],
     {'publication_display': ['First Place; Second Place : Publisher, 2004']}),
    ([('008', 's2004    '),
      ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
               'e', '(M Place :', 'f', 'Printer)'])],
     {'publication_display': ['P Place : Publisher, 2004'],
      'manufacture_display': ['M Place : Printer']}),
    ([('008', 's2004    '),
      ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
               'e', '(M Place :', 'f', 'Printer,', 'g', '2005)'])],
     {'publication_display': ['P Place : Publisher, 2004'],
      'manufacture_display': ['M Place : Printer, 2005']}),
    ([('008', 's2004    '),
      ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
               'g', '(2010 printing)'])],
     {'publication_display': ['P Place : Publisher, 2004'],
      'manufacture_display': ['2010 printing']}),
    ([('008', 's2014    '),
      ('362', ['a', 'Vol. 1, no. 1 (Apr. 1981)-'], '0 ')],
     {'publication_display': ['Vol. 1, no. 1 (Apr. 1981)-', '2014']}),
    ([('008', 's2014    '),
      ('362', ['a', 'Began with vol. 4, published in 1947.'], '1 ')],
     {'publication_display': ['2014'],
      'publication_date_notes': ['Began with vol. 4, published in 1947.']}),
], ids=[
    'Plain 260 => publication_display',
    '264 _1 => publication_display',
    '264 _0 => creation_display',
    '264 _2 => distribution_display',
    '264 _3 => manufacture_display',
    '264 _4 => copyright_display',
    '264 _4 => copyright_display (includes `c`, e.g. `c2004`)',
    'from 008 code b: p_display, no label (interpreted as single pub date)',
    'from 008 code c: p_display, no label, date range (continuing resource)',
    'from 008 code d: p_display, no label, date range (continuing resource)',
    'from 008 code e: p_display, no label, just year (detailed date)',
    'from 008 code i: creation_display, custom label (dates of collection)',
    'from 008 code k: creation_display, custom label (dates of collection)',
    'from 008 code m: p_display, no label (multiple dates, as range)',
    'from 008 code m: p_display, no label (multiple dates, to present)',
    'from 008 code m: p_display, no label (multiple dates, only one date)',
    'from 008 code m: p_display, no label (multiple dates, unkn start date)',
    'from 008 code n: dates unknown',
    'from 008 code p: distribution_ and creation_display, custom labels',
    'from 008 code q: questionable date',
    'from 008 code r: distribution_ and p_display, custom labels',
    'from 008 code r: distribution_ and p_display, custom labels (unk ddate)',
    'from 008 code r: distribution_ and p_display, custom labels (unk pdate)',
    'from 008 code s: p_display, no label (single date)',
    'from 008 code t: publication_ and copyright_display, no label',
    'from 008 code u: continuing resource, status unknown',
    'from 008, generated, decade',
    'from 008, generated, century',
    'from 008, generated, decade range',
    'from 008, generated, century range',
    'from 008, generated, mixed century/decade range',
    '260 with publication date then copyright date',
    '260 with pub and copyright date, extra info w/copyright date',
    '260 with labeled dates',
    '260 with date range',
    '260 with multiple groupings',
    '260 with multiple places',
    '260 with manufacturer information (no mf date)',
    '260 with manufacturer information (has mf date)',
    '260 with manufacturer information (ONLY mf date)',
    '362 with formatted date',
    '362 with non-formatted date'
])
def test_blasmpipeline_getpubinfo_statements(marcfields, expected,
                                             bl_sierra_test_record,
                                             blasm_pipeline_class,
                                             bibrecord_to_pymarc,
                                             add_marc_fields):
    """
    BlacklightASMPipeline.get_pub_info should return display statement
    fields matching the expected parameters.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('260', '264', '362')
    if len(marcfields) and marcfields[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], marcfields[0][1], data[15:])
        marcfields[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_pub_info(bib, bibmarc)
    if len(expected.keys()) == 0:
        assert 'created_display' not in val.keys()
        assert 'publication_display' not in val.keys()
        assert 'distribution_display' not in val.keys()
        assert 'manufacture_display' not in val.keys()
        assert 'copyright_display' not in val.keys()
    for k, v in expected.items():
        assert v == val[k]


@pytest.mark.parametrize('marcfields, expected', [
    ([('008', 's2004    '),
      ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'])],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Publisher']}),
    ([('008', 's2004    '),
      ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
               'e', '(M Place :', 'f', 'Printer,', 'g', '2005)'])],
     {'publication_places_search': ['P Place', 'M Place'],
      'publishers_search': ['Publisher', 'Printer']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Producer']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'], ' 1')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Publisher']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Distributor,', 'c', '2004.'], ' 2')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Distributor']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Manufacturer']}),
    ([('008', 's2004    '),
      ('264', ['a', 'Prod Place :', 'b', 'Producer,', 'c', '2004.'], ' 0'),
      ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'], ' 1'),
      ('264', ['a', 'Place :', 'b', 'Distributor,', 'c', '2004.'], ' 2'),
      ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'publication_places_search': ['Prod Place', 'Place'],
      'publishers_search': ['Producer', 'Publisher', 'Distributor',
                            'Manufacturer']}),
    ([('008', 's2004    '),
      ('260', ['a', '[S.l. :', 'b', 's.n.]', 'c', '2004.']),
      ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0'),
      ('264', ['a', '[Place of publication not identified] :',
               'b', 'Publisher,', 'c', '2004.'], ' 1'),
      ('264', ['a', 'Place :', 'b', '[distributor not identified],',
               'c', '2004.'], ' 2'),
      ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Producer', 'Publisher', 'Manufacturer']}),
    ([('008', 's2004    '),
      ('260', ['c', '2004.']),
      ('264', ['c', '2004.'], ' 4')],
     {'publication_places_search': [],
      'publishers_search': []}),
    ([('008', 's2004    '),],
     {'publication_places_search': [],
      'publishers_search': []}),
], ids=[
    'Plain 260 => publisher and place search values',
    '260 w/manufacturer info, includes mf place and entity',
    '264 _0',
    '264 _1',
    '264 _2',
    '264 _3',
    '264: multiple fields include all relevant info (deduplicated)',
    '260/264: unknown info ([S.l.], [s.n.], X not identified) stripped',
    '260/264: missing pub info okay',
    'no 260/264 okay'
])
def test_blasmpipeline_getpubinfo_pub_and_place_search(marcfields, expected,
                                                       bl_sierra_test_record,
                                                       blasm_pipeline_class,
                                                       bibrecord_to_pymarc,
                                                       add_marc_fields):
    """
    BlacklightASMPipeline.get_pub_info should return publishers_search
    and publication_places_search fields matching the expected
    parameters.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('260', '264')
    if len(marcfields) and marcfields[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], marcfields[0][1], data[15:])
        marcfields[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_pub_info(bib, bibmarc)
    if len(expected.keys()) == 0:
        assert 'publication_places_search' not in val.keys()
        assert 'publishers_search' not in val.keys()
    for k, v in expected.items():
        assert set(v) == set(val[k])


@pytest.mark.parametrize('bib_locations, item_locations, expected', [
    ( (('czm', 'Chilton Media Library'),),
      (('czm', 'Chilton Media Library'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': []},
    ),
    ( (('czm', 'Chilton Media Library'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': []},
    ),
    ( (('blah', 'Blah'),),
      (('blah2', 'Blah2'), ('czm', 'Chilton Media Library'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': []}
    ),
    ( (('blah', 'Blah'),),
      (('blah2', 'Blah2'), ('blah', 'Blah'),),
      {'access_facet': [],
       'collection_facet': [],
       'building_facet': [],
       'shelf_facet': []}
    ),
    ( (('r', 'Discovery Park Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
      {'access_facet': ['Online'],
       'collection_facet': ['General Collection'],
       'building_facet': [],
       'shelf_facet': []}
    ),
    ( (('r', 'Discovery Park Library'), ('lwww', 'UNT ONLINE RESOURCES')),
      tuple(),
      {'access_facet': ['At the Library', 'Online'],
       'collection_facet': ['Discovery Park Library', 'General Collection'],
       'building_facet': ['Discovery Park Library'],
       'shelf_facet': []}
    ),
    ( (('w', 'Willis Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
      {'access_facet': ['Online'],
       'collection_facet': ['General Collection'],
       'building_facet': [],
       'shelf_facet': []}
    ),
    ( (('x', 'Remote Storage'),),
      (('xdoc', 'Government Documents Remote Storage'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Government Documents'],
       'building_facet': ['Remote Storage'],
       'shelf_facet': []}
    ),
    ( (('sd', 'Eagle Commons Library Government Documents'),),
      (('xdoc', 'Government Documents Remote Storage'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Government Documents'],
       'building_facet': ['Remote Storage'],
       'shelf_facet': []}
    ),
    ( (('w', 'Willis Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'), ('w3', 'Willis Library-3rd Floor'),),
      {'access_facet': ['Online', 'At the Library'],
       'collection_facet': ['General Collection'],
       'building_facet': ['Willis Library'],
       'shelf_facet': ['Willis Library-3rd Floor']}
    ),
    ( (('sd', 'Eagle Commons Library Government Documents'),),
      (('gwww', 'GOVT ONLINE RESOURCES'),
       ('sdus', 'Eagle Commons Library US Documents'),
       ('rst', 'Discovery Park Library Storage'),
       ('xdoc', 'Government Documents Remote Storage'),),
      {'access_facet': ['Online', 'At the Library'],
       'collection_facet': ['Government Documents', 'Discovery Park Library'],
       'building_facet': ['Eagle Commons Library', 'Discovery Park Library',
                          'Remote Storage'],
       'shelf_facet': ['Eagle Commons Library US Documents',
                       'Discovery Park Library Storage']}
    ),
], ids=[
    'czm / same bib and item location',
    'czm / bib loc exists, but no items',
    'czm / unknown bib location and one unknown item location',
    'all bib and item locations are unknown',
    'r, lwww / online-only item with bib location in different collection',
    'r, lwww / two different bib locations, no items',
    'w, lwww / online-only item with bib location in same collection',
    'x, xdoc / Remote Storage, bib loc is x',
    'sd, xdoc / Remote Storage, bib loc is not x',
    'w, lwww, w3 / bib with online and physical locations',
    'sd, gwww, sdus, rst, xdoc / multiple items at multiple locations',
])
def test_blasmpipeline_getaccessinfo(bib_locations, item_locations, expected,
                                     bl_sierra_test_record,
                                     blasm_pipeline_class,
                                     update_test_bib_inst,
                                     get_or_make_location_instances):
    """
    BlacklightASMPipeline.get_access_info should return the expected
    access, collection, building, and shelf facet values based on the
    configured bib_ and item_locations.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')

    loc_set = list(set(bib_locations) | set(item_locations))
    loc_info = [{'code': code, 'name': name} for code, name in loc_set]
    items_info = [{'location_id': code} for code, name in item_locations]
    locations = get_or_make_location_instances(loc_info)
    bib = update_test_bib_inst(bib, items=items_info, locations=locations)
    val = pipeline.get_access_info(bib, None)
    for k, v in expected.items():
        assert set(v) == set(val[k])


@pytest.mark.parametrize('bcode2, expected', [
    ('a', {'resource_type': 'ebook',
           'resource_type_facet': ['books'],
           'media_type_facet': ['Digital Files']}),
    ('b', {'resource_type': 'database',
           'resource_type_facet': ['online_databases'],
           'media_type_facet': ['Digital Files']}),
    ('c', {'resource_type': 'score!Online',
           'resource_type_facet': ['music_scores'],
           'media_type_facet': ['Digital Files']}),
])
def test_blasmpipeline_getresourcetypeinfo(bcode2,
                                           expected, bl_sierra_test_record,
                                           blasm_pipeline_class,
                                           setattr_model_instance):
    """
    BlacklightASMPipeline.get_resource_type_info should return the
    expected resource_type and resource_type_facet values based on the
    given bcode2. Note that this doesn't test resource type nor
    category (facet) determination. For that, see base.local_rulesets
    (and associated tests).
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    setattr_model_instance(bib, 'bcode2', bcode2)
    val = pipeline.get_resource_type_info(bib, None)
    for k, v in expected.items():
        assert v == val[k]


@pytest.mark.parametrize('marcfields, expected', [
    ([('600', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 ')], {}),
    ([('100', [], '1 ')], {}),
    ([('110', [], '  ')], {}),
    ([('111', [], '  ')], {}),
    ([('100', ['e', 'something'], '1 ')], {}),
    ([('110', ['e', 'something'], '  ')], {}),
    ([('111', ['j', 'something'], '  ')], {}),
    ([('100', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 '),
      ('700', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.',
               't', 'Title of some related work.'],
       '1 ')],
     {'author_search': ['Churchill, Winston, Sir, 1874-1965',
                        'Winston Churchill, Sir',
                        'Sir Winston Churchill'],
      'author_contributor_facet': ['Churchill, Winston, Sir, 1874-1965'],
      'author_sort': 'churchill, winston, sir, 1874-1965',
      'author_json': {'p': [{'d': 'Churchill, Winston, Sir, 1874-1965'}]}
     }),
    ([('700', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.',
               't', 'Title of some related work.'],
       '1 '),
      ('700', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.',
               't', 'Another related work.'],
       '1 ')],
     {'contributors_search': ['Churchill, Winston, Sir, 1874-1965',
                              'Winston Churchill, Sir',
                              'Sir Winston Churchill'],
      'author_contributor_facet': ['Churchill, Winston, Sir, 1874-1965'],
      'author_sort': 'churchill, winston, sir, 1874-1965',
      'contributors_json': {'p': [{'d': 'Churchill, Winston, Sir, 1874-1965'}]}
     }),
    ([('100', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 ')],
     {'author_search': ['Churchill, Winston, Sir, 1874-1965',
                        'Winston Churchill, Sir',
                        'Sir Winston Churchill'],
      'author_contributor_facet': ['Churchill, Winston, Sir, 1874-1965'],
      'author_sort': 'churchill, winston, sir, 1874-1965',
      'author_json': {'p': [{'d': 'Churchill, Winston, Sir, 1874-1965'}]}
     }),
    ([('100', ['a', 'Thomas,', 'c', 'Aquinas, Saint,', 'd', '1225?-1274.'],
        '0 ')],
     {'author_search': ['Thomas, Aquinas, Saint, 1225?-1274',
                        'Aquinas Thomas, Saint',
                        'Saint Thomas, Aquinas'],
      'author_contributor_facet': ['Thomas, Aquinas, Saint, 1225?-1274'],
      'author_sort': 'thomas, aquinas, saint, 1225?-1274',
      'author_json': {'p': [{'d': 'Thomas, Aquinas, Saint, 1225?-1274'}]}
     }),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964,', 'e', 'writing,',
               'e', 'direction,', 'e', 'production.'], '1 ')],
     {'author_search': ['Hecht, Ben, 1893-1964', 'Ben Hecht'],
      'responsibility_search': ['Ben Hecht writing', 'Ben Hecht direction',
                                'Ben Hecht production'],
      'author_contributor_facet': ['Hecht, Ben, 1893-1964'],
      'author_sort': 'hecht, ben, 1893-1964',
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964'}],
                      'r': ['writing', 'direction', 'production']}
     }),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964,',
               'e', 'writing, direction, production.'], '1 ')],
     {'author_search': ['Hecht, Ben, 1893-1964', 'Ben Hecht'],
      'responsibility_search': ['Ben Hecht writing', 'Ben Hecht direction',
                                'Ben Hecht production'],
      'author_contributor_facet': ['Hecht, Ben, 1893-1964'],
      'author_sort': 'hecht, ben, 1893-1964',
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964'}],
                      'r': ['writing', 'direction', 'production']}
     }),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964.', '4', 'drt', '4', 'pro'],
       '1 ')],
     {'author_search': ['Hecht, Ben, 1893-1964', 'Ben Hecht'],
      'responsibility_search': ['Ben Hecht director', 'Ben Hecht producer'],
      'author_contributor_facet': ['Hecht, Ben, 1893-1964'],
      'author_sort': 'hecht, ben, 1893-1964',
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964'}],
                      'r': ['director', 'producer']}
     }),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964,', 'e', 'writer,',
               'e', 'director.', '4', 'drt', '4', 'pro'], '1 ')],
     {'author_search': ['Hecht, Ben, 1893-1964', 'Ben Hecht'],
      'responsibility_search': ['Ben Hecht writer', 'Ben Hecht director',
                                'Ben Hecht producer'],
      'author_contributor_facet': ['Hecht, Ben, 1893-1964'],
      'author_sort': 'hecht, ben, 1893-1964',
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964'}],
                      'r': ['writer', 'director', 'producer']}
     }),
    ([('700', ['i', 'Container of (work):',
               '4', 'http://rdaregistry.info/Elements/w/P10147',
               'a', 'Dicks, Terrance.',
               't', 'Doctor Who and the Dalek invasion of Earth.'], '12')],
     {'contributors_search': ['Dicks, Terrance', 'Terrance Dicks'],
      'author_contributor_facet': ['Dicks, Terrance'],
      'author_sort': 'dicks, terrance',
      'contributors_json': {'p': [{'d': 'Dicks, Terrance'}]}
     }),
    ([('710', ['a', 'Some Organization,', 't', 'Some Work Title.'], '22')],
     {'contributors_search': ['Some Organization'],
      'author_contributor_facet': ['Some Organization'],
      'author_sort': 'some organization',
      'contributors_json': {'p': [{'d': 'Some Organization'}]}
     }),
    ([('711', ['a', 'Some Festival.'], '2 ')],
     {'meeting_facet': ['Some Festival'],
      'meetings_search': ['Some Festival'],
      'meetings_json': [{'p': [{'d': 'Some Festival'}]}]
     }),
    ([('711', ['a', 'Some Festival.', 'e', 'Orchestra.'], '2 ')],
     {'meeting_facet': ['Some Festival'],
      'meetings_search': ['Some Festival'],
      'meetings_json': [{'p': [{'d': 'Some Festival'}]}],
      'contributors_search': ['Some Festival, Orchestra'],
      'author_contributor_facet': ['Some Festival, Orchestra'],
      'author_sort': 'some festival, orchestra',
      'contributors_json': {'p': [{'d': 'Some Festival, Orchestra'}]}
     }),
    ([('111', ['a', 'White House Conference on Lib and Info Services',
               'd', '(1979 :', 'c', 'Washington, D.C.).',
               'e', 'Ohio Delegation.'], '2 ')],
     {'author_search': [
        'White House Conference on Lib and Info Services, Ohio Delegation'],
      'author_contributor_facet': [
        'White House Conference on Lib and Info Services, Ohio Delegation'],
      'author_sort': 'white house conference on lib and info services, ohio '
                     'delegation',
      'author_json': {
        'p': [{'d': 'White House Conference on Lib and Info Services, Ohio '
                    'Delegation'}]},
      'meetings_search': [
        'White House Conference on Lib and Info Services (1979 : Washington, '
        'D.C.)'],
      'meeting_facet': [
        'White House Conference on Lib and Info Services',
        'White House Conference on Lib and Info Services (1979 : Washington, '
        'D.C.)'],
      'meetings_json': {
        'p': [
            {'d': 'White House Conference on Lib and Info Services'},
            {'d': '(1979 : Washington, D.C.)',
             'v': 'White House Conference on Lib and Info Services (1979 : '
                  'Washington, D.C.)'}]}
     }),
    ([('711', ['a', 'Olympic Games',
               'n', '(21st :', 'd', '1976 :', 'c', 'Montréal, Québec).',
               'e', 'Organizing Committee.', 'e', 'Arts and Culture Program.',
               'e', 'Visual Arts Section.'], '2 ')],
     {'contributors_search': [
        'Olympic Games, Organizing Committee > Arts and Culture Program > '
        'Visual Arts Section'],
      'author_contributor_facet': [
        'Olympic Games, Organizing Committee',
        'Olympic Games, Organizing Committee > Arts and Culture Program',
        'Olympic Games, Organizing Committee > Arts and Culture Program > '
        'Visual Arts Section'],
      'author_sort': 'olympic games, organizing committee > arts and culture '
                     'program > visual arts section',
      'contributors_json': {
        'p': [{'d': 'Olympic Games, Organizing Committee', 's': ' > '},
              {'d': 'Arts and Culture Program',
               'v': 'Olympic Games, Organizing Committee > Arts and Culture '
                    'Program', 's': ' > '},
              {'d': 'Visual Arts Section',
               'v': 'Olympic Games, Organizing Committee > Arts and Culture '
                    'Program > Visual Arts Section'}]},
      'meetings_search': [
        'Olympic Games (21st : 1976 : Montréal, Québec)'],
      'meeting_facet': [
        'Olympic Games',
        'Olympic Games (21st : 1976 : Montréal, Québec)'],
      'meetings_json': {
        'p': [
            {'d': 'Olympic Games'},
            {'d': '(21st : 1976 : Montréal, Québec)',
             'v': 'Olympic Games (21st : 1976 : Montréal, Québec)'}]}
     }),
    ([('111', ['a', 'International Congress of Gerontology.',
               'e', 'Satellite Conference',
               'd', '(1978 :', 'c', 'Sydney, N.S.W.)',
               'e', 'Organizing Committee.'], '2 ')],
     {'author_search': [
        'International Congress of Gerontology Satellite Conference, '
        'Organizing Committee'],
      'author_contributor_facet': [
        'International Congress of Gerontology Satellite Conference, '
        'Organizing Committee'],
      'author_sort': 'international congress of gerontology satellite '
                     'conference, organizing committee',
      'author_json': {
        'p': [{'d': 'International Congress of Gerontology Satellite '
                    'Conference, Organizing Committee'}]},
      'meetings_search': [
        'International Congress of Gerontology > Satellite Conference '
        '(1978 : Sydney, N.S.W.)'],
      'meeting_facet': [
        'International Congress of Gerontology',
        'International Congress of Gerontology > Satellite Conference',
        'International Congress of Gerontology > Satellite Conference '
        '(1978 : Sydney, N.S.W.)'],
      'meetings_json': {
        'p': [
            {'d': 'International Congress of Gerontology', 's': ' > '},
            {'d': 'Satellite Conference',
             'v': 'International Congress of Gerontology > Satellite '
                  'Conference'},
            {'d': '(1978 : Sydney, N.S.W.)',
             'v': 'International Congress of Gerontology > Satellite '
                  'Conference (1978 : Sydney, N.S.W.)'}]}
     }),
    ([('110', ['a', 'Democratic Party (Tex.).',
               'b', 'State Convention', 'd', '(1857 :', 'c', 'Waco, Tex.).',
               'b', 'Houston Delegation.'], '2 ')],
     {'author_search': [
        'Democratic Party (Tex.) > State Convention, Houston Delegation'],
      'author_contributor_facet': [
        'Democratic Party (Tex.)',
        'Democratic Party (Tex.) > State Convention, Houston Delegation'],
      'author_sort': 'democratic party (tex.) > state convention, houston '
                     'delegation',
      'author_json': {
        'p': [
            {'d': 'Democratic Party (Tex.)', 's': ' > '},
            {'d': 'State Convention, Houston Delegation',
             'v': 'Democratic Party (Tex.) > State Convention, Houston '
                  'Delegation'}]},
      'meetings_search': [
        'Democratic Party (Tex.), State Convention (1857 : Waco, Tex.)'],
      'meeting_facet': [
        'Democratic Party (Tex.), State Convention',
        'Democratic Party (Tex.), State Convention (1857 : Waco, Tex.)'],
      'meetings_json': {
        'p': [
            {'d': 'Democratic Party (Tex.), State Convention'},
            {'d': '(1857 : Waco, Tex.)',
             'v': 'Democratic Party (Tex.), State Convention '
                  '(1857 : Waco, Tex.)'}]}
     }),
    ([('110', ['a', 'Democratic Party (Tex.).', 'b', 'State Convention',
               'd', '(1857 :', 'c', 'Waco, Tex.).'], '2 ')],
     {'author_search': ['Democratic Party (Tex.)'],
      'author_contributor_facet': ['Democratic Party (Tex.)'],
      'author_sort': 'democratic party (tex.)',
      'author_json': {'p': [{'d': 'Democratic Party (Tex.)'}]},
      'meetings_search': [
        'Democratic Party (Tex.), State Convention (1857 : Waco, Tex.)'],
      'meeting_facet': [
        'Democratic Party (Tex.), State Convention',
        'Democratic Party (Tex.), State Convention (1857 : Waco, Tex.)'],
      'meetings_json': {
        'p': [
            {'d': 'Democratic Party (Tex.), State Convention'},
            {'d': '(1857 : Waco, Tex.)',
             'v': 'Democratic Party (Tex.), State Convention '
                  '(1857 : Waco, Tex.)'}]}
     }),
    ([('110', ['a', 'United States.', 'b', 'Congress', 
               'n', '(97th, 2nd session :', 'd', '1982).',
               'b', 'House.'], '1 ')],
     {'author_search': ['United States Congress > House'],
      'author_contributor_facet': [
        'United States Congress',
        'United States Congress > House'],
      'author_sort': 'united states congress > house',
      'author_json': {
        'p': [
            {'d': 'United States Congress', 's': ' > '},
            {'d': 'House', 'v': 'United States Congress > House'}]},
      'meetings_search': ['United States Congress (97th, 2nd session : 1982)'],
      'meeting_facet': [
        'United States Congress',
        'United States Congress (97th, 2nd session : 1982)'],
      'meetings_json': {
        'p': [
            {'d': 'United States Congress'},
            {'d': '(97th, 2nd session : 1982)',
             'v': 'United States Congress (97th, 2nd session : 1982)'}]}
     }),
    ([('111', ['a', 'Paris.', 'q', 'Peace Conference,', 'd', '1919.'], '1 ')],
     {'meetings_search': ['Paris Peace Conference, 1919'],
      'meeting_facet': ['Paris Peace Conference',
                        'Paris Peace Conference, 1919'],
      'meetings_json': {
        'p': [
            {'d': 'Paris Peace Conference', 's': ', '},
            {'d': '1919',
             'v': 'Paris Peace Conference, 1919'}]}
     }),
    ([('710', ['i', 'Container of (work):',
               'a', 'Some Organization,',
               'e', 'author.',
               't', 'Some Work Title.'], '22')],
     {'contributors_search': ['Some Organization'],
      'responsibility_search': ['Some Organization author'],
      'author_contributor_facet': ['Some Organization'],
      'author_sort': 'some organization',
      'contributors_json': {'p': [{'d': 'Some Organization'}],
                            'r': ['author']}
     }),
    ([('711', ['a', 'Some Festival.', 'e', 'Orchestra,',
               'j', 'instrumentalist.'], '2 ')],
     {'meeting_facet': ['Some Festival'],
      'meetings_search': ['Some Festival'],
      'meetings_json': [{'p': [{'d': 'Some Festival'}]}],
      'contributors_search': ['Some Festival, Orchestra'],
      'responsibility_search': ['Some Festival, Orchestra instrumentalist'],
      'author_contributor_facet': ['Some Festival, Orchestra'],
      'author_sort': 'some festival, orchestra',
      'contributors_json': {'p': [{'d': 'Some Festival, Orchestra'}],
                            'r': ['instrumentalist']}
     }),
    ([('711', ['a', 'Some Conference', 'c', '(Rome),',
               'j', 'jointly held conference.'], '2 ')],
     {'meeting_facet': ['Some Conference', 'Some Conference (Rome)'],
      'meetings_search': ['Some Conference (Rome)'],
      'responsibility_search': ['Some Conference jointly held conference'],
      'meetings_json': [{'p': [{'d': 'Some Conference'},
                               {'d': '(Rome)', 'v': 'Some Conference (Rome)'}],
                         'r': ['jointly held conference']}]
     }),
    ([('800', ['a', 'Berenholtz, Jim,', 'd', '1957-',
               't', 'Teachings of the feathered serpent ;', 'v', 'bk. 1.'],
       '1 ')],
     {'author_contributor_facet': ['Berenholtz, Jim, 1957-'],
      'contributors_search': ['Berenholtz, Jim, 1957-', 'Jim Berenholtz'],
      'contributors_json': [{'p': [{'d': 'Berenholtz, Jim, 1957-'}]}]
     }),
    ([('810', ['a', 'United States.', 'b', 'Army Map Service.',
               't', 'Special Africa series,', 'v', 'no. 12.'], '1 ')],
     {'author_contributor_facet': ['United States Army Map Service'],
      'contributors_search': ['United States Army Map Service'],
      'contributors_json': [{'p': [{'d': 'United States Army Map Service'}]}]
     }),
    ([('811', ['a', 'International Congress of Nutrition',
               'n', '(11th :', 'd', '1978 :', 'c', 'Rio de Janeiro, Brazil).',
               't', 'Nutrition and food science ;', 'v', 'v. 1.'], '2 ')],
     {'meeting_facet': [
        'International Congress of Nutrition',
        'International Congress of Nutrition (11th : 1978 : Rio de Janeiro, '
        'Brazil)'],
      'meetings_search': ['International Congress of Nutrition (11th : 1978 : '
                          'Rio de Janeiro, Brazil)'],
      'meetings_json': [{'p': [{'d': 'International Congress of Nutrition'},
                               {'d': '(11th : 1978 : Rio de Janeiro, Brazil)',
                                'v': 'International Congress of Nutrition '
                                     '(11th : 1978 : Rio de Janeiro, Brazil)'}]
                       }]
     }),
    ([('100', ['a', 'Author, Main,', 'd', '1910-1990.'], '1 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_search': ['Author, Main, 1910-1990', 'Main Author'],
      'author_sort': 'author, main, 1910-1990',
      'author_json': {'p': [{'d': 'Author, Main, 1910-1990'}]},
      'author_contributor_facet': [
        'Author, Main, 1910-1990', 'Author, Second, 1920-1999',
        'Org Contributor', 'Festival, Orchestra'],
      'contributors_search': [
        'Author, Second, 1920-1999', 'Second Author', 'Org Contributor',
        'Festival, Orchestra'],
      'contributors_json': [
        {'p': [{'d': 'Author, Second, 1920-1999'}]},
        {'p': [{'d': 'Org Contributor'}]},
        {'p': [{'d': 'Festival, Orchestra'}]}],
      'meeting_facet': ['Festival'],
      'meetings_search': ['Festival'],
      'meetings_json': [{'p': [{'d': 'Festival'}]}]
     }),
    ([('110', ['a', 'Some Organization'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_search': ['Some Organization'],
      'author_sort': 'some organization',
      'author_json': {'p': [{'d': 'Some Organization'}]},
      'author_contributor_facet': [
        'Some Organization', 'Author, Second, 1920-1999',
        'Org Contributor', 'Festival, Orchestra'],
      'contributors_search': [
        'Author, Second, 1920-1999', 'Second Author', 'Org Contributor',
        'Festival, Orchestra'],
      'contributors_json': [
        {'p': [{'d': 'Author, Second, 1920-1999'}]},
        {'p': [{'d': 'Org Contributor'}]},
        {'p': [{'d': 'Festival, Orchestra'}]}],
      'meeting_facet': ['Festival'],
      'meetings_search': ['Festival'],
      'meetings_json': [{'p': [{'d': 'Festival'}]}]
     }),
    ([('110', ['a', 'Some Org.', 'b', 'Meeting', 'd', '(1999).'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_search': ['Some Org'],
      'author_sort': 'some org',
      'author_json': {'p': [{'d': 'Some Org'}]},
      'author_contributor_facet': [
        'Some Org', 'Author, Second, 1920-1999',
        'Org Contributor', 'Festival, Orchestra'],
      'contributors_search': [
        'Author, Second, 1920-1999', 'Second Author', 'Org Contributor',
        'Festival, Orchestra'],
      'contributors_json': [
        {'p': [{'d': 'Author, Second, 1920-1999'}]},
        {'p': [{'d': 'Org Contributor'}]},
        {'p': [{'d': 'Festival, Orchestra'}]}],
      'meeting_facet': ['Some Org, Meeting', 'Some Org, Meeting (1999)',
                        'Festival'],
      'meetings_search': ['Some Org, Meeting (1999)', 'Festival'],
      'meetings_json': [
        {'p': [{'d': 'Some Org, Meeting'},
               {'d': '(1999)', 'v': 'Some Org, Meeting (1999)'}]},
        {'p': [{'d': 'Festival'}]}]
     }),
    ([('111', ['a', 'Meeting', 'd', '(1999).'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_sort': 'author, second, 1920-1999',
      'author_contributor_facet': [
        'Author, Second, 1920-1999', 'Org Contributor', 'Festival, Orchestra'],
      'contributors_search': [
        'Author, Second, 1920-1999', 'Second Author', 'Org Contributor',
        'Festival, Orchestra'],
      'contributors_json': [
        {'p': [{'d': 'Author, Second, 1920-1999'}]},
        {'p': [{'d': 'Org Contributor'}]},
        {'p': [{'d': 'Festival, Orchestra'}]}],
      'meeting_facet': ['Meeting', 'Meeting (1999)', 'Festival'],
      'meetings_search': ['Meeting (1999)', 'Festival'],
      'meetings_json': [
        {'p': [{'d': 'Meeting'},
               {'d': '(1999)', 'v': 'Meeting (1999)'}]},
        {'p': [{'d': 'Festival'}]}]
     }),
    ([('111', ['a', 'Conference.', 'e', 'Subcommittee.'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_search': ['Conference, Subcommittee'],
      'author_sort': 'conference, subcommittee',
      'author_json': {'p': [{'d': 'Conference, Subcommittee'}]},
      'author_contributor_facet': [
        'Conference, Subcommittee', 'Author, Second, 1920-1999',
        'Org Contributor', 'Festival, Orchestra'],
      'contributors_search': [
        'Author, Second, 1920-1999', 'Second Author', 'Org Contributor',
        'Festival, Orchestra'],
      'contributors_json': [
        {'p': [{'d': 'Author, Second, 1920-1999'}]},
        {'p': [{'d': 'Org Contributor'}]},
        {'p': [{'d': 'Festival, Orchestra'}]}],
      'meeting_facet': ['Conference', 'Festival'],
      'meetings_search': ['Conference', 'Festival'],
      'meetings_json': [
        {'p': [{'d': 'Conference'}]},
        {'p': [{'d': 'Festival'}]}]
     })
], ids=[
    # Edge cases
    'Nothing: no 1XX, 7XX, or 8XX fields',
    'Blank 100',
    'Blank 110',
    'Blank 111',
    '100 with relator but no heading information',
    '110 with relator but no heading information',
    '111 with relator but no heading information',
    '100 and 700 (name/title) with same name are deduplicated',
    '700 name appearing multiple times is deduplicated',
    # Personal Names (X00) and Relators
    '100, plain personal name',
    '100, name, forename only, with multiple titles',
    '100, name with $e relators, multiple $e instances',
    '100, name with $e relators, multiple vals listed in $e',
    '100, name with $4 relators',
    '100, name with $e and $4 relators',
    '700, name with $i (ignore!), $4 URI (ignore!), and title info (ignore!)',
    # Org and Meeting Names (X10, X11)
    '710, plain org name',
    '711, plain meeting name',
    '711, meeting name w/organization subsection and no event info',
    '111, meeting name w/organization subsection, with event info',
    '711, meeting name w/muti-org subsection',
    '111, multi-structured meeting w/organization subsection',
    '110, org with meeting and sub-org',
    '110, org with meeting (no sub-org)',
    '110, jurisdication w/meeting and sub-org',
    '111, jurisdiction-based meeting',
    '710, organization with $i and relators',
    '711, meeting w/org subsection and $j relators',
    '711, meeting with $j relators applied to meeting',
    # Series Authors etc. (8XX)
    '800, personal name',
    '810, org name, jurisdiction',
    '811, meeting name',
    # Multiple 1XX, 7XX, 8XX fields
    '100 author, 7XX contributors',
    '110 author, 7XX contributors',
    '110 author w/meeting component, 7XX contributors',
    '111 meeting, w/no org component, 7XX contributors',
    '111 meeting, w/org component, 7XX contributors',
])
def test_blasmpipeline_getcontributorinfo(marcfields, expected,
                                          bl_sierra_test_record,
                                          blasm_pipeline_class,
                                          bibrecord_to_pymarc,
                                          add_marc_fields,
                                          assert_json_matches_expected):
    """
    BlacklightASMPipeline.get_contributor_info should return fields
    matching the expected parameters.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('100', '110', '111', '700', '710', '711', '800',
                          '810', '811')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_contributor_info(bib, bibmarc)
    for k, v in val.items():
        print k, v
        if k in expected:
            if k.endswith('_json'):
                assert_json_matches_expected(v, expected[k], exact=True)
            else:
                assert v == expected[k]
        else:
            assert v is None


@pytest.mark.parametrize('tag, subfields, expected', [
    # Start with edge cases: missing data, non-ISBD punctuation, etc.

    ('245', [],
     {'nonfiling_chars': 0,
      'transcribed': [],
      'parallel': []}),

    ('245', ['a', ''],
     {'nonfiling_chars': 0,
      'transcribed': [],
      'parallel': []}),

    ('245', ['a', '', 'b', 'oops mistake /'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['oops mistake']}],
      'parallel': []}),

    ('246', ['a', '   ', 'i', 'Some blank chars at start:', 'a', 'Oops'],
     {'display_text': 'Some blank chars at start',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Oops']}],
      'parallel': []}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title no punctuation'],
         'responsibility': 'by Joe'}],
      'parallel': []}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'n', 'Part 1',
             'p', 'the quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title no punctuation', 'Part 1, the quickening'],
         'responsibility': 'by Joe'}],
      'parallel': []}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'p', 'The quickening',
             'p', 'Subpart A', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title no punctuation', 'The quickening',
                   'Subpart A'],
         'responsibility': 'by Joe'}],
      'parallel': []}),

    ('245 12', ['a', 'A title,', 'b', 'non-ISBD punctuation;', 'n', 'Part 1,',
             'p', 'the quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title, non-ISBD punctuation', 'Part 1, the quickening'],
         'responsibility': 'by Joe'}],
      'parallel': []}),

    ('245', ['a', 'A title!', 'b', 'Non-ISBD punctuation;',
             'p', 'The quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['A title! Non-ISBD punctuation', 'The quickening'],
         'responsibility': 'by Joe'}],
      'parallel': []}),

    ('245 12', ['a', 'A title : with punctuation, all in $a. Part 1 / by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title: with punctuation, all in $a. Part 1 / by Joe']}],
      'parallel': []}),

    ('245', ['b', ' = A parallel title missing a main title'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['A parallel title missing a main title']}],
      'parallel': []}),

    ('245', ['a', '1. One thing, 2. Another, 3. A third :',
             'b', 'This is like some Early English Books Online titles / '
                  'by Joe = 1. One thing, 2. Another, 3. A third : Plus long '
                  'subtitle etc. /'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['1. One thing, 2. Another, 3. A third: This is like some '
                   'Early English Books Online titles / by Joe']}],
      'parallel': [
        {'parts': ['1. One thing, 2. Another, 3. A third: Plus long subtitle '
                   'etc.']}
    ]}),

    ('245', ['a', '1. This is like another Early English Books Online title :',
             'b', 'something: 2. Something else: 3. About the 22th. of June, '
                  '1678. by Richard Greene of Dilwin, etc.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['1. This is like another Early English Books Online title: '
                   'something: 2. Something else: 3. About the 22th. of June, '
                   '1678. by Richard Greene of Dilwin, etc.']}],
      'parallel': []}),

    ('245', ['a', 'A forward slash somewhere in the title / before sf c /',
             'c', 'by Joe.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['A forward slash somewhere in the title / before sf c'],
         'responsibility': 'by Joe'}],
      'parallel': []}),

    ('245', ['a', 'Quotation marks /', 'c', 'by "Joe."'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Quotation marks'],
         'responsibility': 'by "Joe"'}],
      'parallel': []}),

    ('245', ['a', 'Multiple ISBD marks / :', 'b', 'subtitle', 'c', 'by Joe.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Multiple ISBD marks /: subtitle'],
         'responsibility': 'by Joe'}],
      'parallel': []}),

    # Now test cases on more standard data.

    ('245', ['a', 'Title :', 'b', 'with subtitle.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['Title: with subtitle']}],
      'parallel': []}),

    ('245', ['a', 'First title ;', 'b', 'Second title.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']}, {'parts': ['Second title']}],
      'parallel': []}),

    ('245', ['a', 'First title ;', 'b', 'Second title ; Third title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']}, {'parts': ['Second title']},
                      {'parts': ['Third title']}],
      'parallel': []}),

    ('245', ['a', 'First title ;', 'b', 'and Second title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']}, {'parts': ['Second title']}],
      'parallel': []}),

    ('245', ['a', 'First title,', 'b', 'and Second title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']}, {'parts': ['Second title']}],
      'parallel': []}),

    ('245', ['a', 'Title /', 'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title'],
         'responsibility': 'by Author'}],
      'parallel': []}),

    ('245', ['a', 'Title /', 'c', 'Author 1 ; Author 2 ; Author 3.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title'],
         'responsibility': 'Author 1; Author 2; Author 3'}],
      'parallel': []}),

    ('245', ['a', 'Title!', 'b', 'What ending punctuation should we keep?'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title! What ending punctuation should we keep?']}],
      'parallel': []}),

    # Titles that include parts ($n and $p).

    ('245', ['a', 'Title.', 'n', 'Part 1.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1']}],
      'parallel': []}),

    ('245', ['a', 'Title.', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Name of a part']}],
      'parallel': []}),

    ('245', ['a', 'Title.', 'n', 'Part 1,', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1, Name of a part']}],
      'parallel': []}),

    ('245', ['a', 'Title.', 'n', 'Part 1', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1, Name of a part']}],
      'parallel': []}),

    ('245', ['a', 'Title.', 'n', 'Part 1.', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1', 'Name of a part']}],
      'parallel': []}),

    ('245', ['a', 'Title.', 'n', '1. Part', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', '1. Part, Name of a part']}],
      'parallel': []}),

    ('245', ['a', 'Title.', 'n', '1. Part A', 'n', '2. Part B'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', '1. Part A', '2. Part B']}],
      'parallel': []}),

    ('245', ['a', 'Title :', 'b', 'subtitle.', 'n', '1. Part A',
             'n', '2. Part B'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title: subtitle', '1. Part A', '2. Part B']}],
      'parallel': []}),

    ('245', ['a', 'Title one.', 'n', 'Book 2.', 'n', 'Chapter V /',
             'c', 'Author One. Title two. Book 3. Chapter VI / Author Two.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title one', 'Book 2', 'Chapter V'],
         'responsibility': 'Author One'},
        {'parts': ['Title two', 'Book 3. Chapter VI'],
         'responsibility': 'Author Two'}],
      'parallel': []}),

    # Fun with parallel titles!

    ('245', ['a', 'Title in French =', 'b', 'Title in English /',
             'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French'],
         'responsibility': 'by Author'}],
      'parallel': [
        {'parts': ['Title in English']}]}),

    ('245', ['a', 'Title in French /',
             'c', 'by Author in French = Title in English / by Author in '
                  'English.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French'],
         'responsibility': 'by Author in French'}],
      'parallel': [
        {'parts': ['Title in English'],
         'responsibility': 'by Author in English'}]}),

    ('245', ['a', 'Title in French =',
             'b', 'Title in English = Title in German /', 'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French'],
         'responsibility': 'by Author'}],
      'parallel': [
        {'parts': ['Title in English']},
        {'parts': ['Title in German']}]}),

    ('245', ['a', 'First title in French =',
             'b', 'First title in English ; Second title in French = Second '
                  'title in English.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['First title in French']},
        {'parts': ['Second title in French']}],
      'parallel': [
        {'parts': ['First title in English']},
        {'parts': ['Second title in English']}
      ]}),

    ('245', ['a', 'Title in French.', 'p',  'Part One =',
             'b', 'Title in English.', 'p', 'Part One.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French', 'Part One']}],
      'parallel': [
        {'parts': ['Title in English', 'Part One']}]}),

    ('245', ['a', 'Title in French.', 'p',  'Part One :',
             'b', 'subtitle = Title in English.', 'p', 'Part One : subtitle.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French', 'Part One: subtitle']}],
      'parallel': [
        {'parts': ['Title in English', 'Part One: subtitle']}]}),

    # $h (medium) is ignored, except for ISBD punctuation

    ('245', ['a', 'First title', 'h', '[sound recording] ;',
             'b', 'Second title.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']}, {'parts': ['Second title']}],
      'parallel': []}),

    ('245', ['a', 'Title in French.', 'p',  'Part One',
             'h', '[sound recording] =', 'b', 'Title in English.',
             'p', 'Part One.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French', 'Part One']}],
      'parallel': [
        {'parts': ['Title in English', 'Part One']}]}),

    # Subfields for archives and archival collections (fgks)

    ('245', ['a', 'Smith family papers,', 'f', '1800-1920.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920']}],
      'parallel': []}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920']}],
      'parallel': []}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920 (bulk 1850-1860)']}],
      'parallel': []}),

    ('245', ['a', 'Smith family papers', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1850-1860']}],
      'parallel': []}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920,', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920 (bulk 1850-1860)']}],
      'parallel': []}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920,',
             'g', '(1850-1860).'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920, (1850-1860)']}],
      'parallel': []}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920', 'g', '(1850-1860).'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920 (1850-1860)']}],
      'parallel': []}),

    ('245', ['a', 'Some title :', 'k', 'typescript', 'f', '1800.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Some title: typescript, 1800']}],
      'parallel': []}),

    ('245', ['a', 'Hearing Files', 'k', 'Case Files', 'f', '1800',
             'p', 'District 6.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Hearing Files, Case Files, 1800', 'District 6']}],
      'parallel': []}),

    ('245', ['a', 'Hearing Files.', 'k', 'Case Files', 'f', '1800',
             'p', 'District 6.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Hearing Files', 'Case Files, 1800', 'District 6']}],
      'parallel': []}),

    ('245', ['a', 'Report.', 's', 'Executive summary.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Report', 'Executive summary']}],
      'parallel': []}),

    ('245', ['a', 'Title', 'k', 'Form', 's', 'Version', 'f', '1990'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title, Form, Version, 1990']}],
      'parallel': []}),

    ('245', ['k', 'Form', 's', 'Version', 'f', '1990'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Form, Version, 1990']}],
      'parallel': []}),

    # 242s (Translated titles)

    ('242 14', ['a', 'The Annals of chemistry', 'n', 'Series C,',
             'p', 'Organic chemistry and biochemistry.', 'y', 'eng'],
     {'display_text': 'Title translation, English',
      'nonfiling_chars': 4,
      'transcribed': [
        {'parts': ['The Annals of chemistry',
                   'Series C, Organic chemistry and biochemistry']}],
      'parallel': []}),

    # 246s (Variant titles)

    ('246', ['a', 'Archives for meteorology, geophysics, and bioclimatology.',
             'n', 'Serie A,', 'p', 'Meteorology and geophysics'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Archives for meteorology, geophysics, and bioclimatology',
                   'Serie A, Meteorology and geophysics']}],
      'parallel': []}),

    ('246 12', ['a', 'Creating jobs', 'f', '1980'],
     {'display_text': 'Issue title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Creating jobs, 1980']}],
      'parallel': []}),

    ('246 12', ['a', 'Creating jobs', 'g', '(varies slightly)', 'f', '1980'],
     {'display_text': 'Issue title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Creating jobs (varies slightly) 1980']}],
      'parallel': []}),

    ('246 1 ', ['i', 'At head of title:', 'a', 'Science and public affairs',
                'f', 'Jan. 1970-Apr. 1974'],
     {'display_text': 'At head of title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Science and public affairs, Jan. 1970-Apr. 1974']}],
      'parallel': []}),

    ('247', ['a', 'Industrial medicine and surgery', 'x', '0019-8536'],
     {'issn': '0019-8536',
      'display_text': 'Former title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Industrial medicine and surgery']}],
      'parallel': []}),

    # Testing 490s: similar to 245s but less (differently?) structured

    ('490', ['a', 'Series statement / responsibility'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement'],
         'responsibility': 'responsibility'}],
      'parallel': []}),

    ('490', ['a', 'Series statement =', 'a', 'Series statement in English'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement']}],
      'parallel': [
        {'parts': ['Series statement in English']}
      ]}),

    ('490', ['a', 'Series statement ;', 'v', 'v. 1 =',
             'a', 'Series statement in English ;', 'v', 'v. 1'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement; v. 1']}],
      'parallel': [
        {'parts': ['Series statement in English; v. 1']}
      ]}),

    ('490', ['3', 'Vol. 1:', 'a', 'Series statement'],
     {'nonfiling_chars': 0,
      'materials_specified': ['Vol. 1'],
      'transcribed': [
        {'parts': ['Series statement']}],
      'parallel': []}),

    ('490', ['a', 'Series statement,', 'x', '1234-5678 ;', 'v', 'v. 1'],
     {'nonfiling_chars': 0,
      'issn': '1234-5678',
      'transcribed': [
        {'parts': ['Series statement; v. 1']}],
      'parallel': []}),

    ('490', ['a', 'Series statement ;', 'v', 'v. 1.',
             'a', 'Sub-series / Responsibility ;', 'v', 'v. 36'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement; v. 1', 'Sub-series; v. 36'],
         'responsibility': 'Responsibility'}],
      'parallel': []}),

    ('490', ['a', 'Series statement ;', 'v', 'v. 1.', 'l', '(LC12345)'],
     {'nonfiling_chars': 0,
      'lccn': 'LC12345',
      'transcribed': [
        {'parts': ['Series statement; v. 1']}],
      'parallel': []}),

    # ('', [],
    #  {'nonfiling_chars': 0,
    #   'transcribed': [
    #     {'parts': [],
    #      'responsibility': ''}],
    #   'parallel': [
    #     {'parts': [],
    #      'responsibility': ''}
    # ]}),
])
def test_transcribedtitleparser_parse(tag, subfields, expected):
    """
    TranscribedTitleParser `parse` method should return a dict with the
    expected structure, given the provided MARC field. Can handle 242s,
    245s, 246s, and 247s, but is mainly geared toward 245s (for obvious
    reasons).
    """
    if ' ' in tag:
        tag, indicators = tag.split(' ', 1)
    else:
        indicators = '  '
    field = s2m.make_mfield(tag, subfields=subfields, indicators=indicators)
    assert s2m.TranscribedTitleParser(field).parse() == expected


@pytest.mark.parametrize('tag, subfields, expected', [    
    # Start with edge cases: missing data, non-ISBD punctuation, etc.

    ('130', [],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': [],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', ''],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': [],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', '', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'A title,', 'm', 'instruments,', 'n', ',', 'r', 'D major.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['A title, instruments, D major'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130 2 ', ['a', 'A Basic title no punctuation', 'n', 'Part 1'],
     {'nonfiling_chars': 2,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['A Basic title no punctuation', 'Part 1'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Basic title no punctuation', 'p', 'Named part'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Named part'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1',
             'p', 'named part'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1, named part'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1', 'n', 'Part 2'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1', 'Part 2'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Basic title no punctuation', 'p', 'Named part',
             'n', 'Part 2'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Named part', 'Part 2'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1', 'l', 'English'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1'],
      'expression_parts': ['English'],
      'languages': ['English'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # Once the first expression-level subfield appears, the rest are
    # interpreted as expression parts, whatever they are.
    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1',
             's', 'Version A', 'p', 'Subpart C'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1'],
      'expression_parts': ['Version A', 'Subpart C'],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1',
             's', 'Version A', 'p', 'Subpart C'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1'],
      'expression_parts': ['Version A', 'Subpart C'],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # Test cases on more standard data.

    # For music collective titles, the first $n or $p ('op. 10' in this
    # case) becomes a new part even if the preceding comma indicates
    # otherwise.
    ('130', ['a', 'Duets,', 'm', 'violin, viola,', 'n', 'op. 10.',
             'n', 'No. 3.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Duets, violin, viola', 'Op. 10', 'No. 3'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'main'
     }),

    # For other titles, the first subpart becomes part of the main
    # title if there's a preceding comma.
    ('130', ['a', 'Some title,', 'n', 'the first part.', 'n', 'Volume 1.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title, the first part', 'Volume 1'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # The first $n or $p starts a new part if there's a preceding period.
    ('130', ['a', 'Some title.', 'n', 'The first part.', 'n', 'Volume 1.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'The first part', 'Volume 1'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # A $p after $n is combined with the $n if there's a comma (or
    # nothing) preceding $p.
    ('130', ['a', 'Some title.', 'n', 'The first part,', 'p', 'part name.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'The first part, part name'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # A $p after $n becomes a new part if there's a period preceding
    # $p.
    ('130', ['a', 'Some title.', 'n', 'The first part.', 'p', 'Part name.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'The first part', 'Part name'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # For $n's and $p's (after the first), part hierarchy is based on
    # punctuation. Commas denote same part, periods denote new parts.
    ('130', ['a', 'Some title.', 'n', 'Part 1,', 'n', 'Part 2.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'Part 1, Part 2'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # $k is treated as a new part.
    ('130', ['a', 'Works.', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main'
     }),

    # $k following a collective title is always a new part.
    ('130', ['a', 'Works,', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main'
     }),

    # Languages are parsed out if multiple are found.
    ('130', ['a', 'Something.', 'l', 'English and French.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Something'],
      'expression_parts': ['English and French'],
      'languages': ['English', 'French'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Something.', 'l', 'English & French.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Something'],
      'expression_parts': ['English & French'],
      'languages': ['English', 'French'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    ('130', ['a', 'Something.', 'l', 'English, French, and German.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Something'],
      'expression_parts': ['English, French, and German'],
      'languages': ['English', 'French', 'German'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # If a generic collective title, like "Works", is followed by a
    # subfield m, it's interpreted as a music form title.
    ('130', ['a', 'Works,', 'm', 'violin.', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works, violin', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'main'
     }),

    # Anything following a $k results in a new hierarchical part.
    ('130', ['a', 'Works,', 'm', 'violin.', 'k', 'Selections,', 'n', 'op. 8.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works, violin', 'Selections', 'Op. 8'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'main'
     }),

    # "[Instrument] music" is treated as a collective title but not a
    # music form title.
    ('130', ['a', 'Piano music (4 hands)', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Piano music (4 hands)', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main'
     }),

    # $d interacts with collective titles like other subpart sf types.
    ('240', ['a', 'Treaties, etc.', 'd', '1948.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Treaties, etc.', '1948'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main'
     }),

    ('240 14', ['a', 'The Treaty of whatever', 'd', '(1948)'],
     {'nonfiling_chars': 4,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['The Treaty of whatever (1948)'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main'
     }),

    # ... and $d is treated like other subpart types when it occurs
    # elsewhere.
    ('240', ['a', 'Treaties, etc.', 'g', 'Poland,', 'd', '1948 Mar. 2.',
             'k', 'Protocols, etc.,', 'd', '1951 Mar. 6'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Treaties, etc.', 'Poland, 1948 Mar. 2',
                      'Protocols, etc., 1951 Mar. 6'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main'
     }),

    # 700, 710, and 711 fields skip past the "author" subfields but
    # handle the $i, if present.

    ('700', ['a', 'Fauré, Gabriel,', 'd', '1845-1924.', 't', 'Nocturnes,',
             'm', 'piano,', 'n', 'no. 11, op. 104, no. 1,', 'r', 'F♯ minor'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Nocturnes, piano', 'No. 11, op. 104, no. 1, F♯ minor'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'related'
     }),

    # 7XX ind2 == 2 indicates an 'analytic' type title.
    ('700  2', ['a', 'Fauré, Gabriel,', 'd', '1845-1924.', 't', 'Nocturnes,',
                'm', 'piano,', 'n', 'no. 11, op. 104, no. 1,', 'r', 'F♯ minor'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Nocturnes, piano', 'No. 11, op. 104, no. 1, F♯ minor'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'analytic'
     }),

    # 7XX fields with "Container of" in $i indicate an 'analytic' type
    # title, even if ind2 is not 2. In these cases, because the label
    # "Container of" is redundant with the 'analytic' type, the display
    # constant is not generated.
    ('700   ', ['i', 'Container of (work):', 'a', 'Fauré, Gabriel,',
                'd', '1845-1924.', 't', 'Nocturnes,', 'm', 'piano,',
                'n', 'no. 11, op. 104, no. 1,', 'r', 'F♯ minor'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Nocturnes, piano', 'No. 11, op. 104, no. 1, F♯ minor'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'analytic'
     }),

    ('710', ['i', 'Summary of (work):', 'a', 'United States.',
             'b', 'Adjutant-General\'s Office.',
             't', 'Correspondence relating to the war with Spain'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': ['Summary of'],
      'title_parts': ['Correspondence relating to the war with Spain'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'related'
     }),

    ('711', ['a', 'International Conference on Gnosticism', 'd', '(1978 :',
             'c', 'New Haven, Conn.).', 't', 'Rediscovery of Gnosticism.',
             'p', 'Modern writers.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Rediscovery of Gnosticism', 'Modern writers'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'related'
     }),

    ('730 4 ', ['i', 'Container of (expression):', 'a', 'The Bible.',
             'p', 'Epistles.', 'k', 'Selections.', 'l', 'Tabaru.',
             's', 'Common Language.', 'f', '2001'],
     {'nonfiling_chars': 4,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['The Bible', 'Epistles', 'Selections'],
      'expression_parts': ['Tabaru', 'Common Language', '2001'],
      'languages': ['Tabaru'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'analytic'
     }),

    # If $o is present and begins with 'arr', the statement 'arranged'
    # is added to `expression_parts`.
    ('730', ['a', 'God save the king;', 'o', 'arr.', 'f', '1982.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['God save the king'],
      'expression_parts': ['arranged', '1982'],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'related'
     }),

    # 800, 810, 811, and 830 fields are series and may have $v (volume)
    # and/or $x (ISSN)

    ('800', ['a', 'Berenholtz, Jim,', 'd', '1957-',
             't', 'Teachings of the feathered serpent ;', 'v', 'bk. 1'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Teachings of the feathered serpent'],
      'expression_parts': [],
      'languages': [],
      'volume': 'bk. 1',
      'issn': '',
      'is_collective': False,
      'is_music_form': False,
      'type': 'series'
     }),

    # $3 becomes `materials_specified` if present
    ('830  2', ['3', 'v. 1-8', 'a','A Collection Byzantine.', 'x', '0223-3738'],
     {'nonfiling_chars': 2,
      'materials_specified': ['v. 1-8'],
      'display_constants': [],
      'title_parts': ['A Collection Byzantine'],
      'expression_parts': [],
      'languages': [],
      'volume': '',
      'issn': '0223-3738',
      'is_collective': False,
      'is_music_form': False,
      'type': 'series'
     }),

    # ('', [],
    #  {'materials_specified': [],
    #   'display_constants': [],
    #   'title_parts': [],
    #   'expression_parts': [],
    #   'languages': [],
    #   'is_collective': False,
    #   'is_music_form': False,
    #  }),
])
def test_preferredtitleparser_parse(tag, subfields, expected):
    """
    PreferredTitleParser `parse` method should return a dict with the
    expected structure, given the provided MARC field.
    """
    if ' ' in tag:
        tag, indicators = tag.split(' ', 1)
    else:
        indicators = '  '
    field = s2m.make_mfield(tag, subfields=subfields, indicators=indicators)
    assert s2m.PreferredTitleParser(field).parse() == expected


@pytest.mark.parametrize('marcfield, expected', [
    (('100', ['a', 'Adams, Henry,', 'd', '1838-1918.'], '1 '), ['Adams, H.']),
    (('100', ['a', 'Chopin, Frédéric', 'd', '1810-1849.'], '1 '),
     ['Chopin, F.']),
    (('100', ['a', 'Riaño, Juan Facundo,', 'd', '1828-1901.'], '1 '),
     ['Riaño, J.F.']),
    (('100', ['a', 'Fowler, T. M.', 'q', '(Thaddeus Mortimer),',
              'd', '1842-1922.'], '1 '),
     ['Fowler, T.M.']),
    (('100', ['a', 'Isidore of Seville.'], '0 '), ['Isidore of Seville']),
    (('100', ['a', 'Vérez-Peraza, Elena,', 'd', '1919-'], '1 '),
     ['Vérez-Peraza, E.']),
    (('100', ['a', 'John', 'b', 'II Comnenus,', 'c', 'Emperor of the East,',
              'd', '1088-1143.'], '0 '),
     ['John II Comnenus, Emperor of the East']),
    (('100', ['a', 'John Paul', 'b', 'II,', 'c', 'Pope,',
              'd', '1920-'], '0 '),
     ['John Paul II, Pope']),
    (('100', ['a', 'Beeton,', 'c', 'Mrs.', 'q', '(Isabella Mary),',
              'd', '1836-1865.'], '1 '),
     ['Beeton, Mrs.']),
    (('100', ['a', 'Black Foot,', 'c', 'Chief,', 'd', 'd. 1877',
              'c', '(Spirit)'], '0 '),
     ['Black Foot, Chief (Spirit)']),
    (('100', ['a', 'Thomas,', 'c', 'Aquinas, Saint,', 'd', '1225?-1274.'],
        '0 '),
     ['Thomas, Aquinas, Saint']),
    (('110', ['a', 'United States.', 'b', 'Court of Appeals (2nd Circuit)'],
        '1 '),
     ['United States Court of Appeals (2nd Circuit)']),
    (('110', ['a', 'Catholic Church.', 'b', 'Province of Baltimore (Md.).',
              'b', 'Provincial Council.'], '2 '),
     ['Catholic Church ... Provincial Council']),
    (('110', ['a', 'United States.', 'b', 'Congress.',
              'b', 'Joint Committee on the Library.'], '1 '),
     ['United States Congress, Joint Committee on the Library']),
    (('110', ['a', 'Catholic Church.',
              'b', 'Concilium Plenarium Americae Latinae',
              'd', '(1899 :', 'c', 'Rome, Italy)'], '2 '),
     ['Catholic Church',
      'Catholic Church, Concilium Plenarium Americae Latinae']),
    (('111', ['a', 'Governor\'s Conference on Aging (N.Y.)',
              'd', '(1982 :', 'c', 'Albany, N.Y.)'], '2 '),
     ['Governor\'s Conference on Aging (N.Y.)']),
    (('111', ['a', 'Esto \'84', 'd', '(1984 :', 'c', 'Toronto, Ont).',
              'e', 'Raamatunaituse Komitee.'], '2 '),
     ['Esto \'84', 'Esto \'84, Raamatunaituse Komitee'])
])
def test_shortenname(marcfield, expected, make_name_structs):
    """
    The `shorten_name` function should return the expected shortened
    version of a name when passed a structure from a NameParser
    resulting from the given `marcfield` data.
    """
    result = [s2m.shorten_name(n) for n in make_name_structs(marcfield)]
    assert set(result) == set(expected)


@pytest.mark.parametrize('title, nf_chars, expected', [
    ('', 0, '-'),
    ('$', 0, '-'),
    ('$1000', 0, '1000'),
    ('1000', 0, '1000'),
    ('[A] whatever', 0, 'a-whatever'),
    ('[A] whatever', 4, 'whatever'),
    ('[A] whatever', 1, 'a-whatever'),
    ('[A] whatever!', 1, 'a-whatever'),
    ('Romeo and Juliet', 4, 'romeo-and-juliet'),
])
def test_generatetitlekey(title, nf_chars, expected):
    """
    The `generate_title_key` function should return the expected key
    string when passed the given title string and number of non-filing
    characters (`nf_chars`).
    """
    assert s2m.generate_title_key(title, nf_chars) == expected


@pytest.mark.parametrize('marcfields, expected', [
    # Edge cases -- empty / missing fields, etc.

    # 1XX field but no titles => empty title-info
    ([('100', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 ')], {}),

    # Empty 240 field => empty title info
    ([('240', ['a', ''], '  ')], {}),

    # Empty 245 field => empty title info
    ([('245', ['a', ''], '  ')], {}),

    # 700 field with no $t => empty title info
    ([('700', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 ')], {}),

    # 130 but NO 245
    # If there is a 130, 240, or 243 but no 245, then the 130/240/243
    # becomes the main title.
    ([('130', ['a', 'Duets,', 'm', 'violin, viola,', 'n', 'op. 10.',
               'n', 'No. 3.'], '0 '),],
     {'title_display': 'Duets, violin, viola > Op. 10 > No. 3',
      'main_title_search': ['Duets, violin, viola > Op. 10 > No. 3'],
      'title_sort': 'duets-violin-viola-op-10-no-3',
      'included_work_titles_json': [{
        'p': [{'d': 'Duets, violin, viola',
               's': ' > ', 
               'v': 'duets-violin-viola!Duets, violin, viola'},
              {'d': 'Op. 10',
               's': ' > ',
               'v': 'duets-violin-viola-op-10!Duets, violin, viola > Op. 10'},
              {'d': 'No. 3',
               'v': 'duets-violin-viola-op-10-no-3!'
                    'Duets, violin, viola > Op. 10 > No. 3'}]
      }],
      'included_work_titles_search': ['Duets, violin, viola > Op. 10 > No. 3'],
      'title_series_facet': [
        'duets-violin-viola!Duets, violin, viola',
        'duets-violin-viola-op-10!Duets, violin, viola > Op. 10',
        'duets-violin-viola-op-10-no-3!Duets, violin, viola > Op. 10 > No. 3'
      ]}),

    # 830 with $5: control fields should be supressed
    ([('830', ['a', 'Some series.', '5', 'TxDN'], ' 0')],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                'v': 'some-series!Some series'}]},
      ],
      'related_series_titles_search': [
        'Some series',
      ],
      'title_series_facet': [
        'some-series!Some series',
      ]}),

    # 245 with " char following ISBD period; " char should be kept
    ([('245', ['a', 'Las Cantigas de Santa Maria /',
               'c', 'Alfonso X, "el Sabio."'], '1 ')],
     {'title_display': 'Las Cantigas de Santa Maria',
      'main_title_search': ['Las Cantigas de Santa Maria'],
      'title_sort': 'las-cantigas-de-santa-maria',
      'responsibility_display': 'Alfonso X, "el Sabio"',
      'responsibility_search': ['Alfonso X, "el Sabio"'],
      'included_work_titles_json': [{
        'p': [{'d': 'Las Cantigas de Santa Maria', 
               'v': 'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'}]
      }],
      'included_work_titles_search': ['Las Cantigas de Santa Maria'],
      'title_series_facet': [
        'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'
      ]}),

    # 245 with punct following last ISBD period
    ([('245', ['a', 'Las Cantigas de Santa Maria /',
               'c', 'Alfonso X, el Sabio. ,...'], '1 ')],
     {'title_display': 'Las Cantigas de Santa Maria',
      'main_title_search': ['Las Cantigas de Santa Maria'],
      'title_sort': 'las-cantigas-de-santa-maria',
      'responsibility_display': 'Alfonso X, el Sabio,...',
      'responsibility_search': ['Alfonso X, el Sabio,...'],
      'included_work_titles_json': [{
        'p': [{'d': 'Las Cantigas de Santa Maria', 
               'v': 'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'}]
      }],
      'included_work_titles_search': ['Las Cantigas de Santa Maria'],
      'title_series_facet': [
        'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'
      ]}),

    # Basic configurations of MARC Fields => title fields, Included vs
    # Related works, and how single author vs multiple
    # authors/contributors affects display of short authors.

    # 130/245: No author.
    # 245 is the main title and 130 is in IW. No author info is added to
    # any titles because there is no author info to add.
    ([('130', ['a', 'Duets,', 'm', 'violin, viola,', 'n', 'op. 10.',
               'n', 'No. 3.'], '0 '),
      ('245', ['a', 'Duets for violin and viola,', 'n', 'opus 10.',
               'n', 'Number 3 /', 'c', '[various authors]'], '1 ')],
     {'title_display': 'Duets for violin and viola, opus 10 > Number 3',
      'main_title_search': ['Duets for violin and viola, opus 10 > Number 3'],
      'title_sort': 'duets-for-violin-and-viola-opus-10-number-3',
      'responsibility_display': '[various authors]',
      'responsibility_search': ['[various authors]'],
      'included_work_titles_json': [{
        'p': [{'d': 'Duets, violin, viola',
               's': ' > ', 
               'v': 'duets-violin-viola!Duets, violin, viola'},
              {'d': 'Op. 10',
               's': ' > ',
               'v': 'duets-violin-viola-op-10!Duets, violin, viola > Op. 10'},
              {'d': 'No. 3',
               'v': 'duets-violin-viola-op-10-no-3!'
                    'Duets, violin, viola > Op. 10 > No. 3'}]
      }],
      'included_work_titles_search': ['Duets, violin, viola > Op. 10 > No. 3'],
      'title_series_facet': [
        'duets-violin-viola!Duets, violin, viola',
        'duets-violin-viola-op-10!Duets, violin, viola > Op. 10',
        'duets-violin-viola-op-10-no-3!Duets, violin, viola > Op. 10 > No. 3'
      ]}),

    # 100/240/245: Single author (title in included works).
    # 240 is in IW and 245 is main title. Short author info is added
    # to titles.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Specific Preferred Title.'], '10'),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [{
        'a': 'Smith, Joe',
        'p': [{'d': 'Specific Preferred Title [by Smith, J.]',
               'v': 'specific-preferred-title!Specific Preferred Title'}]
      }],
      'included_work_titles_search': ['Specific Preferred Title'],
      'title_series_facet': [
        'specific-preferred-title!Specific Preferred Title'
      ]}),

    # 100/245: No preferred title.
    # 245 is main title AND added to IW. Short author info is added.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [{
        'a': 'Smith, Joe',
        'p': [{'d': 'Transcribed title [by Smith, J.]',
               'v': 'transcribed-title!Transcribed title'}]
      }],
      'included_work_titles_search': ['Transcribed title'],
      'title_series_facet': [
        'transcribed-title!Transcribed title'
      ]}),

    # 130/245/700s (IW): Contribs are in 700s; same person.
    # 245 is main title. 130 and 700s are incl. works. Short author
    # info is added.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '12'),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'Second work.'], '12')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'p': [{'d': 'Specific Preferred Title (1933)',
                'v': 'specific-preferred-title-1933!'
                     'Specific Preferred Title (1933)'}]},
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'Second work [by Jung, C.G.]',
                'v': 'second-work!Second work'}]},
      ],
      'included_work_titles_search': [
        'Specific Preferred Title (1933)',
        'First work',
        'Second work',
      ],
      'title_series_facet': [
        'specific-preferred-title-1933!Specific Preferred Title (1933)',
        'first-work!First work',
        'second-work!Second work',
      ]}),

    # 130/245/700s (IW): Contribs are in 700s; different people.
    # 245 is main title. 130 and 700s are incl. works. Short author
    # info is added because there are multiple different people.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '12'),
      ('700', ['a', 'Walter, Johannes.', 't', 'Second work.'], '12')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'p': [{'d': 'Specific Preferred Title (1933)',
                'v': 'specific-preferred-title-1933!'
                     'Specific Preferred Title (1933)'}]},
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'Walter, Johannes',
         'p': [{'d': 'Second work [by Walter, J.]',
                'v': 'second-work!Second work'}]},
      ],
      'included_work_titles_search': [
        'Specific Preferred Title (1933)',
        'First work',
        'Second work',
      ],
      'title_series_facet': [
        'specific-preferred-title-1933!Specific Preferred Title (1933)',
        'first-work!First work',
        'second-work!Second work',
      ]}),

    # 130/245/700s/730s (IW): Contribs are in 700s; same person.
    # 245 is main title. 130 and 700s are incl. works. Short author
    # info is added, where possible.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '12'),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'Second work.'], '12'),
      ('730', ['a', 'Three little pigs.'], '02')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'p': [{'d': 'Specific Preferred Title (1933)',
                'v': 'specific-preferred-title-1933!'
                     'Specific Preferred Title (1933)'}]},
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'Second work [by Jung, C.G.]',
                'v': 'second-work!Second work'}]},
        {'p': [{'d': 'Three little pigs',
                'v': 'three-little-pigs!Three little pigs'}]}
      ],
      'included_work_titles_search': [
        'Specific Preferred Title (1933)',
        'First work',
        'Second work',
        'Three little pigs',
      ],
      'title_series_facet': [
        'specific-preferred-title-1933!Specific Preferred Title (1933)',
        'first-work!First work',
        'second-work!Second work',
        'three-little-pigs!Three little pigs'
      ]}),

    # 130/245/700s (RW): Contribs are in 700s; same person.
    # 245 is main title. 130 is IW, but 700s are RWs. Short author
    # info is added.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'Second work.'], '1 ')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'p': [{'d': 'Specific Preferred Title (1933)',
                'v': 'specific-preferred-title-1933!'
                     'Specific Preferred Title (1933)'}]},
      ],
      'included_work_titles_search': ['Specific Preferred Title (1933)'],
      'related_work_titles_json': [
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'Second work [by Jung, C.G.]',
                'v': 'second-work!Second work'}]},
      ],
      'related_work_titles_search': [
        'First work',
        'Second work',
      ],
      'title_series_facet': [
        'specific-preferred-title-1933!Specific Preferred Title (1933)',
        'first-work!First work',
        'second-work!Second work'
      ]}),

    # 130/245/700s/730s (both): Contribs are in 700s; mix of people.
    # 245 is main title. 130 and some 700s/730s are incl. works; some
    # 700s/730s are rel. works. Short authors are added where needed.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '1 '),
      ('700', ['a', 'Smith, Joe.', 't', 'Second work.'], '12'),
      ('730', ['a', 'Three little pigs.'], '02'),
      ('730', ['a', 'Fourth work.'], '0 ')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'p': [{'d': 'Specific Preferred Title (1933)',
                'v': 'specific-preferred-title-1933!'
                     'Specific Preferred Title (1933)'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Second work [by Smith, J.]',
                'v': 'second-work!Second work'}]},
        {'p': [{'d': 'Three little pigs',
                'v': 'three-little-pigs!Three little pigs'}]}
      ],
      'included_work_titles_search': [
        'Specific Preferred Title (1933)',
        'Second work',
        'Three little pigs'
      ],
      'related_work_titles_json': [
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'p': [{'d': 'Fourth work',
                'v': 'fourth-work!Fourth work'}]},
      ],
      'related_work_titles_search': [
        'First work',
        'Fourth work',
      ],
      'title_series_facet': [
        'specific-preferred-title-1933!Specific Preferred Title (1933)',
        'first-work!First work',
        'second-work!Second work',
        'three-little-pigs!Three little pigs',
        'fourth-work!Fourth work'
      ]}),

    # 100/245/700s (IW): Same author in 700s, no (main) pref title
    # 245 is main title AND is added to IW. Titles in 700s are IWs.
    # Short author info is added.
    ([('100', ['a', 'Smith, Joe.'], '10'),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '12'),
      ('700', ['a', 'Smith, Joe.', 't', 'Second work.'], '12')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Transcribed title [by Smith, J.]',
                'v': 'transcribed-title!Transcribed title'}]},
        {'a': 'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Second work [by Smith, J.]',
                'v': 'second-work!Second work'}]},
      ],
      'included_work_titles_search': [
        'Transcribed title',
        'First work',
        'Second work',
      ],
      'title_series_facet': [
        'transcribed-title!Transcribed title',
        'first-work!First work',
        'second-work!Second work',
      ]}),


    # Collective titles, short authors, and "Complete" vs "Selections"

    # 700: Short author attaches to top level of multi-part titles.
    ([('700', ['a', 'Smith, Joe.', 't', 'First work.', 'n', 'Part One.',
               'n', 'Part Two.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                's': ' > ',
                'v': 'first-work!First work'},
               {'d': 'Part One',
                's': ' > ',
                'v': 'first-work-part-one!First work > Part One'},
               {'d': 'Part Two',
                'v': 'first-work-part-one-part-two!'
                     'First work > Part One > Part Two'}]},
      ],
      'included_work_titles_search': [
        'First work > Part One > Part Two',
      ],
      'title_series_facet': [
        'first-work!First work',
        'first-work-part-one!First work > Part One',
        'first-work-part-one-part-two!First work > Part One > Part Two'
      ]}),

    # 700: Coll title (non-music), by itself.
    # Short author in facet and display. "Complete" added to top-level
    # facet. Short author conj is "of".
    ([('700', ['a', 'Smith, Joe.', 't', 'Works.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Works [of Smith, J.] (Complete)',
                'v': 'works-of-smith-j-complete!'
                     'Works [of Smith, J.] (Complete)'}]},
      ],
      'included_work_titles_search': [
        'Works [of Smith, J.] (Complete)',
      ],
      'title_series_facet': [
        'works-of-smith-j-complete!Works [of Smith, J.] (Complete)'
      ]}),

    # 700: Coll title (non-music), "Selections".
    # Short author in facet and display. "Selections" added to top-level
    # facet. Short author conj is "of".
    ([('700', ['a', 'Smith, Joe.', 't', 'Works.', 'k', 'Selections.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Works [of Smith, J.] (Selections)',
                'v': 'works-of-smith-j-selections!'
                     'Works [of Smith, J.] (Selections)'}]},
      ],
      'included_work_titles_search': [
        'Works [of Smith, J.] (Selections)',
      ],
      'title_series_facet': [
        'works-of-smith-j-selections!Works [of Smith, J.] (Selections)'
      ]}),

    # 700: Coll title (music form), by itself.
    # Short author in facet and display. Facets are added for both the
    # top-level facet and "Complete". The display only includes the
    # "Complete" facet. Short auth conj is "by".
    ([('700', ['a', 'Smith, Joe.', 't', 'Sonatas,', 'm', 'piano.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Sonatas, piano [by Smith, J.] (Complete)',
                'v': 'sonatas-piano-by-smith-j-complete!'
                     'Sonatas, piano [by Smith, J.] (Complete)'}]},
      ],
      'included_work_titles_search': [
        'Sonatas, piano [by Smith, J.] (Complete)',
      ],
      'title_series_facet': [
        'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]',
        'sonatas-piano-by-smith-j-complete!'
        'Sonatas, piano [by Smith, J.] (Complete)'
      ]}),

    # 700: Coll title (music form), "Selections".
    # Short author in facet and display. Top-level facet remains as-is.
    # "Selections" added to second facet. Short auth conj is "by".
    ([('700', ['a', 'Smith, Joe.', 't', 'Sonatas,', 'm', 'piano.',
        'k', 'Selections.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Sonatas, piano [by Smith, J.] (Selections)',
                'v': 'sonatas-piano-by-smith-j-selections!'
                     'Sonatas, piano [by Smith, J.] (Selections)'}]},
      ],
      'included_work_titles_search': [
        'Sonatas, piano [by Smith, J.] (Selections)',
      ],
      'title_series_facet': [
        'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]',
        'sonatas-piano-by-smith-j-selections!'
        'Sonatas, piano [by Smith, J.] (Selections)'
      ]}),

    # 700: Coll title (music form) with parts.
    # Short author in facet and display. Top-level facet remains as-is.
    # Parts generate additional facets. Short auth conj is "by".
    ([('700', ['a', 'Smith, Joe.', 't', 'Sonatas,', 'm', 'piano,',
               'n', 'op. 31.', 'n', 'No. 2.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Sonatas, piano [by Smith, J.]',
                's': ' > ',
                'v': 'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]'},
               {'d': 'Op. 31',
                's': ' > ',
                'v': 'sonatas-piano-by-smith-j-op-31!'
                     'Sonatas, piano [by Smith, J.] > Op. 31'},
               {'d': 'No. 2',
                'v': 'sonatas-piano-by-smith-j-op-31-no-2!'
                     'Sonatas, piano [by Smith, J.] > Op. 31 > No. 2'},
                ]},
      ],
      'included_work_titles_search': [
        'Sonatas, piano [by Smith, J.] > Op. 31 > No. 2'
      ],
      'title_series_facet': [
        'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]',
        'sonatas-piano-by-smith-j-op-31!'
        'Sonatas, piano [by Smith, J.] > Op. 31',
        'sonatas-piano-by-smith-j-op-31-no-2!'
        'Sonatas, piano [by Smith, J.] > Op. 31 > No. 2'
      ]}),

    # 700: Coll title (music form) with "Selections" then parts.
    # Short author in facet and display. Top-level facet remains as-is.
    # Facet for "Selections" is generated. Parts generate additional
    # facets. Short auth conj is "by".
    ([('700', ['a', 'Smith, Joe.', 't', 'Sonatas,', 'm', 'piano.',
               'k', 'Selections,', 'n', 'op. 31.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Sonatas, piano [by Smith, J.] (Selections)',
                's': ' > ',
                'v': 'sonatas-piano-by-smith-j-selections!'
                     'Sonatas, piano [by Smith, J.] (Selections)'},
               {'d': 'Op. 31',
                'v': 'sonatas-piano-by-smith-j-selections-op-31!'
                     'Sonatas, piano [by Smith, J.] (Selections) > Op. 31'},
                ]},
      ],
      'included_work_titles_search': [
        'Sonatas, piano [by Smith, J.] (Selections) > Op. 31'
      ],
      'title_series_facet': [
        'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]',
        'sonatas-piano-by-smith-j-selections!'
        'Sonatas, piano [by Smith, J.] (Selections)',
        'sonatas-piano-by-smith-j-selections-op-31!'
        'Sonatas, piano [by Smith, J.] (Selections) > Op. 31'
      ]}),

    # 700: Coll title (music form) with parts, then "Selections".
    # Short author in facet and display. Top-level facet remains as-is.
    # Facet for "Selections" is generated. Parts generate additional facets.
    # Short auth conj is "by".
    ([('700', ['a', 'Smith, Joe.', 't', 'Sonatas,', 'm', 'piano,',
               'n', 'op. 31.', 'k', 'Selections,'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Sonatas, piano [by Smith, J.]',
                's': ' > ',
                'v': 'sonatas-piano-by-smith-j!'
                     'Sonatas, piano [by Smith, J.]'},
               {'d': 'Op. 31 (Selections)',
                'v': 'sonatas-piano-by-smith-j-op-31-selections!'
                     'Sonatas, piano [by Smith, J.] > Op. 31 (Selections)'},
                ]},
      ],
      'included_work_titles_search': [
        'Sonatas, piano [by Smith, J.] > Op. 31 (Selections)'
      ],
      'title_series_facet': [
        'sonatas-piano-by-smith-j!'
        'Sonatas, piano [by Smith, J.]',
        'sonatas-piano-by-smith-j-op-31!'
        'Sonatas, piano [by Smith, J.] > Op. 31',
        'sonatas-piano-by-smith-j-op-31-selections!'
        'Sonatas, piano [by Smith, J.] > Op. 31 (Selections)',
      ]}),

    # 700: Non-coll title (singular music form)
    # Some musical works' distinctive titles are a singular musical
    # form that would otherwise (in plural form) indicate a collective
    # title, such as Ravel's Bolero.
    ([('700', ['a', 'Ravel, Maurice,', 'd', '1875-1937.', 't', 'Bolero,',
               'm', 'orchestra.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Ravel, Maurice, 1875-1937',
         'p': [{'d': 'Bolero, orchestra [by Ravel, M.]',
                'v': 'bolero-orchestra!Bolero, orchestra'}]},
      ],
      'included_work_titles_search': [
        'Bolero, orchestra',
      ],
      'title_series_facet': [
        'bolero-orchestra!Bolero, orchestra'
      ]}),

    # 700: Non-coll title plus Selections
    # Something like, "Also sprach Zarathustra (Selections)," should
    # include the short author name prior to "(Selections)".
    ([('700', ['a', 'Strauss, Richard,', 'd', '1864-1949.',
               't', 'Also sprach Zarathustra.', 'k', 'Selections.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Strauss, Richard, 1864-1949',
         'p': [{'d': 'Also sprach Zarathustra [by Strauss, R.] (Selections)',
                'v': 'also-sprach-zarathustra-selections!'
                     'Also sprach Zarathustra (Selections)'}]},
      ],
      'included_work_titles_search': [
        'Also sprach Zarathustra (Selections)',
      ],
      'title_series_facet': [
        'also-sprach-zarathustra!Also sprach Zarathustra',
        'also-sprach-zarathustra-selections!'
        'Also sprach Zarathustra (Selections)'
      ]}),

    # 710: Coll title (jurisdiction), by itself.
    # Short author in facet and display. Top-level facet remains as-is.
    # "Complete" facet is NOT generated. No short auth conj.
    ([('710', ['a', 'United States.', 'b', 'Congress.',
               't', 'Laws, etc.'], '12')],
     {'included_work_titles_json': [
        {'a': 'United States Congress',
         'p': [{'d': 'Laws, etc. [United States Congress]',
                'v': 'laws-etc-united-states-congress!'
                     'Laws, etc. [United States Congress]'}]},
      ],
      'included_work_titles_search': [
        'Laws, etc. [United States Congress]',
      ],
      'title_series_facet': [
        'laws-etc-united-states-congress!Laws, etc. [United States Congress]'
      ]}),

    # 710: Coll title (jurisdiction), with parts.
    # Short author in facet and display. Top-level facet remains as-is.
    # "Complete" facet is NOT generated. No short auth conj.
    ([('710', ['a', 'France.', 't', 'Treaties, etc.', 'g', 'Poland,', 
               'd', '1948 Mar. 2.', 'k', 'Protocols, etc.,',
               'd', '1951 Mar. 6.'], '12')],
     {'included_work_titles_json': [
        {'a': 'France',
         'p': [{'d': 'Treaties, etc. [France]',
                's': ' > ',
                'v': 'treaties-etc-france!Treaties, etc. [France]'},
               {'d': 'Poland, 1948 Mar. 2',
                's': ' > ',
                'v': 'treaties-etc-france-poland-1948-mar-2!'
                     'Treaties, etc. [France] > Poland, 1948 Mar. 2'},
               {'d': 'Protocols, etc., 1951 Mar. 6',
                'v': 'treaties-etc-france-poland-1948-mar-2-protocols-etc-1951-'
                     'mar-6!Treaties, etc. [France] > Poland, 1948 Mar. 2 > '
                     'Protocols, etc., 1951 Mar. 6'}
                ]},
      ],
      'included_work_titles_search': [
        'Treaties, etc. [France] > Poland, 1948 Mar. 2 > '
        'Protocols, etc., 1951 Mar. 6',
      ],
      'title_series_facet': [
        'treaties-etc-france!Treaties, etc. [France]',
        'treaties-etc-france-poland-1948-mar-2!'
        'Treaties, etc. [France] > Poland, 1948 Mar. 2',
        'treaties-etc-france-poland-1948-mar-2-protocols-etc-1951-mar-6!'
        'Treaties, etc. [France] > Poland, 1948 Mar. 2 > '
        'Protocols, etc., 1951 Mar. 6'
      ]}),

    # 730: Coll title with no corresponding author info.
    # No short author is generated.
    ([('730', ['a', 'Poems.'], '02')],
     {'included_work_titles_json': [
        {'p': [{'d': 'Poems (Complete)',
                'v': 'poems-complete!Poems (Complete)'},
               ]},
      ],
      'included_work_titles_search': [
        'Poems (Complete)',
      ],
      'title_series_facet': [
        'poems-complete!Poems (Complete)'
      ]}),


    # Deep dive into titles from 245 => included works entries

    # 100/240/245: 240 is basic Coll Title.
    # 245 is added to IW. (Basic collective titles are not very
    # informative or necessarily representative of the work, so the
    # 245 is added in addition.)
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Poems.'], '10'),
      ('245', ['a', 'Poetry! :', 'b', 'an anthology of collected poems /',
               'c', 'by Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Poetry!: an anthology of collected poems',
      'main_title_search': ['Poetry!: an anthology of collected poems'],
      'title_sort': 'poetry-an-anthology-of-collected-poems',
      'responsibility_display': 'by Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['by Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Poetry!: an anthology of collected poems [by Smith, J.]',
                'v': 'poetry-an-anthology-of-collected-poems!'
                     'Poetry!: an anthology of collected poems'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Poems [of Smith, J.] (Complete)',
                'v': 'poems-of-smith-j-complete!'
                     'Poems [of Smith, J.] (Complete)'}]},
      ],
      'included_work_titles_search': [
        'Poetry!: an anthology of collected poems',
        'Poems [of Smith, J.] (Complete)'
      ],
      'title_series_facet': [
        'poetry-an-anthology-of-collected-poems!'
        'Poetry!: an anthology of collected poems',
        'poems-of-smith-j-complete!Poems [of Smith, J.] (Complete)',
      ]}),

    # 100/240/245: 240 is Coll Title/Selections.
    # 245 is added to IW.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Poems.', 'k', 'Selections.'], '10'),
      ('245', ['a', 'Poetry! :', 'b', 'an anthology of selected poems /',
               'c', 'by Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Poetry!: an anthology of selected poems',
      'main_title_search': ['Poetry!: an anthology of selected poems'],
      'title_sort': 'poetry-an-anthology-of-selected-poems',
      'responsibility_display': 'by Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['by Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Poetry!: an anthology of selected poems [by Smith, J.]',
                'v': 'poetry-an-anthology-of-selected-poems!'
                     'Poetry!: an anthology of selected poems'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Poems [of Smith, J.] (Selections)',
                'v': 'poems-of-smith-j-selections!'
                     'Poems [of Smith, J.] (Selections)'}]},
      ],
      'included_work_titles_search': [
        'Poetry!: an anthology of selected poems',
        'Poems [of Smith, J.] (Selections)'
      ],
      'title_series_facet': [
        'poetry-an-anthology-of-selected-poems!'
        'Poetry!: an anthology of selected poems',
        'poems-of-smith-j-selections!Poems [of Smith, J.] (Selections)',
      ]}),

    # 100/240/245: 240 is Music Form w/parts.
    # 245 is not added to IW, because the 240 is specific enough that
    # it probably isn't needed.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Sonatas,', 'm', 'piano,', 'n', 'op. 32,',
               'r', 'C major.'], '10'),
      ('245', ['a', 'Smith\'s piano sonata in C major, opus 32 /',
               'c', 'by Joe Smith.'], '1 ')],
     {'title_display': 'Smith\'s piano sonata in C major, opus 32',
      'main_title_search': ['Smith\'s piano sonata in C major, opus 32'],
      'title_sort': 'smith-s-piano-sonata-in-c-major-opus-32',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Sonatas, piano [by Smith, J.]',
                's': ' > ',
                'v': 'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]'},
               {'d': 'Op. 32, C major',
                'v': 'sonatas-piano-by-smith-j-op-32-c-major!'
                     'Sonatas, piano [by Smith, J.] > Op. 32, C major'},]},
      ],
      'included_work_titles_search': [
        'Sonatas, piano [by Smith, J.] > Op. 32, C major',
      ],
      'title_series_facet': [
        'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]',
        'sonatas-piano-by-smith-j-op-32-c-major!'
        'Sonatas, piano [by Smith, J.] > Op. 32, C major'
      ]}),

    # 100/240/245: 245 has multiple titles, 240 is not Coll Title.
    # The first title from the 245 is not added if the 240 is not a
    # collective title, but subsequent titles from the 245 are if there
    # are no title added entries (7XXs) that cover them.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Specific Preferred Title.'], '10'),
      ('245', ['a', 'First work ;', 'b', 'Second work /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'First work; Second work',
      'main_title_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Specific Preferred Title [by Smith, J.]',
                'v': 'specific-preferred-title!Specific Preferred Title'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Second work [by Smith, J.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'Specific Preferred Title',
        'Second work',
      ],
      'title_series_facet': [
        'specific-preferred-title!Specific Preferred Title',
        'second-work!Second work'
      ]}),

    # 100/240/245: 245 has multiple titles, 240 is basic Coll Title.
    # All titles from the 245 are added to IW if the 240 is a basic
    # collective title.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Poems.', 'k', 'Selections.'], '10'),
      ('245', ['a', 'First poem ;', 'b', 'Second poem /',
               'c', 'by Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'First poem; Second poem',
      'main_title_search': ['First poem; Second poem'],
      'title_sort': 'first-poem-second-poem',
      'responsibility_display': 'by Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['by Joe Smith; edited by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First poem [by Smith, J.]',
                'v': 'first-poem!First poem'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Second poem [by Smith, J.]',
                'v': 'second-poem!Second poem'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Poems [of Smith, J.] (Selections)',
                'v': 'poems-of-smith-j-selections!'
                     'Poems [of Smith, J.] (Selections)'}]},
      ],
      'included_work_titles_search': [
        'First poem',
        'Second poem',
        'Poems [of Smith, J.] (Selections)'
      ],
      'title_series_facet': [
        'first-poem!First poem',
        'second-poem!Second poem',
        'poems-of-smith-j-selections!Poems [of Smith, J.] (Selections)',
      ]}),

    # 100/245/700 (IW): 2-title 245 w/700 covering 2nd.
    # With a multi-titled 245 where ind1 is 1, we assume that the first
    # title should be added if there is no 130/240 and there are not
    # enough 7XXs to cover all of the titles in the 245.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work tt /',
               'c', 'by Joe Smith. Second work tt / by Edward Copeland.'],
       '1 '),
      ('700', ['a', 'Copeland, Edward.', 't', 'Second work.'], '12')],
     {'title_display': 'First work tt; Second work tt',
      'main_title_search': ['First work tt; Second work tt'],
      'title_sort': 'first-work-tt-second-work-tt',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work tt [by Smith, J.]',
                'v': 'first-work-tt!First work tt'}]},
        {'a': 'Copeland, Edward',
         'p': [{'d': 'Second work [by Copeland, E.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work tt',
        'Second work'
      ],
      'title_series_facet': [
        'first-work-tt!First work tt',
        'second-work!Second work'
      ]}),

    # 100/245/700 (IW): 2-title 245 w/700s covering both.
    # With a multi-titled 245 where ind1 is 1 and no 130/240, we assume
    # that none of the 245 titles should generate IW entries if there
    # are enough 7XXs to cover all of the titles in the 245.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work tt /',
               'c', 'by Joe Smith. Second work tt / by Edward Copeland.'],
       '1 '),
      ('700', ['a', 'Smith, Joe.', 't', 'First work.'], '12'),
      ('700', ['a', 'Copeland, Edward.', 't', 'Second work.'], '12')],
     {'title_display': 'First work tt; Second work tt',
      'main_title_search': ['First work tt; Second work tt'],
      'title_sort': 'first-work-tt-second-work-tt',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'Copeland, Edward',
         'p': [{'d': 'Second work [by Copeland, E.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work',
        'Second work'
      ],
      'title_series_facet': [
        'first-work!First work',
        'second-work!Second work'
      ]}),

    # 100/245/700 (IW): 2-title 245 w/700 covering 2nd author only.
    # With a multi-titled 245 where ind1 is 1, no 130/240, and no 7XXs
    # with titles, the titles in 245 => IW entries. If there are added
    # authors, we attempt to match authorship up appropriately based on
    # the SOR.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work /',
               'c', 'by Joe Smith. Second work / by Edward Copeland.'],
       '1 '),
      ('700', ['a', 'Copeland, Edward.'], '1 ')],
     {'title_display': 'First work; Second work',
      'main_title_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'Copeland, Edward',
         'p': [{'d': 'Second work [by Copeland, E.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work',
        'Second work'
      ],
      'title_series_facet': [
        'first-work!First work',
        'second-work!Second work'
      ]}),

    # 100/245/700: 2-title 245 w/700 RWs
    # 700s that are RWs shouldn't interfere with determining whether or
    # not titles in 245s need to be added to IWs.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work /',
               'c', 'by Joe Smith. Second work / by Edward Copeland.'],
       '1 '),
      ('700', ['a', 'Copeland, Edward.', 't', 'Related work.'], '1 ')],
     {'title_display': 'First work; Second work',
      'main_title_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'Copeland, Edward',
         'p': [{'d': 'Second work [by Copeland, E.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work',
        'Second work'
      ],
      'related_work_titles_json': [
        {'a': 'Copeland, Edward',
         'p': [{'d': 'Related work [by Copeland, E.]',
                'v': 'related-work!Related work'}]},
      ],
      'included_work_titles_search': [
        'First work',
        'Second work'
      ],
      'related_work_titles_search': [
        'Related work',
      ],
      'title_series_facet': [
        'first-work!First work',
        'second-work!Second work',
        'related-work!Related work'
      ]}),


    # 100/245: 2-title 245, same author, no 700s.
    # With a multi-titled 245 where ind1 is 1, no 130/240, and no 7XXs,
    # the titles in 245 => IW entries. The author in the 100 should
    # be matched up with each title in each IW entry.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work ;', 'b', 'Second work /',
               'c', 'by Joe Smith.'], '1 ')],
     {'title_display': 'First work; Second work',
      'main_title_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Second work [by Smith, J.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work',
        'Second work'
      ],
      'title_series_facet': [
        'first-work!First work',
        'second-work!Second work'
      ]}),

    # 100/245: 2-title 245, 245 ind1 is 0.
    # If 245 ind1 is 0, no IW or title/series facet entries are
    # generated at all for it, no matter what.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work ;', 'b', 'Second work /',
               'c', 'by Joe Smith.'], '0 ')],
     {'title_display': 'First work; Second work',
      'main_title_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith']
      }),

    # 100/245/740: 2-title 245 with a duplicate 740
    # 740s should only be added as IW or RW if they don't duplicate
    # an existing title in one of these fields.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work ;', 'b', 'Second work /',
               'c', 'by Joe Smith.'], '1 '),
      ('740', ['a', 'Second work.'], '02')],
     {'title_display': 'First work; Second work',
      'main_title_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Second work [by Smith, J.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work',
        'Second work'
      ],
      'title_series_facet': [
        'first-work!First work',
        'second-work!Second work'
      ]}),

    # 700/740: Duplicate 740s
    # 740s that duplicate other titles are ignored.
    ([('700', ['a', 'Smith, Joe.', 't', 'First work.'], '12'),
      ('740', ['a', 'First work.'], '02')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
      ],
      'included_work_titles_search': [
        'First work',
      ],
      'title_series_facet': [
        'first-work!First work',
      ]}),

    # 700/740: Not-duplicate 740s
    # 740s that are unique and don't duplicate other titles are used.
    ([('700', ['a', 'Smith, Joe.', 't', 'First work.'], '12'),
      ('740', ['a', 'Second work.'], '02')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'p': [{'d': 'Second work',
                'v': 'second-work!Second work'}]},
      ],
      'included_work_titles_search': [
        'First work',
        'Second work',
      ],
      'title_series_facet': [
        'first-work!First work',
        'second-work!Second work',
      ]}),

    # Series titles

    # 800s: Series titles, personal author; collective title.
    # Collective titles in 8XX fields follow the same rules as in other
    # fields.
    ([('800', ['a', 'Smith, Joe.', 't', 'Some series.'], '1 '),
      ('800', ['a', 'Copeland, Edward.', 't', 'Piano music.'], '1 ') ],
     {'related_series_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Some series [by Smith, J.]',
                'v': 'some-series!Some series'}]},
        {'a': 'Copeland, Edward',
         'p': [{'d': 'Piano music [of Copeland, E.] (Complete)',
                'v': 'piano-music-of-copeland-e-complete!'
                     'Piano music [of Copeland, E.] (Complete)'}]},
      ],
      'related_series_titles_search': [
        'Some series',
        'Piano music [of Copeland, E.] (Complete)',
      ],
      'title_series_facet': [
        'some-series!Some series',
        'piano-music-of-copeland-e-complete!'
        'Piano music [of Copeland, E.] (Complete)',
      ]}),

    # 810s: Series titles, org author; collective title.
    # Collective titles in 8XX fields follow the same rules as in other
    # fields.
    ([('810', ['a', 'United States.', 'b', 'Congress.', 'b', 'House.',
               't', 'Some series.'], '1 '),
      ('810', ['a', 'Led Zeppelin', 't', 'Piano music.'], '2 ') ],
     {'related_series_titles_json': [
        {'a': 'United States Congress > House',
         'p': [{'d': 'Some series [United States Congress, House]',
                'v': 'some-series!Some series'}]},
        {'a': 'Led Zeppelin',
         'p': [{'d': 'Piano music [Led Zeppelin] (Complete)',
                'v': 'piano-music-led-zeppelin-complete!'
                     'Piano music [Led Zeppelin] (Complete)'}]},
      ],
      'related_series_titles_search': [
        'Some series',
        'Piano music [Led Zeppelin] (Complete)',
      ],
      'title_series_facet': [
        'some-series!Some series',
        'piano-music-led-zeppelin-complete!'
        'Piano music [Led Zeppelin] (Complete)',
      ]}),

    # 811s: Series titles, event author; collective title.
    # Collective titles in 8XX fields follow the same rules as in other
    # fields.
    ([('811', ['a', 'Some conference.', 'n', '(3rd :', 'd', '1983).',
               't', 'Some series.'], '2 '),
      ('811', ['a', 'Some event.', 'n', '(3rd :', 'd', '1983).',
               'e', 'Orchestra.', 't', 'Incidental music.'], '2 ') ],
     {'related_series_titles_json': [
        {'a': 'Some conference (3rd : 1983)',
         'p': [{'d': 'Some series [Some conference]',
                'v': 'some-series!Some series'}]},
        {'a': 'Some event, Orchestra',
         'p': [{'d': 'Incidental music [Some event, Orchestra] (Complete)',
                'v': 'incidental-music-some-event-orchestra-complete!'
                     'Incidental music [Some event, Orchestra] (Complete)'}]},
      ],
      'related_series_titles_search': [
        'Some series',
        'Incidental music [Some event, Orchestra] (Complete)',
      ],
      'title_series_facet': [
        'some-series!Some series',
        'incidental-music-some-event-orchestra-complete!'
        'Incidental music [Some event, Orchestra] (Complete)',
      ]}),

    # 830s: Series titles, no author
    # Collective titles in 8XX fields follow the same rules as in other
    # fields.
    ([('830', ['a', 'Some series.'], ' 0'),
      ('830', ['a', 'Piano music.'], ' 0') ],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                'v': 'some-series!Some series'}]},
        {'p': [{'d': 'Piano music (Complete)',
                'v': 'piano-music-complete!Piano music (Complete)'}]},
      ],
      'related_series_titles_search': [
        'Some series',
        'Piano music (Complete)',
      ],
      'title_series_facet': [
        'some-series!Some series',
        'piano-music-complete!Piano music (Complete)',
      ]}),

    # 490 (untraced): Untraced 490 => Series titles
    ([('490', ['3', '1990-92:', 'a', 'Some series ,', 'x', '1234-5678 ;',
               'v', 'v. 1', 'l', '(LC 12345)'], '0 '),
      ('490', ['3', '1992-93:', 'a', 'Another series =',
               'a', 'Series in English / Joe Smith ;'], '0 '),
      ('490', ['3', '1993-94:', 'a', 'Third series.', 'v', 'v. 1',
               'a', 'Subseries B ;', 'v', 'v. 2', ], '0 ') ],
     {'related_series_titles_json': [
        {'p': [{'d': '(1990-92) Some series; v. 1 | '
                     'ISSN: 1234-5678; LC Call Number: LC 12345'}]},
        {'p': [{'d': '(1992-93) Another series [Joe Smith]'}]},
        {'p': [{'d': '(1993-94) Third series; v. 1 > Subseries B; v. 2'}]},
      ],
      'related_series_titles_search': [
        '(1990-92) Some series; v. 1 | ISSN: 1234-5678; '
        'LC Call Number: LC 12345',
        '(1992-93) Another series [Joe Smith]',
        '(1993-94) Third series; v. 1 > Subseries B; v. 2'
      ]}),

    # 490/800: Traced 490s are not included in Series titles
    ([('490', ['a', 'Some series (statement).',  '1 ']),
      ('490', ['a', 'Piano music of Edward Copeland.',  '1 ']),
      ('800', ['a', 'Smith, Joe.', 't', 'Some series.'], '1 '),
      ('800', ['a', 'Copeland, Edward.', 't', 'Piano music.'], '1 ') ],
     {'related_series_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'Some series [by Smith, J.]',
                'v': 'some-series!Some series'}]},
        {'a': 'Copeland, Edward',
         'p': [{'d': 'Piano music [of Copeland, E.] (Complete)',
                'v': 'piano-music-of-copeland-e-complete!'
                     'Piano music [of Copeland, E.] (Complete)'}]},
      ],
      'related_series_titles_search': [
        'Some series',
        'Piano music [of Copeland, E.] (Complete)',
      ],
      'title_series_facet': [
        'some-series!Some series',
        'piano-music-of-copeland-e-complete!'
        'Piano music [of Copeland, E.] (Complete)',
      ]}),

    # Materials specified and display constants

    # 700 (IW): "Container of" in $i
    # Because "included works" implies that a resource contains the
    # given resource, the $i "Container of" is ignored in that case.
    ([('700', ['i', 'Container of (work):', 'a', 'Smith, Joe.',
               't', 'First work.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
      ],
      'included_work_titles_search': [
        'First work',
      ],
      'title_series_facet': [
        'first-work!First work',
      ]}),

    # 700 (IW): not "Container of" in $i
    # All other $i labels are used.
    ([('700', ['i', 'Based on (work):', 'a', 'Smith, Joe.',
               't', 'First work.'], '12')],
     {'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'b': 'Based on:',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
      ],
      'included_work_titles_search': [
        'First work',
      ],
      'title_series_facet': [
        'first-work!First work',
      ]}),

    # 830: $3 materials specified
    ([('830', ['3', '1992-93:', 'a', 'Some series.'], ' 0')],
     {'related_series_titles_json': [
        {'b': '(1992-93)',
         'p': [{'d': 'Some series',
                'v': 'some-series!Some series'}]},
      ],
      'related_series_titles_search': [
        'Some series',
      ],
      'title_series_facet': [
        'some-series!Some series',
      ]}),

    # 830: $3 materials specified and $i
    ([('830', ['3', '1992-93:', 'i', 'Based on:', 'a', 'Some series.'], ' 0')],
     {'related_series_titles_json': [
        {'b': '(1992-93) Based on:',
         'p': [{'d': 'Some series',
                'v': 'some-series!Some series'}]},
      ],
      'related_series_titles_search': [
        'Some series',
      ],
      'title_series_facet': [
        'some-series!Some series',
      ]}),

    # Preferred titles and expression/version info

    # 730: $lsf, version/expression info
    ([('730', ['a', 'Work title.', 'p', 'First part.', 'p', 'Second part.',
               'l', 'English.', 's', 'Some version.', 'f', '1994.'], '02')],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title',
                's': ' > ',
                'v': 'work-title!Work title'},
               {'d': 'First part',
                's': ' > ',
                'v': 'work-title-first-part!Work title > First part'},
               {'d': 'Second part',
                's': ' | ',
                'v': 'work-title-first-part-second-part!'
                     'Work title > First part > Second part'},
               {'d': 'English, Some version, 1994',
                'v': 'work-title-first-part-second-part-english-some-version-'
                     '1994!'
                     'Work title > First part > Second part | '
                     'English, Some version, 1994'}]},
      ],
      'included_work_titles_search': [
        'Work title > First part > Second part | English, Some version, 1994',
      ],
      'title_series_facet': [
        'work-title!Work title',
        'work-title-first-part!Work title > First part',
        'work-title-first-part-second-part!'
        'Work title > First part > Second part',
        'work-title-first-part-second-part-english-some-version-1994!'
        'Work title > First part > Second part | English, Some version, 1994'
      ]}),

    # 730: $lsf and $k (Selections)
    ([('730', ['a', 'Work title.', 'p', 'First part.', 'p', 'Second part.',
               'l', 'English.', 's', 'Some version.', 'k', 'Selections.',
               'f', '1994.'], '02')],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title',
                's': ' > ',
                'v': 'work-title!Work title'},
               {'d': 'First part',
                's': ' > ',
                'v': 'work-title-first-part!Work title > First part'},
               {'d': 'Second part',
                's': ' | ',
                'v': 'work-title-first-part-second-part!'
                     'Work title > First part > Second part'},
               {'d': 'English, Some version, Selections, 1994',
                'v': 'work-title-first-part-second-part-english-some-version-'
                     'selections-1994!'
                     'Work title > First part > Second part | '
                     'English, Some version, Selections, 1994'}]},
      ],
      'included_work_titles_search': [
        'Work title > First part > Second part | English, Some version, '
        'Selections, 1994',
      ],
      'title_series_facet': [
        'work-title!Work title',
        'work-title-first-part!Work title > First part',
        'work-title-first-part-second-part!'
        'Work title > First part > Second part',
        'work-title-first-part-second-part-english-some-version-selections-'
        '1994!Work title > First part > Second part | English, Some version, '
        'Selections, 1994'
      ]}),

    # 730: multiple languages ($l), Lang1 & Lang2
    ([('730', ['a', 'Three little pigs.', 'l', 'English & German.'], '02')],
     {'included_work_titles_json': [
        {'p': [{'d': 'Three little pigs',
                's': ' | ',
                'v': 'three-little-pigs!Three little pigs'},
               {'d': 'English & German',
                'v': 'three-little-pigs-english-german!'
                     'Three little pigs | English & German'}]},
      ],
      'included_work_titles_search': [
        'Three little pigs | English & German',
      ],
      'title_series_facet': [
        'three-little-pigs!Three little pigs',
        'three-little-pigs-english-german!Three little pigs | English & German',
      ]}),

    # 830: $v, volume info
    ([('830', ['a', 'Some series ;', 'v', 'v. 2.'], ' 0')],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                's': ' | ',
                'v': 'some-series!Some series'},
               {'d': 'v. 2'}]},
      ],
      'related_series_titles_search': [
        'Some series | v. 2',
      ],
      'title_series_facet': [
        'some-series!Some series',
      ]}),

    # 830: $x, ISSN info
    ([('830', ['a', 'Some series.', 'x', '1234-5678'], ' 0')],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                's': ' | ',
                'v': 'some-series!Some series'},
               {'d': 'ISSN: 1234-5678'}]},
      ],
      'related_series_titles_search': [
        'Some series | ISSN: 1234-5678',
      ],
      'title_series_facet': [
        'some-series!Some series',
      ]}),

    # 830: $v and $x, volume + ISSN info
    ([('830', ['a', 'Some series ;', 'v', 'v. 2.', 'x', '1234-5678'], ' 0')],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                's': ' | ',
                'v': 'some-series!Some series'},
               {'d': 'v. 2, ISSN: 1234-5678'}]},
      ],
      'related_series_titles_search': [
        'Some series | v. 2, ISSN: 1234-5678',
      ],
      'title_series_facet': [
        'some-series!Some series',
      ]}),

    # Main titles and truncation

    # 245, single title: Main title > 200 characters
    ([('245', ['a', 'Title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title.'], '0 ')],
     {'title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title ...',
      'non_truncated_title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title',
      'main_title_search': [
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title'
      ],
      'title_sort': 
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title',
     }),

    # 245, multi titles: One title or part > 200 characters
    ([('245', ['a', 'Title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title ;',
               'b', 'Second title.'], '0 ')],
     {'title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title ...; Second title',
      'non_truncated_title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title; Second title',
      'main_title_search': [
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title; Second title'
      ],
      'title_sort': 
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-second-title',
     }),

    # 245: Truncation to colon/subtitle
    ([('245', ['a', 'Title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title :',
               'b', 'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title.'], '0 ')],
     {'title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title ...',
      'non_truncated_title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title: '
        'title title title title title title title title title title title '
        'title title title title title title title title',
      'main_title_search': [
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title: '
        'title title title title title title title title title title title '
        'title title title title title title title title'
      ],
      'title_sort': 
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title',
     }),

    # 245: Truncation to nearest punctuation
    ([('245', ['a', 'Title title title title :',
               'b', 'title title title title title title title title title '
                    'title title title title title title title title, title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title.'], '0 ')],
     {'title_display':
        'Title title title title: title title title title title title title '
        'title title title title title title title title title title ...',
      'non_truncated_title_display':
        'Title title title title: title title title title title title title '
        'title title title title title title title title title title, title '
        'title title title title title title title title title title title '
        'title title title title title title title title',
      'main_title_search': [
        'Title title title title: title title title title title title title '
        'title title title title title title title title title title, title '
        'title title title title title title title title title title title '
        'title title title title title title title title'
      ],
      'title_sort': 
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title',
     }),

    # 245, single title, truncation and title facet values
    # Essentially, the truncated version of the title is what should be
    # used in faceting, included works display, etc.
    ([('245', ['a', 'Title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title.'], '1 ')],
     {'title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title ...',
      'non_truncated_title_display':
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title',
      'main_title_search': [
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title title title'
      ],
      'title_sort': 
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title',
      'included_work_titles_json': [
        {'p': [{'d': 'Title title title title title title title title title '
                     'title title title title title title title title title '
                     'title title title title title title title ...',
                'v': 'title-title-title-title-title-title-title-title-title-'
                     'title-title-title-title-title-title-title-title-title-'
                     'title-title-title-title-title-title-title!Title title '
                     'title title title title title title title title title '
                     'title title title title title title title title title '
                     'title title title title title ...'}]}
      ],
      'included_work_titles_search': [
        'Title title title title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title ...'
      ],
      'title_series_facet': [
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title-title-title-title-title-title-title-title-title-'
        'title-title-title!Title title title title title title title title '
        'title title title title title title title title title title title '
        'title title title title title title ...'
      ],
     }),

    # Non-filing characters

    # 245, multi-title: num of non-filing chars applies only to first
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'The first work ;', 'b', 'Second work /',
               'c', 'by Joe Smith.'], '14')],
     {'title_display': 'The first work; Second work',
      'main_title_search': ['The first work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'included_work_titles_json': [
        {'a': 'Smith, Joe',
         'p': [{'d': 'The first work [by Smith, J.]',
                'v': 'first-work!The first work'}]},
        {'a': 'Smith, Joe',
         'p': [{'d': 'Second work [by Smith, J.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'The first work',
        'Second work'
      ],
      'title_series_facet': [
        'first-work!The first work',
        'second-work!Second work'
      ]}),

    # 740, ignore num of non-filing chars if it doesn't make sense
    # E.g., the nth character should be a non-word character, otherwise
    # assume the # of non-filing chars is incorrect.
    ([('740', ['a', 'The first work.'], '42'),
      ('740', ['a', 'Second work.'], '42')],
     {'included_work_titles_json': [
        {'p': [{'d': 'The first work',
                'v': 'first-work!The first work'}]},
        {'p': [{'d': 'Second work',
                'v': 'second-work!Second work'}]},
      ],
      'included_work_titles_search': [
        'The first work',
        'Second work',
      ],
      'title_series_facet': [
        'first-work!The first work',
        'second-work!Second work',
      ]}),

    # Variant titles

    # 242: Parallel title in 242, w/language ($y)
    ([('242', ['a', 'Title in English.', 'n', 'Part 1', 'y', 'eng'], '00')],
     {'variant_titles_notes': [
        'Title translation, English: Title in English > Part 1'],
      'variant_titles_search': [
        'Title in English > Part 1'],
      }),

    # 242: Parallel title in 242, no language
    ([('242', ['a', 'Title in English.', 'n', 'Part 1'], '00')],
     {'variant_titles_notes': [
        'Title translation: Title in English > Part 1'],
      'variant_titles_search': [
        'Title in English > Part 1'],
      }),

    # 242: Parallel title w/responsibility
    ([('242', ['a', 'Title in English /', 'c', 'by Joe Smith.'], '00')],
     {'responsibility_search': ['by Joe Smith'],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English'],
      }),

    # 245: Parallel title in 245, no separate responsibilty
    ([('245', ['a', 'Title =', 'b', 'Title in English /',
               'c', 'by Joe Smith.'], '00')],
     {'title_display': 'Title',
      'main_title_search': ['Title'],
      'title_sort': 'title',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English'],
      }),

    # 245: Parallel title in 245 with its own SOR
    ([('245', ['a', 'Title in German /',
               'c', 'by German Author = Title in English / by Joe Smith.'],
       '00')],
     {'title_display': 'Title in German',
      'main_title_search': ['Title in German'],
      'title_sort': 'title-in-german',
      'responsibility_display': 'by German Author',
      'responsibility_search': [
        'by German Author',
        'by Joe Smith'
      ],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English'],
      }),

    # 242/245: Parallel title in 242 and 245 (duplicates)
    ([('242', ['a', 'Title in English /', 'c', 'by Joe Smith.'], '00'),
      ('245', ['a', 'Title in German /',
               'c', 'by German Author = Title in English / by Joe Smith.'],
       '00')],
     {'title_display': 'Title in German',
      'main_title_search': ['Title in German'],
      'title_sort': 'title-in-german',
      'responsibility_display': 'by German Author',
      'responsibility_search': [
        'by German Author',
        'by Joe Smith'
      ],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English'],
      }),

    # 245/246: Parallel title in 246 (duplicates)
    ([('245', ['a', 'Title in German /',
               'c', 'by German Author = Title in English / by Joe Smith.'],
       '00'),
      ('246', ['a', 'Title in English'], '01'),],
     {'title_display': 'Title in German',
      'main_title_search': ['Title in German'],
      'title_sort': 'title-in-german',
      'responsibility_display': 'by German Author',
      'responsibility_search': [
        'by German Author',
        'by Joe Smith'
      ],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English'],
      }),

    # 246: Variant title in 246 w/$i
    ([('246', ['i', 'Title on container:', 'a', 'Some title'], '0 ')],
     {'variant_titles_notes': ['Title on container: Some title'],
      'variant_titles_search': ['Some title'],
      }),

    # 246: Variant title in 246 w/no display constant
    ([('246', ['a', 'Some title'], '0 ')],
     {'variant_titles_notes': ['Some title'],
      'variant_titles_search': ['Some title'],
      }),

    # 247: Former title
    ([('247', ['a', 'Some title', 'f', 'Mar. 1924-Nov. 1927'], '10')],
     {'variant_titles_notes': ['Former title: Some title, Mar. 1924-Nov. 1927'],
      'variant_titles_search': ['Some title, Mar. 1924-Nov. 1927'],
      }),

    # 246: No note if ind1 is NOT 0 or 1
    ([('246', ['i', 'Title on container:', 'a', 'Some title'], '3 ')],
     {'variant_titles_search': ['Some title'],
      }),

    # 247: No note if ind2 is 1
    ([('247', ['a', 'Some title', 'f', 'Mar. 1924-Nov. 1927'], '11')],
     {'variant_titles_search': ['Some title, Mar. 1924-Nov. 1927'],
      }),

], ids=[
    # Edge cases
    '1XX field but no titles => empty title_info',
    'Empty 240 field => empty title info',
    'Empty 245 field => empty title info',
    '700 field with no $t => empty title info',
    '130 but NO 245',
    '830 with $5: control fields should be supressed',
    '245 with " char following ISBD period; " char should be kept',
    '245 with punct following last ISBD period',

    # Basic configurations of MARC Fields => title fields
    '130/245: No author.',
    '100/240/245: Single author (title in included works).',
    '100/245: No preferred title.',
    '130/245/700s (IW): Contribs are in 700s; same person.',
    '130/245/700s (IW): Contribs are in 700s; different people.',
    '130/245/700s/730s (IW): Contribs are in 700s; same person.',
    '130/245/700s (RW): Contribs are in 700s; same person.',
    '130/245/700s/730s (both): Contribs are in 700s; mix of people.',
    '100/245/700s (IW): Same author in 700s, no (main) pref title.',

    # Collective titles, short authors, and "Complete" vs "Selections"
    '700: Short author attaches to top level of multi-part titles.',
    '700: Coll title (non-music), by itself.',
    '700: Coll title (non-music), "Selections".',
    '700: Coll title (music form), by itself.',
    '700: Coll title (music form), "Selections".',
    '700: Coll title (music form) with parts.',
    '700: Coll title (music form) with "Selections" then parts.',
    '700: Coll title (music form) with parts, then "Selections".',
    '700: Non-coll title (singular music form)',
    '700: Non-coll title plus Selections',
    '710: Coll title (jurisdiction), by itself.',
    '710: Coll title (jurisdiction), with parts.',
    '730: Coll title with no corresponding author info.',

    # Deep dive into titles from 245 => included works entries
    '100/240/245: 240 is basic Coll Title.',
    '100/240/245: 240 is Coll Title/Selections.',
    '100/240/245: 240 is Music Form w/parts.',
    '100/240/245: 245 has multiple titles, 240 is not Coll Title.',
    '100/240/245: 245 has multiple titles, 240 is basic Coll Title.',
    '100/245/700 (IW): 2-title 245 w/700 covering 2nd.',
    '100/245/700 (IW): 2-title 245 w/700s covering both.',
    '100/245/700 (IW): 2-title 245 w/700 covering 2nd author only.',
    '100/245/700: 2-title 245 w/700 RWs',
    '100/245: 2-title 245, same author, no 700s.',
    '100/245: 2-title 245, 245 ind1 is 0.',
    '100/245/740s: 2-title 245 with a duplicate 740',
    '700/740: Duplicate 740s',
    '700/740: Not-duplicate 740s',

    # Series titles
    '800s: Series titles, personal author; collective title.',
    '810s: Series titles, org author; collective title',
    '811s: Series titles, event author; collective title.',
    '830s: Series titles, no author',
    '490 (untraced): Untraced 490 => Series titles',
    '490/800: Traced 490s are not included in Series titles',

    # Materials specified and display constants
    '700 (IW): "Container of" in $i',
    '700 (IW): not "Container of" in $i',
    '830: $3 materials specified',
    '830: $3 materials specified and $i',

    # Preferred titles and expression/version info
    '730: $lsf, version/expression info',
    '730: $lsf and $k (Selections)',
    '730: multiple languages ($l), Lang1 & Lang2',
    '830: $v, volume info',
    '830: $x, ISSN info',
    '830: $v and $x, volume + ISSN info',

    # Main titles and truncation
    '245, single title: Main title > 200 characters',
    '245, multi titles: One title or part > 200 characters',
    '245, with subtitle: Truncation to colon/subtitle',
    '245: Truncation to nearest punctuation',
    '245, single title, truncation and title facet values',

    # Non-filing characters
    '245, multi-title: num of non-filing chars applies only to first',
    '740, ignore num of non-filing chars if it does not make sense',

    # Variant titles
    '242: Parallel title in 242, w/language ($y)',
    '242: Parallel title in 242, no language',
    '242: Parallel title w/responsibility',
    '245: Parallel title in 245, no separate responsibilty',
    '245: Parallel title in 245 with its own SOR',
    '242/245: Parallel title in 242 and 245 (duplicates)',
    '245/246: Parallel title in 246 (duplicates)',
    '246: Variant title in 246 w/$i',
    '246: Variant title in 246 w/no display constant',
    '247: Former title',
    '246: No note if ind1 is NOT 0 or 1',
    '247: No note if ind2 is 1',
])
def test_blasmpipeline_gettitleinfo(marcfields, expected, bl_sierra_test_record,
                                    blasm_pipeline_class, bibrecord_to_pymarc,
                                    add_marc_fields,
                                    assert_json_matches_expected):
    """
    BlacklightASMPipeline.get_title_info should return fields matching
    the expected parameters.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('100', '110', '111', '130', '240', '242', '243',
                          '245', '246', '247', '490', '700', '710', '711',
                          '730', '740', '800', '810', '811', '830')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_title_info(bib, bibmarc)
    for k, v in val.items():
        print k, v
        if k in expected:
            if k.endswith('_json'):
                assert_json_matches_expected(v, expected[k], exact=True)
            else:
                assert v == expected[k]
        else:
            assert v is None


@pytest.mark.parametrize('parsed_pm, expected', [
    ({'materials_specified': [],
      'total_performers': '8',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('soprano voice', '2')]}],
        [{'primary': [('mezzo-soprano voice', '1')]}],
        [{'primary': [('tenor saxophone', '1')]},
         {'doubling': [('bass clarinet', '1')]}],
        [{'primary': [('trumpet', '1')]}],
        [{'primary': [('piano', '1')]}],
        [{'primary': [('violin', '1')]}, {'doubling': [('viola', '1')]}],
        [{'primary': [('double bass', '1')]}],
     ]}, '8 performers: soprano voice (2); mezzo-soprano voice; tenor '
         'saxophone doubling bass clarinet; trumpet; piano; violin doubling '
         'viola; double bass'),
    ({'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
        [{'solo': [('flute', '1')]}],
        [{'primary': [('orchestra', '1')]}],
     ]}, '1 performer and 1 ensemble: solo flute; orchestra'),
    ({'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('flute', '1')]},
         {'doubling': [('piccolo', '1'), ('alto flute', '1'),
                       ('bass flute', '1')]}],
     ]}, '1 performer: flute doubling piccolo, alto flute, and bass '
         'flute'),
    ({'materials_specified': [],
      'total_performers': '3',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]}, {'doubling': [('flute', '1')]},
         {'alt': [('piccolo', '1')]}],
        [{'primary': [('cello', '1')]}],
        [{'primary': [('piano', '1')]}],
     ]}, '3 performers: violin doubling flute or piccolo; cello; piano'),
    ({'materials_specified': [],
      'total_performers': '8',
      'total_ensembles': '4',
      'parts': [
        [{'solo': [('soprano voice', '3')]}],
        [{'solo': [('alto voice', '2')]}],
        [{'solo': [('tenor voice', '1')]}],
        [{'solo': [('baritone voice', '1')]}],
        [{'solo': [('bass voice', '1')]}],
        [{'primary': [('mixed chorus', '2', ['SATB, SATB'])]}],
        [{'primary': [('children\'s chorus', '1')]}],
        [{'primary': [('orchestra', '1')]}],
     ]}, '8 performers and 4 ensembles: solo soprano voice (3); solo alto '
         'voice (2); solo tenor voice; solo baritone voice; solo bass voice; '
         'mixed chorus (2) [SATB, SATB]; children\'s chorus; orchestra'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]}, {'alt': [('flute', '1')]},
         {'doubling': [('viola', '1')]}, {'alt': [('alto flute', '1')]},
         {'doubling': [('cello', '1')]}, {'alt': [('saxophone', '1')]},
         {'doubling': [('double bass', '1')]}],
     ]}, 'Violin or flute doubling viola or alto flute doubling cello or '
         'saxophone doubling double bass'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
         {'alt': [('flute', '1')]},
         {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
     ]}, 'Violin doubling viola, cello, and double bass or flute doubling alto '
         'flute and saxophone'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1', ['Note1', 'Note2'])]},
         {'doubling': [('viola', '1', ['Note3']),
                       ('cello', '2', ['Note4', 'Note5'])]}]
    ]}, 'Violin [Note1 / Note2] doubling viola [Note3] and cello (2) [Note4 / '
        'Note5]'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
         {'alt': [('flute', '1'), ('clarinet', '1')]},
         {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
     ]}, 'Violin doubling viola, cello, and double bass, flute, or clarinet '
         'doubling alto flute and saxophone'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'alt': [('flute', '1'), ('trumpet', '1'), ('clarinet', '1')]},
         {'doubling': [('viola', '1')]}, {'alt': [('alto flute', '1')]},
         {'doubling': [('cello', '1')]}, {'alt': [('saxophone', '1')]},
         {'doubling': [('double bass', '1')]}],
     ]}, 'Violin, flute, trumpet, or clarinet doubling viola or alto flute '
         'doubling cello or saxophone doubling double bass'),
    ({'materials_specified': ['Piece One'],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
        [{'solo': [('flute', '1')]}],
        [{'primary': [('orchestra', '1')]}],
     ]}, '(Piece One) 1 performer and 1 ensemble: solo flute; orchestra')
])
def test_blasmpipeline_compileperformancemedium(parsed_pm, expected,
                                                blasm_pipeline_class):
    """
    BlacklightASMPipeline.compile_performance_medium should return
    a value matching `expected`, given the sample `parsed_pm` output
    from parsing a 382 field.
    """
    pipeline = blasm_pipeline_class()
    assert pipeline.compile_performance_medium(parsed_pm) == expected


def test_blasmpipeline_getgeneral3xxinfo(add_marc_fields, blasm_pipeline_class):
    """
    BlacklightASMPipeline.get_general_3xx_info should return fields
    matching the expected parameters.
    """
    exclude = s2m.IGNORED_MARC_FIELDS_BY_GROUP_TAG['r']
    handled = ('310', '321')
    exc_fields = [(''.join(('r', t)), ['a', 'No']) for t in exclude + handled]
    inc_fields = [
        ('r300', ['a', '300 desc 1', '0', 'exclude']),
        ('r300', ['a', '300 desc 2', '1', 'exclude']),
        ('r340', ['3', 'self-portrait', 'a', 'rice paper', 'b', '7" x 9"']),
        ('r342', ['a', 'Polyconic', 'g', '0.9996', 'h', '0', 'i', '500,000']),
        ('r343', ['a', 'Coordinate pair;', 'b', 'meters;', 'c', '22;',
                  'd', '22.']),
        ('r344', ['a', 'analog', '2', 'rdatr']),
        ('r344', ['h', 'Dolby-B encoded', '2', 'rdaspc']),
        ('r345', ['a', 'Cinerama', '2', 'rdapf']),
        ('r345', ['b', '24 fps']),
        ('r346', ['a', 'VHS', '2', 'rdavf']),
        ('r346', ['b', 'NTSC', '2', 'rdabs']),
        ('r347', ['a', 'video file', '2', 'rdaft']),
        ('r347', ['b', 'DVD video']),
        ('r347', ['e', 'region 4', '2', 'rdare']),
        ('r352', ['a', 'Raster :', 'b', 'pixel',
                  'd', '(5,000 x', 'e', '5,000) ;', 'q', 'TIFF.']),
        ('r370', ['a', '370 desc 1']),
        ('r382', ['a', 'soprano voice', 'n', '2', 'a', 'mezzo-soprano voice',
                  'n', '1', 'a', 'tenor saxophone', 'n', '1',
                  'd', 'bass clarinet', 'n', '1', 'a', 'trumpet',
                  'n', '1', 'a', 'piano', 'n', '1', 'a', 'violin',
                  'n', '1', 'd', 'viola', 'n', '1', 'a', 'double bass',
                  'n', '1', 's', '8', '2', 'lcmpt'])
    ]
    expected = {
        'physical_medium': ['(self-portrait) rice paper; 7" x 9"'],
        'geospatial_data': ['Polyconic; 0.9996; 0; 500,000',
                            'Coordinate pair; meters; 22; 22.'],
        'audio_characteristics': ['analog', 'Dolby-B encoded'],
        'projection_characteristics': ['Cinerama', '24 fps'],
        'video_characteristics': ['VHS', 'NTSC'],
        'digital_file_characteristics': ['video file', 'DVD video', 'region 4'],
        'graphic_representation': ['Raster : pixel (5,000 x 5,000) ; TIFF.'],
        'performance_medium': ['8 performers: soprano voice (2); mezzo-soprano '
                               'voice; tenor saxophone doubling bass clarinet; '
                               'trumpet; piano; violin doubling viola; double '
                               'bass'],
        'physical_description': ['300 desc 1', '300 desc 2', '370 desc 1']
    }
    marc = add_marc_fields(s2m.SierraMarcRecord(), (exc_fields + inc_fields))
    pipeline = blasm_pipeline_class()
    results = pipeline.get_general_3xx_info(None, marc)
    assert set(results.keys()) == set(expected.keys())
    for k, v in results.items():
        assert v == expected[k]


def test_blasmpipeline_getgeneral5xxinfo(add_marc_fields, blasm_pipeline_class):
    """
    BlacklightASMPipeline.get_general_5xx_info should return fields
    matching the expected parameters.
    """
    exclude = s2m.IGNORED_MARC_FIELDS_BY_GROUP_TAG['n']
    handled = ('505', '508', '520', '592')
    exc_fields = [(''.join(('r', t)), ['a', 'No']) for t in exclude + handled]
    inc_fields = [
        ('n500', ['a', 'General Note.', '0', 'exclude']),
        ('n502', ['a', 'Karl Schmidt\'s thesis (doctoral), Munich, 1965.']),
        ('n502', ['b', 'Ph. D.', 'c', 'University of North Texas',
                  'd', 'August, 2012.']),
        ('n502', ['g', 'Some diss', 'b', 'Ph. D.',
                  'c', 'University of North Texas', 'd', 'August, 2012.']),
        ('n511', ['a', 'Hosted by Hugh Downs.'], '0 '),
        ('n511', ['a', 'Colin Blakely, Jane Lapotaire.'], '1 '),
        ('n521', ['a', 'Clinical students, postgraduate house officers.'],
         '  '),
        ('n521', ['a', '3.1.'], '0 '),
        ('n521', ['a', '7-10.'], '1 '),
        ('n521', ['a', '7 & up.'], '2 '),
        ('n521', ['a', 'Vision impaired', 'a', 'fine motor skills impaired',
                  'a', 'audio learner', 'b', 'LENOCA.'], '3 '),
        ('n521', ['a', 'Moderately motivated.'], '4 '),
        ('n521', ['a', 'MPAA rating: R.'], '8 '),
        ('n546', ['3', 'Marriage certificate', 'a', 'German;',
                  'b', 'Fraktur.']),
        ('n583', ['3', 'plates', 'a', 'condition reviewed', 'c', '20040915',
                  'l', 'mutilated', '2', 'pda', '5', 'DLC'], '1 '),
        ('n583', ['a', 'will microfilm', 'c', '2004', '2', 'pda', '5', 'ICU'],
                  '0 '),
        ('n588', ['a', 'Cannot determine the relationship to Bowling '
                       'illustrated, also published in New York, 1952-58.',
                       '5', 'DLC'], '  '),
        ('n588', ['a', 'Vol. 2, no. 2 (Feb. 1984); title from cover.'], '0 '),
        ('n588', ['a', '2001.'], '1 '),
    ]
    expected = {
        'performers': [
            'Hosted by Hugh Downs.',
            'Cast: Colin Blakely, Jane Lapotaire.'
        ],
        'language_notes': ['(Marriage certificate) German; Fraktur.'],
        'dissertation_notes': [
            'Karl Schmidt\'s thesis (doctoral), Munich, 1965.',
            'Ph. D. ― University of North Texas, August, 2012.',
            'Some diss. Ph. D. ― University of North Texas, August, 2012.'
        ],
        'notes': [
            'General Note.',
            'Audience: Clinical students, postgraduate house officers.',
            'Reading grade level: 3.1.',
            'Ages: 7-10.',
            'Grades: 7 & up.',
            'Special audience characteristics: Vision impaired; fine motor '
            'skills impaired; audio learner (source: LENOCA)',
            'Motivation/interest level: Moderately motivated.',
            'MPAA rating: R.',
            '(plates) condition reviewed; 20040915; mutilated',
            'Cannot determine the relationship to Bowling illustrated, also '
            'published in New York, 1952-58.',
            'Description based on: Vol. 2, no. 2 (Feb. 1984); title from '
            'cover.',
            'Latest issue consulted: 2001.'
        ],
    }
    marc = add_marc_fields(s2m.SierraMarcRecord(), (exc_fields + inc_fields))
    pipeline = blasm_pipeline_class()
    results = pipeline.get_general_5xx_info(None, marc)
    assert set(results.keys()) == set(expected.keys())
    for k, v in results.items():
        assert v == expected[k]


@pytest.mark.parametrize('bib_cn_info, items_info, expected', [
    ([('c', '050', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [])],
     {'call_numbers_display': ['TEST BIB CN'],
      'call_numbers_search': ['TEST BIB CN']}),

    ([('c', '050', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [('c', None, ['TEST ITEM CN'])])],
     {'call_numbers_display': ['TEST BIB CN', 'TEST ITEM CN'],
      'call_numbers_search': ['TEST BIB CN', 'TEST ITEM CN']}),

    ([('c', '050', ['|aTEST CN'])],
     [({'copy_num': 1}, [('c', None, ['TEST CN'])])],
     {'call_numbers_display': ['TEST CN'],
      'call_numbers_search': ['TEST CN']}),

    ([('c', '092', ['|a100.123|aC35 2002'])],
     [({'copy_num': 1}, [('c', '092', ['|a100.123|aC35 2002 copy 1'])])],
     {'call_numbers_display': ['100.123 C35 2002',
                               '100.123 C35 2002 copy 1'],
      'call_numbers_search': ['100.123C35 2002',
                              '100.123C35 2002copy1']}),

    ([('c', '050', ['|aMT 100 .C35 2002']),
      ('c', '090', ['|aC 35.2 .MT100 2002'])],
     [({'copy_num': 1}, [('c', '050', ['|aMT 100 .C35 2002 vol 1'])]),
      ({'copy_num': 2}, [('c', '090', ['|aC 35.2 .MT100 2002 vol 1'])])],
     {'call_numbers_display': ['MT 100 .C35 2002',
                               'C 35.2 .MT100 2002',
                               'MT 100 .C35 2002 vol 1',
                               'C 35.2 .MT100 2002 vol 1'],
      'call_numbers_search': ['MT100.C35 2002',
                              'C35.2.MT100 2002',
                              'MT100.C35 2002vol1',
                              'C35.2.MT100 2002vol1']}),

    ([('c', '099', ['|aLPCD 100,001-100,050'])],
     [({'copy_num': 1}, [('c', '099', ['|aLPCD 100,001-100,050 +insert'])])],
     {'call_numbers_display': ['LPCD 100,001-100,050',
                               'LPCD 100,001-100,050 +insert'],
      'call_numbers_search': ['LPCD100001-100050',
                              'LPCD100001-100050+insert']}),

    ([('c', '086', ['|aA 1.76:643/989|2ordocs'])], [],
     {'sudocs_display': ['A 1.76:643/989'],
      'sudocs_search': ['A1.76:643/989']}),

    ([('g', '086', ['|aA 1.76:643/989|2ordocs'])], [],
     {'sudocs_display': ['A 1.76:643/989'],
      'sudocs_search': ['A1.76:643/989']}),

    ([('g', '086', ['|aA 1.76:643/989|2ordocs'])],
     [({'copy_num': 1}, [('c', '090', ['|aC 35.2 .MT100 2002'])])],
     {'sudocs_display': ['A 1.76:643/989'],
      'sudocs_search': ['A1.76:643/989'],
      'call_numbers_display': ['C 35.2 .MT100 2002'],
      'call_numbers_search': ['C35.2.MT100 2002']}),

], ids=[
    'Basic test; bib call number by itself',
    'Bib and item call numbers are included, if they are different',
    'Duplicate call numbers are de-duplicated',
    'Dewey CNs in 092 are indexed',
    'LC CNs in 050 and 090 are indexed',
    'Local/other CNs in 099 are indexed',
    'C-tagged sudocs in 086 are indexed (in sudocs fields)',
    'G-tagged sudocs in 086 are indexed (in sudocs fields)',
    'Sudoc in 086 and local CN on item is fine--both are indexed'
])
def test_blasmpipeline_getcallnumberinfo(bib_cn_info, items_info, expected,
                                         bl_sierra_test_record,
                                         blasm_pipeline_class,
                                         update_test_bib_inst):
    """
    The `BlacklightASMPipeline.get_call_number_info` method should
    return the expected values given the provided `bib_cn_info` fields
    and `items_info` parameters.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, varfields=bib_cn_info, items=items_info)
    val = pipeline.get_call_number_info(bib, None)
    for k, v in val.items():
        print k, v
        if k in expected:
            assert v == expected[k]
        else:
            assert v is None


@pytest.mark.parametrize('raw_marcfields, expected', [
    # 020s (ISBN)

    # 020: $a is a valid ISBN.
    (['020 ## $a0567890123'], {
        'isbns_display': ['0567890123'],
        'isbn_numbers': ['0567890123'],
        'standard_numbers_search': ['isbn:0567890123']
    }),

    # 020: $z is an invalid ISBN.
    (['020 ## $z0567890123'], {
        'isbns_display': ['0567890123 [Invalid]'],
        'standard_numbers_search': ['isbn:0567890123']
    }),

    # 020: $a and $z in same field.
    (['020 ## $a0567890123$z0567898123'], {
        'isbns_display': ['0567890123', '0567898123 [Invalid]'],
        'isbn_numbers': ['0567890123'],
        'standard_numbers_search': ['isbn:0567890123', 'isbn:0567898123']
    }),

    # 020: $c and $q apply to both ISBNs, if two are in the same field.
    (['020 ## $a0877790019$qblack leather$z0877780116 :$c14.00'], {
        'isbns_display': ['0877790019 (black leather, 14.00)',
                          '0877780116 (black leather, 14.00) [Invalid]'],
        'isbn_numbers': ['0877790019'],
        'standard_numbers_search': ['isbn:0877790019', 'isbn:0877780116']
    }),

    # 020: Multiple $q's beget multiple qualifiers.
    (['020 ## $a0394170660$qRandom House$qpaperback$c4.95'], {
        'isbns_display': ['0394170660 (Random House, paperback, 4.95)'],
        'isbn_numbers': ['0394170660'],
        'standard_numbers_search': ['isbn:0394170660']
    }),

    # 020: Non-numeric data is treated as qualifying information.
    (['020 ## $a0394170660 (Random House ; paperback)'], {
        'isbns_display': ['0394170660 (Random House, paperback)'],
        'isbn_numbers': ['0394170660'],
        'standard_numbers_search': ['isbn:0394170660']
    }),

    # 020: Parentheses around multiple $q's are stripped.
    (['020 ## $z9780815609520$q(cloth ;$qalk. paper)'], {
        'isbns_display': ['9780815609520 (cloth, alk. paper) [Invalid]'],
        'standard_numbers_search': ['isbn:9780815609520']
    }),

    # 020: Multiple parentheses around $q's are stripped.
    (['020 ## $a1401250564$q(bk. 1)$q(paperback)'], {
        'isbns_display': ['1401250564 (bk. 1, paperback)'],
        'isbn_numbers': ['1401250564'],
        'standard_numbers_search': ['isbn:1401250564']
    }),

    # 022s (ISSN)

    # 022: $a is a valid ISSN.
    (['022 ## $a1234-1231'], {
        'issns_display': ['1234-1231'],
        'issn_numbers': ['1234-1231'],
        'standard_numbers_search': ['issn:1234-1231'],
    }),

    # 022: $a (ISSN) and $l (ISSN-L) are displayed separately
    # (Even if they are the same)
    (['022 ## $a1234-1231$l1234-1231'], {
        'issns_display': ['1234-1231', 'ISSN-L: 1234-1231'],
        'issn_numbers': ['1234-1231'],
        'standard_numbers_search': ['issn:1234-1231', 'issnl:1234-1231'],
    }),

    # 022: $m is a canceled ISSN-L
    (['022 ## $a1560-1560$l1234-1231$m1560-1560'], {
        'issns_display': ['1560-1560', 'ISSN-L: 1234-1231',
                          'ISSN-L: 1560-1560 [Canceled]'],
        'issn_numbers': ['1560-1560', '1234-1231'],
        'standard_numbers_search': ['issn:1560-1560', 'issnl:1234-1231',
                                    'issnl:1560-1560'],
    }),

    # 022: $y is an incorrect ISSN
    (['022 ## $a0046-225X$y0046-2254'], {
        'issns_display': ['0046-225X', '0046-2254 [Incorrect]'],
        'issn_numbers': ['0046-225X'],
        'standard_numbers_search': ['issn:0046-225X', 'issn:0046-2254'],
    }),

    # 022: $z is a canceled ISSN
    (['022 ## $z0046-2254'], {
        'issns_display': ['0046-2254 [Canceled]'],
        'standard_numbers_search': ['issn:0046-2254'],
    }),

    # 022: Lots of ISSNs
    (['022 ## $a1234-1231$l1234-1231',
      '022 ## $a1560-1560$m1560-1560',
      '022 ## $a0046-225X$y0046-2254'], {
        'issns_display': ['1234-1231', 'ISSN-L: 1234-1231', '1560-1560',
                          'ISSN-L: 1560-1560 [Canceled]', '0046-225X',
                          '0046-2254 [Incorrect]'],
        'issn_numbers': ['1234-1231', '1560-1560', '0046-225X'],
        'standard_numbers_search': ['issn:1234-1231', 'issnl:1234-1231',
                                    'issn:1560-1560','issnl:1560-1560',
                                    'issn:0046-225X', 'issn:0046-2254'],
    }),

    # 024

    # 024: IND1 of 0 ==> type is isrc.
    (['024 0# $a1234567890'], {
        'other_standard_numbers_display': [
            'International Standard Recording Code: 1234567890'],
        'standard_numbers_search': ['isrc:1234567890'],
    }),

    # 024: IND1 of 1 ==> type is upc.
    (['024 1# $z1234567890'], {
        'other_standard_numbers_display': [
            'Universal Product Code: 1234567890 [Invalid]'],
        'standard_numbers_search': ['upc:1234567890'],
    }),

    # 024: IND1 of 2 ==> type is ismn.
    (['024 2# $a1234567890$qscore$qsewn$cEUR28.50'], {
        'other_standard_numbers_display': [
            'International Standard Music Number: 1234567890 (score, sewn, '
            'EUR28.50)'],
        'standard_numbers_search': ['ismn:1234567890'],
    }),

    # 024: IND1 of 3 ==> type is ean.
    (['024 3# $a1234567890$d51000'], {
        'other_standard_numbers_display': [
            'International Article Number: 1234567890 51000'],
        'standard_numbers_search': ['ean:1234567890 51000'],
    }),

    # 024: IND1 of 4 ==> type is sici.
    (['024 4# $a1234567890'], {
        'other_standard_numbers_display': [
            'Serial Item and Contribution Identifier: 1234567890'],
        'standard_numbers_search': ['sici:1234567890'],
    }),

    # 024: IND1 of 7 ==> type is in the $2.
    (['024 7# $a1234567890$2istc'], {
        'other_standard_numbers_display': [
            'International Standard Text Code: 1234567890'],
        'standard_numbers_search': ['istc:1234567890'],
    }),

    # 024: IND1 of 8 ==> type is unknown.
    (['024 8# $a1234567890'], {
        'other_standard_numbers_display': ['[Unknown Type]: 1234567890'],
        'standard_numbers_search': ['1234567890'],
    }),

    # 025: type ==> 'oan'
    (['025 ## $aAe-F-355$aAe-F-562'], {
        'other_standard_numbers_display': [
            'Overseas Acquisition Number: Ae-F-355',
            'Overseas Acquisition Number: Ae-F-562'],
        'standard_numbers_search': ['oan:Ae-F-355', 'oan:Ae-F-562'],
    }),

    # 026: type ==> 'fingerprint'; ignore control subfields
    (['026 ## $adete nkck$bvess lodo 3$cAnno Domini MDCXXXVI$d3$2fei$5UkCU'], {
        'other_standard_numbers_display': [
            'Fingerprint ID: dete nkck vess lodo 3 Anno Domini MDCXXXVI 3'],
        'standard_numbers_search': [
            'fingerprint:dete nkck vess lodo 3 Anno Domini MDCXXXVI 3'],
    }),

    # 027: type ==> 'strn'
    (['027 ## $aFOA--89-40265/C--SE'], {
        'other_standard_numbers_display': [
            'Standard Technical Report Number: FOA--89-40265/C--SE'],
        'standard_numbers_search': ['strn:FOA--89-40265/C--SE'],
    }),

    # 028: IND1 != 6 ==> publisher number, (publisher name from $b).
    (['028 02 $a438 953-2$bPhilips Classics$q(set)'], {
        'other_standard_numbers_display': [
            'Publisher Number, Philips Classics: 438 953-2 (set)'],
        'standard_numbers_search': ['pn:438 953-2'],
    }),

    # 028: IND1 == 6 ==> distributor number, (distributor name from $b).
    (['028 62 $aDV98597$bFacets Multimedia'], {
        'other_standard_numbers_display': [
            'Distributor Number, Facets Multimedia: DV98597'],
        'standard_numbers_search': ['dn:DV98597'],
    }),

    # 030: 030 ==> type coden
    (['030 ## $aASIRAF$zASITAF'], {
        'other_standard_numbers_display': [
            'CODEN: ASIRAF', 'CODEN: ASITAF [Invalid]'],
        'standard_numbers_search': ['coden:ASIRAF', 'coden:ASITAF'],
    }),

    # 074: 074 ==> type gpo
    (['074 ## $a1022-A$z1012-A'], {
        'other_standard_numbers_display': [
            'Government Printing Office Item Number: 1022-A',
            'Government Printing Office Item Number: 1012-A [Invalid]'],
        'standard_numbers_search': ['gpo:1022-A', 'gpo:1012-A'],
    }),

    # 088: 088 ==> type report
    (['088 ## $aNASA-RP-1124-REV-3$zNASA-RP-1124-REV-2'], {
        'other_standard_numbers_display': [
            'Report Number: NASA-RP-1124-REV-3',
            'Report Number: NASA-RP-1124-REV-2 [Invalid]'],
        'standard_numbers_search': ['report:NASA-RP-1124-REV-3',
                                    'report:NASA-RP-1124-REV-2'],
    }),

], ids=[
    # 020s (ISBN)
    '020: $a is a valid ISBN.',
    '020: $z is an invalid ISBN.',
    '020: $a and $z in same field.',
    '020: $c and $q apply to both ISBNs, if two are in the same field.',
    '020: Multiple $q\'s beget multiple qualifiers.',
    '020: Non-numeric data is treated as qualifying information.',
    '020: Parentheses around multiple $q\'s are stripped.',
    '020: Multiple parentheses around $q\'s are stripped.',

    # 022s (ISSN)
    '022: $a is a valid ISSN.',
    '022: $a (ISSN) and $l (ISSN-L) are displayed separately',
    '022: $m is a canceled ISSN-L',
    '022: $y is an incorrect ISSN',
    '022: $z is a canceled ISSN',
    '022: Lots of ISSNs',

    # 024-084 (Various)
    '024: IND1 of 0 ==> type is isrc.',
    '024: IND1 of 1 ==> type is upc.',
    '024: IND1 of 2 ==> type is ismn.',
    '024: IND1 of 3 ==> type is ean.',
    '024: IND1 of 4 ==> type is sici.',
    '024: IND1 of 7 ==> type is in the $2.',
    '024: IND1 of 8 ==> type is unknown.',
    '025: type ==> oan',
    '026: type ==> fingerprint; ignore control subfields',
    '027: type ==> strn',
    '028: IND1 != 6 ==> publisher number, (publisher name from $b).',
    '028: IND1 == 6 ==> distributor number, (distributor name from $b).',
    '030: 030 ==> type coden',
    '074: 074 ==> type gpo',
    '088: 088 ==> type report',
])
def test_blasmpipeline_getstandardnumberinfo(raw_marcfields, expected,
                                             bl_sierra_test_record,
                                             blasm_pipeline_class,
                                             bibrecord_to_pymarc,
                                             add_marc_fields,
                                             marcfield_strings_to_params):
    """
    The `BlacklightASMPipeline.get_standard_number_info` method should
    return the expected values given the provided `marcfields`.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('020', '022', '024', '025', '026', '027', '028',
                          '030', '074', '088')
    marcfields = marcfield_strings_to_params(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_standard_number_info(bib, bibmarc)
    for k, v in val.items():
        print k, v
        if k in expected:
            assert v == expected[k]
        else:
            assert v is None


@pytest.mark.parametrize('raw_marcfields, expected', [
    # 010 (LCCN)

    # 010: $a is an LCCN, $b is a National Union Catalog number
    (['010 ## $a   89798632 $bms 89001579'], {
        'lccns_display': ['89798632'],
        'lccn_number': '89798632',
        'other_control_numbers_display': [
            'National Union Catalog Number: ms 89001579 (i.e., ms89001579)'],
        'control_numbers_search': ['lccn:89798632', 'nucmc:ms89001579',
                                   'nucmc:ms 89001579'],
    }),

    # 010: $z is an invalid LCCN
    (['010 ## $zsc 76000587'], {
        'lccns_display': ['sc 76000587 (i.e., sc76000587) [Invalid]'],
        'control_numbers_search': ['lccn:sc76000587', 'lccn:sc 76000587'],
    }),

    # 010: normalization 1
    (['010 ## $a89-456'], {
        'lccns_display': ['89-456 (i.e., 89000456)'],
        'lccn_number': '89000456',
        'control_numbers_search': ['lccn:89000456', 'lccn:89-456'],
    }),

    # 010: normalization 2
    (['010 ## $a2001-1114'], {
        'lccns_display': ['2001-1114 (i.e., 2001001114)'],
        'lccn_number': '2001001114',
        'control_numbers_search': ['lccn:2001001114', 'lccn:2001-1114'],
    }),

    # 010: normalization 3
    (['010 ## $agm 71-2450'], {
        'lccns_display': ['gm 71-2450 (i.e., gm71002450)'],
        'lccn_number': 'gm71002450',
        'control_numbers_search': ['lccn:gm71002450', 'lccn:gm 71-2450'],
    }),

    # 010: normalization 4
    (['010 ## $a   79-139101 /AC/MN'], {
        'lccns_display': ['79-139101 /AC/MN (i.e., 79139101)'],
        'lccn_number': '79139101',
        'control_numbers_search': ['lccn:79139101', 'lccn:79-139101 /AC/MN'],
    }),

    # 016: IND1 != 7 ==> LAC control number
    (['016 ## $a 730032015  rev'], {
        'other_control_numbers_display': [
            'Library and Archives Canada Number: 730032015  rev'],
        'control_numbers_search': ['lac:730032015  rev'],
    }),

    # 016: IND1 == 7 ==> control number source in $2
    (['016 7# $a94.763966.7$2GyFmDB'], {
        'other_control_numbers_display': ['94.763966.7 (source: GyFmDB)'],
        'control_numbers_search': ['GyFmDB:94.763966.7'],
    }),

    # 016: $z indicates invalid/canceled control number
    (['016 7# $a890000298$z89000298$2GyFmDB'], {
        'other_control_numbers_display': [
            '890000298 (source: GyFmDB)',
            '89000298 (source: GyFmDB) [Invalid]'],
        'control_numbers_search': ['GyFmDB:890000298', 'GyFmDB:89000298'],
    }),

    # 035: non-OCLC number
    (['035 ## $a(CaOTULAS)41063988'], {
        'other_control_numbers_display': ['41063988 (source: CaOTULAS)'],
        'control_numbers_search': ['CaOTULAS:41063988'],
    }),

    # 035: Invalid OCLC number in $z
    (['035 ## $z(OCoLC)7374506'], {
        'oclc_numbers_display': ['7374506 [Invalid]'],
        'control_numbers_search': ['oclc:7374506'],
    }),

    # 035: OCLC number normalization (ocm)
    (['035 ## $a(OCoLC)ocm0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'control_numbers_search': ['oclc:194068'],
    }),

    # 035: OCLC number normalization (ocn)
    (['035 ## $a(OCoLC)ocn0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'control_numbers_search': ['oclc:194068'],
    }),

    # 035: OCLC number normalization (on)
    (['035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'control_numbers_search': ['oclc:194068'],
    }),

], ids=[
    # 010 (LCCN)
    '010: $a is an LCCN, $b is a National Union Catalog number',
    '010: $z is an invalid LCCN',
    '010: normalization 1',
    '010: normalization 2',
    '010: normalization 3',
    '010: normalization 4',

    # 016 (Other National control numbers)
    '016: IND1 != 7 ==> LAC control number',
    '016: IND1 == 7 ==> control number source in $2',
    '016: $z indicates invalid/canceled control number',

    # 035 (OCLC or other control numbers)
    '035: non-OCLC number',
    '035: Invalid OCLC number in $z',
    '035: OCLC number normalization (ocm)',
    '035: OCLC number normalization (ocn)',
    '035: OCLC number normalization (on)',
])
def test_blasmpipeline_getcontrolnumberinfo(raw_marcfields, expected,
                                            bl_sierra_test_record,
                                            blasm_pipeline_class,
                                            bibrecord_to_pymarc,
                                            add_marc_fields,
                                            marcfield_strings_to_params):
    """
    The `BlacklightASMPipeline.get_control_number_info` method should
    return the expected values given the provided `marcfields`.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('010', '016', '035')
    marcfields = marcfield_strings_to_params(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    val = pipeline.get_control_number_info(bib, bibmarc)
    for k, v in val.items():
        print k, v
        if k in expected:
            assert v == expected[k]
        else:
            assert v is None


@pytest.mark.parametrize('mapping, bundle, expected', [
    ( (('900', ('name', 'title')),),
      {'name': 'N1', 'title': 'T1'},
      [{'tag': '900', 'data': [('a', 'N1'), ('b', 'T1')]}] ),
    ( (('900', ('names', 'titles')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2', 'T3']},
      [{'tag': '900', 'data': [('a', 'N1'), ('a', 'N2'),
                               ('b', 'T1'), ('b', 'T2'), ('b', 'T3')]}] ),
    ( (('900', ('names', 'titles')),
       ('900', ('subjects', 'eras')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'), ('a', 'N2'),
                               ('b', 'T1'), ('b', 'T2')]},
       {'tag': '900', 'data': [('c', 'S1'), ('d', 'E1'), ('d', 'E2')]}] ),
    ( (('900', ('names', 'titles')),
       ('950', ('subjects', 'eras')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'), ('a', 'N2'),
                               ('b', 'T1'), ('b', 'T2')]},
       {'tag': '950', 'data': [('a', 'S1'), ('b', 'E1'), ('b', 'E2')]}] ),
    ( (('900', ('names',)),),
      {'names': ['N1', 'N2']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]}] ),
    ( (('900', ('names',)),
       ('900', ('titles',))),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2', 'T3']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]},
       {'tag': '900', 'data': [('b', 'T1'),]},
       {'tag': '900', 'data': [('b', 'T2'),]}] ),
    ( (('900', ('names',)),
       ('900', ('titles',)),
       ('900', ('subjects', 'eras')),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]},
       {'tag': '900', 'data': [('b', 'T1'),]},
       {'tag': '900', 'data': [('b', 'T2'),]},
       {'tag': '900', 'data': [('c', 'S1'), ('d', 'E1'), ('d', 'E2')]}] ),
    ( (('900', ('names',)),
       ('900', ('titles',)),
       ('950', ('subjects',)),
       ('950', ('eras',)),),
      {'names': ['N1', 'N2'], 'titles': ['T1', 'T2'], 'subjects': ['S1'],
       'eras': ['E1', 'E2']},
      [{'tag': '900', 'data': [('a', 'N1'),]},
       {'tag': '900', 'data': [('a', 'N2'),]},
       {'tag': '900', 'data': [('b', 'T1'),]},
       {'tag': '900', 'data': [('b', 'T2'),]},
       {'tag': '950', 'data': [('a', 'S1'),]},
       {'tag': '950', 'data': [('b', 'E1'),]},
       {'tag': '950', 'data': [('b', 'E2'),]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'auth': ['A1'], 'contrib': ['C1', 'C2'],
       'auth_display': ['A1', 'C1', 'C2'], 'subjects': ['S1', 'S2', 'S3'],
       'eras': ['E1'], 'regions': ['R1', 'R2'], 'topics': ['T1'],
       'genres': ['G1']},
      [{'tag': '900', 'data': [('a', 'A1'), ('b', 'C1'), ('b', 'C2')]},
       {'tag': '900', 'data': [('c', 'A1')]},
       {'tag': '900', 'data': [('c', 'C1')]},
       {'tag': '900', 'data': [('c', 'C2')]},
       {'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('b', 'E1'), ('c', 'R1'), ('c', 'R2')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'auth': ['A1'], 'contrib': ['C1', 'C2'],
       'auth_display': ['A1', 'C1', 'C2'], 'subjects': ['S1', 'S2', 'S3'],
       'regions': ['R1', 'R2'], 'topics': ['T1'], 'genres': ['G1']},
      [{'tag': '900', 'data': [('a', 'A1'), ('b', 'C1'), ('b', 'C2')]},
       {'tag': '900', 'data': [('c', 'A1')]},
       {'tag': '900', 'data': [('c', 'C1')]},
       {'tag': '900', 'data': [('c', 'C2')]},
       {'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('c', 'R1'), ('c', 'R2')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'auth': ['A1'], 'contrib': ['C1', 'C2'],
       'auth_display': ['A1', 'C1', 'C2'], 'subjects': ['S1', 'S2', 'S3'],
       'topics': ['T1'], 'genres': ['G1']},
      [{'tag': '900', 'data': [('a', 'A1'), ('b', 'C1'), ('b', 'C2')]},
       {'tag': '900', 'data': [('c', 'A1')]},
       {'tag': '900', 'data': [('c', 'C1')]},
       {'tag': '900', 'data': [('c', 'C2')]},
       {'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
    ( (('900', ('auth', 'contrib')),
       ('900', ('auth_display',)),
       ('950', ('subjects',)),
       ('950', ('eras', 'regions')),
       ('950', ('topics', 'genres')),),
      {'subjects': ['S1', 'S2', 'S3'], 'topics': ['T1'], 'genres': ['G1']},
      [{'tag': '950', 'data': [('a', 'S1')]},
       {'tag': '950', 'data': [('a', 'S2')]},
       {'tag': '950', 'data': [('a', 'S3')]},
       {'tag': '950', 'data': [('d', 'T1'), ('e', 'G1')]}] ),
], ids=[
    '1 field with >1 subfields (single vals)',
    '1 field with >1 subfields (multiple vals => repeated subfields)',
    '>1 of same field with >1 subfields (single vals and multiple vals)',
    '>1 of diff fields with >1 subfields (single vals and multiple vals)',
    '1 field with 1 subfield (multiple vals => repeated field)',
    '>1 of same field with 1 subfield (multiple vals => repeated fields)',
    '>1 of same field with mixed subfields',
    '>1 of diff fields with 1 subfield (multiple vals => repeated field)',
    'mixed fields and subfields',
    'missing subfield is skipped',
    'missing row is skipped',
    'entire missing field is skipped'
])
def test_plbundleconverter_do_maps_correctly(mapping, bundle, expected,
                                             plbundleconverter_class):
    """
    PipelineBundleConverter.do should convert the given data dict to
    a list of pymarc Field objects correctly based on the provided
    mapping.
    """
    converter = plbundleconverter_class(mapping=mapping)
    fields = converter.do(bundle)
    for field, exp in zip(fields, expected):
        assert field.tag == exp['tag']
        assert list(field) == exp['data']


def test_s2mmarcbatch_compileoriginalmarc_vf_order(s2mbatch_class,
                                                   bl_sierra_test_record,
                                                   add_varfields_to_record):
    """
    S2MarcBatchBlacklightSolrMarc `compile_original_marc` method should
    put variable-length field into the correct order, based on field
    tag groupings and the vf.occ_num values. This should mirror the
    order catalogers put record fields into in Sierra.
    """
    b = bl_sierra_test_record('bib_no_items')
    add_varfields_to_record(b, 'y', '036', ['|a1'], '  ', 0, True)
    add_varfields_to_record(b, 'y', '036', ['|a2'], '  ', 1, False)
    add_varfields_to_record(b, 'a', '100', ['|a3'], '  ', 0, True)
    add_varfields_to_record(b, 'n', '520', ['|a4'], '  ', 0, True)
    add_varfields_to_record(b, 'n', '520', ['|a5'], '  ', 1, False)
    add_varfields_to_record(b, 'n', '500', ['|a6'], '  ', 2, True)
    add_varfields_to_record(b, 'n', '530', ['|a7'], '  ', 3, True)
    add_varfields_to_record(b, 'y', '856', ['|a8'], '  ', 2, True)
    add_varfields_to_record(b, 'y', '856', ['|a9'], '  ', 3, False)
    rec = s2mbatch_class(b).compile_original_marc(b)
    fields = rec.get_fields('036', '100', '500', '520', '530', '856')
    assert fields == sorted(fields, key=lambda l: l.value())
