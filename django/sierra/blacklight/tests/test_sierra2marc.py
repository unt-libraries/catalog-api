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


@pytest.mark.parametrize('subfields, tags, excl, expected', [
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'abc', False,
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'a', False,
     [('a', 'a1'), ('a', 'a2')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], '', False,
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], '', True,
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'd', True,
     [('a', 'a1'), ('a', 'a2'), ('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'd', False,
     []),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'a', True,
     [('b', 'b'), ('c', 'c')]),
    (['a', 'a1', 'a', 'a2', 'b', 'b', 'c', 'c'], 'bc', True,
     [('a', 'a1'), ('a', 'a2')]),
])
def test_sierramarcfield_filtersubfields(subfields, tags, excl, expected):
    """
    SierraMarcField `filter_subfields` method should return the
    expected tuples, given a field built using the given `subfields`
    and the provided `tags` and `excl` args.
    """
    field = s2m.SierraMarcField('100', subfields=subfields)
    filtered = list(field.filter_subfields(tags, excl))
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
      ('a245', ['a', 'a245_1'])], ('a',), (), ['a100_1', 'a100_2', 'a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], (), ('100',), ['a245_1']),
    ([('a100', ['a', 'a100_1']),
      ('a100', ['a', 'a100_2']),
      ('b100', ['a', 'b100_1']),
      ('a245', ['a', 'a245_1'])], (), (), ['a100_1', 'a100_2', 'b100_1',
                                           'a245_1']),
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


@pytest.mark.parametrize('test_date, expected', [
    (None, None),
    (datetime.datetime(2019, 3, 23, tzinfo=pytz.utc), '2019-03-23T00:00:00Z')
])
def test_blasmpipeline_getdateadded(test_date, expected, bl_sierra_test_record,
                                    blasm_pipeline_class,
                                    setattr_model_instance):
    """
    BlacklightASMPipeline.get_date_added should return the correct date
    (bib CAT DATE) in the datetime format Solr requires.
    """
    pipeline = blasm_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    setattr_model_instance(bib, 'cataloging_date_gmt', test_date)
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
    ([({'location_id': 'w3'}, {}),
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
      ({'location_id': 'w4mr1'}, {}),
      ({'location_id': 'w4mr2'}, {}),
      ({'location_id': 'w4mr3'}, {}),
      ({'location_id': 'w4mrb'}, {}),
      ({'location_id': 'w4mrx'}, {})],
     'aeon'),
    ([({'location_id': 'jlf'}, {})],
     'jlf'),
], ids=[
    'items that are requestable through the catalog (Sierra)',
    'items that are not requestable',
    'items that are requestable through Aeon',
    'items that are at JLF'
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
    'non-formatted date in 362 works'
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
    ('a', {'resource_type': 'book',
           'resource_type_facet': ['books']}),
    ('b', {'resource_type': 'online_database',
           'resource_type_facet': ['online_databases']}),
    ('c', {'resource_type': 'music_score',
           'resource_type_facet': ['music_scores']}),
])
def test_blasmpipeline_getresourcetypeinfo(bcode2,
                                           expected, bl_sierra_test_record,
                                           blasm_pipeline_class,
                                           setattr_model_instance):
    """
    BlacklightASMPipeline.get_resource_type_info should return the
    expected resource_type and resource_type_facet values based on the
    given bcode2. Note that this doesn't thoroughly and exhaustively
    test resource type determination; for that, see base.local_rulesets
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
      'contributors_search': ['Berenholtz, Jim, 1957-', 'Jim Berenholtz']
     }),
    ([('810', ['a', 'United States.', 'b', 'Army Map Service.',
               't', 'Special Africa series,', 'v', 'no. 12.'], '1 ')],
     {'author_contributor_facet': ['United States Army Map Service'],
      'contributors_search': ['United States Army Map Service']
     }),
    ([('811', ['a', 'International Congress of Nutrition',
               'n', '(11th :', 'd', '1978 :', 'c', 'Rio de Janeiro, Brazil).',
               't', 'Nutrition and food science ;', 'v', 'v. 1.'], '2 ')],
     {'meeting_facet': [
        'International Congress of Nutrition',
        'International Congress of Nutrition (11th : 1978 : Rio de Janeiro, '
        'Brazil)'],
      'meetings_search': ['International Congress of Nutrition (11th : 1978 : '
                          'Rio de Janeiro, Brazil)']
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


def test_blasmpipeline_getgeneral3xxinfo(add_marc_fields, blasm_pipeline_class):
    """
    BlacklightASMPipeline.get_general_3xx_info should return fields
    matching the expected parameters.
    """
    exclude = s2m.IGNORED_MARC_FIELDS_BY_GROUP_TAG['r']
    handled = ('382',)
    exc_fields = [(''.join(('r', t)), ['a', 'No']) for t in (exclude + handled)]
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
    ]
    expected = {
        'physical_medium': ['rice paper; 7" x 9"'],
        'geospatial_data': ['Polyconic; 0.9996; 0; 500,000',
                            'Coordinate pair; meters; 22; 22.'],
        'audio_characteristics': ['analog', 'Dolby-B encoded'],
        'projection_characteristics': ['Cinerama', '24 fps'],
        'video_characteristics': ['VHS', 'NTSC'],
        'digital_file_characteristics': ['video file', 'DVD video', 'region 4'],
        'graphic_representation': ['Raster : pixel (5,000 x 5,000) ; TIFF.'],
        'physical_description': ['300 desc 1', '300 desc 2', '370 desc 1']
    }
    marc = add_marc_fields(s2m.SierraMarcRecord(), (exc_fields + inc_fields))
    pipeline = blasm_pipeline_class()
    results = pipeline.get_general_3xx_info(None, marc)
    assert set(results.keys()) == set(expected.keys())
    for k, v in results.items():
        assert v == expected[k]


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
