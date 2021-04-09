# -*- coding: utf-8 -*-

"""
Tests the blacklight.sierra2marc functions.
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
    Pytest fixture; returns the s2m.DiscoverS2MarcBatch
    class.
    """
    return s2m.DiscoverS2MarcBatch


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
def params_to_fields():
    """
    Pytest fixture for creating a list of s2m.SierraMarcField objects
    given a list of tuple-fied parameters: (tag, contents, indicators).

    `tag` can be a 3-digit numeric MARC tag ('245') or a 4-digit tag,
    where the III field group tag is prepended ('t245' is a t-tagged
    245 field).

    `indicators` is optional. If the MARC tag is 001 to 009, then a
    data field is created from `contents`. Otherwise `contents` is used
    as a list of subfields, and `indicators` defaults to blank, blank.
    """
    def _make_smarc_field(tag, contents, indicators='  '):
        group_tag = ''
        if len(tag) == 4:
            group_tag, tag = tag[0], tag[1:]
        if int(tag) < 10:
            return s2m.make_mfield(tag, data=contents)
        return s2m.make_mfield(tag, subfields=contents, indicators=indicators,
                               group_tag=group_tag)

    def _make_smarc_fields(fparams):
        fields = []
        for fp in fparams:
            fields.append(_make_smarc_field(*fp))
        return fields
    return _make_smarc_fields


@pytest.fixture
def add_marc_fields():
    """
    Pytest fixture for adding fields to the given `bib` (pymarc Record
    object). If `overwrite_existing` is True, which is the default,
    then all new MARC fields will overwrite existing fields with the
    same tag.

    `fields` must be a list of pymarc.Field or s2m.SierraMarcField
    objects.
    """
    def _add_marc_fields(bib, fields, overwrite_existing=True):
        if overwrite_existing:
            for f in fields:
                bib.remove_fields(f.tag)
        bib.add_field(*fields)
        return bib
    return _add_marc_fields


@pytest.fixture
def todsc_pipeline_class():
    """
    Pytest fixture; returns the ToDiscoverPipeline class.
    """
    return s2m.ToDiscoverPipeline


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

    Pass True for `complete` if you want to make sure all values in
    each of `json_strs` is in `exp_dict`. (Pass False if it's okay for
    each of `json_strs` to have values not in `exp_dicts`.)
    """
    def _assert_json_matches_expected(json_strs, exp_dicts, complete=True):
        json_strs = json_strs or []
        exp_dicts = exp_dicts or []
        if not isinstance(json_strs, (list, tuple)):
            json_strs = [json_strs]
        if not isinstance(exp_dicts, (list, tuple)):
            exp_dicts = [exp_dicts]
        assert len(json_strs) == len(exp_dicts)
        for json, exp_dict in zip(json_strs, exp_dicts):
            cmp_dict = ujson.loads(json)
            assert len(set(exp_dict.keys()) - set(cmp_dict.keys())) == 0
            for k, v in cmp_dict.items():
                if k in exp_dict:
                    assert v == exp_dict[k]
                elif complete:
                    assert v is None
    return _assert_json_matches_expected


@pytest.fixture
def assert_bundle_matches_expected(assert_json_matches_expected):
    """
    Pytest fixture for asserting that a `bundle` dict resulting from
    running a ToDiscoverPipeline process matches an `expected` dict of
    values.

    Use kwargs to control how much to check and match:
        - `bundle_complete`: If True, then keys in `bundle` that aren't
          in `expected` must be None in order to pass. If False, then
          keys in `bundle` not in `expected` are ignored.
        - `json_complete`: Same as `bundle_complete`, but for JSON
          values within `bundle` and `expected`.
        - `list_order_exact`: If True, then non-JSON list items within
          `bundle` must appear in the same order as in `expected` to
          pass; otherwise, order does not matter.
    """
    def _assert_bundle_matches_expected(bundle, expected, bundle_complete=True,
                                        json_complete=True,
                                        list_order_exact=True):
        # `expected` should never have keys that are not in `bundle`
        assert len(set(expected.keys()) - set(bundle.keys())) == 0
        for k, v in bundle.items():
            print('{} => {}'.format(k, v))
            if k in expected:
                if k.endswith('_json'):
                    assert_json_matches_expected(v, expected[k],
                                                 complete=json_complete)
                elif isinstance(v, list) and not list_order_exact:
                    assert sorted(v) == sorted(expected[k])
                else:
                    assert v == expected[k]
            elif bundle_complete:
                assert v is None
    return _assert_bundle_matches_expected


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
            if isinstance(item, dict):
                attrs, item_vfs = item, []
            else:
                attrs, item_vfs = item
            items_to_add.append({'attrs': attrs, 'varfields': item_vfs})
        return add_items_to_bib(bib, items_to_add)
    return _update_test_bib_inst


@pytest.fixture
def fieldstrings_to_fields():
    """
    Pytest fixture. Given a list of MARC field strings copied/pasted
    from the LC or OCLC website, returns a list of s2m.SierraMarcField
    objects.
    """
    def _fieldstrings_to_fields(field_strings):
        utils = s2m.MarcUtils()
        return [utils.fieldstring_to_field(s) for s in field_strings]
    return _fieldstrings_to_fields


@pytest.fixture
def marcutils_for_subjects():
    """
    Pytest fixture. Returns a custom s2m.MarcUtils object to use for
    subject tests, which injects stable pattern/term maps, for testing
    purposes. (Since the live maps could change over time.)
    """
    war_words = '(?:war|revolution)'
    sample_pattern_map = [
        [r'annexation to (.+)',
            [('topic', 'Annexation (International law)'), ('region', '{}')],
            'Annexation to the United States'],
        [r'art and(?: the)? {}'.format(war_words),
            [('topic','Art and war')],
            'Art and the war'],
        [r'dependency on (?!foreign countries)(.+)',
            [('topic', 'Dependency'), ('region', '{}')],
            'Dependency on the United States'],
        [r'(elections, .+)',
            [('topic', 'Elections'), ('topic', '{}')],
            'Elections, 2016'],
        [r'transliteration into (.+)',
            [('topic', 'Transliteration'), ('topic', '{} language')],
            'Translisteration into English'],
    ]
    sample_term_map = {
        '20th century': {
            'parents': {
                'civilization': [
                    'Civilization, Modern',
                ],
                'economic conditions': [
                    'Economic history',
                ],
                'history': [
                    'History, Modern',
                ],
                'history military': [
                    'Military history, Modern',
                ],
                'history naval': [
                    'Naval history, Modern',
                ],
                'history of doctrines': [
                    'Theology, Doctrinal',
                    'History',
                ],
                'intellectual life': [
                    'Intellectual life',
                    'History',
                ],
                'politics and government': [
                    'World politics',
                ],
                'religion': [
                    'Religious history',
                ],
                'social conditions': [
                    'Social history',
                ],
                'social life and customs': [
                    'Manners and customs',
                ],
            },
        },
        'abandonment': {
            'parents': {
                'nests': [
                    'Abandonment of nests',
                ],
            },
        },
        'absorption and adsorption': {
            'headings': [
                'Absorption',
                'Adsorption',
            ],
        },
        'certification': {
            'headings': [
                'Certification (Occupations)',
            ],
            'parents': {
                'seeds': [
                    'Certification (Seeds)',
                ],
            },
        },
        'juvenile literature': {
            'headings': [
                "Children's literature",
                'Juvenile literature',
            ],
        },
    }

    class MarcUtilsSubjectTests(s2m.MarcUtils):
        subject_sd_pattern_map = sample_pattern_map
        subject_sd_term_map = sample_term_map

    return MarcUtilsSubjectTests()


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
    r = add_marc_fields(s2m.SierraMarcRecord(), fields)
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


@pytest.mark.parametrize('fparams, inc, exc, unq, start, end, limit, expected',
                         [
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
def test_groupsubfields_groups_correctly(fparams, inc, exc, unq, start, end,
                                         limit, expected, params_to_fields):
    """
    `group_subfields` should put subfields from a pymarc Field object
    into groupings based on the provided parameters.
    """
    field = params_to_fields([fparams])[0]
    result = s2m.group_subfields(field, inc, exc, unq, start, end, limit)
    assert len(result) == len(expected)
    for group, exp in zip(result, expected):
        assert group.value() == exp


@pytest.mark.parametrize('fparams, sftags, expected', [
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
def test_pullfromsubfields_and_no_pullfunc(fparams, sftags, expected,
                                           params_to_fields):
    """
    Calling `pull_from_subfields` with no `pull_func` specified should
    return values from the given pymarc Field object and the specified
    sftags, as a list.
    """
    field = params_to_fields([fparams])[0]
    for val, exp in zip(s2m.pull_from_subfields(field, sftags), expected):
        assert  val == exp


def test_pullfromsubfields_with_pullfunc(params_to_fields):
    """
    Calling `pull_from_subfields` with a custom `pull_func` specified
    should return values from the given pymarc Field object and the
    specified sftags, run through pull_func, as a flat list.
    """
    subfields = ['a', 'a1.1 a1.2', 'b', 'b1.1 b1.2', 'c', 'c1',
                 'a', 'a2', 'b', 'b2', 'c', 'c2.1 c2.2']
    field = params_to_fields([('260', subfields)])[0]

    def pf(val):
        return val.split(' ')

    expected = ['a1.1', 'a1.2', 'b1.1', 'b1.2', 'c1', 'a2', 'b2', 'c2.1',
                'c2.2']
    pulled = s2m.pull_from_subfields(field, sftags='abc', pull_func=pf)
    for val, exp in zip(pulled, expected):
        assert val == exp


def test_marcfieldgrouper_make_groups(fieldstrings_to_fields):
    """
    The `MarcFieldGrouper.make_groups` method should put fields from
    a MARC record into the appropriate defined groups, without
    generating duplicates.
    """
    rawfields = [
        'x001 record_id',
        'x035 ##$a(OCoLC)record_id',
        'a100 ##$aTest2',
        't245 ##$aTest3',
        'n500 ##$aTest4',
        'n500 ##$aTest5',
        'w505 ##$aTest6',
        'n505 ##$aTest7',
        'a700 ##$aTest8',
        'a700 ##$aTest9',
        'u856 ##$aTest10'
    ]
    marc_record = s2m.SierraMarcRecord(force_utf8=True)
    fields = fieldstrings_to_fields(rawfields)
    marc_record.add_field(*fields)
    grouper = s2m.MarcFieldGrouper({
        'author': set(['100', '110', '111', '700', '710', '711']),
        'main_author': set(['100', '110', '111']),
        'all_authors': set(['a']),
        'physical_description': set(['r', '300']),
        'notes': set(['505', 'n', '500', 'n505']),
        'other_notes': set(['n']),
        'control_number': set(['001']),
        'weird_note': set(['w505']),
    })
    groups = grouper.make_groups(marc_record)
    assert groups == {
        'author': [fields[2], fields[8], fields[9]],
        'main_author': [fields[2]],
        'all_authors': [fields[2], fields[8], fields[9]],
        'notes': [fields[4], fields[5], fields[6], fields[7]],
        'other_notes': [fields[4], fields[5], fields[7]],
        'control_number': [fields[0]],
        'weird_note': [fields[6]],
    }


@pytest.mark.parametrize('subfields, sep, sff, expected', [
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], '; ', None,
      '(case files) aperture cards; 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], '; ', {'exclude': '3'},
      'aperture cards; 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      '3', 'microfilm', 'f', '48x'], '; ', None,
      '(case files) aperture cards; 9 x 19 cm.; 48x'),
    (['a', 'aperture cards', 'b', '9 x 19 cm.', 'd', 'microfilm',
      'f', '48x', '3', 'case files'], '; ', None,
      'aperture cards; 9 x 19 cm.; microfilm; 48x'),
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
def test_genericdisplayfieldparser_parse(subfields, sep, sff, expected,
                                         params_to_fields):
    """
    The GenericDisplayFieldParser `parse` method should return the
    expected result when parsing a MARC field with the given
    `subfields`, given the provided `sep` (separator) and `sff`
    (subfield filter).
    """
    field = params_to_fields([('300', subfields)])[0]
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
def test_performancemedparser_parse(subfields, expected, params_to_fields):
    """
    PerformanceMedParser `parse` method should return a dict with the
    expected structure, given the provided MARC 382 field.
    """
    field = params_to_fields([('382', subfields)])[0]
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
def test_dissertationnotesfieldparser_parse(subfields, expected,
                                            params_to_fields):
    """
    DissertationNotesFieldParser `parse` method should return a dict
    with the expected structure, given the provided MARC 502 subfields.
    """
    field = params_to_fields([('502', subfields)])[0]
    assert s2m.DissertationNotesFieldParser(field).parse() == expected


@pytest.mark.parametrize('raw_marcfields, expected', [
    (['700 0#$a***,$cMadame de'],
     ['', 'Madame de', 'Madame, Madame de']),
    (['700 1#$aPompadour,$cMadame de'],
     ['Pompadour', 'Madame Pompadour Madame de Pompadour',
      'Madame Pompadour, Madame de Pompadour']),
    (['700 1#$aWinchilsea, Anne Finch,$cCountess of'],
     ['Winchilsea, Anne Finch Winchilsea, A.F Winchilsea',
      'Countess Anne Winchilsea Countess of Winchilsea',
      'Countess Anne Finch Winchilsea, Countess of Winchilsea',]),
    (['700 1#$aPeng + Hu,$eeditor.'],
     ['Peng Hu', 'Peng Hu', 'Peng Hu']),
    (['100 0#$aH. D.$q(Hilda Doolittle),$d1886-1961.'],
     ['H.D', 'Hilda Doolittle', 'Hilda Doolittle', 'H.D']),
    (['100 1#$aGresham, G. A.$q(Geoffrey Austin)'],
     ['Gresham, Geoffrey Austin Gresham, G.A Gresham', 'Geoffrey Gresham',
      'G.A Gresham']),
    (['100 1#$aSmith, Elizabeth$q(Ann Elizabeth)'],
     ['Smith, Elizabeth', 'Smith, E', 'Smith, Ann Elizabeth Smith, A.E Smith',
      'Ann Smith', 'Elizabeth Smith']),
    (['700 1#$aE., Sheila$q(Escovedo),$d1959-'],
     ['E, Sheila E, S.E', 'Escovedo, Sheila Escovedo, S Escovedo',
      'Sheila Escovedo', 'Sheila E']),
    (['100 1#$aBeeton,$cMrs.$q(Isabella Mary),$d1836-1865.'],
     ['Beeton, Isabella Mary Beeton, I.M Beeton', 'Mrs Isabella Beeton',
      'Mrs Beeton']),
    (['100 1#$aHutchison, Thomas W.$q(Thomas William),$eauthor$4aut'],
     ['Hutchison, Thomas W Hutchison, Thomas William Hutchison, T.W Hutchison',
      'Thomas Hutchison', 'Thomas W Hutchison']),
    (['600 10$aKoh, Tommy T. B.$q(Tommy Thong Bee),$d1937-'],
     ['Koh, Tommy T.B Koh, Tommy Thong Bee Koh, T.T.B Koh', 'Tommy Koh',
      'Tommy T.B Koh']),
    (['600 11$aMagellan, Ferdinand,$dd 1521.'],
     ['Magellan, Ferdinand Magellan, F Magellan', 'Ferdinand Magellan',
      'Ferdinand Magellan']),
    (['600 00$aGautama Buddha$vEarly works to 1800.'],
     ['Gautama Buddha', 'Gautama Buddha', 'Gautama Buddha']),
    (['100 00$aThomas,$cAquinas, Saint,$d1225?-1274.'],
     ['Thomas', 'Saint Thomas Aquinas', 'Saint Thomas, Aquinas']),
    (['100 1#$aSeuss,$cDr.'],
     ['Seuss', 'Dr Seuss', 'Dr Seuss']),
    (['100 1#$aBeethoven, Ludwig van,$d1770-1827$c(Spirit)'],
     ['Beethoven, Ludwig van Beethoven, L.v Beethoven',
      'Ludwig van Beethoven Spirit', 'Ludwig van Beethoven, Spirit']),
    (['100 1#$aMasséna, André,$cprince d\'Essling,$d1758-1817.'],
     ['Masséna, André Masséna, A Masséna',
      'André Masséna prince d Essling',
      'André Masséna, prince d Essling']),
    (['100 1#$aWalle-Lissnijder,$cvan de.'],
     ['Walle Lissnijder', 'van de Walle Lissnijder',
      'van de Walle Lissnijder']),
    (['700 0#$aCharles Edward,$cPrince, grandson of James II, King of England,'
      '$d1720-1788.'],
     ['Charles Edward',
      'Prince Charles Edward grandson of James II King of England',
      'Prince Charles Edward, grandson of James II, King of England']),
    (['100 0#$aJohn Paul$bII,$cPope,$d1920-'],
     ['John Paul', 'Pope John Paul II', 'Pope John Paul II']),
    (['100 0#$aJohn$bII Comnenus,$cEmperor of the East,$d1088-1143.'],
     ['John', 'Emperor John II Comnenus Emperor of the East',
      'Emperor John II Comnenus, Emperor of the East']),
    (['100 1#$aSaxon, Joseph$q(Irv).'],
     ['Saxon, Joseph Saxon, J Saxon, Irv Saxon, I Saxon', 'Joseph Saxon',
      'Joseph Irv Saxon']),
    (['100 1#$aSaxon, Joseph (Irv).'],
     ['Saxon, Joseph Saxon, J Saxon, Irv Saxon, I Saxon', 'Joseph Saxon',
      'Joseph Irv Saxon']),
    (['100 1#$aSaxon, J. (Irv)$q(Joseph).'],
     ['Saxon, Joseph Saxon, J Saxon, Irv Saxon, I Saxon', 'Joseph Saxon',
      'J Irv Saxon']),
    (['100 1#$aBannister, D.$q{17} (Donald)'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'D Bannister']),
    (['100 1#$aBannister,$qD. (Donald)'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'Bannister']),
    (['100 1#$aBannister, D.$q(Donald) 1908-'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'D Bannister']),
    (['100 1#$aBannister, D.$qDonald'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'D Bannister']),
])
def test_personalnamepermutator_getsearchperms(raw_marcfields, expected,
                                               fieldstrings_to_fields):
    """
    The `get_search_permutations` method of the PersonalNamePermutator
    class should return the expected list of search permutations for
    the name in the give MARC field input.
    """
    field = fieldstrings_to_fields(raw_marcfields)[0]
    parsed_name = s2m.PersonalNameParser(field).parse()
    permutator = s2m.PersonalNamePermutator(parsed_name)
    result = permutator.get_search_permutations()
    print result
    assert result == expected


def test_todscpipeline_do_creates_compiled_dict(todsc_pipeline_class):
    """
    The `do` method of ToDiscoverPipeline should return a dict
    compiled from the return value of each of the `get` methods--each
    key/value pair from each return value added to the finished value.
    If the same dict key is returned by multiple methods and the vals
    are lists, the lists are concatenated.
    """
    class DummyPipeline(todsc_pipeline_class):
        fields = ['dummy1', 'dummy2', 'dummy3', 'dummy4']
        prefix = 'get_'

        def get_dummy1(self):
            return {'d1': 'd1v'}

        def get_dummy2(self):
            return { 'd2a': 'd2av', 'd2b': 'd2bv' }

        def get_dummy3(self):
            return { 'stuff': ['thing'] }

        def get_dummy4(self):
            return { 'stuff': ['other thing']}

    dummy_pipeline = DummyPipeline()
    bundle = dummy_pipeline.do(None, None)
    assert bundle == { 'd1': 'd1v', 'd2a': 'd2av', 'd2b': 'd2bv',
                       'stuff': ['thing', 'other thing'] }


def test_todscpipeline_getid(bl_sierra_test_record, todsc_pipeline_class):
    """
    ToDiscoverPipeline.get_id should return the bib Record ID
    formatted according to III's specs.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('b6029459')
    val = pipeline.do(bib, None, ['id'])
    assert val == {'id': 'b6029459'}


@pytest.mark.parametrize('in_val, expected', [
    (True, 'true'),
    (False, 'false')
])
def test_todscpipeline_getsuppressed(in_val, expected, bl_sierra_test_record,
                                     todsc_pipeline_class,
                                     setattr_model_instance):
    """
    ToDiscoverPipeline.get_suppressed should return 'false' if the
    record is not suppressed.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('b6029459')
    setattr_model_instance(bib, 'is_suppressed', in_val)
    val = pipeline.do(bib, None, ['suppressed'])
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
def test_todscpipeline_getdateadded(bib_locs, created_date, cat_date, expected,
                                    bl_sierra_test_record, todsc_pipeline_class,
                                    get_or_make_location_instances,
                                    update_test_bib_inst,
                                    setattr_model_instance):
    """
    ToDiscoverPipeline.get_date_added should return the correct date
    in the datetime format Solr requires.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    loc_info = [{'code': code} for code in bib_locs]
    locations = get_or_make_location_instances(loc_info)
    if locations:
        bib = update_test_bib_inst(bib, locations=locations)
    setattr_model_instance(bib, 'cataloging_date_gmt', cat_date)
    setattr_model_instance(bib.record_metadata, 'creation_date_gmt',
                           created_date)
    val = pipeline.do(bib, None, ['date_added'])
    assert val == {'date_added': expected}


def test_todscpipeline_getiteminfo_ids(bl_sierra_test_record,
                                       todsc_pipeline_class,
                                       update_test_bib_inst,
                                       assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    ToDiscoverPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'i' key for each
    JSON object should match the numeric portion of the III rec num for
    that item.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=[{}, {'is_suppressed': True}, {}])
    val = pipeline.do(bib, None, ['item_info'])

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
def test_todscpipeline_getiteminfo_callnum_vol(bib_cn_info, items_info,
                                               expected, bl_sierra_test_record,
                                               todsc_pipeline_class,
                                               update_test_bib_inst,
                                               assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    ToDiscoverPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'c' key for each
    JSON object contains the call number, and the 'v' key contains the
    volume. Various parameters test how the item call numbers and
    volumes are generated.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, varfields=bib_cn_info, items=items_info)
    val = pipeline.do(bib, None, ['item_info'])
    expected = [{'c': cn, 'v': vol} for cn, vol in expected]
    assert_json_matches_expected(val['items_json'], expected, complete=False)


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
def test_todscpipeline_getiteminfo_bcodes_notes(items_info, expected,
                                                bl_sierra_test_record,
                                                todsc_pipeline_class,
                                                update_test_bib_inst,
                                                assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    ToDiscoverPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'b' and 'n' keys
    for each JSON object contain the barcode and public notes,
    respectively. Various parameters test how those are generated.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    val = pipeline.do(bib, None, ['item_info'])
    assert_json_matches_expected(val['items_json'], expected, complete=False)


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
def test_todscpipeline_getiteminfo_num_items(items_info, exp_items,
                                             exp_more_items,
                                             bl_sierra_test_record,
                                             todsc_pipeline_class,
                                             update_test_bib_inst,
                                             assert_json_matches_expected):
    """
    ToDiscoverPipeline.get_item_info return value should be a dict
    with keys `items_json`, `more_items_json`, and `has_more_items`
    that are based on the total number of items on the record. The
    first three attached items are in items_json; others are in
    more_items_json. has_more_items is 'true' if more_items_json is
    not empty. Additionally, items should remain in the order they
    appear on the record.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    val = pipeline.do(bib, None, ['item_info'])
    assert_json_matches_expected(val['items_json'], exp_items, complete=False)
    if exp_more_items:
        assert val['has_more_items'] == 'true'
        assert_json_matches_expected(val['more_items_json'], exp_more_items,
                                     complete=False)
    else:
        assert val['has_more_items'] == 'false'
        assert val['more_items_json'] is None


@pytest.mark.parametrize('items_info, expected_r', [
    # Note: tests that are commented out represent "normal" policies;
    # currently due to COVID-19 a lot of requesting is restricted. We
    # will update these further as policies change.
    ([({'location_id': 'x'}, {}),
      ({'location_id': 'w3'}, {}),
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
      ({'location_id': 'xspe'}, {}),
      # ({'location_id': 'w4mr1'}, {}),
      # ({'location_id': 'w4mr2'}, {}),
      # ({'location_id': 'w4mr3'}, {}),
      # ({'location_id': 'w4mrb'}, {}),
      # ({'location_id': 'w4mrx'}, {})
     ], 'aeon'),
    ([({'location_id': 'jlf'}, {})],
     'jlf'),
], ids=[
    'items that are requestable through the catalog (Sierra)',
    'items that are not requestable',
    'items that are requestable through Aeon',
    'items that are at JLF'
])
def test_todscpipeline_getiteminfo_requesting(items_info, expected_r,
                                              bl_sierra_test_record,
                                              todsc_pipeline_class,
                                              update_test_bib_inst,
                                              assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    ToDiscoverPipeline.get_item_info should be a list of JSON
    objects, each one corresponding to an item. The 'r' key for each
    JSON object contains a string describing how end users request the
    item. (See parameters for details.) Note that this hits the
    highlights but isn't exhaustive.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    val = pipeline.do(bib, None, ['item_info'])
    exp_items = [{'r': expected_r} for i in range(0, len(items_info))]
    assert_json_matches_expected(val['items_json'], exp_items[0:3],
                                 complete=False)
    if val['more_items_json'] is not None:
        assert_json_matches_expected(val['more_items_json'], exp_items[3:],
                                     complete=False)


@pytest.mark.parametrize('bib_locations, bib_cn_info, expected', [
    ([('w', 'Willis Library'), ('czm', 'Chilton Media Library')],
     [('c', '090', ['|aTEST BIB CN'])],
     [{'i': None, 'c': 'TEST BIB CN', 'l': 'w'},
      {'i': None, 'c': 'TEST BIB CN', 'l': 'czm'}]),
    ([],
     [('c', '090', ['|aTEST BIB CN'])],
     [{'i': None, 'c': 'TEST BIB CN', 'l': 'none'}]),
])
def test_todscpipeline_getiteminfo_pseudo_items(bib_locations, bib_cn_info,
                                                expected,
                                                bl_sierra_test_record,
                                                todsc_pipeline_class,
                                                get_or_make_location_instances,
                                                update_test_bib_inst,
                                                assert_json_matches_expected):
    """
    When a bib record has no attached items, the `items_json` key of
    the value returned by ToDiscoverPipeline.get_item_info should
    contain "pseudo-item" entries generated based off the bib locations
    and bib call numbers.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    loc_info = [{'code': code, 'name': name} for code, name in bib_locations]
    locations = get_or_make_location_instances(loc_info)
    bib = update_test_bib_inst(bib, varfields=bib_cn_info, locations=locations)
    val = pipeline.do(bib, None, ['item_info'])
    assert_json_matches_expected(val['items_json'], expected)


@pytest.mark.parametrize('fparams, items_info, expected', [
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
    ([('962', ['t', 'Media Thing 1']),
      ('962', ['t', 'Media Thing 2'])],
     [],
     [{'t': 'fulltext', 'n': 'Media Thing 1',
       'u': 'https://iii.library.unt.edu/search~S12?/.b1/.b1/1,1,1,B/l962'
             '~b1&FF=&1,0,,0,0'},
      {'t': 'fulltext', 'n': 'Media Thing 2',
       'u': 'https://iii.library.unt.edu/search~S12?/.b1/.b1/1,1,1,B/l962'
             '~b1&FF=&1,0,,1,0'}]),
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
    '962 (media manager) fields, no URLs => generate e-reserve URLs',
    '962 (media manager) field, type fulltext based on title',
])
def test_todscpipeline_geturlsjson(fparams, items_info, expected,
                                   bl_sierra_test_record, todsc_pipeline_class,
                                   bibrecord_to_pymarc, update_test_bib_inst,
                                   params_to_fields, add_marc_fields,
                                   assert_json_matches_expected):
    """
    The `urls_json` key of the value returned by
    ToDiscoverPipeline.get_urls_json should be a list of JSON
    objects, each one corresponding to a URL.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('856', '962')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    pipeline.bundle['id'] = '.b1'
    val = pipeline.do(bib, bibmarc, ['urls_json'], False)
    assert_json_matches_expected(val['urls_json'], expected, complete=False)


@pytest.mark.parametrize('fparams, expected_url', [
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
               'u', 'https://library.unt.edu/media/factory_assets/cover.jpg',
               'e', 'https://library.unt.edu/media/factory_assets/thumb.jpg'])],
     'https://library.unt.edu/media/factory_assets/cover.jpg'),
    # We don't actually HAVE anything currently that has a 962 field
    # pointing to an external image, so this is moot. Any images are
    # treated as cover images now.
    ([('962', ['t', 'Cover Image',
               'u', 'http://example.com/media/covers/cover.jpg',
               'e', 'http://example.com/media/covers/thumb.jpg'])],
     'https://example.com/media/covers/cover.jpg'),
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
    'media cover at slightly different URL',
    'other 962 image(s): non-UNTL media images',
    'standard Digital Library cover',
    'standard Portal cover',
    'strip querystrings when formulating DL/Portal URLs',
    'DL/Portal cover with hacked attribute additions on URLs',
    'DL/Portal cover with hacked attribute additions on URLs AND querystring',
    'other 856 link(s): ignore non-DL/Portal URLs'
])
def test_todscpipeline_getthumbnailurl(fparams, expected_url,
                                       bl_sierra_test_record,
                                       todsc_pipeline_class,
                                       bibrecord_to_pymarc, params_to_fields,
                                       add_marc_fields):
    """
    ToDiscoverPipeline.get_thumbnail_url should return a URL for
    a local thumbnail image, if one exists. (Either Digital Library or
    a Media Library thumbnail.)
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('856', '962')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    val = pipeline.do(bib, bibmarc, ['thumbnail_url'])
    assert val['thumbnail_url'] == expected_url


@pytest.mark.parametrize('fparams, exp_pub_sort, exp_pub_year_display, '
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
    ([('008', 's10uu    ')],
     '10--', '11th century', [], ['1000-1009', '1010-1019', '1020-1029',
                                  '1030-1039', '1040-1049', '1050-1059',
                                  '1060-1069', '1070-1079', '1080-1089',
                                  '1090-1099'],
     ['11th century', '1000s', '1010s', '1020s', '1030s', '1040s', '1050s',
      '1060s', '1070s', '1080s', '1090s']),
    ([('008', 's11uu    ')],
     '11--', '12th century', [], ['1100-1109', '1110-1119', '1120-1129',
                                  '1130-1139', '1140-1149', '1150-1159',
                                  '1160-1169', '1170-1179', '1180-1189',
                                  '1190-1199'],
     ['12th century', '1100s', '1110s', '1120s', '1130s', '1140s', '1150s',
      '1160s', '1170s', '1180s', '1190s']),
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
     '2014', '2014', ['2014'], ['2010-2019'], ['2014', '2010s']),
    ([('008', 's2014    '),
      ('362', ['a', 'Began with vol. 4, published in 1947.'], '1 ')],
     '2014', '2014', ['2014'], ['2010-2019'], ['2014', '2010s']),
    ([('008', 's2014    '),
      ('362', ['a', 'Published in 1st century.'], '1 ')],
     '2014', '2014', ['2014'], ['2010-2019',], ['2014', '2010s']),
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
    'century (10uu) in single-date 008',
    'century (11uu) in single-date 008',
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
    'formatted date in 362 (ignored)',
    'non-formatted date in 362 (ignored)',
    'century (1st) in 362 (ignored)'
])
def test_todscpipeline_getpubinfo_dates(fparams, exp_pub_sort,
                                        exp_pub_year_display,
                                        exp_pub_year_facet,
                                        exp_pub_decade_facet,
                                        exp_pub_dates_search,
                                        bl_sierra_test_record,
                                        todsc_pipeline_class,
                                        bibrecord_to_pymarc, params_to_fields,
                                        add_marc_fields,
                                        assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_pub_info should return date-string fields
    matching the expected parameters.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('260', '264', '362')
    if len(fparams) and fparams[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], fparams[0][1], data[15:])
        fparams[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bundle = pipeline.do(bib, bibmarc, ['pub_info'])
    expected = {
        'publication_sort': exp_pub_sort or None,
        'publication_year_display': exp_pub_year_display or None,
        'publication_year_facet': exp_pub_year_facet or None,
        'publication_decade_facet': exp_pub_decade_facet or None,
        'publication_dates_search': exp_pub_dates_search or None
    }
    assert_bundle_matches_expected(bundle, expected, bundle_complete=False,
                                   list_order_exact=False)


@pytest.mark.parametrize('fparams, expected', [
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
def test_todscpipeline_getpubinfo_statements(fparams, expected,
                                             bl_sierra_test_record,
                                             todsc_pipeline_class,
                                             bibrecord_to_pymarc,
                                             params_to_fields, add_marc_fields,
                                             assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_pub_info should return display statement
    fields matching the expected parameters.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('260', '264', '362')
    if len(fparams) and fparams[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], fparams[0][1], data[15:])
        fparams[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bundle = pipeline.do(bib, bibmarc, ['pub_info'])
    disp_fields = set(['creation_display', 'publication_display',
                       'distribution_display', 'manufacture_display',
                       'copyright_display', 'publication_date_notes'])
    check_bundle = {k: v for k, v in bundle.items() if k in disp_fields}
    assert_bundle_matches_expected(check_bundle, expected)


@pytest.mark.parametrize('fparams, expected', [
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
      ('264', ['c', '2004.'], ' 4')], {}),
    ([('008', 's2004    '),], {}),
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
def test_todscpipeline_getpubinfo_pub_search(fparams, expected,
                                             bl_sierra_test_record,
                                             todsc_pipeline_class,
                                             bibrecord_to_pymarc,
                                             params_to_fields,
                                             add_marc_fields,
                                             assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_pub_info should return publishers_search
    and publication_places_search fields matching the expected
    parameters.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('260', '264')
    if len(fparams) and fparams[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], fparams[0][1], data[15:])
        fparams[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bundle = pipeline.do(bib, bibmarc, ['pub_info'])
    search_fields = ('publication_places_search', 'publishers_search')
    check_bundle = {k: v for k, v in bundle.items() if k in search_fields}
    assert_bundle_matches_expected(check_bundle, expected,
                                   list_order_exact=False)


@pytest.mark.parametrize('bib_locations, item_locations, sup_item_locations,'
                         'expected', [
    # czm / same bib and item location
    ( (('czm', 'Chilton Media Library'),),
      (('czm', 'Chilton Media Library'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None},
    ),

    # czm / bib loc exists, but no items
    ( (('czm', 'Chilton Media Library'),),
      tuple(),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None},
    ),

    # czm / all items are suppressed
    ( (('czm', 'Chilton Media Library'),),
      tuple(),
      (('lwww', 'UNT ONLINE RESOURCES'), ('w3', 'Willis Library-3rd Floor'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None},
    ),

    # czm / unknown bib location and one unknown item location
    ( (('blah', 'Blah'),),
      (('blah2', 'Blah2'), ('czm', 'Chilton Media Library'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None}
    ),

    # w3 / one suppressed item, one unsuppressed item, diff locs
    ( (('czm', 'Chilton Media Library'),),
      (('w3', 'Willis Library-3rd Floor'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['General Collection'],
       'building_facet': ['Willis Library'],
       'shelf_facet': ['Willis Library-3rd Floor']},
    ),

    # w3 / one suppressed item, one unsuppressed item, same locs
    ( (('czm', 'Chilton Media Library'),),
      (('w3', 'Willis Library-3rd Floor'),),
      (('w3', 'Willis Library-3rd Floor'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['General Collection'],
       'building_facet': ['Willis Library'],
       'shelf_facet': ['Willis Library-3rd Floor']},
    ),

    # all bib and item locations are unknown
    ( (('blah', 'Blah'),),
      (('blah2', 'Blah2'), ('blah', 'Blah'),),
      tuple(),
      {'access_facet': None,
       'collection_facet': None,
       'building_facet': None,
       'shelf_facet': None}
    ),

    # r, lwww / online-only item with bib location in different collection
    ( (('r', 'Discovery Park Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
      tuple(),
      {'access_facet': ['Online'],
       'collection_facet': ['General Collection'],
       'building_facet': None,
       'shelf_facet': None}
    ),

    # r, lwww / two different bib locations, no items
    ( (('r', 'Discovery Park Library'), ('lwww', 'UNT ONLINE RESOURCES')),
      tuple(),
      tuple(),
      {'access_facet': ['At the Library', 'Online'],
       'collection_facet': ['Discovery Park Library', 'General Collection'],
       'building_facet': ['Discovery Park Library'],
       'shelf_facet': None}
    ),

    # w, lwww / online-only item with bib location in same collection
    ( (('w', 'Willis Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
      tuple(),
      {'access_facet': ['Online'],
       'collection_facet': ['General Collection'],
       'building_facet': None,
       'shelf_facet': None}
    ),

    # x, xdoc / Remote Storage, bib loc is x
    ( (('x', 'Remote Storage'),),
      (('xdoc', 'Government Documents Remote Storage'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Government Documents'],
       'building_facet': ['Remote Storage'],
       'shelf_facet': None}
    ),

    # sd, xdoc / Remote Storage, bib loc is not x
    ( (('sd', 'Eagle Commons Library Government Documents'),),
      (('xdoc', 'Government Documents Remote Storage'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Government Documents'],
       'building_facet': ['Remote Storage'],
       'shelf_facet': None}
    ),

    # w, lwww, w3 / bib with online and physical locations
    ( (('w', 'Willis Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'), ('w3', 'Willis Library-3rd Floor'),),
      tuple(),
      {'access_facet': ['Online', 'At the Library'],
       'collection_facet': ['General Collection'],
       'building_facet': ['Willis Library'],
       'shelf_facet': ['Willis Library-3rd Floor']}
    ),

    # sd, gwww, sdus, rst, xdoc / multiple items at multiple locations
    ( (('sd', 'Eagle Commons Library Government Documents'),),
      (('gwww', 'GOVT ONLINE RESOURCES'),
       ('sdus', 'Eagle Commons Library US Documents'),
       ('rst', 'Discovery Park Library Storage'),
       ('xdoc', 'Government Documents Remote Storage'),),
      tuple(),
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
    'czm / all items are suppressed',
    'czm / unknown bib location and one unknown item location',
    'w3 / one suppressed item, one unsuppressed item, diff locs',
    'w3 / one suppressed item, one unsuppressed item, same locs',
    'all bib and item locations are unknown',
    'r, lwww / online-only item with bib location in different collection',
    'r, lwww / two different bib locations, no items',
    'w, lwww / online-only item with bib location in same collection',
    'x, xdoc / Remote Storage, bib loc is x',
    'sd, xdoc / Remote Storage, bib loc is not x',
    'w, lwww, w3 / bib with online and physical locations',
    'sd, gwww, sdus, rst, xdoc / multiple items at multiple locations',
])
def test_todscpipeline_getaccessinfo(bib_locations, item_locations,
                                     sup_item_locations, expected,
                                     bl_sierra_test_record,
                                     todsc_pipeline_class,
                                     update_test_bib_inst,
                                     get_or_make_location_instances,
                                     assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_access_info should return the expected
    access, collection, building, and shelf facet values based on the
    configured bib_ and item_locations.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')

    all_ilocs = list(set(item_locations) | set(sup_item_locations))
    all_blocs = list(set(bib_locations))
    bloc_info = [{'code': code, 'name': name} for code, name in all_blocs]
    iloc_info = [{'code': code, 'name': name} for code, name in all_ilocs]

    items_info = [{'location_id': code} for code, name in item_locations]
    items_info.extend([{'location_id': code, 'is_suppressed': True}
                       for code, name in sup_item_locations])

    bib_loc_instances = get_or_make_location_instances(bloc_info)
    item_loc_instances = get_or_make_location_instances(iloc_info)

    bib = update_test_bib_inst(bib, items=items_info,
                               locations=bib_loc_instances)
    bundle = pipeline.do(bib, None, ['access_info'])
    assert_bundle_matches_expected(bundle, expected, list_order_exact=False)


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
def test_todscpipeline_getresourcetypeinfo(bcode2,
                                           expected, bl_sierra_test_record,
                                           todsc_pipeline_class,
                                           setattr_model_instance,
                                           assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_resource_type_info should return the
    expected resource_type and resource_type_facet values based on the
    given bcode2. Note that this doesn't test resource type nor
    category (facet) determination. For that, see base.local_rulesets
    (and associated tests).
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    setattr_model_instance(bib, 'bcode2', bcode2)
    bundle = pipeline.do(bib, None, ['resource_type_info'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('f008_lang, raw_marcfields, expected', [
    # Edge cases -- empty / missing fields, etc.
    # No language info at all (no 008s, titles, or 041s)
    ('', [], {'languages': None, 'language_notes': None}),

    # 008 without valid cps 35-37
    ('   ', [], {'languages': None, 'language_notes': None}),

    # Main tests
    # Language info just from 008
    ('eng', [],
     {'languages': ['English'],
      'language_notes': [
        'Item content: English'
      ]}
    ),

    # Language info just from 041, example 1
    ('', ['041 1#$aeng$hger$hswe'],
     {'languages': ['English', 'German', 'Swedish'],
      'language_notes': [
        'Item content: English',
        'Translated from (original): German, Swedish'
      ]}
    ),

    # Language info just from 041, example 2
    ('', ['041 0#$aeng$afre$ager'],
     {'languages': ['English', 'French', 'German'],
      'language_notes': [
        'Item content: English, French, German',
      ]}
    ),

    # Language info just from 041, example 3
    ('', ['041 1#$ifre$jeng$jger'],
     {'languages': ['English', 'French', 'German'],
      'language_notes': [
        'Intertitles: French',
        'Subtitles: English, German'
      ]}
    ),

    # Language info just from 041, example 4 -- multiple 041s
    ('', ['041 0#$deng$eeng$efre$eger',
          '041 0#$geng',
          '041 1#$deng$hrus$eeng$nrus$geng$gfre$gger'],
     {'languages': ['English', 'French', 'German', 'Russian'],
      'language_notes': [
        'Item content: English',
        'Translated from (original): Russian',
        'Librettos: English, French, German',
        'Librettos translated from (original): Russian',
        'Accompanying materials: English, French, German'
      ]}
    ),

    # Ignore 041 if it uses something other than MARC relator codes
    ('', ['041 07$aen$afr$ait$2iso639-1'],
     {'languages': None, 'language_notes': None}),

    # Language info just from titles
    ('', ['130 0#$aBible.$pN.T.$pRomans.$lEnglish.$sRevised standard.',
          '730 02$aBible.$pO.T.$pJudges V.$lGerman$sGrether.'],
     {'languages': ['English', 'German'],
      'language_notes': [
        'Item content: English, German',
      ]}
    ),

    # Language info from related titles is not used
    ('', ['730 0#$aBible.$pO.T.$pJudges V.$lGerman$sGrether.'],
     {'languages': None, 'language_notes': None}),

    # If there are 546s, those lang notes override generated ones
    ('hun', ['041 0#$ahun$bfre$bger$brus',
             '546 ##$aIn Hungarian; summaries in French, German, or Russian.'],
     {'languages': ['Hungarian', 'French', 'German', 'Russian'],
      'language_notes': [
        'In Hungarian; summaries in French, German, or Russian.'
      ]}
    ),

    # Language info from combined sources
    ('eng', ['041 0#$deng$eeng$efre$eger',
             '041 0#$geng',
             '041 1#$deng$hrus$eeng$nrus$geng$gfre$gger',
             '130 0#$aBible.$pN.T.$pRomans.$lEnglish.$sRevised standard.',
             '730 02$aSome title.$lKlingon.'],
     {'languages': ['English', 'French', 'German', 'Russian', 'Klingon'],
      'language_notes': [
        'Item content: English, Klingon',
        'Translated from (original): Russian',
        'Librettos: English, French, German',
        'Librettos translated from (original): Russian',
        'Accompanying materials: English, French, German'
      ]}
    ),
], ids=[
    # Edge cases
    'No language info at all (no 008s, titles, or 041s)',
    '008 without valid cps 35-37',

    # Main tests
    'Language info just from 008',
    'Language info just from 041, example 1',
    'Language info just from 041, example 2',
    'Language info just from 041, example 3',
    'Language info just from 041, example 4  -- multiple 041s',
    'Ignore 041 if it uses something other than MARC relator codes',
    'Language info just from titles',
    'Language info from related titles is not used',
    'If there are 546s, those lang notes override generated ones',
    'Language info from combined sources',
])
def test_todscpipeline_getlanguageinfo(f008_lang, raw_marcfields, expected,
                                       fieldstrings_to_fields,
                                       bl_sierra_test_record,
                                       todsc_pipeline_class,
                                       marcutils_for_subjects,
                                       bibrecord_to_pymarc,
                                       add_marc_fields,
                                       assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_language_info should return fields
    matching the expected parameters.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    if f008_lang:
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:35], f008_lang, data[38:])
        raw_marcfields = [('008 {}'.format(data))] + raw_marcfields
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc.remove_fields('008', '041', '130', '240', '546', '700', '710',
                          '711', '730', '740')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    fields_to_process = ['title_info', 'general_5xx_info', 'language_info']
    bundle = pipeline.do(bib, bibmarc, fields_to_process)
    assert_bundle_matches_expected(bundle, expected, bundle_complete=False,
                                   list_order_exact=False)


@pytest.mark.parametrize('fparams, expected', [
    ([('600', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 ')], {}),
    ([('100', [], '1 ')], {}),
    ([('110', [], '  ')], {}),
    ([('111', [], '  ')], {}),
    ([('100', ['e', 'something'], '1 ')], {}),
    ([('110', ['e', 'something'], '  ')], {}),
    ([('111', ['j', 'something'], '  ')], {}),
    ([('100', ['a', 'Name', '0', 'http://example.com/12345'], '0 ')],
     {'author_search': ['Name', 'Name', 'Name', 'Name'],
      'contributors_search': ['Name', 'Name', 'Name', 'Name'],
      'author_contributor_facet': ['name!Name'],
      'author_sort': 'name',
      'author_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('110', ['a', 'Name', '0', 'http://example.com/12345'], '0 ')],
     {'author_search': ['Name'],
      'contributors_search': ['Name'],
      'author_contributor_facet': ['name!Name'],
      'author_sort': 'name',
      'author_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('111', ['a', 'Name', '0', 'http://example.com/12345'], '0 ')],
     {'meetings_search': ['Name'],
      'meeting_facet': ['name!Name'],
      'meetings_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('100', ['a', 'Name', '5', 'TxDN'], '0 ')],
     {'author_search': ['Name', 'Name', 'Name', 'Name'],
      'contributors_search': ['Name', 'Name', 'Name', 'Name'],
      'author_contributor_facet': ['name!Name'],
      'author_sort': 'name',
      'author_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('110', ['a', 'Name', '5', 'TxDN'], '0 ')],
     {'author_search': ['Name'],
      'contributors_search': ['Name'],
      'author_contributor_facet': ['name!Name'],
      'author_sort': 'name',
      'author_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('111', ['a', 'Name', '5', 'TxDN'], '0 ')],
     {'meetings_search': ['Name'],
      'meeting_facet': ['name!Name'],
      'meetings_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('100', ['6', '880-04', 'a', 'Name'], '0 ')],
     {'author_search': ['Name', 'Name', 'Name', 'Name'],
      'contributors_search': ['Name', 'Name', 'Name', 'Name'],
      'author_contributor_facet': ['name!Name'],
      'author_sort': 'name',
      'author_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('110', ['6', '880-04', 'a', 'Name'], '0 ')],
     {'author_search': ['Name'],
      'contributors_search': ['Name'],
      'author_contributor_facet': ['name!Name'],
      'author_sort': 'name',
      'author_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('111', ['6', '880-04', 'a', 'Name'], '0 ')],
     {'meetings_search': ['Name'],
      'meeting_facet': ['name!Name'],
      'meetings_json': {'p': [{'d': 'Name', 'v': 'name!Name'}]}
     }),
    ([('100', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 '),
    ('700', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.',
             't', 'Title of some related work.'], '1 ')],
     {'author_contributor_facet': ['churchill-winston-sir-1874-1965!Churchill, '
                                   'Winston, Sir, 1874-1965'],
      'author_json': {'p': [{'d': 'Churchill, Winston, Sir, 1874-1965',
                             'v': 'churchill-winston-sir-1874-1965!Churchill, '
                                  'Winston, Sir, 1874-1965'}]},
      'author_search': ['Churchill, Winston, Sir, 1874-1965',
                        'Churchill, Winston Churchill, W Churchill',
                        'Sir Winston Churchill', 'Sir Winston Churchill'],
      'contributors_search': ['Churchill, Winston, Sir, 1874-1965',
                              'Churchill, Winston Churchill, W Churchill',
                              'Sir Winston Churchill', 'Sir Winston Churchill'],
      'author_sort': 'churchill-winston-sir-1874-1965'}),
    ([('700', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.',
               't', 'Title of some related work.'], '1 '),
      ('700', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.',
               't', 'Another related work.'], '1 ')],
     {'author_contributor_facet': ['churchill-winston-sir-1874-1965!Churchill, '
                                   'Winston, Sir, 1874-1965'],
      'author_sort': 'churchill-winston-sir-1874-1965',
      'contributors_json': [{'p': [{'d': 'Churchill, Winston, Sir, 1874-1965',
                                    'v': 'churchill-winston-sir-1874-1965!'
                                         'Churchill, Winston, Sir, '
                                         '1874-1965'}]}],
      'contributors_search': ['Churchill, Winston, Sir, 1874-1965',
                              'Churchill, Winston Churchill, W Churchill',
                              'Sir Winston Churchill',
                              'Sir Winston Churchill']}),
    ([('100', ['a', 'Churchill, Winston,', 'c', 'Sir,', 'd', '1874-1965.'],
       '1 ')],
     {'author_contributor_facet': ['churchill-winston-sir-1874-1965!Churchill, '
                                   'Winston, Sir, 1874-1965'],
      'author_json': {'p': [{'d': 'Churchill, Winston, Sir, 1874-1965',
                             'v': 'churchill-winston-sir-1874-1965!Churchill, '
                                  'Winston, Sir, 1874-1965'}]},
      'author_search': ['Churchill, Winston, Sir, 1874-1965',
                        'Churchill, Winston Churchill, W Churchill',
                        'Sir Winston Churchill', 'Sir Winston Churchill'],
      'contributors_search': ['Churchill, Winston, Sir, 1874-1965',
                              'Churchill, Winston Churchill, W Churchill',
                              'Sir Winston Churchill', 'Sir Winston Churchill'],
      'author_sort': 'churchill-winston-sir-1874-1965'}),
    ([('100', ['a', 'Thomas,', 'c', 'Aquinas, Saint,', 'd', '1225?-1274.'], '0 ')],
     {'author_contributor_facet': ['thomas-aquinas-saint-1225-1274!Thomas, '
                                   'Aquinas, Saint, 1225?-1274'],
      'author_json': {'p': [{'d': 'Thomas, Aquinas, Saint, 1225?-1274',
                             'v': 'thomas-aquinas-saint-1225-1274!Thomas, '
                                  'Aquinas, Saint, 1225?-1274'}]},
      'author_search': ['Thomas, Aquinas, Saint, 1225?-1274',
                        'Thomas', 'Saint Thomas Aquinas',
                        'Saint Thomas, Aquinas'],
      'contributors_search': ['Thomas, Aquinas, Saint, 1225?-1274',
                              'Thomas', 'Saint Thomas Aquinas',
                              'Saint Thomas, Aquinas'],
      'author_sort': 'thomas-aquinas-saint-1225-1274'}),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964,', 'e', 'writing,',
               'e', 'direction,', 'e', 'production.'], '1 ')],
     {'author_contributor_facet': ['hecht-ben-1893-1964!Hecht, Ben, 1893-1964'],
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964',
                             'v': 'hecht-ben-1893-1964!Hecht, Ben, '
                                  '1893-1964'}],
                      'r': ['writing', 'direction', 'production']},
      'author_search': ['Hecht, Ben, 1893-1964', 'Hecht, Ben Hecht, B Hecht',
                        'Ben Hecht', 'Ben Hecht'],
      'contributors_search': ['Hecht, Ben, 1893-1964',
                              'Hecht, Ben Hecht, B Hecht', 'Ben Hecht',
                              'Ben Hecht'],
      'author_sort': 'hecht-ben-1893-1964',
      'responsibility_search': ['Ben Hecht writing', 'Ben Hecht direction',
                                'Ben Hecht production']}),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964,',
               'e', 'writing, direction, production.'], '1 ')],
     {'author_contributor_facet': ['hecht-ben-1893-1964!Hecht, Ben, 1893-1964'],
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964',
                             'v': 'hecht-ben-1893-1964!Hecht, Ben, '
                                  '1893-1964'}],
                      'r': ['writing', 'direction', 'production']},
      'author_search': ['Hecht, Ben, 1893-1964', 'Hecht, Ben Hecht, B Hecht',
                        'Ben Hecht', 'Ben Hecht'],
      'contributors_search': ['Hecht, Ben, 1893-1964',
                              'Hecht, Ben Hecht, B Hecht', 'Ben Hecht',
                              'Ben Hecht'],
      'author_sort': 'hecht-ben-1893-1964',
      'responsibility_search': ['Ben Hecht writing', 'Ben Hecht direction',
                                'Ben Hecht production']}),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964.', '4', 'drt', '4', 'pro'],
       '1 ')],
     {'author_contributor_facet': ['hecht-ben-1893-1964!Hecht, Ben, 1893-1964'],
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964',
                             'v': 'hecht-ben-1893-1964!Hecht, Ben, '
                                  '1893-1964'}],
                      'r': ['director', 'producer']},
      'author_search': ['Hecht, Ben, 1893-1964', 'Hecht, Ben Hecht, B Hecht',
                        'Ben Hecht', 'Ben Hecht'],
      'contributors_search': ['Hecht, Ben, 1893-1964',
                              'Hecht, Ben Hecht, B Hecht', 'Ben Hecht',
                              'Ben Hecht'],
      'author_sort': 'hecht-ben-1893-1964',
      'responsibility_search': ['Ben Hecht director', 'Ben Hecht producer']}),
    ([('100', ['a', 'Hecht, Ben,', 'd', '1893-1964,', 'e', 'writer,',
               'e', 'director.', '4', 'drt', '4', 'pro'], '1 ')],
     {'author_contributor_facet': ['hecht-ben-1893-1964!Hecht, Ben, 1893-1964'],
      'author_json': {'p': [{'d': 'Hecht, Ben, 1893-1964',
                             'v': 'hecht-ben-1893-1964!Hecht, Ben, '
                                  '1893-1964'}],
                      'r': ['writer', 'director', 'producer']},
      'author_search': ['Hecht, Ben, 1893-1964', 'Hecht, Ben Hecht, B Hecht',
                        'Ben Hecht', 'Ben Hecht'],
      'contributors_search': ['Hecht, Ben, 1893-1964',
                              'Hecht, Ben Hecht, B Hecht', 'Ben Hecht',
                              'Ben Hecht'],
      'author_sort': 'hecht-ben-1893-1964',
      'responsibility_search': ['Ben Hecht writer', 'Ben Hecht director',
                                'Ben Hecht producer']}),
    ([('700', ['i', 'Container of (work):',
               '4', 'http://rdaregistry.info/Elements/w/P10147',
               'a', 'Dicks, Terrance.',
               't', 'Doctor Who and the Dalek invasion of Earth.'], '12')],
     {'author_contributor_facet': ['dicks-terrance!Dicks, Terrance'],
      'author_sort': 'dicks-terrance',
      'contributors_json': [{'p': [{'d': 'Dicks, Terrance',
                                    'v': 'dicks-terrance!Dicks, Terrance'}]}],
      'contributors_search': ['Dicks, Terrance',
                              'Dicks, Terrance Dicks, T Dicks',
                              'Terrance Dicks', 'Terrance Dicks']}),
    ([('710', ['a', 'Some Organization,', 't', 'Some Work Title.'], '22')],
     {'author_contributor_facet': ['some-organization!Some Organization'],
      'author_sort': 'some-organization',
      'contributors_json': [{'p': [{'d': 'Some Organization',
                                    'v': 'some-organization!Some '
                                         'Organization'}]}],
      'contributors_search': ['Some Organization']}),
    ([('711', ['a', 'Some Festival.'], '2 ')],
     {'meeting_facet': ['some-festival!Some Festival'],
      'meetings_json': [{'p': [{'d': 'Some Festival',
                                'v': 'some-festival!Some Festival'}]}],
      'meetings_search': ['Some Festival']}),
    ([('711', ['a', 'Some Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_contributor_facet': ['some-festival-orchestra!Some Festival, '
                                   'Orchestra'],
      'author_sort': 'some-festival-orchestra',
      'contributors_json': [{'p': [{'d': 'Some Festival, Orchestra',
                                    'v': 'some-festival-orchestra!Some '
                                         'Festival, Orchestra'}]}],
      'contributors_search': ['Some Festival, Orchestra'],
      'meeting_facet': ['some-festival!Some Festival'],
      'meetings_json': [{'p': [{'d': 'Some Festival',
                                'v': 'some-festival!Some Festival'}]}],
      'meetings_search': ['Some Festival']}),
    ([('111', ['a', 'White House Conference on Lib and Info Services',
               'd', '(1979 :', 'c', 'Washington, D.C.).',
               'e', 'Ohio Delegation.'], '2 ')],
     {'author_contributor_facet': ['white-house-conference-on-lib-and-info-'
                                   'services-ohio-delegation!White House '
                                   'Conference on Lib and Info Services, Ohio '
                                   'Delegation'],
      'author_json': {'p': [{'d': 'White House Conference on Lib and Info '
                                  'Services, Ohio Delegation',
                             'v': 'white-house-conference-on-lib-and-info-'
                                  'services-ohio-delegation!White House '
                                  'Conference on Lib and Info Services, Ohio '
                                  'Delegation'}]},
      'author_search': ['White House Conference on Lib and Info Services, Ohio '
                        'Delegation'],
      'contributors_search': ['White House Conference on Lib and Info '
                              'Services, Ohio Delegation'],
      'author_sort': 'white-house-conference-on-lib-and-info-services-ohio-'
                     'delegation',
      'meeting_facet': ['white-house-conference-on-lib-and-info-services!White '
                        'House Conference on Lib and Info Services',
                        'white-house-conference-on-lib-and-info-services-1979-'
                        'washington-d-c!White House Conference on Lib and Info '
                        'Services (1979 : Washington, D.C.)'],
      'meetings_json': [{'p': [{'d': 'White House Conference on Lib and Info '
                                     'Services',
                                'v': 'white-house-conference-on-lib-and-info-'
                                     'services!White House Conference on Lib '
                                     'and Info Services'},
                               {'d': '(1979 : Washington, D.C.)',
                                'v': 'white-house-conference-on-lib-and-info-'
                                     'services-1979-washington-d-c!White House '
                                     'Conference on Lib and Info Services '
                                     '(1979 : Washington, D.C.)'}]}],
      'meetings_search': ['White House Conference on Lib and Info Services '
                          '(1979 : Washington, D.C.)']}),
    ([('711', ['a', 'Olympic Games', 'n', '(21st :', 'd', '1976 :',
               'c', 'Montréal, Québec).', 'e', 'Organizing Committee.',
               'e', 'Arts and Culture Program.', 'e', 'Visual Arts Section.'],
               '2 ')],
     {'author_contributor_facet': ['olympic-games-organizing-committee!Olympic '
                                   'Games, Organizing Committee',
                                   'olympic-games-organizing-committee-arts-'
                                   'and-culture-program!Olympic Games, '
                                   'Organizing Committee > Arts and Culture '
                                   'Program',
                                   'olympic-games-organizing-committee-arts-'
                                   'and-culture-program-visual-arts-section!'
                                   'Olympic Games, Organizing Committee > Arts '
                                   'and Culture Program > Visual Arts Section'],
      'author_sort': 'olympic-games-organizing-committee-arts-and-culture-'
                     'program-visual-arts-section',
      'contributors_json': [{'p': [{'d': 'Olympic Games, Organizing Committee',
                                    'v': 'olympic-games-organizing-committee!'
                                         'Olympic Games, Organizing Committee',
                                    's': ' > '},
                                   {'d': 'Arts and Culture Program',
                                    'v': 'olympic-games-organizing-committee-'
                                         'arts-and-culture-program!Olympic '
                                         'Games, Organizing Committee > Arts '
                                         'and Culture Program',
                                    's': ' > '},
                                   {'d': 'Visual Arts Section',
                                    'v': 'olympic-games-organizing-committee-'
                                         'arts-and-culture-program-visual-arts-'
                                         'section!Olympic Games, Organizing '
                                         'Committee > Arts and Culture Program '
                                         '> Visual Arts Section'}]}],
      'contributors_search': ['Olympic Games, Organizing Committee > Arts and '
                              'Culture Program > Visual Arts Section'],
      'meeting_facet': ['olympic-games!Olympic Games',
                        'olympic-games-21st-1976-montreal-quebec!Olympic Games '
                        '(21st : 1976 : Montréal, Québec)'],
      'meetings_json': [{'p': [{'d': 'Olympic Games',
                                'v': 'olympic-games!Olympic Games'},
                               {'d': '(21st : 1976 : Montréal, Québec)',
                                'v': 'olympic-games-21st-1976-montreal-quebec!'
                                     'Olympic Games (21st : 1976 : Montréal, '
                                     'Québec)'}]}],
      'meetings_search': ['Olympic Games (21st : 1976 : Montréal, Québec)']}),
    ([('111', ['a', 'International Congress of Gerontology.',
               'e', 'Satellite Conference', 'd', '(1978 :',
               'c', 'Sydney, N.S.W.)', 'e', 'Organizing Committee.'], '2 ')],
     {'author_contributor_facet': ['international-congress-of-gerontology-'
                                   'satellite-conference-organizing-committee!'
                                   'International Congress of Gerontology '
                                   'Satellite Conference, Organizing '
                                   'Committee'],
      'author_json': {'p': [{'d': 'International Congress of Gerontology '
                                  'Satellite Conference, Organizing Committee',
                             'v': 'international-congress-of-gerontology-'
                                  'satellite-conference-organizing-committee!'
                                  'International Congress of Gerontology '
                                  'Satellite Conference, Organizing '
                                  'Committee'}]},
      'author_search': ['International Congress of Gerontology Satellite '
                        'Conference, Organizing Committee'],
      'contributors_search': ['International Congress of Gerontology Satellite '
                              'Conference, Organizing Committee'],
      'author_sort': 'international-congress-of-gerontology-satellite-'
                     'conference-organizing-committee',
      'meeting_facet': ['international-congress-of-gerontology!International '
                        'Congress of Gerontology',
                        'international-congress-of-gerontology-satellite-'
                        'conference!International Congress of Gerontology > '
                        'Satellite Conference',
                        'international-congress-of-gerontology-satellite-'
                        'conference-1978-sydney-n-s-w!International Congress '
                        'of Gerontology > Satellite Conference (1978 : Sydney, '
                        'N.S.W.)'],
      'meetings_json': [{'p': [{'d': 'International Congress of Gerontology',
                                'v': 'international-congress-of-gerontology!'
                                     'International Congress of Gerontology',
                                's': ' > '},
                               {'d': 'Satellite Conference',
                                'v': 'international-congress-of-gerontology-'
                                     'satellite-conference!International '
                                     'Congress of Gerontology > Satellite '
                                     'Conference'},
                               {'d': '(1978 : Sydney, N.S.W.)',
                                'v': 'international-congress-of-gerontology-'
                                     'satellite-conference-1978-sydney-n-s-w!'
                                     'International Congress of Gerontology > '
                                     'Satellite Conference (1978 : Sydney, '
                                     'N.S.W.)'}]}],
      'meetings_search': ['International Congress of Gerontology > Satellite '
                          'Conference (1978 : Sydney, N.S.W.)']}),
    ([('110', ['a', 'Democratic Party (Tex.).', 'b', 'State Convention',
               'd', '(1857 :', 'c', 'Waco, Tex.).', 'b', 'Houston Delegation.'],
       '2 ')],
     {'author_contributor_facet': ['democratic-party-tex!Democratic Party '
                                   '(Tex.)',
                                   'democratic-party-tex-state-convention-'
                                   'houston-delegation!Democratic Party (Tex.) '
                                   '> State Convention, Houston Delegation'],
      'author_json': {'p': [{'d': 'Democratic Party (Tex.)',
                             'v': 'democratic-party-tex!Democratic Party '
                                  '(Tex.)',
                             's': ' > '},
                            {'d': 'State Convention, Houston Delegation',
                             'v': 'democratic-party-tex-state-convention-'
                                   'houston-delegation!Democratic Party (Tex.) '
                                   '> State Convention, Houston Delegation'}]},
      'author_search': ['Democratic Party (Tex.) > State Convention, Houston '
                        'Delegation'],
      'contributors_search': ['Democratic Party (Tex.) > State Convention, '
                              'Houston Delegation'],
      'author_sort': 'democratic-party-tex-state-convention-houston-delegation',
      'meeting_facet': ['democratic-party-tex-state-convention!Democratic '
                        'Party (Tex.), State Convention',
                        'democratic-party-tex-state-convention-1857-waco-tex!'
                        'Democratic Party (Tex.), State Convention (1857 : '
                        'Waco, Tex.)'],
      'meetings_json': [{'p': [{'d': 'Democratic Party (Tex.), State '
                                     'Convention',
                                'v': 'democratic-party-tex-state-convention!'
                                     'Democratic Party (Tex.), State '
                                     'Convention'},
                               {'d': '(1857 : Waco, Tex.)',
                                'v': 'democratic-party-tex-state-convention-'
                                     '1857-waco-tex!'
                                     'Democratic Party (Tex.), State '
                                     'Convention (1857 : Waco, Tex.)'}]}],
      'meetings_search': ['Democratic Party (Tex.), State Convention (1857 : '
                          'Waco, Tex.)']}),
    ([('110', ['a', 'Democratic Party (Tex.).', 'b', 'State Convention',
               'd', '(1857 :', 'c', 'Waco, Tex.).'], '2 ')],
     {'author_contributor_facet': ['democratic-party-tex!Democratic Party '
                                   '(Tex.)'],
      'author_json': {'p': [{'d': 'Democratic Party (Tex.)',
                             'v': 'democratic-party-tex!Democratic Party '
                                  '(Tex.)'}]},
      'author_search': ['Democratic Party (Tex.)'],
      'contributors_search': ['Democratic Party (Tex.)'],
      'author_sort': 'democratic-party-tex',
      'meeting_facet': ['democratic-party-tex-state-convention!Democratic '
                        'Party (Tex.), State Convention',
                        'democratic-party-tex-state-convention-1857-waco-tex!'
                        'Democratic Party (Tex.), State Convention (1857 : '
                        'Waco, Tex.)'],
      'meetings_json': [{'p': [{'d': 'Democratic Party (Tex.), State '
                                     'Convention',
                                'v': 'democratic-party-tex-state-convention!'
                                     'Democratic Party (Tex.), State '
                                     'Convention'},
                               {'d': '(1857 : Waco, Tex.)',
                                'v': 'democratic-party-tex-state-convention-'
                                     '1857-waco-tex!'
                                     'Democratic Party (Tex.), State '
                                     'Convention (1857 : Waco, Tex.)'}]}],
      'meetings_search': ['Democratic Party (Tex.), State Convention (1857 : '
                          'Waco, Tex.)']}),
    ([('110', ['a', 'United States.', 'b', 'Congress',
               'n', '(97th, 2nd session :', 'd', '1982).', 'b', 'House.'],
       '1 ')],
     {'author_contributor_facet': ['united-states-congress!United States '
                                   'Congress', 'united-states-congress-house!'
                                   'United States Congress > House'],
      'author_json': {'p': [{'d': 'United States Congress',
                             'v': 'united-states-congress!United States '
                                  'Congress',
                             's': ' > '},
                            {'d': 'House',
                             'v': 'united-states-congress-house!'
                                  'United States Congress > House'}]},
      'author_search': ['United States Congress > House'],
      'contributors_search': ['United States Congress > House'],
      'author_sort': 'united-states-congress-house',
      'meeting_facet': ['united-states-congress!United States Congress',
                        'united-states-congress-97th-2nd-session-1982!United '
                        'States Congress (97th, 2nd session : 1982)'],
      'meetings_json': [{'p': [{'d': 'United States Congress',
                                'v': 'united-states-congress!United States '
                                     'Congress'},
                               {'d': '(97th, 2nd session : 1982)',
                                'v': 'united-states-congress-97th-2nd-session-'
                                     '1982!United States Congress (97th, 2nd '
                                     'session : 1982)'}]}],
      'meetings_search': ['United States Congress (97th, 2nd session : '
                          '1982)']}),
    ([('111', ['a', 'Paris.', 'q', 'Peace Conference,', 'd', '1919.'], '1 ')],
     {'meeting_facet': ['paris-peace-conference!Paris Peace Conference',
                        'paris-peace-conference-1919!Paris Peace Conference, '
                        '1919'],
      'meetings_json': [{'p': [{'d': 'Paris Peace Conference',
                                'v': 'paris-peace-conference!Paris Peace '
                                     'Conference',
                                's': ', '},
                               {'d': '1919',
                                'v': 'paris-peace-conference-1919!Paris Peace '
                                     'Conference, 1919'}]}],
      'meetings_search': ['Paris Peace Conference, 1919']}),
    ([('710', ['i', 'Container of (work):', 'a', 'Some Organization,',
               'e', 'author.', 't', 'Some Work Title.'], '22')],
     {'author_contributor_facet': ['some-organization!Some Organization'],
      'author_sort': 'some-organization',
      'contributors_json': [{'p': [{'d': 'Some Organization',
                                    'v': 'some-organization!Some '
                                         'Organization'}],
                             'r': ['author']}],
      'contributors_search': ['Some Organization'],
      'responsibility_search': ['Some Organization author']}),
    ([('711', ['a', 'Some Festival.', 'e', 'Orchestra,',
               'j', 'instrumentalist.'], '2 ')],
     {'author_contributor_facet': ['some-festival-orchestra!Some Festival, '
                                   'Orchestra'],
      'author_sort': 'some-festival-orchestra',
      'contributors_json': [{'p': [{'d': 'Some Festival, Orchestra',
                                    'v': 'some-festival-orchestra!Some '
                                         'Festival, Orchestra'}],
                             'r': ['instrumentalist']}],
      'contributors_search': ['Some Festival, Orchestra'],
      'meeting_facet': ['some-festival!Some Festival'],
      'meetings_json': [{'p': [{'d': 'Some Festival',
                                'v': 'some-festival!Some Festival'}]}],
      'meetings_search': ['Some Festival'],
      'responsibility_search': ['Some Festival, Orchestra instrumentalist']}),
    ([('711', ['a', 'Some Conference', 'c', '(Rome),',
               'j', 'jointly held conference.'], '2 ')],
     {'meeting_facet': ['some-conference!Some Conference',
                        'some-conference-rome!Some Conference (Rome)'],
      'meetings_json': [{'p': [{'d': 'Some Conference',
                                'v': 'some-conference!Some Conference'},
                               {'d': '(Rome)',
                                'v': 'some-conference-rome!Some Conference '
                                     '(Rome)'}],
                         'r': ['jointly held conference']}],
      'meetings_search': ['Some Conference (Rome)'],
      'responsibility_search': ['Some Conference jointly held conference']}),
    ([('800', ['a', 'Berenholtz, Jim,', 'd', '1957-',
               't', 'Teachings of the feathered serpent ;', 'v', 'bk. 1.'],
       '1 ')],
     {'author_contributor_facet': ['berenholtz-jim-1957!Berenholtz, Jim, '
                                   '1957-'],
      'contributors_json': [{'p': [{'d': 'Berenholtz, Jim, 1957-',
                                    'v': 'berenholtz-jim-1957!Berenholtz, Jim, '
                                         '1957-'}]}],
      'contributors_search': ['Berenholtz, Jim, 1957-',
                              'Berenholtz, Jim Berenholtz, J Berenholtz',
                              'Jim Berenholtz', 'Jim Berenholtz']}),
    ([('810', ['a', 'United States.', 'b', 'Army Map Service.',
               't', 'Special Africa series,', 'v', 'no. 12.'], '1 ')],
     {'author_contributor_facet': ['united-states-army-map-service!United '
                                   'States Army Map Service'],
      'contributors_json': [{'p': [{'d': 'United States Army Map Service',
                                    'v': 'united-states-army-map-service!'
                                         'United States Army Map Service'}]}],
      'contributors_search': ['United States Army Map Service']}),
    ([('811', ['a', 'International Congress of Nutrition',
               'n', '(11th :', 'd', '1978 :', 'c', 'Rio de Janeiro, Brazil).',
               't', 'Nutrition and food science ;', 'v', 'v. 1.'], '2 ')],
     {'meeting_facet': ['international-congress-of-nutrition!International '
                        'Congress of Nutrition',
                        'international-congress-of-nutrition-11th-1978-rio-de-'
                        'janeiro-brazil!International Congress of Nutrition '
                        '(11th : 1978 : Rio de Janeiro, Brazil)'],
      'meetings_json': [{'p': [{'d': 'International Congress of Nutrition',
                                'v': 'international-congress-of-nutrition!'
                                     'International Congress of Nutrition'},
                               {'d': '(11th : 1978 : Rio de Janeiro, Brazil)',
                                'v': 'international-congress-of-nutrition-11th-'
                                     '1978-rio-de-janeiro-brazil!International '
                                     'Congress of Nutrition (11th : 1978 : Rio '
                                     'de Janeiro, Brazil)'}]}],
      'meetings_search': ['International Congress of Nutrition (11th : 1978 : '
                          'Rio de Janeiro, Brazil)']}),
    ([('100', ['a', 'Author, Main,', 'd', '1910-1990.'], '1 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_contributor_facet': ['author-main-1910-1990!Author, Main, '
                                   '1910-1990',
                                   'author-second-1920-1999!Author, Second, '
                                   '1920-1999',
                                   'org-contributor!Org Contributor',
                                   'festival-orchestra!Festival, Orchestra'],
      'author_json': {'p': [{'d': 'Author, Main, 1910-1990',
                             'v': 'author-main-1910-1990!Author, Main, '
                                  '1910-1990'}]},
      'author_search': ['Author, Main, 1910-1990',
                        'Author, Main Author, M Author', 'Main Author',
                        'Main Author'],
      'author_sort': 'author-main-1910-1990',
      'contributors_json': [{'p': [{'d': 'Author, Second, 1920-1999',
                                    'v': 'author-second-1920-1999!Author, '
                                         'Second, 1920-1999'}]},
                            {'p': [{'d': 'Org Contributor',
                                    'v': 'org-contributor!Org Contributor'}]},
                            {'p': [{'d': 'Festival, Orchestra',
                                    'v': 'festival-orchestra!Festival, '
                                         'Orchestra'}]}],
      'contributors_search': ['Author, Main, 1910-1990',
                              'Author, Main Author, M Author', 'Main Author',
                              'Main Author', 'Author, Second, 1920-1999',
                              'Author, Second Author, S Author',
                              'Second Author', 'Second Author',
                              'Org Contributor', 'Festival, Orchestra'],
      'meeting_facet': ['festival!Festival'],
      'meetings_json': [{'p': [{'d': 'Festival', 'v': 'festival!Festival'}]}],
      'meetings_search': ['Festival']}),
    ([('110', ['a', 'Some Organization'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_contributor_facet': ['some-organization!Some Organization',
                                   'author-second-1920-1999!Author, Second, '
                                   '1920-1999',
                                   'org-contributor!Org Contributor',
                                   'festival-orchestra!Festival, Orchestra'],
      'author_json': {'p': [{'d': 'Some Organization',
                             'v': 'some-organization!Some Organization'}]},
      'author_search': ['Some Organization'],
      'author_sort': 'some-organization',
      'contributors_json': [{'p': [{'d': 'Author, Second, 1920-1999',
                                    'v': 'author-second-1920-1999!Author, '
                                         'Second, 1920-1999'}]},
                            {'p': [{'d': 'Org Contributor',
                                    'v': 'org-contributor!Org Contributor'}]},
                            {'p': [{'d': 'Festival, Orchestra',
                                    'v': 'festival-orchestra!Festival, '
                                         'Orchestra'}]}],
      'contributors_search': ['Some Organization', 'Author, Second, 1920-1999',
                              'Author, Second Author, S Author',
                              'Second Author', 'Second Author',
                              'Org Contributor', 'Festival, Orchestra'],
      'meeting_facet': ['festival!Festival'],
      'meetings_json': [{'p': [{'d': 'Festival',
                                'v': 'festival!Festival'}]}],
      'meetings_search': ['Festival']}),
    ([('110', ['a', 'Some Org.', 'b', 'Meeting', 'd', '(1999).'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_contributor_facet': ['some-org!Some Org',
                                   'author-second-1920-1999!Author, Second, '
                                   '1920-1999',
                                   'org-contributor!Org Contributor',
                                   'festival-orchestra!Festival, Orchestra'],
      'author_json': {'p': [{'d': 'Some Org',
                             'v': 'some-org!Some Org'}]},
      'author_search': ['Some Org'],
      'author_sort': 'some-org',
      'contributors_json': [{'p': [{'d': 'Author, Second, 1920-1999',
                                    'v': 'author-second-1920-1999!Author, '
                                         'Second, 1920-1999'}]},
                            {'p': [{'d': 'Org Contributor',
                                    'v': 'org-contributor!Org Contributor'}]},
                            {'p': [{'d': 'Festival, Orchestra',
                                    'v': 'festival-orchestra!Festival, '
                                         'Orchestra'}]}],
      'contributors_search': ['Some Org', 'Author, Second, 1920-1999',
                              'Author, Second Author, S Author',
                              'Second Author', 'Second Author',
                              'Org Contributor', 'Festival, Orchestra'],
      'meeting_facet': ['some-org-meeting!Some Org, Meeting',
                        'some-org-meeting-1999!Some Org, Meeting (1999)',
                        'festival!Festival'],
      'meetings_json': [{'p': [{'d': 'Some Org, Meeting',
                                'v': 'some-org-meeting!Some Org, Meeting'},
                               {'d': '(1999)',
                                'v': 'some-org-meeting-1999!Some Org, Meeting '
                                     '(1999)',}]},
                        {'p': [{'d': 'Festival',
                                'v': 'festival!Festival'}]}],
      'meetings_search': ['Some Org, Meeting (1999)', 'Festival']}),
    ([('111', ['a', 'Meeting', 'd', '(1999).'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_contributor_facet': ['author-second-1920-1999!Author, Second, '
                                   '1920-1999', 'org-contributor!Org '
                                   'Contributor',
                                   'festival-orchestra!Festival, Orchestra'],
      'author_sort': 'author-second-1920-1999',
      'contributors_json': [{'p': [{'d': 'Author, Second, 1920-1999',
                                    'v': 'author-second-1920-1999!Author, '
                                         'Second, 1920-1999'}]},
                            {'p': [{'d': 'Org Contributor',
                                    'v': 'org-contributor!Org Contributor'}]},
                            {'p': [{'d': 'Festival, Orchestra',
                                    'v': 'festival-orchestra!Festival, '
                                         'Orchestra'}]}],
      'contributors_search': ['Author, Second, 1920-1999',
                              'Author, Second Author, S Author',
                              'Second Author', 'Second Author',
                              'Org Contributor', 'Festival, Orchestra'],
      'meeting_facet': ['meeting!Meeting', 'meeting-1999!Meeting (1999)',
                       'festival!Festival'],
      'meetings_json': [{'p': [{'d': 'Meeting',
                                'v': 'meeting!Meeting'},
                               {'d': '(1999)',
                                'v': 'meeting-1999!Meeting (1999)'}]},
                        {'p': [{'d': 'Festival',
                                'v': 'festival!Festival'}]}],
      'meetings_search': ['Meeting (1999)', 'Festival']}),
    ([('111', ['a', 'Conference.', 'e', 'Subcommittee.'], '2 '),
      ('700', ['a', 'Author, Second,', 'd', '1920-1999.'], '1 '),
      ('710', ['a', 'Org Contributor.', 't', 'Title'], '22'),
      ('711', ['a', 'Festival.', 'e', 'Orchestra.'], '2 ')],
     {'author_contributor_facet': ['conference-subcommittee!Conference, '
                                   'Subcommittee',
                                   'author-second-1920-1999!Author, Second, '
                                   '1920-1999',
                                   'org-contributor!Org Contributor',
                                   'festival-orchestra!Festival, Orchestra'],
      'author_json': {'p': [{'d': 'Conference, Subcommittee',
                             'v': 'conference-subcommittee!Conference, '
                                  'Subcommittee'}]},
      'author_search': ['Conference, Subcommittee'],
      'author_sort': 'conference-subcommittee',
      'contributors_json': [{'p': [{'d': 'Author, Second, 1920-1999',
                                    'v': 'author-second-1920-1999!Author, '
                                         'Second, 1920-1999'}]},
                            {'p': [{'d': 'Org Contributor',
                                    'v': 'org-contributor!Org Contributor'}]},
                            {'p': [{'d': 'Festival, Orchestra',
                                    'v': 'festival-orchestra!Festival, '
                                         'Orchestra'}]}],
      'contributors_search': ['Conference, Subcommittee',
                              'Author, Second, 1920-1999',
                              'Author, Second Author, S Author',
                              'Second Author', 'Second Author',
                              'Org Contributor', 'Festival, Orchestra'],
      'meeting_facet': ['conference!Conference', 'festival!Festival'],
      'meetings_json': [{'p': [{'d': 'Conference',
                                'v': 'conference!Conference'}]},
                        {'p': [{'d': 'Festival',
                                'v': 'festival!Festival'}]}],
      'meetings_search': ['Conference', 'Festival']})
], ids=[
    # Edge cases
    'Nothing: no 1XX, 7XX, or 8XX fields',
    'Blank 100',
    'Blank 110',
    'Blank 111',
    '100 with relator but no heading information',
    '110 with relator but no heading information',
    '111 with relator but no heading information',
    '$0 should be ignored (X00)',
    '$0 should be ignored (X10)',
    '$0 should be ignored (X11)',
    '$5 should be ignored (X00)',
    '$5 should be ignored (X10)',
    '$5 should be ignored (X11)',
    '$6 should be ignored (X00)',
    '$6 should be ignored (X10)',
    '$6 should be ignored (X11)',
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
def test_todscpipeline_getcontributorinfo(fparams, expected,
                                          bl_sierra_test_record,
                                          todsc_pipeline_class,
                                          bibrecord_to_pymarc,
                                          params_to_fields,
                                          add_marc_fields,
                                          assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_contributor_info should return fields
    matching the expected parameters.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('100', '110', '111', '700', '710', '711', '800',
                          '810', '811')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bundle = pipeline.do(bib, bibmarc, ['contributor_info'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('tag, subfields, expected', [
    # Start with edge cases: missing data, non-ISBD punctuation, etc.

    ('245', [],
     {'nonfiling_chars': 0,
      'transcribed': []}),

    ('245', ['a', ''],
     {'nonfiling_chars': 0,
      'transcribed': []}),

    ('245', ['a', '', 'b', 'oops mistake /'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['oops mistake']}]}),

    ('246', ['a', '   ', 'i', 'Some blank chars at start:', 'a', 'Oops'],
     {'display_text': 'Some blank chars at start',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Oops']}]}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title no punctuation'],
         'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'n', 'Part 1',
             'p', 'the quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title no punctuation', 'Part 1, the quickening'],
         'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'p', 'The quickening',
             'p', 'Subpart A', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title no punctuation', 'The quickening',
                   'Subpart A'],
         'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title,', 'b', 'non-ISBD punctuation;', 'n', 'Part 1,',
             'p', 'the quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title, non-ISBD punctuation', 'Part 1, the quickening'],
         'responsibility': 'by Joe'}]}),

    ('245', ['a', 'A title!', 'b', 'Non-ISBD punctuation;',
             'p', 'The quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['A title! Non-ISBD punctuation', 'The quickening'],
         'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title : with punctuation, all in $a. Part 1 / by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
        {'parts': ['A title: with punctuation, all in $a. Part 1 / by Joe']}]}),

    ('245', ['b', ' = A parallel title missing a main title'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['A parallel title missing a main title']}]}),

    ('245', ['a', '1. One thing, 2. Another, 3. A third :',
             'b', 'This is like some Early English Books Online titles / '
                  'by Joe = 1. One thing, 2. Another, 3. A third : Plus long '
                  'subtitle etc. /'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['1. One thing, 2. Another, 3. A third: This is like some '
                   'Early English Books Online titles / by Joe'],
         'parallel': [
            {'parts': ['1. One thing, 2. Another, 3. A third: Plus long subtitle '
                       'etc.']}
         ]}],
    }),

    ('245', ['a', '1. This is like another Early English Books Online title :',
             'b', 'something: 2. Something else: 3. About the 22th. of June, '
                  '1678. by Richard Greene of Dilwin, etc.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['1. This is like another Early English Books Online title: '
                   'something: 2. Something else: 3. About the 22th. of June, '
                   '1678. by Richard Greene of Dilwin, etc.']}]}),

    ('245', ['a', 'A forward slash somewhere in the title / before sf c /',
             'c', 'by Joe.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['A forward slash somewhere in the title / before sf c'],
         'responsibility': 'by Joe'}]}),

    ('245', ['a', 'Quotation marks /', 'c', 'by "Joe."'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Quotation marks'],
         'responsibility': 'by "Joe"'}]}),

    ('245', ['a', 'Multiple ISBD marks / :', 'b', 'subtitle', 'c', 'by Joe.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Multiple ISBD marks /: subtitle'],
         'responsibility': 'by Joe'}]}),

    # Now test cases on more standard data.

    ('245', ['a', 'Title :', 'b', 'with subtitle.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['Title: with subtitle']}]}),

    ('245', ['a', 'First title ;', 'b', 'Second title.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'First title ;', 'b', 'Second title ; Third title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']}, {'parts': ['Second title']},
                      {'parts': ['Third title']}]}),

    ('245', ['a', 'First title ;', 'b', 'and Second title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'First title,', 'b', 'and Second title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'Title /', 'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title'],
         'responsibility': 'by Author'}]}),

    ('245', ['a', 'Title /', 'c', 'Author 1 ; Author 2 ; Author 3.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title'],
         'responsibility': 'Author 1; Author 2; Author 3'}]}),

    ('245', ['a', 'Title!', 'b', 'What ending punctuation should we keep?'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title! What ending punctuation should we keep?']}]}),

    # Titles that include parts ($n and $p).

    ('245', ['a', 'Title.', 'n', 'Part 1.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1']}]}),

    ('245', ['a', 'Title.', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', 'Part 1,', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1, Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', 'Part 1', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1, Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', 'Part 1.', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', 'Part 1', 'Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', '1. Part', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', '1. Part, Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', '1. Part A', 'n', '2. Part B'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title', '1. Part A', '2. Part B']}]}),

    ('245', ['a', 'Title :', 'b', 'subtitle.', 'n', '1. Part A',
             'n', '2. Part B'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title: subtitle', '1. Part A', '2. Part B']}]}),

    ('245', ['a', 'Title one.', 'n', 'Book 2.', 'n', 'Chapter V /',
             'c', 'Author One. Title two. Book 3. Chapter VI / Author Two.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title one', 'Book 2', 'Chapter V'],
         'responsibility': 'Author One'},
        {'parts': ['Title two', 'Book 3. Chapter VI'],
         'responsibility': 'Author Two'}]}),

    # Fun with parallel titles!

    ('245', ['a', 'Title in French =', 'b', 'Title in English /',
             'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French'],
         'responsibility': 'by Author',
         'parallel': [
            {'parts': ['Title in English']}]
        }],
     }),

    ('245', ['a', 'Title in French /',
             'c', 'by Author in French = Title in English / by Author in '
                  'English.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French'],
         'responsibility': 'by Author in French',
         'parallel': [
            {'parts': ['Title in English'],
             'responsibility': 'by Author in English'}]
        }],
     }),

    ('245', ['a', 'Title in French =',
             'b', 'Title in English = Title in German /', 'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French'],
         'responsibility': 'by Author',
         'parallel': [
            {'parts': ['Title in English']},
            {'parts': ['Title in German']}],
        }],
     }),

    ('245', ['a', 'First title in French =',
             'b', 'First title in English ; Second title in French = Second '
                  'title in English.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['First title in French'],
         'parallel': [
            {'parts': ['First title in English']}]
        },
        {'parts': ['Second title in French'],
         'parallel': [
            {'parts': ['Second title in English']}]
        }],
     }),

    ('245', ['a', 'Title in French.', 'p',  'Part One =',
             'b', 'Title in English.', 'p', 'Part One.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French', 'Part One'],
         'parallel': [
            {'parts': ['Title in English', 'Part One']}]
        }],
     }),

    ('245', ['a', 'Title in French.', 'p',  'Part One :',
             'b', 'subtitle = Title in English.', 'p', 'Part One : subtitle.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French', 'Part One: subtitle'],
         'parallel': [
            {'parts': ['Title in English', 'Part One: subtitle']}]
        }],
     }),

    ('245', ['a', 'Title in French /',
             'c', 'by Author in French = by Author in English'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French'],
         'responsibility': 'by Author in French',
         'parallel': [
            {'responsibility': 'by Author in English'}]
        }],
     }),

    ('245', ['a', 'Title in French.', 'p',  'Part One :',
             'b', 'subtitle = Title in English.', 'p', 'Part One : subtitle.',
             'c', 'by Author in French = by Author in English'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French', 'Part One: subtitle'],
         'responsibility': 'by Author in French',
         'parallel': [
            {'parts': ['Title in English', 'Part One: subtitle']},
            {'responsibility': 'by Author in English'}],
        }],
     }),

    # $h (medium) is ignored, except for ISBD punctuation

    ('245', ['a', 'First title', 'h', '[sound recording] ;',
             'b', 'Second title.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'Title in French.', 'p',  'Part One',
             'h', '[sound recording] =', 'b', 'Title in English.',
             'p', 'Part One.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title in French', 'Part One'],
         'parallel': [
            {'parts': ['Title in English', 'Part One']}]
        }],
     }),

    # Subfields for archives and archival collections (fgks)

    ('245', ['a', 'Smith family papers,', 'f', '1800-1920.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920 (bulk 1850-1860)']}]}),

    ('245', ['a', 'Smith family papers', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1850-1860']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920,', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920 (bulk 1850-1860)']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920,',
             'g', '(1850-1860).'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920, (1850-1860)']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920', 'g', '(1850-1860).'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Smith family papers, 1800-1920 (1850-1860)']}]}),

    ('245', ['a', 'Some title :', 'k', 'typescript', 'f', '1800.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Some title: typescript, 1800']}]}),

    ('245', ['a', 'Hearing Files', 'k', 'Case Files', 'f', '1800',
             'p', 'District 6.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Hearing Files, Case Files, 1800', 'District 6']}]}),

    ('245', ['a', 'Hearing Files.', 'k', 'Case Files', 'f', '1800',
             'p', 'District 6.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Hearing Files', 'Case Files, 1800', 'District 6']}]}),

    ('245', ['a', 'Report.', 's', 'Executive summary.'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Report', 'Executive summary']}]}),

    ('245', ['a', 'Title', 'k', 'Form', 's', 'Version', 'f', '1990'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Title, Form, Version, 1990']}]}),

    ('245', ['k', 'Form', 's', 'Version', 'f', '1990'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Form, Version, 1990']}]}),

    # 242s (Translated titles)

    ('242 14', ['a', 'The Annals of chemistry', 'n', 'Series C,',
             'p', 'Organic chemistry and biochemistry.', 'y', 'eng'],
     {'display_text': 'Title translation, English',
      'nonfiling_chars': 4,
      'transcribed': [
        {'parts': ['The Annals of chemistry',
                   'Series C, Organic chemistry and biochemistry']}]}),

    # 246s (Variant titles)

    ('246', ['a', 'Archives for meteorology, geophysics, and bioclimatology.',
             'n', 'Serie A,', 'p', 'Meteorology and geophysics'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Archives for meteorology, geophysics, and bioclimatology',
                   'Serie A, Meteorology and geophysics']}]}),

    ('246 12', ['a', 'Creating jobs', 'f', '1980'],
     {'display_text': 'Issue title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Creating jobs, 1980']}]}),

    ('246 12', ['a', 'Creating jobs', 'g', '(varies slightly)', 'f', '1980'],
     {'display_text': 'Issue title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Creating jobs (varies slightly) 1980']}]}),

    ('246 1 ', ['i', 'At head of title:', 'a', 'Science and public affairs',
                'f', 'Jan. 1970-Apr. 1974'],
     {'display_text': 'At head of title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Science and public affairs, Jan. 1970-Apr. 1974']}]}),

    ('247', ['a', 'Industrial medicine and surgery', 'x', '0019-8536'],
     {'issn': '0019-8536',
      'display_text': 'Former title',
      'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Industrial medicine and surgery']}]}),

    # Testing 490s: similar to 245s but less (differently?) structured

    ('490', ['a', 'Series statement / responsibility'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement'],
         'responsibility': 'responsibility'}]}),

    ('490', ['a', 'Series statement =', 'a', 'Series statement in English'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement'],
         'parallel': [
            {'parts': ['Series statement in English']}]
        }],
     }),

    ('490', ['a', 'Series statement ;', 'v', 'v. 1 =',
             'a', 'Series statement in English ;', 'v', 'v. 1'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement; v. 1'],
         'parallel': [
            {'parts': ['Series statement in English; v. 1']}]
        }],
     }),

    ('490', ['3', 'Vol. 1:', 'a', 'Series statement'],
     {'nonfiling_chars': 0,
      'materials_specified': ['Vol. 1'],
      'transcribed': [
        {'parts': ['Series statement']}]}),

    ('490', ['a', 'Series statement,', 'x', '1234-5678 ;', 'v', 'v. 1'],
     {'nonfiling_chars': 0,
      'issn': '1234-5678',
      'transcribed': [
        {'parts': ['Series statement; v. 1']}]}),

    ('490', ['a', 'Series statement ;', 'v', '1.',
             'a', 'Sub-series / Responsibility ;', 'v', 'v. 36'],
     {'nonfiling_chars': 0,
      'transcribed': [
        {'parts': ['Series statement; [volume] 1', 'Sub-series; v. 36'],
         'responsibility': 'Responsibility'}]}),

    ('490', ['a', 'Series statement ;', 'v', 'v. 1.', 'l', '(LC12345)'],
     {'nonfiling_chars': 0,
      'lccn': 'LC12345',
      'transcribed': [
        {'parts': ['Series statement; v. 1']}]}),

])
def test_transcribedtitleparser_parse(tag, subfields, expected,
                                      params_to_fields):
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
    field = params_to_fields([(tag, subfields, indicators)])[0]
    parsed = s2m.TranscribedTitleParser(field).parse()
    print parsed
    assert parsed == expected


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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
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
      'type': 'main',
      'relations': None,
     }),

    # 6XX$e and $4 are parsed as relators.
    ('630', ['a', 'Domesday book', 'z', 'United States.', 'e', 'depicted.',
             '4', 'dpc'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Domesday book'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'subject',
      'relations': ['depicted'],
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
      'type': 'related',
      'relations': None,
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
      'type': 'analytic',
      'relations': None,
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
      'type': 'analytic',
      'relations': None,
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
      'type': 'related',
      'relations': None,
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
      'type': 'related',
      'relations': None,
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
      'type': 'analytic',
      'relations': None,
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
      'type': 'related',
      'relations': None,
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
      'type': 'series',
      'relations': None,
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
      'type': 'series',
      'relations': None,
     }),
])
def test_preferredtitleparser_parse(tag, subfields, expected, params_to_fields):
    """
    PreferredTitleParser `parse` method should return a dict with the
    expected structure, given the provided MARC field.
    """
    if ' ' in tag:
        tag, indicators = tag.split(' ', 1)
    else:
        indicators = '  '
    fields = params_to_fields([(tag, subfields, indicators)])[0]
    assert s2m.PreferredTitleParser(fields).parse() == expected


@pytest.mark.parametrize('name_str, expected', [
    ('Author of The diary of a physician, 1807-1877.', {
        'heading': 'Author of The diary of a physician, 1807-1877',
        'forename': 'Author of The diary of a physician',
        'type': 'person'
    }),
    ('Claude, d\'Abbeville, pere, d. 1632.', {
        'heading': 'Claude, d\'Abbeville, pere, d. 1632',
        'surname': 'Claude',
        'person_titles': ['d\'Abbeville', 'pere'],
        'type': 'person'
    }),
    ('Dickinson, David K., author.', {
        'heading': 'Dickinson, David K.',
        'forename': 'David K.',
        'surname': 'Dickinson',
        'relations': ['author'],
        'type': 'person'
    }),
    ('Hecht, Ben, 1893-1964, writing, direction, production.', {
        'heading': 'Hecht, Ben, 1893-1964',
        'forename': 'Ben',
        'surname': 'Hecht',
        'relations': ['writing', 'direction', 'production'],
        'type': 'person'
    }),
    ('John, the Baptist, Saint.', {
        'heading': 'John, the Baptist, Saint',
        'surname': 'John',
        'person_titles': ['the Baptist', 'Saint'],
        'type': 'person'
    }),
    ('Charles II, Prince of Wales', {
        'heading': 'Charles II, Prince of Wales',
        'surname': 'Charles II',
        'person_titles': ['Prince of Wales'],
        'type': 'person'
    }),
    ('El-Abiad, Ahmed H., 1926-', {
        'heading': 'El-Abiad, Ahmed H., 1926-',
        'surname': 'El-Abiad',
        'forename': 'Ahmed H.',
        'type': 'person'
    }),
    ('Thomas, Aquinas, Saint, 1225?-1274.', {
        'heading': 'Thomas, Aquinas, Saint, 1225?-1274',
        'surname': 'Thomas',
        'forename': 'Aquinas',
        'person_titles': ['Saint'],
        'type': 'person'
    }),
    ('Levi, James, fl. 1706-1739.', {
        'heading': 'Levi, James, fl. 1706-1739',
        'surname': 'Levi',
        'forename': 'James',
        'type': 'person'
    }),
    ('Joannes Aegidius, Zamorensis, 1240 or 41-ca. 1316.', {
        'heading': 'Joannes Aegidius, Zamorensis, 1240 or 41-ca. 1316',
        'surname': 'Joannes Aegidius',
        'forename': 'Zamorensis',
        'type': 'person'
    }),
    ('Churchill, Winston, Sir, 1874-1965.', {
        'heading': 'Churchill, Winston, Sir, 1874-1965',
        'surname': 'Churchill',
        'forename': 'Winston',
        'person_titles': ['Sir'],
        'type': 'person'
    }),
    ('Beethoven, Ludwig van, 1770-1827.', {
        'heading': 'Beethoven, Ludwig van, 1770-1827',
        'surname': 'Beethoven',
        'forename': 'Ludwig van',
        'type': 'person'
    }),
    ('H. D. (Hilda Doolittle), 1886-1961.', {
        'heading': 'H. D. (Hilda Doolittle), 1886-1961',
        'forename': 'H. D.',
        'fuller_form_of_name': 'Hilda Doolittle',
        'type': 'person'
    }),
    ('Fowler, T. M. (Thaddeus Mortimer), 1842-1922.', {
        'heading': 'Fowler, T. M. (Thaddeus Mortimer), 1842-1922',
        'forename': 'T. M.',
        'surname': 'Fowler',
        'fuller_form_of_name': 'Thaddeus Mortimer',
        'type': 'person'
    }),
    ('United States. Congress (97th, 2nd session : 1982). House.', {
        'heading_parts': [{'name': 'United States'},
                          {'name': 'Congress',
                           'qualifier': '97th, 2nd session : 1982'},
                          {'name': 'House'}],
        'is_jurisdiction': False,
        'type': 'organization'
    }),
    ('Cyprus (Archdiocese)', {
        'heading_parts': [{'name': 'Cyprus',
                           'qualifier': 'Archdiocese'}],
        'is_jurisdiction': False,
        'type': 'organization'
    }),
    ('United States. President (1981-1989 : Reagan)', {
        'heading_parts': [{'name': 'United States'},
                          {'name': 'President',
                           'qualifier': '1981-1989 : Reagan'}],
        'is_jurisdiction': False,
        'type': 'organization'
    }),
    ('New York Public Library', {
        'heading_parts': [{'name': 'New York Public Library'}],
        'is_jurisdiction': False,
        'type': 'organization'
    }),
    ('International American Conference (8th : 1938 : Lima, Peru). '
     'Delegation from Mexico.', {
        'heading_parts': [{'name': 'International American Conference',
                           'qualifier': '8th : 1938 : Lima, Peru'},
                          {'name': 'Delegation from Mexico'}],
        'is_jurisdiction': False,
        'type': 'organization'
    }),
    ('Paris. Peace Conference, 1919.', {
        'heading_parts': [{'name': 'Paris'},
                          {'name': 'Peace Conference, 1919'}],
        'is_jurisdiction': False,
        'type': 'organization'
    }),
    ('Paris Peace Conference (1919-1920)', {
        'heading_parts': [{'name': 'Paris Peace Conference',
                           'qualifier': '1919-1920'}],
        'is_jurisdiction': False,
        'type': 'organization'
    }),
])
def test_parsenamestring(name_str, expected):
    """
    The `parse_name_string` function should return the expected result
    when given the provided `name_str`.
    """
    val = s2m.parse_name_string(name_str)
    for k, v in val.items():
        print k, v
        if k in expected:
            assert v == expected[k]
        else:
            assert v is None


@pytest.mark.parametrize('fparams, expected', [
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
def test_shortenname(fparams, expected, params_to_fields):
    """
    The `shorten_name` function should return the expected shortened
    version of a name when passed a structure from a NameParser
    resulting from the given `fparams` data.
    """
    field = params_to_fields([fparams])[0]
    name_structs = s2m.extract_name_structs_from_field(field)
    result = [s2m.shorten_name(n) for n in name_structs]
    assert set(result) == set(expected)


@pytest.mark.parametrize('fval, nf_chars, expected', [
    ('', 0, '~'),
    ('$', 0, '~'),
    ('日本食品化学学会誌', 0, '~'),
    ('$1000', 0, '1000'),
    ('1000', 0, '1000'),
    ('[A] whatever', 0, 'a-whatever'),
    ('[A] whatever', 4, 'whatever'),
    ('[A] whatever', 1, 'a-whatever'),
    ('[A] whatever!', 1, 'a-whatever'),
    ('Romeo and Juliet', 4, 'romeo-and-juliet'),
    ('Lastname, Firstname, 1800-1922', 0, 'lastname-firstname-1800-1922'),
])
def test_generatefacetkey(fval, nf_chars, expected):
    """
    The `generate_facet_key` function should return the expected key
    string when passed the given facet value string and number of non-
    filing characters (`nf_chars`).
    """
    assert s2m.generate_facet_key(fval, nf_chars) == expected


@pytest.mark.parametrize('fparams, expected', [
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
      'main_title_search': ['Duets, violin, viola'],
      'variant_titles_search': ['Duets, violin, viola > Op. 10 > No. 3'],
      'title_sort': 'duets-violin-viola-op-10-no-3',
      'main_work_title_json': {
        'p': [{'d': 'Duets, violin, viola',
               's': ' > ',
               'v': 'duets-violin-viola!Duets, violin, viola'},
              {'d': 'Op. 10',
               's': ' > ',
               'v': 'duets-violin-viola-op-10!Duets, violin, viola > Op. 10'},
              {'d': 'No. 3',
               'v': 'duets-violin-viola-op-10-no-3!'
                    'Duets, violin, viola > Op. 10 > No. 3'}]
      },
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
      'variant_titles_search': ['Las Cantigas de Santa Maria'],
      'title_sort': 'las-cantigas-de-santa-maria',
      'responsibility_display': 'Alfonso X, "el Sabio"',
      'responsibility_search': ['Alfonso X, "el Sabio"'],
      'main_work_title_json': {
        'p': [{'d': 'Las Cantigas de Santa Maria',
               'v': 'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'}]
      },
      'included_work_titles_search': ['Las Cantigas de Santa Maria'],
      'title_series_facet': [
        'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'
      ]}),

    # 245 with punct following last ISBD period
    ([('245', ['a', 'Las Cantigas de Santa Maria /',
               'c', 'Alfonso X, el Sabio. ,...'], '1 ')],
     {'title_display': 'Las Cantigas de Santa Maria',
      'main_title_search': ['Las Cantigas de Santa Maria'],
      'variant_titles_search': ['Las Cantigas de Santa Maria'],
      'title_sort': 'las-cantigas-de-santa-maria',
      'responsibility_display': 'Alfonso X, el Sabio,...',
      'responsibility_search': ['Alfonso X, el Sabio,...'],
      'main_work_title_json': {
        'p': [{'d': 'Las Cantigas de Santa Maria',
               'v': 'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'}]
      },
      'included_work_titles_search': ['Las Cantigas de Santa Maria'],
      'title_series_facet': [
        'las-cantigas-de-santa-maria!Las Cantigas de Santa Maria'
      ]}),

    # 245 with non-roman-charset
    ([('245', ['a', '日本食品化学学会誌'], '1 ')],
     {'title_display': '日本食品化学学会誌',
      'main_title_search': ['日本食品化学学会誌'],
      'variant_titles_search': ['日本食品化学学会誌'],
      'title_sort': '~',
      'main_work_title_json': {
        'p': [{'d': '日本食品化学学会誌',
               'v': '~!日本食品化学学会誌'}]
      },
      'included_work_titles_search': ['日本食品化学学会誌'],
      'title_series_facet': [
        '~!日本食品化学学会誌'
      ]}),

    # Basic configurations of MARC Fields => title fields, Included vs
    # Related works, and how single author vs multiple
    # authors/contributors affects display of short authors.

    # 130/245: No author.
    # 245 is the main title and 130 is the main work. No author info is
    # added to any titles because there is no author info to add.
    ([('130', ['a', 'Duets,', 'm', 'violin, viola,', 'n', 'op. 10.',
               'n', 'No. 3.'], '0 '),
      ('245', ['a', 'Duets for violin and viola,', 'n', 'opus 10.',
               'n', 'Number 3 /', 'c', '[various authors]'], '1 ')],
     {'title_display': 'Duets for violin and viola, opus 10 > Number 3',
      'main_title_search': ['Duets for violin and viola, opus 10'],
      'variant_titles_search': ['Duets for violin and viola, opus 10 > '
                                'Number 3'],
      'title_sort': 'duets-for-violin-and-viola-opus-10-number-3',
      'responsibility_display': '[various authors]',
      'responsibility_search': ['[various authors]'],
      'main_work_title_json': {
        'p': [{'d': 'Duets, violin, viola',
               's': ' > ',
               'v': 'duets-violin-viola!Duets, violin, viola'},
              {'d': 'Op. 10',
               's': ' > ',
               'v': 'duets-violin-viola-op-10!Duets, violin, viola > Op. 10'},
              {'d': 'No. 3',
               'v': 'duets-violin-viola-op-10-no-3!'
                    'Duets, violin, viola > Op. 10 > No. 3'}]
      },
      'included_work_titles_search': ['Duets, violin, viola > Op. 10 > No. 3'],
      'title_series_facet': [
        'duets-violin-viola!Duets, violin, viola',
        'duets-violin-viola-op-10!Duets, violin, viola > Op. 10',
        'duets-violin-viola-op-10-no-3!Duets, violin, viola > Op. 10 > No. 3'
      ]}),

    # 100/240/245: Single author (title in included works).
    # 240 is the main work and 245 is main title. Short author info is
    # added to titles.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Specific Preferred Title.'], '10'),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Specific Preferred Title [by Smith, J.]',
               'v': 'specific-preferred-title!Specific Preferred Title'}]
      },
      'included_work_titles_search': ['Specific Preferred Title'],
      'title_series_facet': [
        'specific-preferred-title!Specific Preferred Title'
      ]}),

    # 100/245: No preferred title.
    # 245 is main title AND main work. Short author info is added.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Transcribed title [by Smith, J.]',
               'v': 'transcribed-title!Transcribed title'}]
      },
      'included_work_titles_search': ['Transcribed title'],
      'title_series_facet': [
        'transcribed-title!Transcribed title'
      ]}),

    # 130/245/700s (IW): Contribs are in 700s; same person.
    # 245 is main title. 130 is main work, and 700s are incl. works.
    # Short author info is added.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '12'),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'Second work.'], '12')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'p': [{'d': 'Specific Preferred Title (1933)',
               'v': 'specific-preferred-title-1933!'
                    'Specific Preferred Title (1933)'}]
      },
      'included_work_titles_json': [
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
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
    # 245 is main title. 130 is main work and 700s are incl. works.
    # Short author info is added because there are multiple different
    # people.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '12'),
      ('700', ['a', 'Walter, Johannes.', 't', 'Second work.'], '12')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'p': [{'d': 'Specific Preferred Title (1933)',
               'v': 'specific-preferred-title-1933!'
                    'Specific Preferred Title (1933)'}],
      },
      'included_work_titles_json': [
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'walter-johannes!Walter, Johannes',
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
    # 245 is main title. 130 is main work and 700s are incl. works.
    # Short author info is added, where possible.
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
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'p': [{'d': 'Specific Preferred Title (1933)',
               'v': 'specific-preferred-title-1933!'
                    'Specific Preferred Title (1933)'}]
      },
      'included_work_titles_json': [
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
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
    # 245 is main title. 130 is main work, but 700s are RWs.
    # Short author info is added.
    ([('130', ['a', 'Specific Preferred Title (1933)'], '0 '),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'Second work.'], '1 ')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'p': [{'d': 'Specific Preferred Title (1933)',
               'v': 'specific-preferred-title-1933!'
                    'Specific Preferred Title (1933)'}]
      },
      'included_work_titles_search': ['Specific Preferred Title (1933)'],
      'related_work_titles_json': [
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
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
    # 245 is main title. 130 is main work; some 700s/730s are incl.
    # works; some 700s/730s are rel. works. Short authors are added
    # where needed.
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
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'p': [{'d': 'Specific Preferred Title (1933)',
               'v': 'specific-preferred-title-1933!'
                    'Specific Preferred Title (1933)'}]
      },
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
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
    # 245 is main title AND main work. Titles in 700s are IWs.
    # Short author info is added.
    ([('100', ['a', 'Smith, Joe.'], '10'),
      ('245', ['a', 'Transcribed title /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 '),
      ('700', ['a', 'Jung, C. G.', 'q', '(Carl Gustav),', 'd', '1875-1961.',
               't', 'First work.'], '12'),
      ('700', ['a', 'Smith, Joe.', 't', 'Second work.'], '12')],
     {'title_display': 'Transcribed title',
      'main_title_search': ['Transcribed title'],
      'variant_titles_search': ['Transcribed title'],
      'title_sort': 'transcribed-title',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Transcribed title [by Smith, J.]',
               'v': 'transcribed-title!Transcribed title'}]
      },
      'included_work_titles_json': [
        {'a': 'jung-c-g-carl-gustav-1875-1961!'
              'Jung, C. G. (Carl Gustav), 1875-1961',
         'p': [{'d': 'First work [by Jung, C.G.]',
                'v': 'first-work!First work'}]},
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'ravel-maurice-1875-1937!Ravel, Maurice, 1875-1937',
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
        {'a': 'strauss-richard-1864-1949!Strauss, Richard, 1864-1949',
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
        {'a': 'united-states-congress!United States Congress',
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
        {'a': 'france!France',
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
    # The 240 is the main work.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Poems.'], '10'),
      ('245', ['a', 'Poetry! :', 'b', 'an anthology of collected poems /',
               'c', 'by Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Poetry!: an anthology of collected poems',
      'main_title_search': ['Poetry!'],
      'variant_titles_search': ['Poetry!: an anthology of collected poems'],
      'title_sort': 'poetry-an-anthology-of-collected-poems',
      'responsibility_display': 'by Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['by Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Poems [of Smith, J.] (Complete)',
               'v': 'poems-of-smith-j-complete!'
                    'Poems [of Smith, J.] (Complete)'}]
      },
      'included_work_titles_search': [
        'Poems [of Smith, J.] (Complete)'
      ],
      'title_series_facet': [
        'poems-of-smith-j-complete!Poems [of Smith, J.] (Complete)',
      ]}),

    # 100/240/245: 240 is Coll Title/Selections.
    # 240 is the main work.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Poems.', 'k', 'Selections.'], '10'),
      ('245', ['a', 'Poetry! :', 'b', 'an anthology of selected poems /',
               'c', 'by Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'Poetry!: an anthology of selected poems',
      'main_title_search': ['Poetry!'],
      'variant_titles_search': ['Poetry!: an anthology of selected poems'],
      'title_sort': 'poetry-an-anthology-of-selected-poems',
      'responsibility_display': 'by Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['by Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Poems [of Smith, J.] (Selections)',
               'v': 'poems-of-smith-j-selections!'
                    'Poems [of Smith, J.] (Selections)'}]
      },
      'included_work_titles_search': [
        'Poems [of Smith, J.] (Selections)'
      ],
      'title_series_facet': [
        'poems-of-smith-j-selections!Poems [of Smith, J.] (Selections)',
      ]}),

    # 100/240/245: 240 is Music Form w/parts.
    # 240 is main work.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Sonatas,', 'm', 'piano,', 'n', 'op. 32,',
               'r', 'C major.'], '10'),
      ('245', ['a', 'Smith\'s piano sonata in C major, opus 32 /',
               'c', 'by Joe Smith.'], '1 ')],
     {'title_display': 'Smith\'s piano sonata in C major, opus 32',
      'main_title_search': ['Smith\'s piano sonata in C major, opus 32'],
      'variant_titles_search': ['Smith\'s piano sonata in C major, opus 32'],
      'title_sort': 'smith-s-piano-sonata-in-c-major-opus-32',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Sonatas, piano [by Smith, J.]',
               's': ' > ',
               'v': 'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]'},
              {'d': 'Op. 32, C major',
               'v': 'sonatas-piano-by-smith-j-op-32-c-major!'
                    'Sonatas, piano [by Smith, J.] > Op. 32, C major'},]
      },
      'included_work_titles_search': [
        'Sonatas, piano [by Smith, J.] > Op. 32, C major',
      ],
      'title_series_facet': [
        'sonatas-piano-by-smith-j!Sonatas, piano [by Smith, J.]',
        'sonatas-piano-by-smith-j-op-32-c-major!'
        'Sonatas, piano [by Smith, J.] > Op. 32, C major'
      ]}),

    # 100/240/245: 245 has multiple titles, 240 is not Coll Title.
    # The first title from the 245 is not the main work if the 240 is
    # not a collective title, but titles from the 245 are added to IW
    # if there are no title added entries (7XXs) that cover them.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Specific Preferred Title.'], '10'),
      ('245', ['a', 'First work ;', 'b', 'Second work /',
               'c', 'Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'First work; Second work',
      'main_title_search': ['First work'],
      'variant_titles_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Specific Preferred Title [by Smith, J.]',
               'v': 'specific-preferred-title!Specific Preferred Title'}]
      },
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'Second work [by Smith, J.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work',
        'Second work',
        'Specific Preferred Title',
      ],
      'title_series_facet': [
        'first-work!First work',
        'second-work!Second work',
        'specific-preferred-title!Specific Preferred Title',
      ]}),

    # 100/240/245: 245 has multiple titles, 240 is basic Coll Title.
    # 240 is main work. All titles from the 245 are added to IW.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('240', ['a', 'Poems.', 'k', 'Selections.'], '10'),
      ('245', ['a', 'First poem ;', 'b', 'Second poem /',
               'c', 'by Joe Smith ; edited by Edward Copeland.'], '1 ')],
     {'title_display': 'First poem; Second poem',
      'main_title_search': ['First poem'],
      'variant_titles_search': ['First poem; Second poem'],
      'title_sort': 'first-poem-second-poem',
      'responsibility_display': 'by Joe Smith; edited by Edward Copeland',
      'responsibility_search': ['by Joe Smith; edited by Edward Copeland'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Poems [of Smith, J.] (Selections)',
               'v': 'poems-of-smith-j-selections!'
                    'Poems [of Smith, J.] (Selections)'}]
      },
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First poem [by Smith, J.]',
                'v': 'first-poem!First poem'}]},
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'Second poem [by Smith, J.]',
                'v': 'second-poem!Second poem'}]},
      ],
      'included_work_titles_search': [
        'First poem',
        'Second poem',
        'Poems [of Smith, J.] (Selections)',
      ],
      'title_series_facet': [
        'first-poem!First poem',
        'second-poem!Second poem',
        'poems-of-smith-j-selections!Poems [of Smith, J.] (Selections)',
      ]}),

    # 100/245/700 (IW): 2-title 245 w/700 covering 2nd.
    # With a multi-titled 245 where ind1 is 1, there is no main work.
    # Titles from 245 / 7XXs are added to IW.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work tt /',
               'c', 'by Joe Smith. Second work tt / by Edward Copeland.'],
       '1 '),
      ('700', ['a', 'Copeland, Edward.', 't', 'Second work.'], '12')],
     {'title_display': 'First work tt; Second work tt',
      'main_title_search': ['First work tt'],
      'variant_titles_search': ['First work tt; Second work tt'],
      'title_sort': 'first-work-tt-second-work-tt',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First work tt [by Smith, J.]',
                'v': 'first-work-tt!First work tt'}]},
        {'a': 'copeland-edward!Copeland, Edward',
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
    # There is no main work.
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'First work tt /',
               'c', 'by Joe Smith. Second work tt / by Edward Copeland.'],
       '1 '),
      ('700', ['a', 'Smith, Joe.', 't', 'First work.'], '12'),
      ('700', ['a', 'Copeland, Edward.', 't', 'Second work.'], '12')],
     {'title_display': 'First work tt; Second work tt',
      'main_title_search': ['First work tt'],
      'variant_titles_search': ['First work tt; Second work tt'],
      'title_sort': 'first-work-tt-second-work-tt',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'copeland-edward!Copeland, Edward',
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
      'main_title_search': ['First work'],
      'variant_titles_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'copeland-edward!Copeland, Edward',
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
      'main_title_search': ['First work'],
      'variant_titles_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith; by Edward Copeland',
      'responsibility_search': ['by Joe Smith', 'by Edward Copeland'],
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'copeland-edward!Copeland, Edward',
         'p': [{'d': 'Second work [by Copeland, E.]',
                'v': 'second-work!Second work'}]}
      ],
      'included_work_titles_search': [
        'First work',
        'Second work'
      ],
      'related_work_titles_json': [
        {'a': 'copeland-edward!Copeland, Edward',
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
      'main_title_search': ['First work'],
      'variant_titles_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'smith-joe!Smith, Joe',
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
      'main_title_search': ['First work'],
      'variant_titles_search': ['First work; Second work'],
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
      'main_title_search': ['First work'],
      'variant_titles_search': ['First work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'First work [by Smith, J.]',
                'v': 'first-work!First work'}]},
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'Some series [by Smith, J.]',
                'v': 'some-series!Some series'}]},
        {'a': 'copeland-edward!Copeland, Edward',
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
        {'a': 'united-states-congress-house!United States Congress > House',
         'p': [{'d': 'Some series [United States Congress, House]',
                'v': 'some-series!Some series'}]},
        {'a': 'led-zeppelin!Led Zeppelin',
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
        {'a': 'some-conference-3rd-1983!Some conference (3rd : 1983)',
         'p': [{'d': 'Some series [Some conference]',
                'v': 'some-series!Some series'}]},
        {'a': 'some-event-orchestra!Some event, Orchestra',
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
               'v', '76', 'l', '(LC 12345)'], '0 '),
      ('490', ['3', '1992-93:', 'a', 'Another series =',
               'a', 'Series in English / by Joe Smith ;'], '0 '),
      ('490', ['3', '1993-94:', 'a', 'Third series.', 'v', 'v. 1',
               'a', 'Subseries B ;', 'v', 'v. 2', ], '0 ') ],
     {'related_series_titles_json': [
        {'b': '(1990-92)',
         'p': [{'d': 'Some series; [volume] 76',
                's': ' ('},
               {'d': 'ISSN 1234-5678; LC Call Number LC 12345',
                's': ')'}]},
        {'b': '(1992-93)',
         'p': [{'d': 'Another series [by Joe Smith]'}]},
        {'b': '(1993-94)',
         'p': [{'d': 'Third series; v. 1 > Subseries B; v. 2'}]},
      ],
      'related_series_titles_search': [
        'Some series; [volume] 76',
        'Another series [by Joe Smith]',
        'Third series; v. 1 > Subseries B; v. 2'
      ]}),

    # 490/800: Traced 490s are not included in Series titles
    ([('490', ['a', 'Some series (statement).',  '1 ']),
      ('490', ['a', 'Piano music of Edward Copeland.',  '1 ']),
      ('800', ['a', 'Smith, Joe.', 't', 'Some series.'], '1 '),
      ('800', ['a', 'Copeland, Edward.', 't', 'Piano music.'], '1 ') ],
     {'related_series_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'Some series [by Smith, J.]',
                'v': 'some-series!Some series'}]},
        {'a': 'copeland-edward!Copeland, Edward',
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
        {'a': 'smith-joe!Smith, Joe',
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
        {'a': 'smith-joe!Smith, Joe',
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
                's': ' (',
                'v': 'work-title-first-part-second-part!'
                     'Work title > First part > Second part'},
               {'d': 'English; Some version; 1994',
                's': ')',
                'v': 'work-title-first-part-second-part-english-some-version-'
                     '1994!'
                     'Work title > First part > Second part '
                     '(English; Some version; 1994)'}]},
      ],
      'included_work_titles_search': [
        'Work title > First part > Second part (English; Some version; 1994)',
      ],
      'title_series_facet': [
        'work-title!Work title',
        'work-title-first-part!Work title > First part',
        'work-title-first-part-second-part!'
        'Work title > First part > Second part',
        'work-title-first-part-second-part-english-some-version-1994!'
        'Work title > First part > Second part (English; Some version; 1994)'
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
                's': ' (',
                'v': 'work-title-first-part-second-part!'
                     'Work title > First part > Second part'},
               {'d': 'English; Some version; Selections; 1994',
                's': ')',
                'v': 'work-title-first-part-second-part-english-some-version-'
                     'selections-1994!'
                     'Work title > First part > Second part '
                     '(English; Some version; Selections; 1994)'}]},
      ],
      'included_work_titles_search': [
        'Work title > First part > Second part (English; Some version; '
        'Selections; 1994)',
      ],
      'title_series_facet': [
        'work-title!Work title',
        'work-title-first-part!Work title > First part',
        'work-title-first-part-second-part!'
        'Work title > First part > Second part',
        'work-title-first-part-second-part-english-some-version-selections-'
        '1994!Work title > First part > Second part (English; Some version; '
        'Selections; 1994)'
      ]}),

    # 730: multiple languages ($l), Lang1 & Lang2
    ([('730', ['a', 'Three little pigs.', 'l', 'English & German.'], '02')],
     {'included_work_titles_json': [
        {'p': [{'d': 'Three little pigs',
                's': ' (',
                'v': 'three-little-pigs!Three little pigs'},
               {'d': 'English & German',
                's': ')',
                'v': 'three-little-pigs-english-german!'
                     'Three little pigs (English & German)'}]},
      ],
      'included_work_titles_search': [
        'Three little pigs (English & German)',
      ],
      'title_series_facet': [
        'three-little-pigs!Three little pigs',
        'three-little-pigs-english-german!Three little pigs (English & German)',
      ]}),

    # 830: $v, volume info
    ([('830', ['a', 'Some series ;', 'v', 'v. 2.'], ' 0')],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                's': '; ',
                'v': 'some-series!Some series'},
               {'d': 'v. 2',
                'v': 'some-series-v-2!Some series; v. 2'}]},
      ],
      'related_series_titles_search': [
        'Some series; v. 2',
      ],
      'title_series_facet': [
        'some-series!Some series',
        'some-series-v-2!Some series; v. 2',
      ]}),

    # 830: $x, ISSN info
    ([('830', ['a', 'Some series.', 'x', '1234-5678'], ' 0')],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                's': ' (',
                'v': 'some-series!Some series'},
               {'d': 'ISSN 1234-5678', 's': ')'}]},
      ],
      'related_series_titles_search': [
        'Some series',
      ],
      'title_series_facet': [
        'some-series!Some series',
      ]}),

    # 830: $v and $x, volume + ISSN info
    ([('830', ['a', 'Some series,', 'x', '1234-5678 ;', 'v', 'v. 2.', ], ' 0')],
     {'related_series_titles_json': [
        {'p': [{'d': 'Some series',
                's': '; ',
                'v': 'some-series!Some series'},
               {'d': 'v. 2', 
                'v': 'some-series-v-2!Some series; v. 2',
                's': ' ('},
               {'d': 'ISSN 1234-5678', 's': ')'}]},
      ],
      'related_series_titles_search': [
        'Some series; v. 2',
      ],
      'title_series_facet': [
        'some-series!Some series',
        'some-series-v-2!Some series; v. 2',
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
      'variant_titles_search': [
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
        'title title title title title title title title'
      ],
      'variant_titles_search': [
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
        'title title title title title title title title title title title'
      ],
      'variant_titles_search': [
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
        'Title title title title'
      ],
      'variant_titles_search': [
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
      'variant_titles_search': [
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
      'main_work_title_json':{
        'p': [{'d': 'Title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title title title ...',
               'v': 'title-title-title-title-title-title-title-title-title-'
                    'title-title-title-title-title-title-title-title-title-'
                    'title-title-title-title-title-title-title!Title title '
                    'title title title title title title title title title '
                    'title title title title title title title title title '
                    'title title title title title ...'}]
      },
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
      'main_title_search': ['The first work'],
      'variant_titles_search': ['The first work; Second work'],
      'title_sort': 'first-work-second-work',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'included_work_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'The first work [by Smith, J.]',
                'v': 'first-work!The first work'}]},
        {'a': 'smith-joe!Smith, Joe',
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
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'Title =', 'b', 'Title in English /',
               'c', 'by Joe Smith.'], '10')],
     {'title_display': 'Title [translated: Title in English]',
      'main_title_search': ['Title'],
      'title_sort': 'title',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English', 'Title'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Title [by Smith, J.]',
               'v': 'title!Title',
               's': ' '},
               {'d': '[translated: Title in English]'}]
      },
      'included_work_titles_search': ['Title', 'Title in English'],
      'title_series_facet': ['title!Title',
                             'title-in-english!Title in English'],
      }),

    # 245: Parallel title in 245 with its own SOR
    ([('100', ['a', 'Author, German'], '1 '),
      ('245', ['a', 'Title in German /',
               'c', 'by German Author = Title in English / by Joe Smith.'],
       '10')],
     {'title_display': 'Title in German [translated: Title in English]',
      'main_title_search': ['Title in German'],
      'title_sort': 'title-in-german',
      'responsibility_display': 'by German Author [translated: by Joe Smith]',
      'responsibility_search': ['by German Author', 'by Joe Smith'],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English', 'Title in German'],
      'main_work_title_json': {
        'a': 'author-german!Author, German',
        'p': [{'d': 'Title in German [by Author, G.]',
               'v': 'title-in-german!Title in German',
               's': ' '},
              {'d': '[translated: Title in English]'}]
      },
      'included_work_titles_search': ['Title in German', 'Title in English'],
      'title_series_facet': ['title-in-german!Title in German',
                             'title-in-english!Title in English']
      }),

    # 245: Multiple parallel titles in 245
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'Title =', 'b', 'Title in English = Title in Spanish /',
               'c', 'by Joe Smith.'], '10')],
     {'title_display': 'Title [translated: Title in English; Title in Spanish]',
      'main_title_search': ['Title'],
      'title_sort': 'title',
      'responsibility_display': 'by Joe Smith',
      'responsibility_search': ['by Joe Smith'],
      'variant_titles_notes': [
        'Title translation: Title in English',
        'Title translation: Title in Spanish'],
      'variant_titles_search': [
        'Title in English', 'Title in Spanish', 'Title'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Title [by Smith, J.]',
               'v': 'title!Title',
               's': ' '},
              {'d': '[translated: Title in English; Title in Spanish]'}]
      },
      'included_work_titles_search': [
        'Title', 'Title in English', 'Title in Spanish'],
      'title_series_facet': ['title!Title',
                             'title-in-english!Title in English',
                             'title-in-spanish!Title in Spanish'],
      }),

    # 245: No parallel title, but parallel SOR
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'Title /', 'c', 'por Joe Smith = by Joe Smith.'], '10')],
     {'title_display': 'Title',
      'main_title_search': ['Title'],
      'title_sort': 'title',
      'responsibility_display': 'por Joe Smith [translated: by Joe Smith]',
      'responsibility_search': ['por Joe Smith', 'by Joe Smith'],
      'variant_titles_search': ['Title'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Title [by Smith, J.]',
               'v': 'title!Title'}]
      },
      'included_work_titles_search': ['Title'],
      'title_series_facet': ['title!Title'],
      }),

    # 245: Parallel title and separate parallel SOR
    ([('100', ['a', 'Smith, Joe'], '1 '),
      ('245', ['a', 'Title in Spanish.', 'p',  'Part One :',
               'b', 'subtitle = Title in English.', 'p', 'Part One : subtitle.',
               'c', 'por Joe Smith = by Joe Smith.'], '10')],
     {'title_display': 'Title in Spanish > Part One: subtitle '
                       '[translated: Title in English > Part One: subtitle]',
      'main_title_search': ['Title in Spanish'],
      'title_sort': 'title-in-spanish-part-one-subtitle',
      'responsibility_display': 'por Joe Smith [translated: by Joe Smith]',
      'responsibility_search': ['por Joe Smith', 'by Joe Smith'],
      'variant_titles_search': [
        'Title in English > Part One: subtitle',
        'Title in Spanish > Part One: subtitle'],
      'variant_titles_notes': [
        'Title translation: Title in English > Part One: subtitle'],
      'main_work_title_json': {
        'a': 'smith-joe!Smith, Joe',
        'p': [{'d': 'Title in Spanish [by Smith, J.]',
               'v': 'title-in-spanish!Title in Spanish',
               's': ' > '},
              {'d': 'Part One: subtitle',
               'v': 'title-in-spanish-part-one-subtitle!'
                    'Title in Spanish > Part One: subtitle',
               's': ' '},
              {'d':'[translated: Title in English > Part One: subtitle]'}]
      },
      'included_work_titles_search': [
        'Title in Spanish > Part One: subtitle',
        'Title in English > Part One: subtitle'],
      'title_series_facet': ['title-in-spanish!Title in Spanish',
                             'title-in-spanish-part-one-subtitle!'
                             'Title in Spanish > Part One: subtitle',
                             'title-in-english-part-one-subtitle!'
                             'Title in English > Part One: subtitle'],
      }),

    # 242/245: Parallel title in 242 and 245 (duplicates)
    ([('242', ['a', 'Title in English /', 'c', 'by Joe Smith.'], '00'),
      ('245', ['a', 'Title in German /',
               'c', 'by German Author = Title in English / by Joe Smith.'],
       '00')],
     {'title_display': 'Title in German [translated: Title in English]',
      'main_title_search': ['Title in German'],
      'title_sort': 'title-in-german',
      'responsibility_display': 'by German Author [translated: by Joe Smith]',
      'responsibility_search': [
        'by German Author',
        'by Joe Smith'
      ],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English', 'Title in German'],
      }),

    # 245/246: Parallel title in 246 (duplicates)
    ([('245', ['a', 'Title in German /',
               'c', 'by German Author = Title in English / by Joe Smith.'],
       '00'),
      ('246', ['a', 'Title in English'], '01'),],
     {'title_display': 'Title in German [translated: Title in English]',
      'main_title_search': ['Title in German'],
      'title_sort': 'title-in-german',
      'responsibility_display': 'by German Author [translated: by Joe Smith]',
      'responsibility_search': [
        'by German Author',
        'by Joe Smith'
      ],
      'variant_titles_notes': [
        'Title translation: Title in English'],
      'variant_titles_search': [
        'Title in English', 'Title in German'],
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
    '245 with non-roman-charset',

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
    '245: Multiple parallel titles in 245',
    '245: No parallel title, but parallel SOR',
    '245: Parallel title and separate parallel SOR',
    '242/245: Parallel title in 242 and 245 (duplicates)',
    '245/246: Parallel title in 246 (duplicates)',
    '246: Variant title in 246 w/$i',
    '246: Variant title in 246 w/no display constant',
    '247: Former title',
    '246: No note if ind1 is NOT 0 or 1',
    '247: No note if ind2 is 1',
])
def test_todscpipeline_gettitleinfo(fparams, expected, bl_sierra_test_record,
                                    todsc_pipeline_class, bibrecord_to_pymarc,
                                    params_to_fields, add_marc_fields,
                                    assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_title_info should return fields matching
    the expected parameters.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('100', '110', '111', '130', '240', '242', '243',
                          '245', '246', '247', '490', '700', '710', '711',
                          '730', '740', '800', '810', '811', '830')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bundle = pipeline.do(bib, bibmarc, ['title_info'])
    assert_bundle_matches_expected(bundle, expected)


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
def test_todscpipeline_compileperformancemedium(parsed_pm, expected,
                                                todsc_pipeline_class):
    """
    ToDiscoverPipeline.compile_performance_medium should return
    a value matching `expected`, given the sample `parsed_pm` output
    from parsing a 382 field.
    """
    pipeline = todsc_pipeline_class()
    assert pipeline.compile_performance_medium(parsed_pm) == expected


def test_todscpipeline_getgeneral3xxinfo(params_to_fields, add_marc_fields,
                                         todsc_pipeline_class,
                                         assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_general_3xx_info should return fields
    matching the expected parameters.
    """
    exclude = s2m.IGNORED_MARC_FIELDS_BY_GROUP_TAG['r']
    exc_fields = [(''.join(('r', t)), ['a', 'No']) for t in exclude]
    inc_fields = [
        ('r300', ['a', '300 desc 1', '0', 'exclude']),
        ('r300', ['a', '300 desc 2', '1', 'exclude']),
        ('r310', ['a', 'Monthly,', 'b', '1958-']),
        ('r321', ['a', 'Bimonthly,', 'b', '1954-1957']),
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
        'current_publication_frequency': ['Monthly, 1958-'],
        'former_publication_frequency': ['Bimonthly, 1954-1957'],
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
    fields = params_to_fields(exc_fields + inc_fields)
    marc = add_marc_fields(s2m.SierraMarcRecord(), fields)
    pipeline = todsc_pipeline_class()
    bundle = pipeline.do(None, marc, ['general_3xx_info'])
    assert_bundle_matches_expected(bundle, expected)


def test_todscpipeline_getgeneral5xxinfo(params_to_fields, add_marc_fields,
                                         todsc_pipeline_class,
                                         assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_general_5xx_info should return fields
    matching the expected parameters.
    """
    exclude = s2m.IGNORED_MARC_FIELDS_BY_GROUP_TAG['n']
    handled = ('592',)
    exc_fields = [(''.join(('r', t)), ['a', 'No']) for t in exclude + handled]
    inc_fields = [
        ('n500', ['a', 'General Note.', '0', 'exclude']),
        ('n502', ['a', 'Karl Schmidt\'s thesis (doctoral), Munich, 1965.']),
        ('n502', ['b', 'Ph. D.', 'c', 'University of North Texas',
                  'd', 'August, 2012.']),
        ('n502', ['g', 'Some diss', 'b', 'Ph. D.',
                  'c', 'University of North Texas', 'd', 'August, 2012.']),
        ('n505', ['a', 'Future land use plan -- Recommended capital '
                       'improvements -- Existing land use -- Existing '
                       'zoning.']),
        ('n505', ['g', 'Nr. 1.', 't', 'Region Neusiedlersee --', 'g', 'Nr. 2.',
                  't', 'Region Rosalia/Lithagebirge /',
                  'r', 'by L. H. Fellows.']),
        ('n508', ['a', 'Educational consultant, Roseanne Gillis.']),
        ('n511', ['a', 'Hosted by Hugh Downs.'], '0 '),
        ('n511', ['a', 'Colin Blakely, Jane Lapotaire.'], '1 '),
        ('n520', ['a', 'Short summary.', 'b', 'Long summary.'], '  '),
        ('n520', ['a', 'Short summary.', 'b', 'Long summary.'], '8 '),
        ('n520', ['a', 'Two head-and-shoulder portraits ...'], '0 '),
        ('n520', ['a', 'This book is great!'], '1 '),
        ('n520', ['a', 'Item consists of XYZ.'], '2 '),
        ('n520', ['a', 'The study examines ...'], '3 '),
        ('n520', ['a', 'Contains violence',
                  'c', '[Revealweb organization code]'], '4 '),
        ('n520', ['a', '"Not safe for life."', 'c', 'Family Filmgoer.'], '4 '),
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
        'toc_notes': [
            'Future land use plan -- Recommended capital improvements -- '
            'Existing land use -- Existing zoning.',
            'Nr. 1. Region Neusiedlersee -- Nr. 2. Region Rosalia/Lithagebirge '
            '/ by L. H. Fellows.'
        ],
        'summary_notes': [
            'Short summary. Long summary.',
            'Short summary. Long summary.',
            'Subject: Two head-and-shoulder portraits ...',
            'Review: This book is great!',
            'Scope and content: Item consists of XYZ.',
            'Abstract: The study examines ...',
            'Content advice: Contains violence [Revealweb organization code]',
            'Content advice: "Not safe for life." [Family Filmgoer]'
        ],
        'production_credits': ['Educational consultant, Roseanne Gillis.'],
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
    fields = params_to_fields(exc_fields + inc_fields)
    marc = add_marc_fields(s2m.SierraMarcRecord(), fields)
    pipeline = todsc_pipeline_class()
    bundle = pipeline.do(None, marc, ['general_5xx_info'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('bib_cn_info, items_info, expected', [
    ([('c', '050', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [])],
     {'call_numbers_display': ['TEST BIB CN'],
      'call_numbers_search': ['TEST', 'TEST BIB', 'TEST BIB CN']}),

    ([('c', '050', ['|aTEST BIB CN'])],
     [({'copy_num': 1}, [('c', None, ['TEST ITEM CN'])])],
     {'call_numbers_display': ['TEST BIB CN', 'TEST ITEM CN'],
      'call_numbers_search': ['TEST', 'TEST BIB', 'TEST BIB CN',
                              'TEST', 'TEST ITEM', 'TEST ITEM CN']}),

    ([('c', '050', ['|aTEST CN'])],
     [({'copy_num': 1}, [('c', None, ['TEST CN'])])],
     {'call_numbers_display': ['TEST CN'],
      'call_numbers_search': ['TEST', 'TEST CN']}),

    ([('c', '092', ['|a100.123|aC35 2002'])],
     [({'copy_num': 1}, [('c', '092', ['|a100.123|aC35 2002 copy 1'])])],
     {'call_numbers_display': ['100.123 C35 2002',
                               '100.123 C35 2002 copy 1'],
      'call_numbers_search': ['100', '100.123', '100.123C', '100.123C35',
                              '100.123C35 2002', '100', '100.123',
                              '100.123C', '100.123C35', '100.123C35 2002',
                              '100.123C35 2002copy', '100.123C35 2002copy1']}),

    ([('c', '050', ['|aMT 100 .C35 2002']),
      ('c', '090', ['|aC 35.2 .MT100 2002'])],
     [({'copy_num': 1}, [('c', '050', ['|aMT 100 .C35 2002 vol 1'])]),
      ({'copy_num': 2}, [('c', '090', ['|aC 35.2 .MT100 2002 vol 1'])])],
     {'call_numbers_display': ['MT 100 .C35 2002',
                              'C 35.2 .MT100 2002',
                              'MT 100 .C35 2002 vol 1',
                              'C 35.2 .MT100 2002 vol 1'],
      'call_numbers_search': ['MT', 'MT100', 'MT100.C', 'MT100.C35',
                               'MT100.C35 2002', 'C', 'C35', 'C35.2',
                               'C35.2.MT', 'C35.2.MT100',
                               'C35.2.MT100 2002', 'MT', 'MT100',
                               'MT100.C', 'MT100.C35', 'MT100.C35 2002',
                               'MT100.C35 2002vol', 'MT100.C35 2002vol1',
                               'C', 'C35', 'C35.2', 'C35.2.MT', 'C35.2.MT100',
                               'C35.2.MT100 2002', 'C35.2.MT100 2002vol',
                               'C35.2.MT100 2002vol1']}),

    ([('c', '099', ['|aLPCD 100,001-100,050'])],
     [({'copy_num': 1}, [('c', '099', ['|aLPCD 100,001-100,050 +insert'])])],
     {'call_numbers_display': ['LPCD 100,001-100,050',
                               'LPCD 100,001-100,050 +insert'],
      'call_numbers_search': ['LPCD', 'LPCD100001', 'LPCD100001-100050',
                              'LPCD', 'LPCD100001', 'LPCD100001-100050',
                              'LPCD100001-100050+insert']}),

    ([('c', '086', ['|aA 1.76:643/989|2ordocs'])], [],
     {'sudocs_display': ['A 1.76:643/989'],
      'sudocs_search': ['A', 'A1', 'A1.76', 'A1.76:643', 'A1.76:643/989']}),

    ([('g', '086', ['|aA 1.76:643/989|2ordocs'])], [],
     {'sudocs_display': ['A 1.76:643/989'],
      'sudocs_search': ['A', 'A1', 'A1.76', 'A1.76:643', 'A1.76:643/989']}),

    ([('g', '086', ['|aA 1.76:643/989|2ordocs'])],
     [({'copy_num': 1}, [('c', '090', ['|aC 35.2 .MT100 2002'])])],
     {'sudocs_display': ['A 1.76:643/989'],
      'sudocs_search': ['A', 'A1', 'A1.76', 'A1.76:643', 'A1.76:643/989'],
      'call_numbers_display': ['C 35.2 .MT100 2002'],
      'call_numbers_search': ['C', 'C35', 'C35.2', 'C35.2.MT', 'C35.2.MT100',
                              'C35.2.MT100 2002']}),

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
def test_todscpipeline_getcallnumberinfo(bib_cn_info, items_info, expected,
                                         bl_sierra_test_record,
                                         todsc_pipeline_class,
                                         update_test_bib_inst,
                                         assert_bundle_matches_expected):
    """
    The `ToDiscoverPipeline.get_call_number_info` method should
    return the expected values given the provided `bib_cn_info` fields
    and `items_info` parameters.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, varfields=bib_cn_info, items=items_info)
    bundle = pipeline.do(bib, None, ['call_number_info'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('raw_marcfields, expected', [
    # 020s (ISBN)

    # 020: $a is a valid ISBN.
    (['020 ## $a0567890123'], {
        'isbns_display': ['0567890123'],
        'isbn_numbers': ['0567890123'],
        'all_standard_numbers': ['0567890123'],
        'standard_numbers_search': ['0567890123']
    }),

    # 020: $z is an invalid ISBN.
    (['020 ## $z0567890123'], {
        'isbns_display': ['0567890123 [Invalid]'],
        'all_standard_numbers': ['0567890123'],
        'standard_numbers_search': ['0567890123']
    }),

    # 020: $a and $z in same field.
    (['020 ## $a0567890123$z0567898123'], {
        'isbns_display': ['0567890123', '0567898123 [Invalid]'],
        'isbn_numbers': ['0567890123'],
        'all_standard_numbers': ['0567890123', '0567898123'],
        'standard_numbers_search': ['0567890123', '0567898123']
    }),

    # 020: $c and $q apply to both ISBNs, if two are in the same field.
    (['020 ## $a0877790019$qblack leather$z0877780116 :$c14.00'], {
        'isbns_display': ['0877790019 (black leather, 14.00)',
                          '0877780116 (black leather, 14.00) [Invalid]'],
        'isbn_numbers': ['0877790019'],
        'all_standard_numbers': ['0877790019', '0877780116'],
        'standard_numbers_search': ['0877790019', '0877780116']
    }),

    # 020: Multiple $q's beget multiple qualifiers.
    (['020 ## $a0394170660$qRandom House$qpaperback$c4.95'], {
        'isbns_display': ['0394170660 (Random House, paperback, 4.95)'],
        'isbn_numbers': ['0394170660'],
        'all_standard_numbers': ['0394170660'],
        'standard_numbers_search': ['0394170660']
    }),

    # 020: Non-numeric data is treated as qualifying information.
    (['020 ## $a0394170660 (Random House ; paperback)'], {
        'isbns_display': ['0394170660 (Random House, paperback)'],
        'isbn_numbers': ['0394170660'],
        'all_standard_numbers': ['0394170660'],
        'standard_numbers_search': ['0394170660']
    }),

    # 020: Parentheses around multiple $q's are stripped.
    (['020 ## $z9780815609520$q(cloth ;$qalk. paper)'], {
        'isbns_display': ['9780815609520 (cloth, alk. paper) [Invalid]'],
        'all_standard_numbers': ['9780815609520'],
        'standard_numbers_search': ['9780815609520']
    }),

    # 020: Multiple parentheses around $q's are stripped.
    (['020 ## $a1401250564$q(bk. 1)$q(paperback)'], {
        'isbns_display': ['1401250564 (bk. 1, paperback)'],
        'isbn_numbers': ['1401250564'],
        'all_standard_numbers': ['1401250564'],
        'standard_numbers_search': ['1401250564']
    }),

    # 022s (ISSN)

    # 022: $a is a valid ISSN.
    (['022 ## $a1234-1231'], {
        'issns_display': ['1234-1231'],
        'issn_numbers': ['1234-1231'],
        'all_standard_numbers': ['1234-1231'],
        'standard_numbers_search': ['1234-1231'],
    }),

    # 022: $a (ISSN) and $l (ISSN-L) are displayed separately
    # (Even if they are the same)
    (['022 ## $a1234-1231$l1234-1231'], {
        'issns_display': ['1234-1231', 'ISSN-L: 1234-1231'],
        'issn_numbers': ['1234-1231'],
        'all_standard_numbers': ['1234-1231', '1234-1231'],
        'standard_numbers_search': ['1234-1231', '1234-1231'],
    }),

    # 022: $m is a canceled ISSN-L
    (['022 ## $a1560-1560$l1234-1231$m1560-1560'], {
        'issns_display': ['1560-1560', 'ISSN-L: 1234-1231',
                          'ISSN-L: 1560-1560 [Canceled]'],
        'issn_numbers': ['1560-1560', '1234-1231'],
        'all_standard_numbers': ['1560-1560', '1234-1231', '1560-1560'],
        'standard_numbers_search': ['1560-1560', '1234-1231', '1560-1560'],
    }),

    # 022: $y is an incorrect ISSN
    (['022 ## $a0046-225X$y0046-2254'], {
        'issns_display': ['0046-225X', '0046-2254 [Incorrect]'],
        'issn_numbers': ['0046-225X'],
        'all_standard_numbers': ['0046-225X', '0046-2254'],
        'standard_numbers_search': ['0046-225X', '0046-2254'],
    }),

    # 022: $z is a canceled ISSN
    (['022 ## $z0046-2254'], {
        'issns_display': ['0046-2254 [Canceled]'],
        'all_standard_numbers': ['0046-2254'],
        'standard_numbers_search': ['0046-2254'],
    }),

    # 022: Lots of ISSNs
    (['022 ## $a1234-1231$l1234-1231',
      '022 ## $a1560-1560$m1560-1560',
      '022 ## $a0046-225X$y0046-2254'], {
        'issns_display': ['1234-1231', 'ISSN-L: 1234-1231', '1560-1560',
                          'ISSN-L: 1560-1560 [Canceled]', '0046-225X',
                          '0046-2254 [Incorrect]'],
        'issn_numbers': ['1234-1231', '1560-1560', '0046-225X'],
        'all_standard_numbers': ['1234-1231', '1234-1231', '1560-1560',
                                 '1560-1560', '0046-225X', '0046-2254'],
        'standard_numbers_search': ['1234-1231', '1234-1231', '1560-1560',
                                    '1560-1560', '0046-225X', '0046-2254'],
    }),

    # 024

    # 024: IND1 of 0 ==> type is isrc.
    (['024 0# $a1234567890'], {
        'other_standard_numbers_display': [
            'International Standard Recording Code: 1234567890'],
        'all_standard_numbers': ['1234567890'],
        'standard_numbers_search': ['1234567890'],
    }),

    # 024: IND1 of 1 ==> type is upc.
    (['024 1# $z1234567890'], {
        'other_standard_numbers_display': [
            'Universal Product Code: 1234567890 [Invalid]'],
        'all_standard_numbers': ['1234567890'],
        'standard_numbers_search': ['1234567890'],
    }),

    # 024: IND1 of 2 ==> type is ismn.
    (['024 2# $a1234567890$qscore$qsewn$cEUR28.50'], {
        'other_standard_numbers_display': [
            'International Standard Music Number: 1234567890 (score, sewn, '
            'EUR28.50)'],
        'all_standard_numbers': ['1234567890'],
        'standard_numbers_search': ['1234567890'],
    }),

    # 024: IND1 of 3 ==> type is ean.
    (['024 3# $a1234567890$d51000'], {
        'other_standard_numbers_display': [
            'International Article Number: 1234567890 51000'],
        'all_standard_numbers': ['1234567890 51000'],
        'standard_numbers_search': ['1234567890 51000'],
    }),

    # 024: IND1 of 4 ==> type is sici.
    (['024 4# $a1234567890'], {
        'other_standard_numbers_display': [
            'Serial Item and Contribution Identifier: 1234567890'],
        'all_standard_numbers': ['1234567890'],
        'standard_numbers_search': ['1234567890'],
    }),

    # 024: IND1 of 7 ==> type is in the $2.
    (['024 7# $a1234567890$2istc'], {
        'other_standard_numbers_display': [
            'International Standard Text Code: 1234567890'],
        'all_standard_numbers': ['1234567890'],
        'standard_numbers_search': ['1234567890'],
    }),

    # 024: IND1 of 8 ==> type is unknown.
    (['024 8# $a1234567890'], {
        'other_standard_numbers_display': ['[Unknown Type]: 1234567890'],
        'all_standard_numbers': ['1234567890'],
        'standard_numbers_search': ['1234567890'],
    }),

    # 025: type ==> 'oan'
    (['025 ## $aAe-F-355$aAe-F-562'], {
        'other_standard_numbers_display': [
            'Overseas Acquisition Number: Ae-F-355',
            'Overseas Acquisition Number: Ae-F-562'],
        'all_standard_numbers': ['Ae-F-355', 'Ae-F-562'],
        'standard_numbers_search': ['Ae-F-355', 'Ae-F-562'],
    }),

    # 026: type ==> 'fingerprint'; ignore control subfields
    (['026 ## $adete nkck$bvess lodo 3$cAnno Domini MDCXXXVI$d3$2fei$5UkCU'], {
        'other_standard_numbers_display': [
            'Fingerprint ID: dete nkck vess lodo 3 Anno Domini MDCXXXVI 3'],
        'all_standard_numbers': [
            'dete nkck vess lodo 3 Anno Domini MDCXXXVI 3'],
        'standard_numbers_search': [
            'dete nkck vess lodo 3 Anno Domini MDCXXXVI 3'],
    }),

    # 027: type ==> 'strn'
    (['027 ## $aFOA--89-40265/C--SE'], {
        'other_standard_numbers_display': [
            'Standard Technical Report Number: FOA--89-40265/C--SE'],
        'all_standard_numbers': ['FOA--89-40265/C--SE'],
        'standard_numbers_search': ['FOA--89-40265/C--SE'],
    }),

    # 028: IND1 != 6 ==> publisher number, (publisher name from $b).
    (['028 02 $a438 953-2$bPhilips Classics$q(set)'], {
        'other_standard_numbers_display': [
            'Publisher Number, Philips Classics: 438 953-2 (set)'],
        'all_standard_numbers': ['438 953-2'],
        'standard_numbers_search': ['438 953-2'],
    }),

    # 028: IND1 == 6 ==> distributor number, (distributor name from $b).
    (['028 62 $aDV98597$bFacets Multimedia'], {
        'other_standard_numbers_display': [
            'Distributor Number, Facets Multimedia: DV98597'],
        'all_standard_numbers': ['DV98597'],
        'standard_numbers_search': ['DV98597'],
    }),

    # 030: 030 ==> type coden
    (['030 ## $aASIRAF$zASITAF'], {
        'other_standard_numbers_display': [
            'CODEN: ASIRAF', 'CODEN: ASITAF [Invalid]'],
        'all_standard_numbers': ['ASIRAF', 'ASITAF'],
        'standard_numbers_search': ['ASIRAF', 'ASITAF'],
    }),

    # 074: 074 ==> type gpo
    (['074 ## $a1022-A$z1012-A'], {
        'other_standard_numbers_display': [
            'Government Printing Office Item Number: 1022-A',
            'Government Printing Office Item Number: 1012-A [Invalid]'],
        'all_standard_numbers': ['1022-A', '1012-A'],
        'standard_numbers_search': ['1022-A', '1012-A'],
    }),

    # 088: 088 ==> type report
    (['088 ## $aNASA-RP-1124-REV-3$zNASA-RP-1124-REV-2'], {
        'other_standard_numbers_display': [
            'Report Number: NASA-RP-1124-REV-3',
            'Report Number: NASA-RP-1124-REV-2 [Invalid]'],
        'all_standard_numbers': ['NASA-RP-1124-REV-3', 'NASA-RP-1124-REV-2'],
        'standard_numbers_search': ['NASA-RP-1124-REV-3', 'NASA-RP-1124-REV-2'],
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
def test_todscpipeline_getstandardnumberinfo(raw_marcfields, expected,
                                             bl_sierra_test_record,
                                             todsc_pipeline_class,
                                             bibrecord_to_pymarc,
                                             add_marc_fields,
                                             fieldstrings_to_fields,
                                             assert_bundle_matches_expected):
    """
    The `ToDiscoverPipeline.get_standard_number_info` method should
    return the expected values given the provided `marcfields`.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('020', '022', '024', '025', '026', '027', '028',
                          '030', '074', '088')
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, ['standard_number_info'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('raw_marcfields, expected', [
    # Edge cases
    
    # No data ==> no control numbers
    ([], {}),

    # ONLY an 003 ==> no control numbers
    # I.e., an 003 with no 001 should be ignored.
    (['003 OCoLC'], {}),

    # 001 (Control number -- may or may not be OCLC)

    # 001: Plain number is OCLC number
    (['001 194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001: OCLC number w/prefix (ocm)
    (['001 ocm194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001: OCLC number w/prefix (ocn)
    (['001 ocn194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001: OCLC number w/prefix (on)
    (['001 on194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001: OCLC number w/prefix and leading zeros
    (['001 on0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001: OCLC number w/leading zeros
    (['001 0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001: OCLC number w/provider suffix
    (['001 on0194068/springer',
      '003 OCoLC'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068', '194068/springer'],
    }),

    # 001: Non-OCLC vendor number
    (['001 ybp0194068',
      '003 YBP'], {
        'other_control_numbers_display': ['ybp0194068 (source: YBP)'],
        'all_control_numbers': ['ybp0194068'],
        'control_numbers_search': ['ybp0194068'],
    }),

    # 001: Non-OCLC vendor number with no 003
    (['001 ybp0194068'], {
        'other_control_numbers_display': ['[Unknown Type]: ybp0194068'],
        'all_control_numbers': ['ybp0194068'],
        'control_numbers_search': ['ybp0194068'],
    }),

    # 001: OCLC number w/incorrect 003
    (['001 194068',
      '003 YBP'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 010 (LCCN)

    # 010: $a is an LCCN, $b is a National Union Catalog number
    (['010 ## $a   89798632 $bms 89001579'], {
        'lccns_display': ['89798632'],
        'lccn_number': '89798632',
        'all_control_numbers': ['89798632', 'ms89001579', 'ms 89001579'],
        'other_control_numbers_display': [
            'National Union Catalog Number: ms 89001579 (i.e., ms89001579)'],
        'control_numbers_search': ['89798632', 'ms89001579', 'ms 89001579'],
    }),

    # 010: $z is an invalid LCCN
    (['010 ## $zsc 76000587'], {
        'lccns_display': ['sc 76000587 (i.e., sc76000587) [Invalid]'],
        'all_control_numbers': ['sc76000587', 'sc 76000587'],
        'control_numbers_search': ['sc76000587', 'sc 76000587'],
    }),

    # 010: normalization 1
    (['010 ## $a89-456'], {
        'lccns_display': ['89-456 (i.e., 89000456)'],
        'lccn_number': '89000456',
        'all_control_numbers': ['89000456', '89-456'],
        'control_numbers_search': ['89000456', '89-456'],
    }),

    # 010: normalization 2
    (['010 ## $a2001-1114'], {
        'lccns_display': ['2001-1114 (i.e., 2001001114)'],
        'lccn_number': '2001001114',
        'all_control_numbers': ['2001001114', '2001-1114'],
        'control_numbers_search': ['2001001114', '2001-1114'],
    }),

    # 010: normalization 3
    (['010 ## $agm 71-2450'], {
        'lccns_display': ['gm 71-2450 (i.e., gm71002450)'],
        'lccn_number': 'gm71002450',
        'all_control_numbers': ['gm71002450', 'gm 71-2450'],
        'control_numbers_search': ['gm71002450', 'gm 71-2450'],
    }),

    # 010: normalization 4
    (['010 ## $a   79-139101 /AC/MN'], {
        'lccns_display': ['79-139101 /AC/MN (i.e., 79139101)'],
        'lccn_number': '79139101',
        'all_control_numbers': ['79139101', '79-139101 /AC/MN'],
        'control_numbers_search': ['79139101', '79-139101 /AC/MN'],
    }),

    # 016: IND1 != 7 ==> LAC control number
    (['016 ## $a 730032015  rev'], {
        'other_control_numbers_display': [
            'Library and Archives Canada Number: 730032015  rev'],
        'all_control_numbers': ['730032015  rev'],
        'control_numbers_search': ['730032015  rev'],
    }),

    # 016: IND1 == 7 ==> control number source in $2
    (['016 7# $a94.763966.7$2GyFmDB'], {
        'other_control_numbers_display': ['94.763966.7 (source: GyFmDB)'],
        'all_control_numbers': ['94.763966.7'],
        'control_numbers_search': ['94.763966.7'],
    }),

    # 016: $z indicates invalid/canceled control number
    (['016 7# $a890000298$z89000298$2GyFmDB'], {
        'other_control_numbers_display': [
            '890000298 (source: GyFmDB)',
            '89000298 (source: GyFmDB) [Invalid]'],
        'all_control_numbers': ['890000298', '89000298'],
        'control_numbers_search': ['890000298', '89000298'],
    }),

    # 035: non-OCLC number
    (['035 ## $a(CaO-TULAS)41063988'], {
        'other_control_numbers_display': ['41063988 (source: CaO-TULAS)'],
        'all_control_numbers': ['41063988'],
        'control_numbers_search': ['41063988'],
    }),

    # 035: Invalid OCLC number in $z
    (['035 ## $z(OCoLC)7374506'], {
        'oclc_numbers_display': ['7374506 [Invalid]'],
        'all_control_numbers': ['7374506'],
        'control_numbers_search': ['7374506'],
    }),

    # 035: OCLC number normalization (ocm)
    (['035 ## $a(OCoLC)ocm0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 035: OCLC number normalization (ocn)
    (['035 ## $a(OCoLC)ocn0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 035: OCLC number normalization (on)
    (['035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001 and 035 interaction

    # 001 and 035: Duplicate OCLC numbers
    (['001 194068',
      '003 OCoLC',
      '035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001 and 035: Not-duplicate OCLC numbers
    (['001 123456',
      '003 OCoLC',
      '035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068', '123456 [Invalid]'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', '123456'],
        'control_numbers_search': ['194068', '123456'],
    }),

    # 001 and 035: Duplicate OCLC numbers, but one has provider suffix
    (['001 194068/springer',
      '003 OCoLC',
      '035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068', '194068/springer'],
    }),

    # 001 and 035: Duplicate invalid OCLC numbers
    (['001 12345',
      '003 OCoLC',
      '035 ## $a(OCoLC)on0194068',
      '035 ## $z(OCoLC)on12345'], {
        'oclc_numbers_display': ['194068', '12345 [Invalid]'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', '12345'],
        'control_numbers_search': ['194068', '12345'],
    }),

    # 001 and 035: Not-duplicates, only 035 is invalid
    # Notes:
    #   - This is an edge case that should never actually happen due
    #     to the way we maintain OCLC holdings.
    #   - The `search` value is transposed because, during processing,
    #     we defer parsing of the 001 until the end. In this case, the
    #     001 is forced DISPLAY before invalid numbers, but the search
    #     value is left alone because it does not matter.
    (['001 194068',
      '003 OCoLC',
      '035 ## $z(OCoLC)on12345'], {
        'oclc_numbers_display': ['194068', '12345 [Invalid]'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', '12345'],
        'control_numbers_search': ['12345', '194068'],
    }),

    # 001 and 035: Not-duplicates, 035 is not OCLC
    (['001 194068',
      '003 OCoLC',
      '035 ## $a(YBP)ybp12345'], {
        'oclc_numbers_display': ['194068'],
        'other_control_numbers_display': ['ybp12345 (source: YBP)'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', 'ybp12345'],
        'control_numbers_search': ['ybp12345', '194068'],
    }),

    # 001 and 035: Not-duplicates, 001 is not OCLC
    (['001 ybp12345',
      '003 YBP',
      '035 ## $a(OCoLC)194068'], {
        'oclc_numbers_display': ['194068'],
        'other_control_numbers_display': ['ybp12345 (source: YBP)'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', 'ybp12345'],
        'control_numbers_search': ['194068', 'ybp12345'],
    }),

], ids=[
    # Edge cases
    'No data ==> no control numbers',
    'ONLY an 003 ==> no control numbers',

    # 001 (Control number -- may or may not be OCLC)
    '001: Plain number is OCLC number',
    '001: OCLC number w/prefix (ocm)',
    '001: OCLC number w/prefix (ocn)',
    '001: OCLC number w/prefix (on)',
    '001: OCLC number w/prefix and leading zeros',
    '001: OCLC number w/leading zeros',
    '001: OCLC number w/provider suffix',
    '001: Non-OCLC vendor number',
    '001: Non-OCLC vendor number with no 003',
    '001: OCLC number w/incorrect 003',

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

    # 001 and 035 interaction
    '001 and 035: Duplicate OCLC numbers',
    '001 and 035: Not-duplicate OCLC numbers',
    '001 and 035: Duplicate OCLC numbers, but one has provider suffix',
    '001 and 035: Duplicate invalid OCLC numbers',
    '001 and 035: Not-duplicates, only 035 is invalid',
    '001 and 035: Not-duplicates, 035 is not OCLC',
    '001 and 035: Not-duplicates, 001 is not OCLC'
])
def test_todscpipeline_getcontrolnumberinfo(raw_marcfields, expected,
                                            bl_sierra_test_record,
                                            todsc_pipeline_class,
                                            bibrecord_to_pymarc,
                                            add_marc_fields,
                                            fieldstrings_to_fields,
                                            assert_bundle_matches_expected):
    """
    The `ToDiscoverPipeline.get_control_number_info` method should
    return the expected values given the provided `marcfields`.
    """
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('001', '003', '010', '016', '035')
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, ['control_number_info'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('raw_marcfields, expected', [
    (['592 ## $aa1'], {
        'games_ages_facet': ['001-001!1 year'],
    }),
    (['592 ## $aa2'], {
        'games_ages_facet': ['002-002!2 years'],
    }),
    (['592 ## $aa1t4'], {
        'games_ages_facet': ['001-004!1 to 4 years'],
    }),
    (['592 ## $aa5t9'], {
        'games_ages_facet': ['005-009!5 to 9 years'],
    }),
    (['592 ## $aa14t16'], {
        'games_ages_facet': ['014-016!14 to 16 years'],
    }),
    (['592 ## $aa17t100'], {
        'games_ages_facet': ['017-100!17 years and up'],
    }),
    (['592 ## $aa1t100'], {
        'games_ages_facet': ['001-100!1 year and up'],
    }),
    (['592 ## $ap1'], {
        'games_players_facet': ['01-01!1 player'],
    }),
    (['592 ## $ap2to4'], {
        'games_players_facet': ['02-04!2 to 4 players'],
    }),
    (['592 ## $ap4to8'], {
        'games_players_facet': ['04-08!4 to 8 players'],
    }),
    (['592 ## $ap9to99'], {
        'games_players_facet': ['09-99!more than 8 players'],
    }),
    (['592 ## $ap2to99'], {
        'games_players_facet': ['02-99!more than 1 player'],
    }),
    (['592 ## $ap1to99'], {
        'games_players_facet': ['01-99!more than 0 players'],
    }),
    (['592 ## $ad1to29'], {
        'games_duration_facet': ['001-029!less than 30 minutes'],
    }),
    (['592 ## $ad30to59'], {
        'games_duration_facet': ['030-059!30 minutes to 1 hour'],
    }),
    (['592 ## $ad60to120'], {
        'games_duration_facet': ['060-120!1 to 2 hours'],
    }),
    (['592 ## $ad120to500'], {
        'games_duration_facet': ['120-500!more than 2 hours'],
    }),
    (['592 ## $ad180to500'], {
        'games_duration_facet': ['180-500!more than 3 hours'],
    }),
    (['592 ## $aa1t4;a5t9;d120t500;p1'], {
        'games_ages_facet': ['001-004!1 to 4 years',
                             '005-009!5 to 9 years'],
        'games_duration_facet': ['120-500!more than 2 hours'],
        'games_players_facet': ['01-01!1 player'],
    }),
    (['592 ## $aa1t4;a5t9;d120t500;p1;'], {
        'games_ages_facet': ['001-004!1 to 4 years',
                             '005-009!5 to 9 years'],
        'games_duration_facet': ['120-500!more than 2 hours'],
        'games_players_facet': ['01-01!1 player'],
    }),
    (['592 ## $aa1t4;a5t9',
      '592 ## $ad120t500',
      '592 ## $ap1'], {
        'games_ages_facet': ['001-004!1 to 4 years',
                             '005-009!5 to 9 years'],
        'games_duration_facet': ['120-500!more than 2 hours'],
        'games_players_facet': ['01-01!1 player'],
    })
])
def test_todscpipeline_getgamesfacetsinfo(raw_marcfields, expected,
                                          bl_sierra_test_record,
                                          get_or_make_location_instances,
                                          update_test_bib_inst,
                                          todsc_pipeline_class,
                                          bibrecord_to_pymarc,
                                          add_marc_fields,
                                          fieldstrings_to_fields,
                                          assert_bundle_matches_expected):
    """
    The `ToDiscoverPipeline.get_games_facets_info` method should
    return the expected values given the provided `raw_marcfields`.
    """
    bib = bl_sierra_test_record('bib_no_items')
    czm = [{'code': 'czm', 'name': 'Chilton Media Library'}]
    czm_instance = get_or_make_location_instances(czm)
    bib = update_test_bib_inst(bib, locations=czm_instance)
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('592')
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    pipeline = todsc_pipeline_class()
    bundle = pipeline.do(bib, bibmarc, ['games_facets_info'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('raw_marcfields, expected', [
    # Edge cases -- empty / missing fields, etc.

    # No 6XXs => empty subject_info
    (['100 1#$aChurchill, Winston,$cSir,$d1874-1965.'], {}),

    # Empty 600 field => empty subjects_info
    (['600 ##$a'], {}),

    # Empty 650 field => empty subjects_info
    (['650 ##$a'], {}),

    # Empty 655 field => empty subjects_info
    (['655 ##$a'], {}),

    # 650 with relator but no heading => empty subjects_info
    (['650 ##$edepicted'], {}),

    # $0 should be ignored (600)
    (['600 1#$aChurchill, Winston,$cSir,$d1874-1965.$0http://example.com'], {
        'subject_headings_json': {
            'p': [{'d': 'Churchill, Winston, Sir, 1874-1965',
                   'v': 'churchill-winston-sir-1874-1965!'
                        'Churchill, Winston, Sir, 1874-1965'}]
        },
        'subject_heading_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965'
        ],
        'topic_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965'
        ],
        'subjects_search_exact_headings': [
            'Churchill, Winston, Sir, 1874-1965'
        ],
        'subjects_search_main_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill'
        ],
        'subjects_search_all_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill'
        ]
    }),

    # 600 with name but empty $t => ignore empty $t
    (['600 1#$aChurchill, Winston,$cSir,$d1874-1965.$t'], {
        'subject_headings_json': {
            'p': [{'d': 'Churchill, Winston, Sir, 1874-1965',
                   'v': 'churchill-winston-sir-1874-1965!'
                        'Churchill, Winston, Sir, 1874-1965'}]
        },
        'subject_heading_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965'
        ],
        'topic_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965'
        ],
        'subjects_search_exact_headings': [
            'Churchill, Winston, Sir, 1874-1965'
        ],
        'subjects_search_main_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill'
        ],
        'subjects_search_all_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill'
        ]
    }),

    # Tests for different kinds of main terms (different field types)
    # 600, name (no title)
    (['600 00$aElijah,$c(Biblical prophet)'], {
        'subject_headings_json': {
            'p': [{'d': 'Elijah, (Biblical prophet)',
                   'v': 'elijah-biblical-prophet!'
                        'Elijah, (Biblical prophet)'}]
        },
        'subject_heading_facet': [
            'elijah-biblical-prophet!'
            'Elijah, (Biblical prophet)'
        ],
        'topic_facet': [
            'elijah-biblical-prophet!'
            'Elijah, (Biblical prophet)'
        ],
        'subjects_search_exact_headings': [
            'Elijah, (Biblical prophet)'
        ],
        'subjects_search_main_terms': [
            'Elijah',
            'Elijah Biblical prophet',
            'Elijah, Biblical prophet'
        ],
        'subjects_search_all_terms': [
            'Elijah',
            'Elijah Biblical prophet',
            'Elijah, Biblical prophet'
        ]
    }),

    # 600, name/title -- single part title
    (['600 10Surname, Forename,$d1900-2000$tSingle title'], {
        'subject_headings_json': {
            'p': [{'d': 'Surname, Forename, 1900-2000',
                   'v': 'surname-forename-1900-2000!'
                        'Surname, Forename, 1900-2000',
                   's': ' > '},
                  {'d': 'Single title',
                   'v': 'surname-forename-1900-2000-single-title!'
                        'Surname, Forename, 1900-2000 > Single title'}]
        },
        'subject_heading_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'surname-forename-1900-2000-single-title!'
            'Surname, Forename, 1900-2000 > Single title'
        ],
        'topic_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'single-title!Single title'
        ],
        'subjects_search_exact_headings': [
            'Surname, Forename, 1900-2000 > Single title',
        ],
        'subjects_search_main_terms': [
            'Single title'
        ],
        'subjects_search_all_terms': [
            'Single title',
            'Surname, Forename Surname, F Surname',
            'Forename Surname',
            'Forename Surname'
        ]
    }),

    # 600, name/title -- multi-part title
    (['600 10Surname, Forename,$d1900-2000$tMulti-title$pPart 1'], {
        'subject_headings_json': {
            'p': [{'d': 'Surname, Forename, 1900-2000',
                   'v': 'surname-forename-1900-2000!'
                        'Surname, Forename, 1900-2000',
                   's': ' > '},
                  {'d': 'Multi-title',
                   'v': 'surname-forename-1900-2000-multi-title!'
                        'Surname, Forename, 1900-2000 > Multi-title',
                   's': ' > '},
                  {'d': 'Part 1',
                   'v': 'surname-forename-1900-2000-multi-title-part-1!'
                        'Surname, Forename, 1900-2000 > Multi-title > Part 1'}]
        },
        'subject_heading_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'surname-forename-1900-2000-multi-title!'
            'Surname, Forename, 1900-2000 > Multi-title',
            'surname-forename-1900-2000-multi-title-part-1!'
            'Surname, Forename, 1900-2000 > Multi-title > Part 1',
        ],
        'topic_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'multi-title!Multi-title',
            'multi-title-part-1!Multi-title > Part 1'
        ],
        'subjects_search_exact_headings': [
            'Surname, Forename, 1900-2000 > Multi-title > Part 1',
        ],
        'subjects_search_main_terms': [
            'Multi-title > Part 1'
        ],
        'subjects_search_all_terms': [
            'Multi-title > Part 1',
            'Surname, Forename Surname, F Surname',
            'Forename Surname',
            'Forename Surname'
        ]
    }),

    # 600, name/title -- collective title
    (['600 10Surname, Forename,$d1900-2000$tWorks'], {
        'subject_headings_json': {
            'p': [{'d': 'Surname, Forename, 1900-2000',
                   'v': 'surname-forename-1900-2000!'
                        'Surname, Forename, 1900-2000',
                   's': ' > '},
                  {'d': 'Works (Complete)',
                   'v': 'surname-forename-1900-2000-works-complete!'
                        'Surname, Forename, 1900-2000 > Works (Complete)'}]
        },
        'subject_heading_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'surname-forename-1900-2000-works-complete!'
            'Surname, Forename, 1900-2000 > Works (Complete)'
        ],
        'topic_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'works-of-surname-f-complete!Works [of Surname, F.] (Complete)'
        ],
        'subjects_search_exact_headings': [
            'Surname, Forename, 1900-2000 > Works (Complete)',
        ],
        'subjects_search_main_terms': [
            'Works (Complete)'
        ],
        'subjects_search_all_terms': [
            'Works (Complete)',
            'Surname, Forename Surname, F Surname',
            'Forename Surname',
            'Forename Surname'
        ]
    }),

    # 600, name/title -- multi-part musical work title
    (['600 10Surname, Forename,$d1900-2000$tSymphonies,$nno. 4, op. 98,'
      '$rE minor$pAndante moderato'], {
        'subject_headings_json': {
            'p': [{'d': 'Surname, Forename, 1900-2000',
                   'v': 'surname-forename-1900-2000!'
                        'Surname, Forename, 1900-2000',
                   's': ' > '},
                  {'d': 'Symphonies',
                   'v': 'surname-forename-1900-2000-symphonies!'
                        'Surname, Forename, 1900-2000 > Symphonies',
                   's': ' > '},
                  {'d': 'No. 4, op. 98, E minor',
                   'v': 'surname-forename-1900-2000-symphonies-'
                        'no-4-op-98-e-minor!'
                        'Surname, Forename, 1900-2000 > Symphonies > '
                        'No. 4, op. 98, E minor',
                   's': ' > '},
                  {'d': 'Andante moderato',
                   'v': 'surname-forename-1900-2000-symphonies-'
                        'no-4-op-98-e-minor-andante-moderato!'
                        'Surname, Forename, 1900-2000 > Symphonies > '
                        'No. 4, op. 98, E minor > Andante moderato'}
                   ]
        },
        'subject_heading_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'surname-forename-1900-2000-symphonies!'
            'Surname, Forename, 1900-2000 > Symphonies',
            'surname-forename-1900-2000-symphonies-no-4-op-98-e-minor!'
            'Surname, Forename, 1900-2000 > Symphonies > '
            'No. 4, op. 98, E minor',
            'surname-forename-1900-2000-symphonies-'
            'no-4-op-98-e-minor-andante-moderato!'
            'Surname, Forename, 1900-2000 > Symphonies > '
            'No. 4, op. 98, E minor > Andante moderato'
        ],
        'topic_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'symphonies-by-surname-f!Symphonies [by Surname, F.]',
            'symphonies-by-surname-f-no-4-op-98-e-minor!'
            'Symphonies [by Surname, F.] > No. 4, op. 98, E minor',
            'symphonies-by-surname-f-no-4-op-98-e-minor-andante-moderato!'
            'Symphonies [by Surname, F.] > No. 4, op. 98, E minor > '
            'Andante moderato',
        ],
        'subjects_search_exact_headings': [
            'Surname, Forename, 1900-2000 > Symphonies > '
            'No. 4, op. 98, E minor > Andante moderato',
        ],
        'subjects_search_main_terms': [
            'Symphonies > No. 4, op. 98, E minor > Andante moderato'
        ],
        'subjects_search_all_terms': [
            'Symphonies > No. 4, op. 98, E minor > Andante moderato',
            'Surname, Forename Surname, F Surname',
            'Forename Surname',
            'Forename Surname'
        ]
    }),

    # 610, name (no title)
    (['610 10$aUnited States.$bArmy.'], {
        'subject_headings_json': {
            'p': [{'d': 'United States Army',
                   'v': 'united-states-army!United States Army'}]
        },
        'subject_heading_facet': [
            'united-states-army!United States Army'
        ],
        'topic_facet': [
            'united-states-army!United States Army'
        ],
        'subjects_search_exact_headings': [
            'United States Army'
        ],
        'subjects_search_main_terms': [
            'United States Army'
        ],
        'subjects_search_all_terms': [
            'United States Army'
        ]
    }),

    # 610, name (no title) -- multi-level org
    (['610 10$aUnited States.$bArmy.$bCavalry, 7th.$bCompany E.'], {
        'subject_headings_json': {
            'p': [{'d': 'United States Army',
                   'v': 'united-states-army!United States Army',
                   's': ' > '},
                  {'d': 'Cavalry, 7th',
                   'v': 'united-states-army-cavalry-7th!'
                        'United States Army > Cavalry, 7th',
                   's': ' > '},
                  {'d': 'Company E.',
                   'v': 'united-states-army-cavalry-7th-company-e!'
                        'United States Army > Cavalry, 7th > Company E.'}
                 ]
        },
        'subject_heading_facet': [
            'united-states-army!United States Army',
            'united-states-army-cavalry-7th!United States Army > Cavalry, 7th',
            'united-states-army-cavalry-7th-company-e!'
            'United States Army > Cavalry, 7th > Company E.'
        ],
        'topic_facet': [
            'united-states-army!United States Army',
            'united-states-army-cavalry-7th!United States Army > Cavalry, 7th',
            'united-states-army-cavalry-7th-company-e!'
            'United States Army > Cavalry, 7th > Company E.'
        ],
        'subjects_search_exact_headings': [
            'United States Army > Cavalry, 7th > Company E.'
        ],
        'subjects_search_main_terms': [
            'United States Army > Cavalry, 7th > Company E.'
        ],
        'subjects_search_all_terms': [
            'United States Army > Cavalry, 7th > Company E.'
        ]
    }),

    # 610, name (no title) -- org + meeting
    (['610 10$aUnited States.$bArmy.$bConvention$d(1962).'], {
        'subject_headings_json': {
            'p': [{'d': 'United States Army',
                   'v': 'united-states-army!United States Army',
                   's': ' > '},
                  {'d': 'Convention',
                   'v': 'united-states-army-convention!'
                        'United States Army > Convention'},
                  {'d': '(1962)',
                   'v': 'united-states-army-convention-1962!'
                        'United States Army > Convention (1962)'}
                 ]
        },
        'subject_heading_facet': [
            'united-states-army!United States Army',
            'united-states-army-convention!United States Army > Convention',
            'united-states-army-convention-1962!'
            'United States Army > Convention (1962)'
        ],
        'topic_facet': [
            'united-states-army!United States Army',
            'united-states-army-convention!United States Army > Convention',
            'united-states-army-convention-1962!'
            'United States Army > Convention (1962)'
        ],
        'subjects_search_exact_headings': [
            'United States Army > Convention (1962)'
        ],
        'subjects_search_main_terms': [
            'United States Army > Convention (1962)'
        ],
        'subjects_search_all_terms': [
            'United States Army > Convention (1962)'
        ]
    }),

    # 610, name/title
    (['610 10$aUnited States.$bArmy.$bCavalry, 7th.$bCompany E.'
            '$tRules and regulations.'], {
        'subject_headings_json': {
            'p': [{'d': 'United States Army',
                   'v': 'united-states-army!United States Army',
                   's': ' > '},
                  {'d': 'Cavalry, 7th',
                   'v': 'united-states-army-cavalry-7th!'
                        'United States Army > Cavalry, 7th',
                   's': ' > '},
                  {'d': 'Company E.',
                   'v': 'united-states-army-cavalry-7th-company-e!'
                        'United States Army > Cavalry, 7th > Company E.',
                   's': ' > '},
                  {'d': 'Rules and regulations',
                   'v': 'united-states-army-cavalry-7th-company-e-'
                        'rules-and-regulations!'
                        'United States Army > Cavalry, 7th > Company E. > '
                        'Rules and regulations'}
                 ]
        },
        'subject_heading_facet': [
            'united-states-army!United States Army',
            'united-states-army-cavalry-7th!United States Army > Cavalry, 7th',
            'united-states-army-cavalry-7th-company-e!'
            'United States Army > Cavalry, 7th > Company E.',
            'united-states-army-cavalry-7th-company-e-rules-and-regulations!'
            'United States Army > Cavalry, 7th > Company E. > '
            'Rules and regulations'
        ],
        'topic_facet': [
            'united-states-army!United States Army',
            'united-states-army-cavalry-7th!United States Army > Cavalry, 7th',
            'united-states-army-cavalry-7th-company-e!'
            'United States Army > Cavalry, 7th > Company E.',
            'rules-and-regulations!Rules and regulations'
        ],
        'subjects_search_exact_headings': [
            'United States Army > Cavalry, 7th > Company E. > '
            'Rules and regulations'
        ],
        'subjects_search_main_terms': [
            'Rules and regulations',
        ],
        'subjects_search_all_terms': [
            'Rules and regulations',
            'United States Army > Cavalry, 7th > Company E.'
        ]
    }),

    # 611, name (no title)
    (['611 20$aSome Festival$n(1st :$d1985 :$cTexas).'], {
        'subject_headings_json': {
            'p': [{'d': 'Some Festival',
                   'v': 'some-festival!Some Festival'},
                  {'d': '(1st : 1985 : Texas)',
                   'v': 'some-festival-1st-1985-texas!'
                        'Some Festival (1st : 1985 : Texas)'}]
        },
        'subject_heading_facet': [
            'some-festival!Some Festival',
            'some-festival-1st-1985-texas!Some Festival (1st : 1985 : Texas)'
        ],
        'topic_facet': [
            'some-festival!Some Festival',
            'some-festival-1st-1985-texas!Some Festival (1st : 1985 : Texas)'
        ],
        'subjects_search_exact_headings': [
            'Some Festival (1st : 1985 : Texas)'
        ],
        'subjects_search_main_terms': [
            'Some Festival (1st : 1985 : Texas)'
        ],
        'subjects_search_all_terms': [
            'Some Festival (1st : 1985 : Texas)'
        ]
    }),

    # 611, name (no title) -- multi-level meeting
    (['611 20$aSome Festival$ePlanning meeting$n(1st :$d1985 :$cTexas).'], {
        'subject_headings_json': {
            'p': [{'d': 'Some Festival',
                   'v': 'some-festival!Some Festival',
                   's': ' > '},
                  {'d': 'Planning meeting',
                   'v': 'some-festival-planning-meeting!'
                        'Some Festival > Planning meeting'},
                  {'d': '(1st : 1985 : Texas)',
                   'v': 'some-festival-planning-meeting-1st-1985-texas!'
                        'Some Festival > Planning meeting '
                        '(1st : 1985 : Texas)'}]
        },
        'subject_heading_facet': [
            'some-festival!Some Festival',
            'some-festival-planning-meeting!'
            'Some Festival > Planning meeting',
            'some-festival-planning-meeting-1st-1985-texas!'
            'Some Festival > Planning meeting (1st : 1985 : Texas)'
        ],
        'topic_facet': [
            'some-festival!Some Festival',
            'some-festival-planning-meeting!'
            'Some Festival > Planning meeting',
            'some-festival-planning-meeting-1st-1985-texas!'
            'Some Festival > Planning meeting (1st : 1985 : Texas)'
        ],
        'subjects_search_exact_headings': [
            'Some Festival > Planning meeting (1st : 1985 : Texas)'
        ],
        'subjects_search_main_terms': [
            'Some Festival > Planning meeting (1st : 1985 : Texas)'
        ],
        'subjects_search_all_terms': [
            'Some Festival > Planning meeting (1st : 1985 : Texas)'
        ]
    }),

    # 611, name (no title) -- meeting + org
    (['611 20$aSome Festival$eOrchestra.'], {
        'subject_headings_json': {
            'p': [{'d': 'Some Festival',
                   'v': 'some-festival!Some Festival',
                   's': ' > '},
                  {'d': 'Orchestra',
                   'v': 'some-festival-orchestra!'
                        'Some Festival > Orchestra'}]
        },
        'subject_heading_facet': [
            'some-festival!Some Festival',
            'some-festival-orchestra!Some Festival > Orchestra'
        ],
        'topic_facet': [
            'some-festival!Some Festival',
            'some-festival-orchestra!Some Festival > Orchestra'
        ],
        'subjects_search_exact_headings': [
            'Some Festival > Orchestra'
        ],
        'subjects_search_main_terms': [
            'Some Festival > Orchestra'
        ],
        'subjects_search_all_terms': [
            'Some Festival > Orchestra'
        ]
    }),

    # 611, name/title
    (['611 20$aSome Festival$eOrchestra.$tProgram.'], {
        'subject_headings_json': {
            'p': [{'d': 'Some Festival',
                   'v': 'some-festival!Some Festival',
                   's': ' > '},
                  {'d': 'Orchestra',
                   'v': 'some-festival-orchestra!'
                        'Some Festival > Orchestra',
                   's': ' > '},
                  {'d': 'Program',
                   'v': 'some-festival-orchestra-program!'
                        'Some Festival > Orchestra > Program'}
                 ]
        },
        'subject_heading_facet': [
            'some-festival!Some Festival',
            'some-festival-orchestra!Some Festival > Orchestra',
            'some-festival-orchestra-program!'
            'Some Festival > Orchestra > Program'
        ],
        'topic_facet': [
            'some-festival!Some Festival',
            'some-festival-orchestra!Some Festival > Orchestra',
            'program!Program'
        ],
        'subjects_search_exact_headings': [
            'Some Festival > Orchestra > Program'
        ],
        'subjects_search_main_terms': [
            'Program'
        ],
        'subjects_search_all_terms': [
            'Program',
            'Some Festival > Orchestra'
        ]
    }),

    # 630, title
    (['630 00$aStudio magazine.$pContemporary paintings.'], {
        'subject_headings_json': {
            'p': [{'d': 'Studio magazine',
                   'v': 'studio-magazine!Studio magazine',
                   's': ' > '},
                  {'d': 'Contemporary paintings',
                   'v': 'studio-magazine-contemporary-paintings!'
                        'Studio magazine > Contemporary paintings'}
                 ]
        },
        'subject_heading_facet': [
            'studio-magazine!Studio magazine',
            'studio-magazine-contemporary-paintings!'
            'Studio magazine > Contemporary paintings'
        ],
        'topic_facet': [
            'studio-magazine!Studio magazine',
            'studio-magazine-contemporary-paintings!'
            'Studio magazine > Contemporary paintings'
        ],
        'subjects_search_exact_headings': [
            'Studio magazine > Contemporary paintings'
        ],
        'subjects_search_main_terms': [
            'Studio magazine > Contemporary paintings'
        ],
        'subjects_search_all_terms': [
            'Studio magazine > Contemporary paintings'
        ]
    }),

    # 630, title plus expression info
    (['630 00$aStudio magazine.$pContemporary paintings.$lEnglish.'], {
        'subject_headings_json': {
            'p': [{'d': 'Studio magazine',
                   'v': 'studio-magazine!Studio magazine',
                   's': ' > '},
                  {'d': 'Contemporary paintings',
                   'v': 'studio-magazine-contemporary-paintings!'
                        'Studio magazine > Contemporary paintings',
                   's': ' ('},
                  {'d': 'English',
                   's': ')',
                   'v': 'studio-magazine-contemporary-paintings-english!'
                        'Studio magazine > Contemporary paintings (English)'},
                 ]
        },
        'subject_heading_facet': [
            'studio-magazine!Studio magazine',
            'studio-magazine-contemporary-paintings!'
            'Studio magazine > Contemporary paintings',
            'studio-magazine-contemporary-paintings-english!'
            'Studio magazine > Contemporary paintings (English)'
        ],
        'topic_facet': [
            'studio-magazine!Studio magazine',
            'studio-magazine-contemporary-paintings!'
            'Studio magazine > Contemporary paintings',
            'studio-magazine-contemporary-paintings-english!'
            'Studio magazine > Contemporary paintings (English)'
        ],
        'subjects_search_exact_headings': [
            'Studio magazine > Contemporary paintings (English)'
        ],
        'subjects_search_main_terms': [
            'Studio magazine > Contemporary paintings (English)'
        ],
        'subjects_search_all_terms': [
            'Studio magazine > Contemporary paintings (English)'
        ]
    }),

    # 647, event (local, non-LCSH, non-FAST)
    (['647 #7$aBunker Hill, Battle of$c(Boston, Massachusetts :$d1775)'], {
        'subject_headings_json': {
            'p': [{'d': 'Bunker Hill, Battle of (Boston, Massachusetts : 1775)',
                   'v': 'bunker-hill-battle-of-boston-massachusetts-1775!'
                        'Bunker Hill, Battle of (Boston, Massachusetts : 1775)'}
                 ]
        },
        'subject_heading_facet': [
            'bunker-hill-battle-of-boston-massachusetts-1775!'
            'Bunker Hill, Battle of (Boston, Massachusetts : 1775)',
        ],
        'topic_facet': [
            'bunker-hill-battle-of-boston-massachusetts-1775!'
            'Bunker Hill, Battle of (Boston, Massachusetts : 1775)'
        ],
        'subjects_search_exact_headings': [
            'Bunker Hill, Battle of (Boston, Massachusetts : 1775)'
        ],
        'subjects_search_main_terms': [
            'Bunker Hill, Battle of (Boston, Massachusetts : 1775)'
        ],
        'subjects_search_all_terms': [
            'Bunker Hill, Battle of (Boston, Massachusetts : 1775)'
        ]
    }),

    # 648, chronological term (local, non-LCSH, non-FAST)
    (['648 #7$a1900-1999$2local'], {
        'subject_headings_json': {
            'p': [{'d': '1900-1999',
                   'v': '1900-1999!1900-1999'}]
        },
        'subject_heading_facet': [
            '1900-1999!1900-1999'
        ],
        'era_facet': [
            '1900-1999!1900-1999'
        ],
        'subjects_search_exact_headings': [
            '1900-1999'
        ],
        'subjects_search_main_terms': [
            '1900-1999'
        ],
        'subjects_search_all_terms': [
            '1900-1999'
        ]
    }),

    # 650, topical term (main term only)
    (['650 #0$aAstronauts.'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts'}]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'subjects_search_exact_headings': [
            'Astronauts'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts'
        ]
    }),

    # 651, geographic term (main term only)
    (['651 #0$aKing Ranch (Tex.)'], {
        'subject_headings_json': {
            'p': [{'d': 'King Ranch (Tex.)',
                   'v': 'king-ranch-tex!King Ranch (Tex.)'}]
        },
        'subject_heading_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'region_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'subjects_search_exact_headings': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_main_terms': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_all_terms': [
            'King Ranch (Tex.)'
        ]
    }),

    # 653, uncontrolled keyword term
    (['653 ##$aStamp collecting (United States)'], {
        'subject_headings_json': {
            'p': [{'d': 'Stamp collecting (United States)'}]
        },
        'subjects_search_exact_headings': [
            'Stamp collecting (United States)'
        ],
        'subjects_search_all_terms': [
            'Stamp collecting (United States)'
        ]
    }),

    # 655, genre term (main term only), LCSH
    (['655 #4$aAudiobooks.'], {
        'genre_headings_json': {
            'p': [{'d': 'Audiobooks',
                   'v': 'audiobooks!Audiobooks'}]
        },
        'genre_heading_facet': [
            'audiobooks!Audiobooks'
        ],
        'genre_facet': [
            'audiobooks!Audiobooks'
        ],
        'genres_search_exact_headings': [
            'Audiobooks'
        ],
        'genres_search_main_terms': [
            'Audiobooks'
        ],
        'genres_search_all_terms': [
            'Audiobooks'
        ]
    }),

    # 655, genre term, AAT-style
    (['655 07$ck$bLaminated$cm$bmarblewood$cv$abust.$2aat'], {
        'genre_headings_json': {
            'p': [{'d': 'Laminated marblewood bust',
                   'v': 'laminated-marblewood-bust!Laminated marblewood bust'}]
        },
        'genre_heading_facet': [
            'laminated-marblewood-bust!Laminated marblewood bust'
        ],
        'genre_facet': [
            'laminated-marblewood-bust!Laminated marblewood bust'
        ],
        'genres_search_exact_headings': [
            'Laminated marblewood bust'
        ],
        'genres_search_main_terms': [
            'Laminated marblewood bust'
        ],
        'genres_search_all_terms': [
            'Laminated marblewood bust'
        ]
    }),

    # 656, occupation term, local
    (['656 #7$aAstronauts.$2local'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts'}]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'subjects_search_exact_headings': [
            'Astronauts'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts'
        ]
    }),

    # 657, function term, local
    (['657 #7$aAstronauts.$2local'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts'}]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'subjects_search_exact_headings': [
            'Astronauts'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts'
        ]
    }),

    # 690, local topical term
    (['690 #7$aAstronauts.$2local'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts'}]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'subjects_search_exact_headings': [
            'Astronauts'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts'
        ]
    }),

    # 691, local geographic term
    (['691 #7$aKing Ranch (Tex.)$2local'], {
        'subject_headings_json': {
            'p': [{'d': 'King Ranch (Tex.)',
                   'v': 'king-ranch-tex!King Ranch (Tex.)'}]
        },
        'subject_heading_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'region_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'subjects_search_exact_headings': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_main_terms': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_all_terms': [
            'King Ranch (Tex.)'
        ]
    }),

    # 692, defunct heading or other term used for search only
    (['692 #0$aAstronauts.'], {
        'subjects_search_exact_headings': [
            'Astronauts'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts'
        ]
    }),


    # Tests for handling subdivisions
    # $v on a non-genre field => genre
    (['650 #0$aAstronauts$vJuvenile films.'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': 'Juvenile films',
                   'v': 'astronauts-juvenile-films!Astronauts > Juvenile films'}
                 ]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts',
            'astronauts-juvenile-films!Astronauts > Juvenile films'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'genre_facet': [
            'juvenile-films!Juvenile films'
        ],
        'subjects_search_exact_headings': [
            'Astronauts > Juvenile films'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts'
        ],
        'genres_search_all_terms': [
            'Juvenile films'
        ]
    }),

    # $v on a 655 (genre) => genre
    (['655 #4$aAudiobooks$vBiography.'], {
        'genre_headings_json': {
            'p': [{'d': 'Audiobooks',
                   'v': 'audiobooks!Audiobooks',
                   's': ' > '},
                  {'d': 'Biography',
                   'v': 'audiobooks-biography!Audiobooks > Biography'}
                 ]
        },
        'genre_heading_facet': [
            'audiobooks!Audiobooks',
            'audiobooks-biography!Audiobooks > Biography'
        ],
        'genre_facet': [
            'audiobooks!Audiobooks',
            'biography!Biography'
        ],
        'genres_search_exact_headings': [
            'Audiobooks > Biography'
        ],
        'genres_search_main_terms': [
            'Audiobooks'
        ],
        'genres_search_all_terms': [
            'Audiobooks',
            'Biography'
        ]
    }),

    # $x on non-genre field => topic
    (['650 #0$aAstronauts$xHistory.'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': 'History',
                   'v': 'astronauts-history!Astronauts > History'}
                 ]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts',
            'astronauts-history!Astronauts > History'
        ],
        'topic_facet': [
            'astronauts!Astronauts',
            'history!History'
        ],
        'subjects_search_exact_headings': [
            'Astronauts > History'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts',
            'History'
        ]
    }),

    # $x on a 655 (genre) immediately after $ab => compound term
    (['655 #4$aAudiobooks$xFrench.'], {
        'genre_headings_json': {
            'p': [{'d': 'Audiobooks',
                   'v': 'audiobooks!Audiobooks',
                   's': ' > '},
                  {'d': 'French',
                   'v': 'audiobooks-french!Audiobooks > French'}
                 ]
        },
        'genre_heading_facet': [
            'audiobooks!Audiobooks',
            'audiobooks-french!Audiobooks > French'
        ],
        'genre_facet': [
            'audiobooks!Audiobooks',
            'audiobooks-french!Audiobooks, French'
        ],
        'genres_search_exact_headings': [
            'Audiobooks > French'
        ],
        'genres_search_main_terms': [
            'Audiobooks'
        ],
        'genres_search_all_terms': [
            'Audiobooks',
            'French'
        ]
    }),

    # $x on a 655 (genre) after $ab, with sf between => compound term
    (['655 #4$aAudiobooks$y1990-2000$xFrench.'], {
        'genre_headings_json': {
            'p': [{'d': 'Audiobooks',
                   'v': 'audiobooks!Audiobooks',
                   's': ' > '},
                  {'d': '1990-2000',
                   'v': 'audiobooks-1990-2000!Audiobooks > 1990-2000',
                   's': ' > '},
                  {'d': 'French',
                   'v': 'audiobooks-1990-2000-french!'
                        'Audiobooks > 1990-2000 > French'}
                 ]
        },
        'genre_heading_facet': [
            'audiobooks!Audiobooks',
            'audiobooks-1990-2000!Audiobooks > 1990-2000',
            'audiobooks-1990-2000-french!Audiobooks > 1990-2000 > French'
        ],
        'genre_facet': [
            'audiobooks!Audiobooks',
            'audiobooks-french!Audiobooks, French'
        ],
        'era_facet': [
            '1990-2000!1990-2000'
        ],
        'genres_search_exact_headings': [
            'Audiobooks > 1990-2000 > French'
        ],
        'genres_search_main_terms': [
            'Audiobooks'
        ],
        'genres_search_all_terms': [
            'Audiobooks',
            '1990-2000',
            'French'
        ]
    }),

    # $y => era
    (['650 #0$aAstronauts$y20th century.'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': '20th century',
                   'v': 'astronauts-20th-century!Astronauts > 20th century'}
                 ]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts',
            'astronauts-20th-century!Astronauts > 20th century'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'era_facet': [
            '20th-century!20th century'
        ],
        'subjects_search_exact_headings': [
            'Astronauts > 20th century'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts',
            '20th century'
        ]
    }),

    # $z => region
    (['650 #0$aAstronauts$zUnited States.'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': 'United States',
                   'v': 'astronauts-united-states!Astronauts > United States'}
                 ]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts',
            'astronauts-united-states!Astronauts > United States'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'region_facet': [
            'united-states!United States'
        ],
        'subjects_search_exact_headings': [
            'Astronauts > United States'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts',
            'United States'
        ]
    }),

    # SD mapping (simple) -- search term deduplication
    # Note that both of the terms the subdivision maps to are fully
    # contained within the subdivision name, so they are deduplicated
    # when generating search terms (so as not to artificially inflate
    # TF scores for those terms)
    (['650 #0$aChemicals$xAbsorption and adsorption.'], {
        'subject_headings_json': {
            'p': [{'d': 'Chemicals',
                   'v': 'chemicals!Chemicals',
                   's': ' > '},
                  {'d': 'Absorption and adsorption',
                   'v': 'chemicals-absorption-and-adsorption!'
                        'Chemicals > Absorption and adsorption'}
                 ]
        },
        'subject_heading_facet': [
            'chemicals!Chemicals',
            'chemicals-absorption-and-adsorption!'
            'Chemicals > Absorption and adsorption'
        ],
        'topic_facet': [
            'chemicals!Chemicals',
            'absorption!Absorption',
            'adsorption!Adsorption'
        ],
        'subjects_search_exact_headings': [
            'Chemicals > Absorption and adsorption'
        ],
        'subjects_search_main_terms': [
            'Chemicals'
        ],
        'subjects_search_all_terms': [
            'Chemicals',
            'Absorption and adsorption',
        ]
    }),

    # SD mapping (pattern)
    (['650 #0$aChinese language$xTransliteration into English.'], {
        'subject_headings_json': {
            'p': [{'d': 'Chinese language',
                   'v': 'chinese-language!Chinese language',
                   's': ' > '},
                  {'d': 'Transliteration into English',
                   'v': 'chinese-language-transliteration-into-english!'
                        'Chinese language > Transliteration into English'}
                 ]
        },
        'subject_heading_facet': [
            'chinese-language!Chinese language',
            'chinese-language-transliteration-into-english!'
            'Chinese language > Transliteration into English'
        ],
        'topic_facet': [
            'chinese-language!Chinese language',
            'transliteration!Transliteration',
            'english-language!English language'
        ],
        'subjects_search_exact_headings': [
            'Chinese language > Transliteration into English'
        ],
        'subjects_search_main_terms': [
            'Chinese language'
        ],
        'subjects_search_all_terms': [
            'Chinese language',
            'Transliteration into English',
            'English language'
        ]
    }),

    # SD mapping (simple/parents)
    (['650 #0$aSeeds$xCertification.'], {
        'subject_headings_json': {
            'p': [{'d': 'Seeds',
                   'v': 'seeds!Seeds',
                   's': ' > '},
                  {'d': 'Certification',
                   'v': 'seeds-certification!Seeds > Certification'}
                 ]
        },
        'subject_heading_facet': [
            'seeds!Seeds',
            'seeds-certification!Seeds > Certification'
        ],
        'topic_facet': [
            'seeds!Seeds',
            'certification-seeds!Certification (Seeds)'
        ],
        'subjects_search_exact_headings': [
            'Seeds > Certification'
        ],
        'subjects_search_main_terms': [
            'Seeds'
        ],
        'subjects_search_all_terms': [
            # Note: 'Seeds' is not included because it's duplicated
            # in the phrase 'Certification (Seeds)'
            'Certification (Seeds)',
        ]
    }),

    # SD mapping ($v genres)
    (['650 #0$aAstronauts$vJuvenile literature.'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': 'Juvenile literature',
                   'v': 'astronauts-juvenile-literature!'
                        'Astronauts > Juvenile literature'}
                 ]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts',
            'astronauts-juvenile-literature!Astronauts > Juvenile literature'
        ],
        'topic_facet': [
            'astronauts!Astronauts'
        ],
        'genre_facet': [
            "children-s-literature!Children's literature",
            'juvenile-literature!Juvenile literature'
        ],
        'subjects_search_exact_headings': [
            'Astronauts > Juvenile literature'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts'
        ],
        'genres_search_all_terms': [
            "Children's literature",
            'Juvenile literature'
        ]
    }),

    # SD mapping ($y eras)
    (['650 #0$aAstronauts$xHistory$y20th century.'], {
        'subject_headings_json': {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': 'History',
                   'v': 'astronauts-history!Astronauts > History',
                   's': ' > '},
                  {'d': '20th century',
                   'v': 'astronauts-history-20th-century!'
                        'Astronauts > History > 20th century'}
                 ]
        },
        'subject_heading_facet': [
            'astronauts!Astronauts',
            'astronauts-history!Astronauts > History',
            'astronauts-history-20th-century!'
            'Astronauts > History > 20th century'
        ],
        'topic_facet': [
            'astronauts!Astronauts',
            'history!History',
            'history-modern!History, Modern'
        ],
        'era_facet': [
            '20th-century!20th century'
        ],
        'subjects_search_exact_headings': [
            'Astronauts > History > 20th century'
        ],
        'subjects_search_main_terms': [
            'Astronauts'
        ],
        'subjects_search_all_terms': [
            'Astronauts',
            'History, Modern',
            '20th century'
        ]
    }),

    # 600 name-only plus SD
    (['600 1#$aChurchill, Winston,$cSir,$d1874-1965$vFiction.'], {
        'subject_headings_json': {
            'p': [{'d': 'Churchill, Winston, Sir, 1874-1965',
                   'v': 'churchill-winston-sir-1874-1965!'
                        'Churchill, Winston, Sir, 1874-1965',
                   's': ' > '},
                  {'d': 'Fiction',
                   'v': 'churchill-winston-sir-1874-1965-fiction!'
                        'Churchill, Winston, Sir, 1874-1965 > Fiction'}
                 ]
        },
        'subject_heading_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965',
            'churchill-winston-sir-1874-1965-fiction!'
            'Churchill, Winston, Sir, 1874-1965 > Fiction'
        ],
        'topic_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965'
        ],
        'genre_facet': [
            'fiction!Fiction'
        ],
        'subjects_search_exact_headings': [
            'Churchill, Winston, Sir, 1874-1965 > Fiction'
        ],
        'subjects_search_main_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill'
        ],
        'subjects_search_all_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill'
        ],
        'genres_search_all_terms': [
            'Fiction'
        ]
    }),

    # 600 name/title plus SD
    (['600 10$aSurname, Forename,$d1900-2000$tSingle title'
            '$xCriticism and interpretation'], {
        'subject_headings_json': {
            'p': [{'d': 'Surname, Forename, 1900-2000',
                   'v': 'surname-forename-1900-2000!'
                        'Surname, Forename, 1900-2000',
                   's': ' > '},
                  {'d': 'Single title',
                   'v': 'surname-forename-1900-2000-single-title!'
                        'Surname, Forename, 1900-2000 > Single title',
                   's': ' > '},
                  {'d': 'Criticism and interpretation',
                   'v': 'surname-forename-1900-2000-single-title-'
                        'criticism-and-interpretation!'
                        'Surname, Forename, 1900-2000 > Single title > '
                        'Criticism and interpretation'}
                 ]
        },
        'subject_heading_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'surname-forename-1900-2000-single-title!'
            'Surname, Forename, 1900-2000 > Single title',
            'surname-forename-1900-2000-single-title-'
            'criticism-and-interpretation!'
            'Surname, Forename, 1900-2000 > Single title > '
            'Criticism and interpretation'
        ],
        'topic_facet': [
            'surname-forename-1900-2000!'
            'Surname, Forename, 1900-2000',
            'single-title!Single title',
            'criticism-and-interpretation!Criticism and interpretation'
        ],
        'subjects_search_exact_headings': [
            'Surname, Forename, 1900-2000 > Single title > '
            'Criticism and interpretation',
        ],
        'subjects_search_main_terms': [
            'Single title'
        ],
        'subjects_search_all_terms': [
            'Single title',
            'Surname, Forename Surname, F Surname',
            'Forename Surname',
            'Forename Surname',
            'Criticism and interpretation'
        ]
    }),

    # 630 title plus SD
    (['630 00$aStudio magazine.$pContemporary paintings$xHistory'], {
        'subject_headings_json': {
            'p': [{'d': 'Studio magazine',
                   'v': 'studio-magazine!Studio magazine',
                   's': ' > '},
                  {'d': 'Contemporary paintings',
                   'v': 'studio-magazine-contemporary-paintings!'
                        'Studio magazine > Contemporary paintings',
                   's': ' > '},
                  {'d': 'History',
                   'v': 'studio-magazine-contemporary-paintings-history!'
                        'Studio magazine > Contemporary paintings > History'}
                 ]
        },
        'subject_heading_facet': [
            'studio-magazine!Studio magazine',
            'studio-magazine-contemporary-paintings!'
            'Studio magazine > Contemporary paintings',
            'studio-magazine-contemporary-paintings-history!'
            'Studio magazine > Contemporary paintings > History'
        ],
        'topic_facet': [
            'studio-magazine!Studio magazine',
            'studio-magazine-contemporary-paintings!'
            'Studio magazine > Contemporary paintings',
            'history!History'
        ],
        'subjects_search_exact_headings': [
            'Studio magazine > Contemporary paintings > History'
        ],
        'subjects_search_main_terms': [
            'Studio magazine > Contemporary paintings'
        ],
        'subjects_search_all_terms': [
            'Studio magazine > Contemporary paintings',
            'History'
        ]
    }),


    # Relator tests
    # Relators appear as json['r']
    (['651 #0$aKing Ranch (Tex.),$edepicted'], {
        'subject_headings_json': {
            'p': [{'d': 'King Ranch (Tex.)',
                   'v': 'king-ranch-tex!King Ranch (Tex.)'}],
            'r': ['depicted']
        },
        'subject_heading_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'region_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'subjects_search_exact_headings': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_main_terms': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_all_terms': [
            'King Ranch (Tex.)'
        ]
    }),

    # Multiple relators are deduplicated
    (['651 #0$aKing Ranch (Tex.),$edepicted$4dpc'], {
        'subject_headings_json': {
            'p': [{'d': 'King Ranch (Tex.)',
                   'v': 'king-ranch-tex!King Ranch (Tex.)'}],
            'r': ['depicted']
        },
        'subject_heading_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'region_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'subjects_search_exact_headings': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_main_terms': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_all_terms': [
            'King Ranch (Tex.)'
        ]
    }),

    # URIs in $4 do not display
    (['651 #0$aKing Ranch (Tex.),$edepicted$4http://example.com'], {
        'subject_headings_json': {
            'p': [{'d': 'King Ranch (Tex.)',
                   'v': 'king-ranch-tex!King Ranch (Tex.)'}],
            'r': ['depicted']
        },
        'subject_heading_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'region_facet': [
            'king-ranch-tex!King Ranch (Tex.)'
        ],
        'subjects_search_exact_headings': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_main_terms': [
            'King Ranch (Tex.)'
        ],
        'subjects_search_all_terms': [
            'King Ranch (Tex.)'
        ]
    }),


    # Multi-field tests
    # Multiple 6XXs, genres, etc.
    (['600 1#$aChurchill, Winston,$cSir,$d1874-1965$vFiction.',
      '650 #0$aAstronauts$xHistory$y20th century.',
      '650 #0$aAstronauts$vJuvenile films.',
      '655 #4$aAudiobooks$xFrench.',
      '692 #0$aSpacemen.'
      ], {
        'subject_headings_json': [{
            'p': [{'d': 'Churchill, Winston, Sir, 1874-1965',
                   'v': 'churchill-winston-sir-1874-1965!'
                        'Churchill, Winston, Sir, 1874-1965',
                   's': ' > '},
                  {'d': 'Fiction',
                   'v': 'churchill-winston-sir-1874-1965-fiction!'
                        'Churchill, Winston, Sir, 1874-1965 > Fiction'},
                 ]
        }, {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': 'History',
                   'v': 'astronauts-history!Astronauts > History',
                   's': ' > '},
                  {'d': '20th century',
                   'v': 'astronauts-history-20th-century!'
                        'Astronauts > History > 20th century'}
                 ]
        }, {
            'p': [{'d': 'Astronauts',
                   'v': 'astronauts!Astronauts',
                   's': ' > '},
                  {'d': 'Juvenile films',
                   'v': 'astronauts-juvenile-films!Astronauts > Juvenile films'}
                 ]
        }],
        'genre_headings_json': [{
            'p': [{'d': 'Audiobooks',
                   'v': 'audiobooks!Audiobooks',
                   's': ' > '},
                  {'d': 'French',
                   'v': 'audiobooks-french!Audiobooks > French'}
                 ]
        }],
        'subject_heading_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965',
            'churchill-winston-sir-1874-1965-fiction!'
            'Churchill, Winston, Sir, 1874-1965 > Fiction',
            'astronauts!Astronauts',
            'astronauts-history!Astronauts > History',
            'astronauts-history-20th-century!'
            'Astronauts > History > 20th century',
            'astronauts-juvenile-films!Astronauts > Juvenile films'
        ],
        'genre_heading_facet': [
            'audiobooks!Audiobooks',
            'audiobooks-french!Audiobooks > French'
        ],
        'topic_facet': [
            'churchill-winston-sir-1874-1965!'
            'Churchill, Winston, Sir, 1874-1965',
            'astronauts!Astronauts',
            'history!History',
            'history-modern!History, Modern'
        ],
        'genre_facet': [
            'fiction!Fiction',
            'juvenile-films!Juvenile films',
            'audiobooks!Audiobooks',
            'audiobooks-french!Audiobooks, French'
        ],
        'era_facet': [
            '20th-century!20th century'
        ],
        'subjects_search_exact_headings': [
            'Churchill, Winston, Sir, 1874-1965 > Fiction',
            'Astronauts > History > 20th century',
            'Astronauts > Juvenile films',
            'Spacemen'
        ],
        'genres_search_exact_headings': [
            'Audiobooks > French'
        ],
        'subjects_search_main_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill',
            'Astronauts',
            'Spacemen'
        ],
        'genres_search_main_terms': [
            'Audiobooks'
        ],
        'subjects_search_all_terms': [
            'Churchill, Winston Churchill, W Churchill',
            'Sir Winston Churchill',
            'Sir Winston Churchill',
            'Astronauts',
            'History, Modern',
            '20th century',
            'Spacemen'
        ],
        'genres_search_all_terms': [
            'Fiction',
            'Juvenile films',
            'Audiobooks',
            'French'
        ]
    }),

    # Duplicate headings are deduplicated
    # This example illustrates a few things. 1) FAST headings in
    # 600-630 fields are ignored completely. 2) Exactly duplicate
    # headings are ignored. 3) For headings such as:
    #     "Military education -- History" and "Military education"
    # The latter currently is not considered a duplicate of the former,
    # even though it is redundant. (This behavior is subject to
    # change.)
    (['610 10$aUnited States.$bArmy.',
      '610 17$aUnited States.$bArmy.$2fast',
      '610 10$aUnited States.$bArmy.$xHistory.',
      '650 #0$aMilitary uniforms.',
      '650 #7$aMilitary uniforms.$2fast',
      '650 #0$aMilitary education$xHistory',
      '650 #0$aMilitary education$2fast',
      '655 07$aHistory.$2fast',
      ], {
        'subject_headings_json': [{
            'p': [{'d': 'United States Army',
                   'v': 'united-states-army!United States Army'},
                 ]
        }, {
            'p': [{'d': 'United States Army',
                   'v': 'united-states-army!United States Army',
                   's': ' > '},
                  {'d': 'History',
                   'v': 'united-states-army-history!'
                        'United States Army > History'}
                 ]
        }, {
            'p': [{'d': 'Military uniforms',
                   'v': 'military-uniforms!Military uniforms'}
                 ]
        }, {
            'p': [{'d': 'Military education',
                   'v': 'military-education!Military education',
                   's': ' > '},
                  {'d': 'History',
                   'v': 'military-education-history!'
                        'Military education > History'}
                 ]
        }, {
            'p': [{'d': 'Military education',
                   'v': 'military-education!Military education'}
                 ]
        }],
        'genre_headings_json': [{
            'p': [{'d': 'History',
                   'v': 'history!History'}
                 ]

        }],
        'subject_heading_facet': [
            'united-states-army!United States Army',
            'united-states-army-history!United States Army > History',
            'military-uniforms!Military uniforms',
            'military-education!Military education',
            'military-education-history!Military education > History'
        ],
        'genre_heading_facet': [
            'history!History'
        ],
        'topic_facet': [
            'united-states-army!United States Army',
            'history!History',
            'military-uniforms!Military uniforms',
            'military-education!Military education'
        ],
        'genre_facet': [
            'history!History'
        ],
        'subjects_search_exact_headings': [
            'United States Army',
            'United States Army > History',
            'Military uniforms',
            'Military education > History',
            'Military education'
        ],
        'genres_search_exact_headings': [
            'History'
        ],
        'subjects_search_main_terms': [
            'United States Army',
            'Military uniforms',
            'Military education'
        ],
        'genres_search_main_terms': [
            'History'
        ],
        'subjects_search_all_terms': [
            'United States Army',
            'History',
            'Military uniforms',
            'Military education'
        ],
        'genres_search_all_terms': [
            'History'
        ]
    }),
], ids=[
    # Edge cases
    'No 6XXs => empty subjects_info',
    'Empty 600 field => empty subjects_info',
    'Empty 650 field => empty subjects_info',
    'Empty 655 field => empty subjects_info',
    '650 with relator but no heading => empty subjects_info',
    '$0 should be ignored (600)',
    '600 with name but empty $t => ignore empty $t',

    # Tests for different kinds of main terms (different field types)
    '600, name (no title)',
    '600, name/title -- single part title',
    '600, name/title -- multi-part title',
    '600, name/title -- collective title',
    '600, name/title -- multi-part musical work title',
    '610, name (no title)',
    '610, name (no title) -- multi-level org',
    '610, name (no title) -- org + meeting',
    '610, name/title',
    '611, name (no title)',
    '611, name (no title) -- multi-level meeting',
    '611, name (no title) -- meeting + org',
    '611, name/title',
    '630, title',
    '630, title plus expression info',
    '647, event (local, non-LCSH, non-FAST)',
    '648, chronological term (local, non-LCSH, non-FAST)',
    '650, topical term (main term only)',
    '651, geographic term (main term only)',
    '653, uncontrolled keyword term',
    '655, genre term (main term only), LCSH',
    '655, genre term, AAT-style',
    '656, occupation term, local',
    '657, function term, local',
    '690, local topical term',
    '691, local geographic term',
    '692, defunct heading or other term used for search only',

    # Tests for handling subdivisions
    '$v on a non-genre field => genre',
    '$v on a 655 (genre) => genre',
    '$x on non-genre field => topic',
    '$x on a 655 (genre) immediately after $ab => compound term',
    '$x on a 655 (genre) after $ab, with sf between => compound term',
    '$y => era',
    '$z => region',
    'SD mapping (simple) -- search term deduplication',
    'SD mapping (pattern)',
    'SD mapping (simple/parents)',
    'SD mapping ($v genres)',
    'SD mapping ($y eras)',
    '600 name-only plus SD',
    '600 name/title plus SD',
    '630 title plus SD',

    # Relator tests
    "Relators appear as json['r']",
    'Multiple relators are deduplicated',
    'URIs in $4 do not display',

    # Multi-field tests
    'Multiple 6XXs, genres, etc.',
    'Duplicate headings are deduplicated',
])
def test_todscpipeline_getsubjectsinfo(raw_marcfields, expected,
                                       fieldstrings_to_fields,
                                       bl_sierra_test_record,
                                       todsc_pipeline_class,
                                       marcutils_for_subjects,
                                       bibrecord_to_pymarc,
                                       add_marc_fields,
                                       assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_subjects_info should return fields
    matching the expected parameters.
    """
    class SubjectTestBLASMPL(todsc_pipeline_class):
        utils = marcutils_for_subjects

    pipeline = SubjectTestBLASMPL()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('600', '610', '611', '630', '647', '648', '650',
                          '651', '653', '655', '656', '657', '690', '691',
                          '692')
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, ['subjects_info'])
    assert_bundle_matches_expected(bundle, expected, list_order_exact=False)


@pytest.mark.parametrize('this_year, pyears, pdecades, bib_type, expected', [
    (2020, ['2020'], ['2011-2020'], '-', '1000'),
    (2021, ['2020'], ['2011-2020'], '-', '1000'),
    (2020, ['2020'], ['2011-2020'], 'd', '1000'),
    (2020, ['2019'], ['2011-2020'], '-', '999'),
    (2020, ['1920'], ['1911-1920'], '-', '900'),
    (2020, ['1820'], ['1811-1820'], '-', '800'),
    (2020, ['1520'], ['1511-1520'], '-', '501'),
    (2020, ['1420'], ['1411-1420'], '-', '501'),
    (2020, ['2020'], ['2011-2020'], 'a', '500'),
    (2020, ['2020'], ['2011-2020'], 'b', '500'),
    (2020, ['2020'], ['2011-2020'], 'r', '500'),
    (2020, ['2020'], ['2011-2020'], 'p', '500'),
    (2020, ['2020'], ['2011-2020'], 'i', '500'),
    (2020, ['2020'], ['2011-2020'], 's', '500'),
    (2020, ['2020'], ['2011-2020'], 't', '500'),
    (2020, ['2020'], ['2011-2020'], 'z', '500'),
    (2020, ['2020'], ['2011-2020'], '0', '500'),
    (2020, ['2020'], ['2011-2020'], '2', '500'),
    (2020, ['2020'], ['2011-2020'], '4', '500'),
    (2020, ['2019'], ['2011-2020'], 'a', '499'),
    (2020, ['1920'], ['1911-1920'], 'a', '400'),
    (2020, ['1820'], ['1811-1820'], 'a', '300'),
    (2020, ['1520'], ['1511-1520'], 'a', '1'),
    (2020, ['1420'], ['1411-1420'], 'a', '1'),
    (2020, [''], [''], 'a', '460'),
    (2020, None, None, 'a', '460'),
    (2020, None, None, '-', '960'),
    (2020, ['9999'], ['9991-9999'], '-', '960'),
    (2020, ['9999'], ['9991-9999'], '-', '960'),
    (2020, ['2021'], ['2021-2030'], '-', '1001'),
    (2020, ['2022'], ['2021-2030'], '-', '1002'),
    (2020, ['2023'], ['2021-2030'], '-', '1003'),
    (2020, ['2024'], ['2021-2030'], '-', '1004'),
    (2020, ['2025'], ['2021-2030'], '-', '1005'),
    (2020, ['2026'], ['2021-2030'], '-', '960'),
    (2020, ['2026', '2025'], ['2021-2030'], '-', '1005'),
    (2020, ['1993', '1982'], ['1981-1990', '1991-2000'], '-', '973'),
    (2020, None, ['1981-1990', '1991-2000'], '-', '975'),
    (2020, ['9999', '2020'], ['9991-9999', '2011-2020'], '-', '1000'),
])
def test_todscpipeline_getrecordboost(this_year, pyears, pdecades, bib_type,
                                      expected, bl_sierra_test_record,
                                      bibrecord_to_pymarc, todsc_pipeline_class,
                                      setattr_model_instance):
    pipeline = todsc_pipeline_class()
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    pipeline.bundle['publication_year_facet'] = pyears
    pipeline.bundle['publication_decade_facet'] = pdecades
    pipeline.this_year = this_year
    setattr_model_instance(bib, 'bcode1', bib_type)
    bundle = pipeline.do(bib, bibmarc, ['record_boost'], False)
    assert bundle['record_boost'] == expected


@pytest.mark.parametrize('marc_tags_ind, sf_i, equals, test_value', [
    # General behavior
    # Most fields: use $i when ind2 is 8 and $i exists
    (['765 #8', '767 #8', '770 #8', '772 #8', '773 #8', '774 #8', '775 #8',
      '776 #8', '777 #8', '786 #8', '787 #8'],
     'Custom label:', True, 'Custom label'),

    # Most fields: no display label when ind2 is 8 and no $i exists
    (['765 #8', '767 #8', '770 #8', '772 #8', '773 #8', '774 #8', '775 #8',
      '776 #8', '777 #8', '786 #8', '787 #8'],
     None, True, None),

    # All fields: ignore $i when ind2 is not 8
    (['760 ##', '762 ##', '765 ##', '767 ##', '770 ##', '772 ##', '773 ##',
      '774 ##', '775 ##', '776 ##', '777 ##', '780 ##', '785 ##', '786 ##',
      '787 ##'],
     'Custom label:', False, 'Custom label'),

    # All fields: no display label when ind2 is outside the valid range
    (['760 #0', '762 #0', '765 #0', '767 #0', '770 #0', '772 #1', '773 #0',
      '774 #0', '775 #0', '776 #0', '777 #0', '780 #9', '785 #9', '786 #0',
      '787 #0'],
     None, True, None),

    # Exceptions to general behavior
    # Certain fields: no display label when ind2 is blank
    (['760 ##', '762 ##', '774 ##', '787 ##'],
     None, True, None),

    # Certain fields: ignore $i even when ind2 is 8 and $i exists
    (['760 #8', '762 #8', '780 #8', '785 #8'],
     'Custom label:', False, 'Custom label'),

    # Field-specific labels
    (['765 ##'], None, True, 'Translation of'),
    (['767 ##'], None, True, 'Translated as'),
    (['770 ##'], None, True, 'Supplement'),
    (['772 ##'], None, True, 'Supplement to'),
    (['772 #0'], None, True, 'Parent'),
    (['774 #8'], 'Container of:', True, None),
    (['780 ##'], None, True, None),
    (['780 #0'], None, True, 'Continues'),
    (['780 #1'], None, True, 'Continues in part'),
    (['780 #2'], None, True, 'Supersedes'),
    (['780 #3'], None, True, 'Supersedes in part'),
    (['780 #4'], None, True, 'Merger of'),
    (['780 #5'], None, True, 'Absorbed'),
    (['780 #6'], None, True, 'Absorbed in part'),
    (['780 #7'], None, True, 'Separated from'),
    (['785 ##'], None, True, None),
    (['785 #0'], None, True, 'Continued by'),
    (['785 #1'], None, True, 'Continued in part by'),
    (['785 #2'], None, True, 'Superseded by'),
    (['785 #3'], None, True, 'Superseded in part by'),
    (['785 #4'], None, True, 'Absorbed by'),
    (['785 #5'], None, True, 'Absorbed in part by'),
    (['785 #6'], None, True, 'Split into'),
    (['785 #7'], None, True, 'Merged with'),
    (['785 #8'], None, True, 'Changed back to'),
], ids=[
    # General behavior
    'Most fields: use $i when ind2 is 8 and $i exists',
    'Most fields: no display label when ind2 is 8 and no $i exists',
    'All fields: ignore $i when ind2 is not 8',
    'All fields: no display label when ind2 is outside the valid range',

    # Exceptions to general behavior
    'Certain fields: no display label when ind2 is blank',
    'Certain fields: no display label even when ind2 is 8 and $i exists',

    # Field-specific labels
    '765 ind2 blank => `Translation of`',
    '767 ind2 blank => `Translated as`',
    '770 ind2 blank => `Supplement`',
    '772 ind2 blank => `Supplement to`',
    '772 ind2 0 => `Parent`',
    '774 ind2 8 and $i is "Container of" => No display label',
    '780 ind2 blank => No display label',
    '780 ind2 0: => `Continues`',
    '780 ind2 1: => `Continues in part`',
    '780 ind2 2: => `Supersedes`',
    '780 ind2 3: => `Supersedes in part`',
    '780 ind2 4: => `Merger of`',
    '780 ind2 5: => `Absorbed`',
    '780 ind2 6: => `Absorbed in part`',
    '780 ind2 7: => `Separated from`',
    '785 ind2 blank => No display label',
    '785 ind2 0: => `Continued by`',
    '785 ind2 1: => `Continued in part by`',
    '785 ind2 2: => `Superseded by`',
    '785 ind2 3: => `Superseded in part by`',
    '785 ind2 4: => `Absorbed by`',
    '785 ind2 5: => `Absorbed in part by`',
    '785 ind2 6: => `Split into`',
    '785 ind2 7: => `Merged with`',
    '785 ind2 8: => `Changed back to`',
])
def test_linkingfieldparser_display_labels(marc_tags_ind, sf_i, equals,
                                           test_value, fieldstrings_to_fields):
    """
    For a field constructed using each in the given list of MARC tags
    plus indicators (`marc_tags_ind`), using the given $i value
    (`sf_i`), the `LinkingFieldParser.parse` method will return a dict
    where the display_label entry either `equals` (or does not equal)
    the `test_value`.
    """
    for tag_ind in marc_tags_ind:
        rawfield = tag_ind
        if sf_i:
            rawfield = '{}$i{}'.format(rawfield, sf_i)
        rawfield = '{}$tSample title'.format(rawfield)
        field = fieldstrings_to_fields([rawfield])[0]
        result = s2m.LinkingFieldParser(field).parse()
        assert (result['display_label'] == test_value) == equals


@pytest.mark.parametrize('raw_marcfield, expected', [
    # Edge cases
    # Empty field
    ('787 ##$a', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # No $s or $t title
    ('787 ##$aSome author.$dPub date.$w(OCoLC)646108719', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Some author',
        'short_author': 'Some author',
        'author_type': 'organization',
        'display_metadata': ['Pub date'],
        'identifiers_map': {
            'oclc': {'number': '646108719', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '646108719'}],
        'materials_specified': None,
    }),

    # Title, author, short author, display metadata
    # $s and $t title, use $t as title
    ('787 ##$sUniform title.$tTranscribed title.', {
        'display_label': None,
        'title_parts': ['Transcribed title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $t title only, use $t as title
    ('787 ##$tTranscribed title.', {
        'display_label': None,
        'title_parts': ['Transcribed title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s title only, use $s as title
    ('787 ##$sUniform title.', {
        'display_label': None,
        'title_parts': ['Uniform title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # title with multiple parts
    ('787 ##$sRiigi teataja (1990). English. Selections.', {
        'display_label': None,
        'title_parts': ['Riigi teataja (1990)', 'English', 'Selections'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, non-music collective title
    ('787 ##$aBeethoven, Ludwig van.$sWorks. Selections', {
        'display_label': None,
        'title_parts': ['Works', 'Selections'],
        'title_is_collective': True,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, music form collective title
    ('787 ##$aBeethoven, Ludwig van.$sSonatas.', {
        'display_label': None,
        'title_parts': ['Sonatas'],
        'title_is_collective': True,
        'title_is_music_form': True,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, music form plus instrument collective title
    ('787 ##$aBeethoven, Ludwig van.$sSonatas, piano.', {
        'display_label': None,
        'title_parts': ['Sonatas, piano'],
        'title_is_collective': True,
        'title_is_music_form': True,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, "N music" collective title
    ('787 ##$aBeethoven, Ludwig van.$sPiano music.', {
        'display_label': None,
        'title_parts': ['Piano music'],
        'title_is_collective': True,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # author, personal name
    ('787 ##$aBeethoven, Ludwig van, 1770-1827.', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Beethoven, Ludwig van, 1770-1827',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # author, organizational name
    ('787 ##$aUnited States. Congress.', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'United States Congress',
        'short_author': 'United States, Congress',
        'author_type': 'organization',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),
    
    # author, meeting name
    ('787 ##$aFestival of Britain (1951 : London, England)', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Festival of Britain',
        'short_author': 'Festival of Britain',
        'author_type': 'organization',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # multiple metadata subfields -- should stay in order
    # also: $e, $f, $q, and $v are ignored.
    ('787 ##$b[English edition]$c(London, 1958)$dChennai : Westland, 2011'
     '$eeng$fdcu$gJan. 1992$hmicrofilm$j20100101'
     '$kAsia Pacific legal culture and globalization$mScale 1:760,320.'
     '$n"July 2011"$oN 84-11142$q15:5<30$vBase map data', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': [
            '[English edition]', 'London, 1958', 'Chennai : Westland, 2011',
            'Jan. 1992', 'microfilm',
            'Asia Pacific legal culture and globalization', 'Scale 1:760,320',
            '"July 2011"', 'N 84-11142', 'Base map data'
        ],
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # 760: $g following the title is treated as volume
    ('760 ##$tSeries title.$gVol. 1.', {
        'display_label': None,
        'title_parts': ['Series title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': 'vol. 1',
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # 762: $g following the title is treated as volume
    ('762 ##$tSeries title.$gNO. 23.', {
        'display_label': None,
        'title_parts': ['Series title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': 'NO. 23',
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # Identifiers
    # $r => Report Number
    ('787 ##$rEPA 430-H-02-001', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'r': {'number': 'EPA 430-H-02-001', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'r',
            'numtype': 'standard',
            'label': 'Report Number',
            'number': 'EPA 430-H-02-001'}],
        'materials_specified': None,
    }),

    # $u => STRN
    ('787 ##$uFHWA/NC/95-002', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'u': {'number': 'FHWA/NC/95-002', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'u',
            'numtype': 'standard',
            'label': 'STRN',
            'number': 'FHWA/NC/95-002'}],
        'materials_specified': None,
    }),

    # $w (OCoLC) => OCLC Number
    ('787 ##$w(OCoLC)12700508', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'oclc': {'number': '12700508', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '12700508'}],
        'materials_specified': None,
    }),

    # $w (DLC) => LCCN
    ('787 ##$w(DLC)   92643478', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'lccn': {'number': '92643478', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'lccn',
            'numtype': 'control',
            'label': 'LCCN',
            'number': '92643478'}],
        'materials_specified': None,
    }),

    # $w (CaOONL) => CaOONL Number
    ('787 ##$w(CaOONL)890390894', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'w': {'number': '890390894', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'w',
            'numtype': 'control',
            'label': 'CaOONL Number',
            'number': '890390894'}],
        'materials_specified': None,
    }),

    # $w with no qualifier => Control Number
    ('787 ##$w890390894', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'w': {'number': '890390894', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'w',
            'numtype': 'control',
            'label': 'Control Number',
            'number': '890390894'}],
        'materials_specified': None,
    }),

    # $x => ISSN
    ('787 ##$x1544-7227', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'issn': {'number': '1544-7227', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'issn',
            'numtype': 'standard',
            'label': 'ISSN',
            'number': '1544-7227'}],
        'materials_specified': None,
    }),

    # $y => CODEN
    ('787 ##$yFBKRAT', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'coden': {'number': 'FBKRAT', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'coden',
            'numtype': 'standard',
            'label': 'CODEN',
            'number': 'FBKRAT'}],
        'materials_specified': None,
    }),

    # $z => ISBN
    ('787 ##$z477440490X', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'isbn': {'number': '477440490X', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '477440490X'}],
        'materials_specified': None,
    }),

    # Multiple different identifiers
    ('787 ##$z9781598847611$w(DLC)   2012034673$w(OCoLC)768800369', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'isbn': {'number': '9781598847611', 'numtype': 'standard'},
            'lccn': {'number': '2012034673', 'numtype': 'control'},
            'oclc': {'number': '768800369', 'numtype': 'control'},
        },
        'identifiers_list': [{
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '9781598847611'
        }, {
            'code': 'lccn',
            'numtype': 'control',
            'label': 'LCCN',
            'number': '2012034673'
        }, {
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '768800369'
        }],
        'materials_specified': None,
    }),

    # Multiple identifiers of the same type
    # Only the first is used in `identifiers_map`.
    ('787 ##$z477440490X$z9784774404905$w(OCoLC)883612986', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'isbn': {'number': '477440490X', 'numtype': 'standard'},
            'oclc': {'number': '883612986', 'numtype': 'control'},
        },
        'identifiers_list': [{
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '477440490X'
        }, {
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '9784774404905'
        }, {
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '883612986'
        }],
        'materials_specified': None,
    }),
], ids=[
    # Edge cases
    'Empty field',
    'No $s or $t title',

    # Title, author, short author, display metadata
    '$s and $t title, use $t as title',
    '$t title only, use $t as title',
    '$s title only, use $s as title',
    '$s, non-music collective title',
    '$s, music form collective title',
    '$s, music form plus instrument collective title',
    '$s, "N music" collective title',
    'title with multiple parts',
    'author, personal name',
    'author, organizational name',
    'author, meeting name',
    'multiple metadata subfields -- should stay in order',
    '760: $g following the title is treated as volume',
    '762: $g following the title is treated as volume',

    # Identifiers
    '$r => Report Number',
    '$u => STRN',
    '$w (OCoLC) => OCLC Number',
    '$w (DLC) => LCCN',
    '$w (CaOONL) => CaOONL Number',
    '$w with no qualifier => Control Number',
    '$x => ISSN',
    '$y => CODEN',
    '$z => ISBN',
    'Multiple different identifiers',
    'Multiple identifiers of the same type',
])
def test_linkingfieldparser_parse(raw_marcfield, expected,
                                  fieldstrings_to_fields):
    """
    When passed the given MARC field, the `LinkingFieldParser.parse`
    method should return the expected results.
    """
    field = fieldstrings_to_fields([raw_marcfield])[0]
    result = s2m.LinkingFieldParser(field).parse()
    print result
    assert result == expected


@pytest.mark.parametrize('raw_marcfields, expected', [
    # 760 and 762 (additional Related Series)
    # 760-762: Empty MARC fields => no results
    (['760 ## $a', '762 ## $a'], {}),

    # 760-762: No title ($s or $t) => no results
    (['760 ## $aSurname, Forename$w(OCoLC)12345',
      '762 ## $aSurname, Forename$w(OCoLC)12345'], {}),

    # 760-762: Title alone
    (['760 0# $tSeries title',
      '762 0# $tSubseries title'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title',
                'v': 'series-title!Series title'}]},
        {'p': [{'d': 'Subseries title',
                'v': 'subseries-title!Subseries title'}]},
      ],
      'related_series_titles_search': [
        'Series title',
        'Subseries title',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'subseries-title!Subseries title',
      ]}),

    # 760-762: Multi-part title
    (['760 0# $tSeries title. Part one',
      '762 0# $tSubseries title. Part one'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title',
                'v': 'series-title!Series title',
                's': ' > '},
               {'d': 'Part one',
                'v': 'series-title-part-one!Series title > Part one'}]},
        {'p': [{'d': 'Subseries title',
                'v': 'subseries-title!Subseries title',
                's': ' > '},
               {'d': 'Part one',
                'v': 'subseries-title-part-one!Subseries title > Part one'}]},
      ],
      'related_series_titles_search': [
        'Series title > Part one',
        'Subseries title > Part one',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'series-title-part-one!Series title > Part one',
        'subseries-title!Subseries title',
        'subseries-title-part-one!Subseries title > Part one'
      ]}),

    # 760-762: Personal author and title
    (['760 0# $aSmith, John A., 1900-1980.$tSeries title',
      '762 0# $aSmith, John A., 1900-1980.$tSubseries title'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title [by Smith, J.A.]',
                'v': 'series-title!Series title'}]},
        {'p': [{'d': 'Subseries title [by Smith, J.A.]',
                'v': 'subseries-title!Subseries title'}]},
      ],
      'related_series_titles_search': [
        'Series title',
        'Subseries title',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'subseries-title!Subseries title',
      ]}),

    # 760-762: Multi-part org author and title
    (['760 0# $aUnited States. Congress$tSeries title',
      '762 0# $aUnited States. Congress$tSubseries title'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title [United States, Congress]',
                'v': 'series-title!Series title'}]},
        {'p': [{'d': 'Subseries title [United States, Congress]',
                'v': 'subseries-title!Subseries title'}]},
      ],
      'related_series_titles_search': [
        'Series title',
        'Subseries title',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'subseries-title!Subseries title',
      ]}),

    # 760-762: Author and multi-part title
    (['760 0# $aSmith, John A., 1900-1980.$tSeries title. Part one',
      '762 0# $aUnited States. Congress. House.$tSubseries title. Part one'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title [by Smith, J.A.]',
                'v': 'series-title!Series title',
                's': ' > '},
               {'d': 'Part one',
                'v': 'series-title-part-one!Series title > Part one'}]},
        {'p': [{'d': 'Subseries title [United States ... House]',
                'v': 'subseries-title!Subseries title',
                's': ' > '},
               {'d': 'Part one',
                'v': 'subseries-title-part-one!Subseries title > Part one'}]},
      ],
      'related_series_titles_search': [
        'Series title > Part one',
        'Subseries title > Part one',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'series-title-part-one!Series title > Part one',
        'subseries-title!Subseries title',
        'subseries-title-part-one!Subseries title > Part one'
      ]}),

    # 760-762: Author/title plus additional metadata
    (['760 0# $aSmith, John A., 1900-1980.$tSeries title.$gNo. 1-$nSome note.',
      '762 0# $aSmith, John A., 1900-1980.$tSubseries title.$oABC12345'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title [by Smith, J.A.]',
                'v': 'series-title!Series title',
                's': '; '},
               {'d': 'no. 1-',
                'v': 'series-title-no-1!Series title; no. 1-',
                's': ' ('},
               {'d': 'Some note',
                's': ')'}]},
        {'p': [{'d': 'Subseries title [by Smith, J.A.]',
                'v': 'subseries-title!Subseries title',
                's': ' ('},
               {'d': 'ABC12345',
                's': ')'}]},
      ],
      'related_series_titles_search': [
        'Series title; no. 1-',
        'Subseries title',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'series-title-no-1!Series title; no. 1-',
        'subseries-title!Subseries title',
      ]}),

    # 760-762: Author/title plus identifiers
    (['760 0# $aSmith, John A., 1900-1980.$tSeries title$x0084-1358'
             '$w(DLC)sf 81008035 ',
      '762 0# $aSmith, John A., 1900-1980.$tSubseries title$w(OCoLC)856411436'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title [by Smith, J.A.]',
                'v': 'series-title!Series title',
                's': ' ('},
               {'d': 'ISSN 0084-1358; LCCN sf81008035',
                's': ')'}]},
        {'p': [{'d': 'Subseries title [by Smith, J.A.]',
                'v': 'subseries-title!Subseries title',
                's': ' ('},
               {'d': 'OCLC Number 856411436',
                's': ')'}]},
      ],
      'related_series_titles_search': [
        'Series title',
        'Subseries title',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'subseries-title!Subseries title',
      ]}),

    # 760-762: Author/title plus additional metadata and identifiers
    (['760 0# $aSmith, John A., 1900-1980.$tSeries title.$gNo. 1-$nSome note.'
             '$x0084-1358$w(DLC)sf 81008035 ',
      '762 0# $aSmith, John A., 1900-1980.$tSubseries title.$oABC12345'
             '$w(OCoLC)856411436'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title [by Smith, J.A.]',
                'v': 'series-title!Series title',
                's': '; '},
               {'d': 'no. 1-',
                'v': 'series-title-no-1!Series title; no. 1-',
                's': ' ('},
               {'d': 'Some note',
                's': ' — '},
               {'d': 'ISSN 0084-1358; LCCN sf81008035',
                's': ')'}]},
        {'p': [{'d': 'Subseries title [by Smith, J.A.]',
                'v': 'subseries-title!Subseries title',
                's': ' ('},
               {'d': 'ABC12345',
                's': ' — '},
               {'d': 'OCLC Number 856411436',
                's': ')'}]},
      ],
      'related_series_titles_search': [
        'Series title; no. 1-',
        'Subseries title',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'series-title-no-1!Series title; no. 1-',
        'subseries-title!Subseries title',
      ]}),

    # 760-762: Do not duplicate existing Related Series (from 490/8XX)
    # De-duplication is based on a generated "work title key."
    #   - Each key contains the full title only, plus the volumes, if
    #     present. Author short names are not included, unless they are
    #     part of the facet value itself (from a collective title).
    #     Expression and ID components are not included.
    #   - The `get_title_info` method generates these for 490s and 8XXs
    #     and then stores them on the pipeline object for comparison.
    #   - One is generated for each 760 and 762 and compared against
    #     the existing set. If found, that field is skipped.
    (['490 0# $aSeries one$x1111-1111',
      '490 0# $aSeries two$x2222-2222 ;$v76$l(LC 12345)',
      '760 0# $aSmith, Joe.$tSeries one.',
      '760 0# $aSmith, Joe.$tSeries one.$gNo. 2.',
      '762 0# $aSmith, Joe.$tSeries two.',
      '762 0# $tSeries three.',
      '800 1# $aSmith, Joe.$tSeries three.'],
     {'related_series_titles_json': [
        {'a': 'smith-joe!Smith, Joe',
         'p': [{'d': 'Series three [by Smith, J.]',
                'v': 'series-three!Series three'}]},
        {'p': [{'d': 'Series one',
                's': ' ('},
               {'d': 'ISSN 1111-1111',
                's': ')'}]},
        {'p': [{'d': 'Series two; [volume] 76',
                's': ' ('},
               {'d': 'ISSN 2222-2222; LC Call Number LC 12345',
                's': ')'}]},
        {'p': [{'d': 'Series one [by Smith, J.]',
                'v': 'series-one!Series one',
                's': '; '},
               {'d': 'no. 2',
                'v': 'series-one-no-2!Series one; no. 2'}]},
        {'p': [{'d': 'Series two [by Smith, J.]',
                'v': 'series-two!Series two'}]},
      ],
      'related_series_titles_search': [
        'Series three',
        'Series one',
        'Series two; [volume] 76',
        'Series one; no. 2',
        'Series two',
      ],
      'title_series_facet': [
        'series-three!Series three',
        'series-one!Series one',
        'series-one-no-2!Series one; no. 2',
        'series-two!Series two',
      ]}),


    # 774 (addition Included Works)
    # 774: Empty MARC fields => no results
    (['774 ## $a'], {}),

    # 774: No title ($s or $t) => no results
    (['774 ## $aSurname, Forename$w(OCoLC)12345'], {}),

    # 774: Title alone
    (['774 0# $tWork title one',
      '774 0# $tWork title two'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one',
                'v': 'work-title-one!Work title one'}]},
        {'p': [{'d': 'Work title two',
                'v': 'work-title-two!Work title two'}]},
      ],
      'included_work_titles_search': [
        'Work title one',
        'Work title two',
      ],
      'title_series_facet': [
        'work-title-one!Work title one',
        'work-title-two!Work title two',
      ]}),

    # 774: Multi-part title
    (['774 0# $tWork title one. Part one',
      '774 0# $tWork title two. Part one'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one',
                'v': 'work-title-one!Work title one', 
                's': ' > '},
               {'d': 'Part one',
                'v': 'work-title-one-part-one!Work title one > Part one'}]},
        {'p': [{'d': 'Work title two',
                'v': 'work-title-two!Work title two', 
                's': ' > '},
               {'d': 'Part one',
                'v': 'work-title-two-part-one!Work title two > Part one'}]},
      ],
      'included_work_titles_search': [
        'Work title one > Part one',
        'Work title two > Part one',
      ],
      'title_series_facet': [
        'work-title-one!Work title one',
        'work-title-one-part-one!Work title one > Part one',
        'work-title-two!Work title two',
        'work-title-two-part-one!Work title two > Part one',
      ]}),

    # 774: Personal author and title
    (['774 0# $aSmith, John A., 1900-1980.$tWork title one',
      '774 0# $aSmith, John A., 1900-1980.$tWork title two'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one [by Smith, J.A.]',
                'v': 'work-title-one!Work title one'}]},
        {'p': [{'d': 'Work title two [by Smith, J.A.]',
                'v': 'work-title-two!Work title two'}]},
      ],
      'included_work_titles_search': [
        'Work title one',
        'Work title two',
      ],
      'title_series_facet': [
        'work-title-one!Work title one',
        'work-title-two!Work title two',
      ]}),

    # 774: Multi-part org author and title
    (['774 0# $aUnited States. Congress.$tWork title one',
      '774 0# $aUnited States. Congress.$tWork title two'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one [United States, Congress]',
                'v': 'work-title-one!Work title one'}]},
        {'p': [{'d': 'Work title two [United States, Congress]',
                'v': 'work-title-two!Work title two'}]},
      ],
      'included_work_titles_search': [
        'Work title one',
        'Work title two',
      ],
      'title_series_facet': [
        'work-title-one!Work title one',
        'work-title-two!Work title two',
      ]}),

    # 774: Author and multi-part title
    (['774 0# $aSmith, John A., 1900-1980.$tWork title one. Part one',
      '774 0# $aUnited States. Congress. House.$tWork title two. Part one'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one [by Smith, J.A.]',
                'v': 'work-title-one!Work title one', 
                's': ' > '},
               {'d': 'Part one',
                'v': 'work-title-one-part-one!Work title one > Part one'}]},
        {'p': [{'d': 'Work title two [United States ... House]',
                'v': 'work-title-two!Work title two',
                's': ' > '},
               {'d': 'Part one',
                'v': 'work-title-two-part-one!Work title two > Part one'}]},
      ],
      'included_work_titles_search': [
        'Work title one > Part one',
        'Work title two > Part one',
      ],
      'title_series_facet': [
        'work-title-one!Work title one',
        'work-title-one-part-one!Work title one > Part one',
        'work-title-two!Work title two',
        'work-title-two-part-one!Work title two > Part one',
      ]}),

    # 774: Personal author plus collective titles
    (['774 0# $aSmith, John A., 1900-1980.$sPiano music. Selections.',
      '774 0# $aSmith, John A., 1900-1980.$sPoems.',
      '774 0# $aSmith, John A., 1900-1980.$sSonatas, piano.'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Piano music [of Smith, J.A.] (Selections)',
                'v': 'piano-music-of-smith-j-a-selections!'
                     'Piano music [of Smith, J.A.] (Selections)'}]},
        {'p': [{'d': 'Poems [of Smith, J.A.] (Complete)',
                'v': 'poems-of-smith-j-a-complete!'
                     'Poems [of Smith, J.A.] (Complete)'}]},
        {'p': [{'d': 'Sonatas, piano [by Smith, J.A.] (Complete)',
                'v': 'sonatas-piano-by-smith-j-a-complete!'
                     'Sonatas, piano [by Smith, J.A.] (Complete)'}]},
      ],
      'included_work_titles_search': [
        'Piano music [of Smith, J.A.] (Selections)',
        'Poems [of Smith, J.A.] (Complete)',
        'Sonatas, piano [by Smith, J.A.] (Complete)',
      ],
      'title_series_facet': [
        'piano-music-of-smith-j-a-selections!'
        'Piano music [of Smith, J.A.] (Selections)',
        'poems-of-smith-j-a-complete!Poems [of Smith, J.A.] (Complete)',
        'sonatas-piano-by-smith-j-a!Sonatas, piano [by Smith, J.A.]',
        'sonatas-piano-by-smith-j-a-complete!'
        'Sonatas, piano [by Smith, J.A.] (Complete)',
      ]}),

    # 774: Author/title plus additional metadata
    (['774 0# $aSmith, John A., 1900-1980.$tWork title one (2014).$bFirst ed.'
             '$dNew York : Publisher, 2014.',
      '774 0# $aSmith, John A., 1900-1980.$tWork title two.$c(London : 1958)'
             '$hvolumes : ill. ; 29 cm'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one (2014) [by Smith, J.A.]',
                'v': 'work-title-one-2014!Work title one (2014)',
                's': ' ('},
               {'d': 'First ed.; New York : Publisher, 2014',
                's': ')'}]},
        {'p': [{'d': 'Work title two [by Smith, J.A.]',
                'v': 'work-title-two!Work title two',
                's': ' ('},
               {'d': 'London : 1958; volumes : ill. ; 29 cm',
                's': ')'}]},
      ],
      'included_work_titles_search': [
        'Work title one (2014)',
        'Work title two',
      ],
      'title_series_facet': [
        'work-title-one-2014!Work title one (2014)',
        'work-title-two!Work title two',
      ]}),

    # 774: Author/title plus identifiers
    (['774 0# $aSmith, John A., 1900-1980.$tWork title one$z12345',
      '774 0# $aSmith, John A., 1900-1980.$tWork title two$rAB-12345'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one [by Smith, J.A.]',
                'v': 'work-title-one!Work title one',
                's': ' ('},
               {'d': 'ISBN 12345',
                's': ')'}]},
        {'p': [{'d': 'Work title two [by Smith, J.A.]',
                'v': 'work-title-two!Work title two',
                's': ' ('},
               {'d': 'Report Number AB-12345',
                's': ')'}]},
      ],
      'included_work_titles_search': [
        'Work title one',
        'Work title two',
      ],
      'title_series_facet': [
        'work-title-one!Work title one',
        'work-title-two!Work title two',
      ]}),

    # 774: Author/title plus additional metadata and identifiers
    (['774 0# $aSmith, John A., 1900-1980.$tWork title one (2014).$bFirst ed.'
             '$dNew York : Publisher, 2014.$z12345',
      '774 0# $aSmith, John A., 1900-1980.$tWork title two.$c(London : 1958)'
             '$hvolumes : ill. ; 29 cm$rAB-12345'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title one (2014) [by Smith, J.A.]',
                'v': 'work-title-one-2014!Work title one (2014)',
                's': ' ('},
               {'d': 'First ed.; New York : Publisher, 2014',
                's': ' — '},
               {'d': 'ISBN 12345',
                's': ')'}]},
        {'p': [{'d': 'Work title two [by Smith, J.A.]',
                'v': 'work-title-two!Work title two',
                's': ' ('},
               {'d': 'London : 1958; volumes : ill. ; 29 cm',
                's': ' — '},
               {'d': 'Report Number AB-12345',
                's': ')'}]},
      ],
      'included_work_titles_search': [
        'Work title one (2014)',
        'Work title two',
      ],
      'title_series_facet': [
        'work-title-one-2014!Work title one (2014)',
        'work-title-two!Work title two',
      ]}),

    # 774: Do not duplicate existing Included Works (from 2XX/7XX)
    # De-duplication is based on a generated "work title key."
    #   - Each key contains the full title only. Author short names are
    #     not included, unless they are part of the facet value itself
    #     (from a collective title). Expression and ID components are
    #     not included.
    #   - The `get_title_info` method generates these for analytical
    #     titles and stores them on the pipeline object for comparison.
    #   - One is generated for each 774 and compared against the
    #     existing set. If found, that field is skipped.
    (['730 02 $aWork title.$pFirst part.$lEnglish.$sSome version.$f1994.',
      '774 0# $aSmith, John A., 1900-1980.$sWork title. First part. English. '
             'Some version. 1994.',
      '774 0# $aSmith, John A., 1900-1980.$tAnother title.$bFirst ed.'
             '$dNew York : Publisher, 2014.$z12345'],
     {'included_work_titles_json': [
        {'p': [{'d': 'Work title',
                's': ' > ',
                'v': 'work-title!Work title'},
               {'d': 'First part',
                's': ' (',
                'v': 'work-title-first-part!Work title > First part'},
               {'d': 'English; Some version; 1994',
                's': ')',
                'v': 'work-title-first-part-english-some-version-1994!'
                     'Work title > First part (English; Some version; 1994)'}]},
        {'p': [{'d': 'Another title [by Smith, J.A.]',
                'v': 'another-title!Another title',
                's': ' ('},
               {'d': 'First ed.; New York : Publisher, 2014',
                's': ' — '},
               {'d': 'ISBN 12345',
                's': ')'}]},
      ],
      'included_work_titles_search': [
        'Work title > First part (English; Some version; 1994)',
        'Another title'
      ],
      'title_series_facet': [
        'work-title!Work title',
        'work-title-first-part!Work title > First part',
        'work-title-first-part-english-some-version-1994!'
        'Work title > First part (English; Some version; 1994)',
        'another-title!Another title'
      ]}),

    # 780-785 (serial_continuity_linking_json)
    # 780-785: Empty MARC fields => no results
    (['780 ## $a', '785 ## $a'], {}),

    # 780-785: No title ($s or $t) => no results
    (['780 00 $aSurname, Forename$w(OCoLC)12345',
      '785 00 $aSurname, Forename$w(OCoLC)12345'], {}),

    # 780-785: Title alone
    (['780 00 $tPrevious title',
      '785 00 $tNext title'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title',
                't': 'Previous title'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title',
                't': 'Next title'}]},
      ]}),

    # 780-785: Multi-part title
    (['780 00 $tPrevious title. Vol 1.',
      '785 00 $tNext title. Vol 1.'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title > Vol 1',
                't': 'Previous title > Vol 1'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title > Vol 1',
                't': 'Next title > Vol 1'}]},
      ]}),

    # 780-785: Personal author and title
    (['780 00 $aSmith, John A., 1900-1980.$tPrevious title',
      '785 00 $aSmith, John A., 1900-1980.$tNext title'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title [by Smith, J.A.]',
                't': 'Previous title',
                'a': 'Smith, John A., 1900-1980'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title [by Smith, J.A.]',
                't': 'Next title',
                'a': 'Smith, John A., 1900-1980'}]},
      ]}),

    # 780-785: Multi-part org author and title
    (['780 00 $aUnited States. Congress.$tPrevious title',
      '785 00 $aUnited States. Congress.$tNext title'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title [United States, Congress]',
                't': 'Previous title',
                'a': 'United States Congress'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title [United States, Congress]',
                't': 'Next title',
                'a': 'United States Congress'}]},
      ]}),

    # 780-785: Author and multi-part title
    (['780 00 $aSmith, John A., 1900-1980.$tPrevious title. Vol 1.',
      '785 00 $aUnited States. Congress. House.$tNext title. Vol 1.'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title [by Smith, J.A.] > Vol 1',
                'a': 'Smith, John A., 1900-1980',
                't': 'Previous title > Vol 1'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title [United States ... House] > Vol 1',
                'a': 'United States Congress House',
                't': 'Next title > Vol 1'}]},
      ]}),

    # 780-785: Author/title plus additional metadata
    (['780 00 $aSmith, John A., 1900-1980.$tPrevious title.$kSeries.'
             '$kSubseries.',
      '785 00 $aSmith, John A., 1900-1980.$tNext title.$dNew York : 2010.'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title [by Smith, J.A.]',
                't': 'Previous title',
                'a': 'Smith, John A., 1900-1980',
                's': ' ('},
               {'d': 'Series; Subseries',
                's': ')'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title [by Smith, J.A.]',
                't': 'Next title',
                'a': 'Smith, John A., 1900-1980',
                's': ' ('},
               {'d': 'New York : 2010',
                's': ')'}]},
      ]}),

    # 780-785: Author/title plus identifiers
    (['780 00 $aSmith, John A., 1900-1980.$tPrevious title.$x1234-5678'
             '$w(DLC)sc 83007721 ',
      '785 00 $aSmith, John A., 1900-1980.$tNext title.$x1234-5679'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title [by Smith, J.A.]',
                't': 'Previous title',
                'a': 'Smith, John A., 1900-1980',
                'sn': '1234-5678',
                's': ' ('},
               {'d': 'ISSN 1234-5678',
                'sn': '1234-5678', 
                's': '; '},
               {'d': 'LCCN sc83007721',
                'cn': 'sc83007721', 
                's': ')'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title [by Smith, J.A.]',
                't': 'Next title',
                'a': 'Smith, John A., 1900-1980',
                'sn': '1234-5679',
                's': ' ('},
               {'d': 'ISSN 1234-5679',
                'sn': '1234-5679',
                's': ')'}]}
      ]}),

    # 780-785: Author/title plus additional metadata and identifiers
    (['780 00 $aSmith, John A., 1900-1980.$tPrevious title.$kSeries.'
             '$kSubseries.$x1234-5678$w(DLC)sc 83007721 ',
      '785 00 $aSmith, John A., 1900-1980.$tNext title.$dNew York : 2010.'
             '$x1234-5679'],
     {'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Previous title [by Smith, J.A.]',
                't': 'Previous title',
                'a': 'Smith, John A., 1900-1980',
                'sn': '1234-5678',
                's': ' ('},
               {'d': 'Series; Subseries',
                's': ' — '},
               {'d': 'ISSN 1234-5678',
                'sn': '1234-5678', 
                's': '; '},
               {'d': 'LCCN sc83007721',
                'cn': 'sc83007721', 
                's': ')'}]},
        {'b': 'Continued by:',
         'p': [{'d': 'Next title [by Smith, J.A.]',
                't': 'Next title',
                'a': 'Smith, John A., 1900-1980',
                'sn': '1234-5679',
                's': ' ('},
               {'d': 'New York : 2010',
                's': ' — '},
               {'d': 'ISSN 1234-5679',
                'sn': '1234-5679',
                's': ')'}]},
      ]}),


    # 765-773, 776-777, 786-787 (related_resources_linking_json)
    # Others: Empty MARC fields => no results
    (['765 ## $a', '767 ## $a', '770 ## $a', '772 ## $a', '773 ## $a',
      '776 ## $a', '777 ## $a', '786 ## $a', '787 ## $a'], {}),

    # Others: No title ($s or $t) => no results
    (['765 ## $aSurname, Forename$w(OCoLC)12345',
      '767 ## $aSurname, Forename$w(OCoLC)12345',
      '770 ## $aSurname, Forename$w(OCoLC)12345',
      '772 ## $aSurname, Forename$w(OCoLC)12345',
      '773 ## $aSurname, Forename$w(OCoLC)12345',
      '776 ## $aSurname, Forename$w(OCoLC)12345',
      '777 ## $aSurname, Forename$w(OCoLC)12345',
      '786 ## $aSurname, Forename$w(OCoLC)12345',
      '787 ## $aSurname, Forename$w(OCoLC)12345'], {}),

    # Others: Title alone
    (['765 0# $tTítulo en Español',
      '787 08 $iSequel to:$tSequel title'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español',
                't': 'Título en Español'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title',
                't': 'Sequel title'}]},
      ]}),

    # Others: Multi-part title
    (['765 0# $tTítulo en Español. Parte uno.',
      '787 08 $iSequel to:$tSequel title. Part one.'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español > Parte uno',
                't': 'Título en Español > Parte uno'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title > Part one',
                't': 'Sequel title > Part one'}]},
      ]}),

    # Others: Personal author and title
    (['765 0# $aSmith, John A., 1900-1980.$tTítulo en Español',
      '787 08 $iSequel to:$aSmith, John A., 1900-1980.$tSequel title'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Título en Español'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Sequel title'}]},
      ]}),

    # Others: Multi-part org author and title
    (['765 0# $aUnited States. Congress.$tTítulo en Español',
      '787 08 $iSequel to:$aUnited States. Congress.$tSequel title'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español [United States, Congress]',
                'a': 'United States Congress',
                't': 'Título en Español'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title [United States, Congress]',
                'a': 'United States Congress',
                't': 'Sequel title'}]},
      ]}),

    # Others: Author and multi-part title
    (['765 0# $aSmith, John A., author.$tTítulo en Español. Parte uno.',
      '787 08 $iSequel to:$aUnited States. Congress. House.'
             '$tSequel title. Part one.'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español [by Smith, J.A.] > Parte uno',
                'a': 'Smith, John A.',
                't': 'Título en Español > Parte uno'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title [United States ... House] > Part one',
                'a': 'United States Congress House',
                't': 'Sequel title > Part one'}]},
      ]}),

    # Others: Author/title plus additional metadata
    (['765 0# $aSmith, John A., 1900-1980.$tTítulo en Español.'
             '$b[Spanish edition].$mMaterial-specific info.',
      '787 08 $iSequel to:$aSmith, John A., 1900-1980.$tSequel title.'
             '$dPublisher.'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Título en Español',
                's': ' ('},
               {'d': '[Spanish edition]; Material-specific info',
                's': ')'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Sequel title',
                's': ' ('},
               {'d': 'Publisher',
                's': ')'}]},
      ]}),

    # Others: Author/title plus identifiers
    (['765 0# $aSmith, John A., 1900-1980.$tTítulo en Español'
             '$w(DLC)   90646274 $z12345$w(OCoLC)6258868',
      '787 08 $iSequel to:$aSmith, John A., 1900-1980.$tSequel title'
             '$yIJMTAW'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Título en Español',
                'cn': '6258868',
                's': ' ('},
               {'d': 'LCCN 90646274',
                'cn': '90646274',
                's': '; '},
               {'d': 'ISBN 12345',
                'sn': '12345',
                's': '; '},
               {'d': 'OCLC Number 6258868',
                'cn': '6258868',
                's': ')'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Sequel title',
                'sn': 'IJMTAW',
                's': ' ('},
               {'d': 'CODEN IJMTAW',
                'sn': 'IJMTAW',
                's': ')'}]},
      ]}),

    # Others: Author/title plus additional metadata and identifiers
    (['765 0# $aSmith, John A., 1900-1980.$tTítulo en Español.'
             '$b[Spanish edition].$mMaterial-specific info.$w(DLC)   90646274 '
             '$z12345$w(OCoLC)6258868',
      '787 08 $iSequel to:$aSmith, John A., 1900-1980.$tSequel title.'
             '$dPublisher.$yIJMTAW'],
     {'related_resources_linking_json': [
        {'b': 'Translation of:',
         'p': [{'d': 'Título en Español [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Título en Español',
                'cn': '6258868',
                's': ' ('},
               {'d': '[Spanish edition]; Material-specific info',
                's': ' — '},
               {'d': 'LCCN 90646274',
                'cn': '90646274',
                's': '; '},
               {'d': 'ISBN 12345',
                'sn': '12345',
                's': '; '},
               {'d': 'OCLC Number 6258868',
                'cn': '6258868',
                's': ')'}]},
        {'b': 'Sequel to:',
         'p': [{'d': 'Sequel title [by Smith, J.A.]',
                'a': 'Smith, John A., 1900-1980',
                't': 'Sequel title',
                'sn': 'IJMTAW',
                's': ' ('},
               {'d': 'Publisher',
                's': ' — '},
               {'d': 'CODEN IJMTAW',
                'sn': 'IJMTAW',
                's': ')'}]},
      ]}),

    # Others: Main link picks best identifier (first OCLC)
    (['787 08 $tSequel title$r9999$uAAAA$w(OCoLC)1111$x2222$x3333$z4444'
             '$w(DLC)5555$w(ABC)6666$w7777$y8888'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'cn': '1111',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': '; '},
               {'d': 'STRN AAAA', 'sn': 'AAAA', 's': '; '},
               {'d': 'OCLC Number 1111', 'cn': '1111', 's': '; '},
               {'d': 'ISSN 2222', 'sn': '2222', 's': '; '},
               {'d': 'ISSN 3333', 'sn': '3333', 's': '; '},
               {'d': 'ISBN 4444', 'sn': '4444', 's': '; '},
               {'d': 'LCCN 5555', 'cn': '5555', 's': '; '},
               {'d': 'ABC Number 6666', 'cn': '6666', 's': '; '},
               {'d': 'Control Number 7777', 'cn': '7777', 's': '; '},
               {'d': 'CODEN 8888', 'sn': '8888', 's': ')'}]},
      ]}),

    # Others: Main link picks best identifier (first ISBN)
    (['787 08 $tSequel title$r9999$uAAAA$x2222$x3333$z4444$w(DLC)5555'
             '$w(ABC)6666$w7777$y8888'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'sn': '4444',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': '; '},
               {'d': 'STRN AAAA', 'sn': 'AAAA', 's': '; '},
               {'d': 'ISSN 2222', 'sn': '2222', 's': '; '},
               {'d': 'ISSN 3333', 'sn': '3333', 's': '; '},
               {'d': 'ISBN 4444', 'sn': '4444', 's': '; '},
               {'d': 'LCCN 5555', 'cn': '5555', 's': '; '},
               {'d': 'ABC Number 6666', 'cn': '6666', 's': '; '},
               {'d': 'Control Number 7777', 'cn': '7777', 's': '; '},
               {'d': 'CODEN 8888', 'sn': '8888', 's': ')'}]},
      ]}),

    # Others: Main link picks best identifier (first ISSN)
    (['787 08 $tSequel title$r9999$uAAAA$x2222$x3333$w(DLC)5555$w(ABC)6666'
             '$w7777$y8888'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'sn': '2222',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': '; '},
               {'d': 'STRN AAAA', 'sn': 'AAAA', 's': '; '},
               {'d': 'ISSN 2222', 'sn': '2222', 's': '; '},
               {'d': 'ISSN 3333', 'sn': '3333', 's': '; '},
               {'d': 'LCCN 5555', 'cn': '5555', 's': '; '},
               {'d': 'ABC Number 6666', 'cn': '6666', 's': '; '},
               {'d': 'Control Number 7777', 'cn': '7777', 's': '; '},
               {'d': 'CODEN 8888', 'sn': '8888', 's': ')'}]},
      ]}),

    # Others: Main link picks best identifier (first LCCN)
    (['787 08 $tSequel title$r9999$uAAAA$w(DLC)5555$w(ABC)6666$w7777$y8888'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'cn': '5555',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': '; '},
               {'d': 'STRN AAAA', 'sn': 'AAAA', 's': '; '},
               {'d': 'LCCN 5555', 'cn': '5555', 's': '; '},
               {'d': 'ABC Number 6666', 'cn': '6666', 's': '; '},
               {'d': 'Control Number 7777', 'cn': '7777', 's': '; '},
               {'d': 'CODEN 8888', 'sn': '8888', 's': ')'}]},
      ]}),

    # Others: Main link picks best identifier (first $w)
    (['787 08 $tSequel title$r9999$uAAAA$w(ABC)6666$w7777$y8888'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'cn': '6666',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': '; '},
               {'d': 'STRN AAAA', 'sn': 'AAAA', 's': '; '},
               {'d': 'ABC Number 6666', 'cn': '6666', 's': '; '},
               {'d': 'Control Number 7777', 'cn': '7777', 's': '; '},
               {'d': 'CODEN 8888', 'sn': '8888', 's': ')'}]},
      ]}),

    # Others: Main link picks best identifier (CODEN)
    (['787 08 $tSequel title$r9999$uAAAA$y8888'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'sn': '8888',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': '; '},
               {'d': 'STRN AAAA', 'sn': 'AAAA', 's': '; '},
               {'d': 'CODEN 8888', 'sn': '8888', 's': ')'}]},
      ]}),

    # Others: Main link picks best identifier ($u)
    (['787 08 $tSequel title$r9999$uAAAA'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'sn': 'AAAA',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': '; '},
               {'d': 'STRN AAAA', 'sn': 'AAAA', 's': ')'}]},
      ]}),

    # Others: Main link picks best identifier ($r)
    (['787 08 $tSequel title$r9999'],
     {'related_resources_linking_json': [
        {'p': [{'d': 'Sequel title',
                't': 'Sequel title',
                'sn': '9999',
                's': ' ('},
               {'d': 'Report Number 9999', 'sn': '9999', 's': ')'}]},
      ]}),

    # General tests
    # `Materials specified` and display labels play well together
    (['772 00 $31990-1991$tParent title'],
     {'related_resources_linking_json': [
         {'b': '(1990-1991) Parent:',
          'p': [{'d': 'Parent title',
                 't': 'Parent title'}]}
     ]}),

    # 775$e and $f are ignored
    (['775 0# $tEdition title$eng$filu'],
     {'related_resources_linking_json': [
         {'b': 'Other edition:',
          'p': [{'d': 'Edition title',
                 't': 'Edition title'}]}
     ]}),

    # 773$p and $q are ignored
    (['773 0# $pHost$tHost title.$q96:4<23'],
     {'related_resources_linking_json': [
         {'b': 'In:',
          'p': [{'d': 'Host title',
                 't': 'Host title'}]}
     ]}),

    # 786$j is ignored but $v is used
    (['786 0# $tSource title.$j013000$vData source information'],
     {'related_resources_linking_json': [
         {'b': 'Data source:',
          'p': [{'d': 'Source title',
                 't': 'Source title',
                 's': ' ('},
                {'d': 'Data source information',
                 's': ')'}]}
     ]}),

    # All four types of fields on one record
    (['760 0# $tSeries title.$gvol 2.$x1111-1111',
      '767 0# $tTranslated title.$bEnglish edition.$w(OCoLC)11111111',
      '770 0# $tSupplement title.',
      '774 08 $iContainer of:$aSmith, John, 1900-1980.$tIncluded title.',
      '776 08 $iOnline version:$tMain title.$w(OCoLC)22222222',
      '777 0# $aSmith, John, 1900-1980.$tOther title.',
      '780 00 $tEarlier title.$x2222-2222$w(OCoLC)33333333'],
     {'related_series_titles_json': [
        {'p': [{'d': 'Series title',
                'v': 'series-title!Series title',
                's': '; '},
               {'d': 'vol 2',
                'v': 'series-title-vol-2!Series title; vol 2',
                's': ' ('},
               {'d': 'ISSN 1111-1111',
                's': ')'}]},
      ],
      'related_series_titles_search': [
        'Series title; vol 2',
      ],
      'included_work_titles_json': [
        {'p': [{'d': 'Included title [by Smith, J.]',
                'v': 'included-title!Included title'}]},
      ],
      'included_work_titles_search': [
        'Included title',
      ],
      'title_series_facet': [
        'series-title!Series title',
        'series-title-vol-2!Series title; vol 2',
        'included-title!Included title',
      ],
      'related_resources_linking_json': [
        {'b': 'Translated as:',
         'p': [{'d': 'Translated title',
                't': 'Translated title',
                'cn': '11111111',
                's': ' ('},
               {'d': 'English edition',
                's': ' — '},
               {'d': 'OCLC Number 11111111',
                'cn': '11111111',
                's': ')'}]},
        {'b': 'Supplement:',
         'p': [{'d': 'Supplement title',
                't': 'Supplement title'}]},
        {'b': 'Online version:',
         'p': [{'d': 'Main title',
                't': 'Main title',
                'cn': '22222222',
                's': ' ('},
               {'d': 'OCLC Number 22222222',
                'cn': '22222222',
                's': ')'}]},
        {'b': 'Issued with:',
         'p': [{'d': 'Other title [by Smith, J.]',
                'a': 'Smith, John, 1900-1980',
                't': 'Other title'},]},
      ],
      'serial_continuity_linking_json': [
        {'b': 'Continues:',
         'p': [{'d': 'Earlier title',
                't': 'Earlier title',
                'cn': '33333333',
                's': ' ('},
               {'d': 'ISSN 2222-2222',
                'sn': '2222-2222',
                's': '; '},
               {'d': 'OCLC Number 33333333',
                'cn': '33333333',
                's': ')'}]},
      ]
    })


], ids=[
    # 760 and 762 (additional Related Series)
    '760-762: Empty MARC fields => no results',
    '760-762: No title ($s or $t) => no results',
    '760-762: Title alone',
    '760-762: Multi-part title',
    '760-762: Personal author and title',
    '760-762: Multi-part org author and title',
    '760-762: Author and multi-part title',
    '760-762: Author/title plus additional metadata',
    '760-762: Author/title plus identifiers',
    '760-762: Author/title plus additional metadata and identifiers',
    '760-762: Do not duplicate existing Related Series (from 490/8XX)',

    # 774 (addition Included Works)
    '774: Empty MARC fields => no results',
    '774: No title ($s or $t) => no results',
    '774: Title alone',
    '774: Multi-part title',
    '774: Personal author and title',
    '774: Multi-part org author and title',
    '774: Author and multi-part title',
    '774: Personal author plus collective titles',
    '774: Author/title plus additional metadata',
    '774: Author/title plus identifiers',
    '774: Author/title plus additional metadata and identifiers',
    '774: Do not duplicate existing Included Works (from 2XX/7XX)',

    # 780-785 (serial_continuity_linking_json)
    '780-785: Empty MARC fields => no results',
    '780-785: No title ($s or $t) => no results',
    '780-785: Title alone',
    '780-785: Multi-part title',
    '780-785: Personal author and title',
    '780-785: Multi-part org author and title',
    '780-785: Author and multi-part title',
    '780-785: Author/title plus additional metadata',
    '780-785: Author/title plus identifiers',
    '780-785: Author/title plus additional metadata and identifiers',

    # 765-773, 776-777, 786-787 (related_resources_linking_json)
    'Others: Empty MARC fields => no results',
    'Others: No title ($s or $t) => no results',
    'Others: Title alone',
    'Others: Multi-part title',
    'Others: Personal author and title',
    'Others: Multi-part org author and title',
    'Others: Author and multi-part title',
    'Others: Author/title plus additional metadata',
    'Others: Author/title plus identifiers',
    'Others: Author/title plus additional metadata and identifiers',
    'Others: Main link picks best identifier (OCLC)',
    'Others: Main link picks best identifier (ISBN)',
    'Others: Main link picks best identifier (ISSN)',
    'Others: Main link picks best identifier (LCCN)',
    'Others: Main link picks best identifier ($w)',
    'Others: Main link picks best identifier (CODEN)',
    'Others: Main link picks best identifier ($u)',
    'Others: Main link picks best identifier ($r)',

    # General tests
    '`Materials specified` and display labels play well together',
    '775$e and $f are ignored',
    '773$p and $q are ignored',
    '786$j and $v are used',
    'All four types of fields on one record',
])
def test_todscpipeline_getlinkingfields(raw_marcfields, expected,
                                        fieldstrings_to_fields,
                                        bl_sierra_test_record,
                                        todsc_pipeline_class,
                                        bibrecord_to_pymarc,
                                        add_marc_fields,
                                        assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_linking_fields should return data matching
    the expected parameters.
    """
    pipeline = todsc_pipeline_class()
    marcfields = fieldstrings_to_fields(raw_marcfields)
    to_do = ['linking_fields']
    to_remove = ['760', '762', '765', '767', '770', '772', '773', '774', '776',
                 '777', '780', '785', '786', '787']
    title_tags = ('130', '240', '242', '243', '245', '246', '247', '490', '700',
                  '710', '711', '730', '740', '800', '810', '811', '830')
    if any([f.tag in title_tags for f in marcfields]):
        to_do.insert(0, 'title_info')
        to_remove.extend(title_tags)

    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields(*to_remove)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, to_do)
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('raw_marcfield, expected', [
    # 250 Edition Statements
    # Simple edition by itself
    ('250 ## $a1st ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}]
        },
        'materials_specified': None,
    }),
    
    # Edition and materials specified
    ('250 ## $31998-2005:$a1st ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}]
        },
        'materials_specified': ['1998-2005'],
    }),

    # Edition with bracketed portion
    ('250 ## $a1st [ed.]', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st [ed.]'}]
        },
        'materials_specified': None,
    }),

    # Edition all in brackets
    ('250 ## $a[1st ed.]', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '[1st ed.]'}]
        },
        'materials_specified': None,
    }),

    # Simple edition plus responsibility
    ('250 ## $a1st ed. /$bedited by J. Smith', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Edition plus responsibility and revision
    ('250 ## $a1st ed. /$bedited by J. Smith, 2nd rev.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith, 2nd rev.'
            }]
        },
        'materials_specified': None,
    }),

    # Edition plus responsibility and revision plus responsibility
    ('250 ## $a1st ed. /$bedited by J. Smith, 2nd rev. / by N. Smith.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith, 2nd rev., by N. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Edition and parallel edition
    ('250 ## $a1st ed. =$b1a ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}],
            'parallel': [{'value': '1a ed.'}]
        },
        'materials_specified': None,
    }),

    # Edition/parallel, with one SOR at end
    ('250 ## $a1st ed. =$b1a ed. / edited by J. Smith.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }],
            'parallel': [{'value': '1a ed.'}]
        },
        'materials_specified': None,
    }),

    # Edition, with SOR and parallel SOR
    ('250 ## $a1st ed. /$bedited by J. Smith = editado por J. Smith.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }],
            'parallel': [{'responsibility': 'editado por J. Smith'}]
        },
        'materials_specified': None,
    }),

    # Edition/SOR plus parallel edition/SOR
    ('250 ## $a1st ed. /$bedited by J. Smith = 1a ed. / editado por J. Smith.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }],
            'parallel': [{
                'value': '1a ed.',
                'responsibility': 'editado por J. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Edition/revision plus parallel (including SORs)
    ('250 ## $a1st ed. /$bedited by J. Smith = 1a ed. / editado por J. Smith, '
             '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }, {
                'value': '2nd rev.',
                'responsibility': 'by B. Roberts'
            }],
            'parallel': [{
                'value': '1a ed.',
                'responsibility': 'editado por J. Smith'
            }, {
                'value': '2a rev.',
                'responsibility': 'por B. Roberts'
            }]
        },
        'materials_specified': None,
    }),

    # 250s, edges of "revision" detection
    # AACR2 allows for a "named revision" following the main edition,
    # which denotes a specific version of an edition, and appears like
    # an additonal edition (following a ", "). It's very similar to a
    # multi-title 245 but follows ", " instead of ". " and is therefore
    # much harder to detect reliably. The AACR2 examples all show ", "
    # plus a number or capitalized word, but naively looking for that
    # pattern gives rise to many, many false positives -- lists of
    # names, for example (1st ed. / edited by J. Smith, B. Roberts).
    #
    # In reality, failing to detect a named revision may or may not be
    # a problem, depending on the situation.
    #    - `1st edition, New revision` -- In this case it's all treated
    #      as one contiguous edition string, which is fine.
    #    - `1st edition / edited by J. Smith, New revision` -- In this
    #      case, "New revision" is treated as part of the SOR; this
    #      will display relatively clearly, with the downside that the
    #      named revision becomes searchable as part of the SOR search
    #      field. Materially this shouldn't have a big impact -- the
    #      text is still searchable, just arguably with a small effect
    #      on relevance for terms that match.
    #    - `1st ed. = 1a ed., New rev. = Nueva rev.` -- In this case,
    #      not detecting the named revision makes it part of the
    #      parallel text, "1a ed." This is blatantly incorrect and
    #      ends up displaying as, `1st ed. [translated: 1a ed., New
    #      rev.]`.
    #
    # The third scenario listed above is the most problematic but also
    # the rarest. Through testing against our catalog data it does
    # appear possible to find reliably with minimal false positives.

    # Otherwise valid pattern not following " = " is not recognized
    ('250 ## $a1st ed. /$bedited by J. Smith, New revision.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith, New revision'
            }]
        },
        'materials_specified': None,
    }),

    # Obvious names are not recognized
    ('250 ## $a1st ed. =$b1a ed. / edited by J. Smith, Bob Roberts.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith, Bob Roberts'
            }],
            'parallel': [{
                'value': '1a ed.'
            }]
        },
        'materials_specified': None,
    }),

    # Valid numeric pattern is recognizzed
    ('250 ## $a1st ed. =$b1a ed., 2nd rev.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
            }, {
                'value': '2nd rev.'
            }],
            'parallel': [{
                'value': '1a ed.'
            }]
        },
        'materials_specified': None,
    }),

    # Valid one-word pattern is recognized
    ('250 ## $a1st ed. =$b1a ed., Klavierauszug = Piano reduction',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
            }, {
                'value': 'Klavierauszug'
            }],
            'parallel': [{
                'value': '1a ed.'
            }, {
                'value': 'Piano reduction'
            }]
        },
        'materials_specified': None,
    }),

    # Valid multi-word pattern is recognized
    ('250 ## $a1st ed. =$b1a ed., New Blah rev.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
            }, {
                'value': 'New Blah rev.'
            }],
            'parallel': [{
                'value': '1a ed.'
            }]
        },
        'materials_specified': None,
    }),

    # 250s, other edge cases
    # Missing `/` before $b
    ('250 ## $a1st ed.,$bedited by J. Smith', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Full edition statement is in $a (no $b)
    ('250 ## $a1st ed. / edited by J. Smith = 1a ed. / editado por J. Smith, '
             '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }, {
                'value': '2nd rev.',
                'responsibility': 'by B. Roberts'
            }],
            'parallel': [{
                'value': '1a ed.',
                'responsibility': 'editado por J. Smith'
            }, {
                'value': '2a rev.',
                'responsibility': 'por B. Roberts'
            }]
        },
        'materials_specified': None,
    }),

    # $b follows 2nd (or 3rd, etc.) ISBD punctuation mark
    ('250 ## $a1st ed. / edited by J. Smith =$b1a ed. / editado por J. Smith, '
             '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }, {
                'value': '2nd rev.',
                'responsibility': 'by B. Roberts'
            }],
            'parallel': [{
                'value': '1a ed.',
                'responsibility': 'editado por J. Smith'
            }, {
                'value': '2a rev.',
                'responsibility': 'por B. Roberts'
            }]
        },
        'materials_specified': None,
    }),

    # Multiple $bs
    ('250 ## $a1st ed. /$bedited by J. Smith =$b1a ed. / editado por J. Smith, '
             '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }, {
                'value': '2nd rev.',
                'responsibility': 'by B. Roberts'
            }],
            'parallel': [{
                'value': '1a ed.',
                'responsibility': 'editado por J. Smith'
            }, {
                'value': '2a rev.',
                'responsibility': 'por B. Roberts'
            }]
        },
        'materials_specified': None,
    }),

    # Extra spacing between `/` and $b
    ('250 ## $a1st ed. / $bedited by J. Smith', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Extra spacing between `=` and $b
    ('250 ## $a1st ed. = $b1a ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}],
            'parallel': [{'value': '1a ed.'}]
        },
        'materials_specified': None,
    }),

    # 251 Versions
    # Single version
    ('251 ## $aFirst draft.',
     {
        'edition_type': 'version',
        'edition_info': {
            'editions': [{
                'value': 'First draft',
            }]
        },
        'materials_specified': None,
    }),

    # Version plus materials specified
    ('251 ## $31988-1989:$aFirst draft.',
     {
        'edition_type': 'version',
        'edition_info': {
            'editions': [{
                'value': 'First draft',
            }]
        },
        'materials_specified': ['1988-1989'],
    }),

    # Multiple versions, in multiple $as
    ('251 ## $aFirst draft$aSecond version',
     {
        'edition_type': 'version',
        'edition_info': {
            'editions': [{
                'value': 'First draft; Second version',
            }]
        },
        'materials_specified': None,
    }),


    # 254 Music Presentation Statements
    # Single statement
    ('254 ## $aFull score.',
     {
        'edition_type': 'musical_presentation_statement',
        'edition_info': {
            'editions': [{
                'value': 'Full score',
            }]
        },
        'materials_specified': None,
    }),

    # Multiple statements in multiple $as
    ('254 ## $aFull score$aPartitur.',
     {
        'edition_type': 'musical_presentation_statement',
        'edition_info': {
            'editions': [{
                'value': 'Full score; Partitur',
            }]
        },
        'materials_specified': None,
    }),

], ids=[
    # 250 Edition Statements
    'Simple edition by itself',
    'Edition and materials specified',
    'Edition with bracketed portion',
    'Edition all in brackets',
    'Simple edition plus responsibility',
    'Edition plus responsibility and revision',
    'Edition plus responsibility and revision plus responsibility',
    'Edition and parallel edition',
    'Edition/parallel, with one SOR at end',
    'Edition, with SOR and parallel SOR',
    'Edition/SOR plus parallel edition/SOR',
    'Edition/revision plus parallel (including SORs)',

    # 250s, edges of "revision" detection
    'Otherwise valid pattern not following " = " not recognized',
    'Obvious names are not recognized',
    'Valid numeric pattern is recognized',
    'Valid one-word pattern is recognized',
    'Valid multi-word pattern is recognized',

    # 250s, other edge cases
    'Missing `/` before $b',
    'Full edition statement is in $a (no $b)',
    '$b follows 2nd (or 3rd, etc.) ISBD punctuation mark',
    'Multiple $bs',
    'Extra spacing between `/` and $b',
    'Extra spacing between `=` and $b',

    # 251 Versions
    'Single version',
    'Version plus materials specified;',
    'Multiple versions, in multiple $as',

    # 254 Music Presentation Statements
    'Single statement',
    'Multiple statements in multiple $as',
])
def test_editionparser_parse(raw_marcfield, expected, fieldstrings_to_fields):
    """
    When passed the given MARC field, the `EditionParser.parse` method
    should return the expected results.
    """
    field = fieldstrings_to_fields([raw_marcfield])[0]
    result = s2m.EditionParser(field).parse()
    print result
    assert result == expected


@pytest.mark.parametrize('raw_marcfields, expected', [
    # Edge cases
    # No 250, 251, 254 => no editions
    ([], {}),
    # 250, 251, 254 with no $ab => no editions
    (['250 ## $cThis is$dincorrect.'], {}),

    # 250 Edition Statements
    # Simple edition by itself
    (['250 ## $a1st ed.'],
     {
        'editions_display': ['1st ed.'],
        'editions_search': ['1st ed.']
    }),
    
    # Edition and materials specified
    (['250 ## $31988-1989:$a1st ed.'],
     {
        'editions_display': ['(1988-1989): 1st ed.'],
        'editions_search': ['1st ed.']
    }),

    # Simple edition plus responsibility
    (['250 ## $a1st ed. /$bedited by J. Smith'],
     {
        'editions_display': ['1st ed., edited by J. Smith'],
        'editions_search': ['1st ed.'],
        'responsibility_search': ['edited by J. Smith']
    }),

    # Edition plus responsibility and revision
    # Note: the revision goes into `responsibility_search` -- not much
    # we can do about that.
    (['250 ## $a1st ed. /$bedited by J. Smith, 2nd rev.'],
     {
        'editions_display': ['1st ed., edited by J. Smith, 2nd rev.'],
        'editions_search': ['1st ed.'],
        'responsibility_search': ['edited by J. Smith, 2nd rev.']
    }),

    # Edition plus responsibility and revision plus responsibility
    (['250 ## $a1st ed. /$bedited by J. Smith, 2nd rev. / by B. Roberts'],
     {
        'editions_display': [
            '1st ed., edited by J. Smith, 2nd rev., by B. Roberts'
        ],
        'editions_search': ['1st ed.'],
        'responsibility_search': [
            'edited by J. Smith, 2nd rev., by B. Roberts'
        ]
    }),

    # Edition and parallel edition
    (['250 ## $a1st ed. =$b1a ed.'],
     {
        'editions_display': ['1st ed. [translated: 1a ed.]'],
        'editions_search': ['1st ed.', '1a ed.'],
    }),

    # Edition/parallel, with one SOR at end
    (['250 ## $a1st ed. =$b1a ed. / edited by J. Smith'],
     {
        'editions_display': [
            '1st ed., edited by J. Smith [translated: 1a ed.]'
        ],
        'editions_search': ['1st ed.', '1a ed.'],
        'responsibility_search': ['edited by J. Smith']
    }),

    # Edition, with SOR and parallel SOR
    (['250 ## $a1st ed. /$bedited by J. Smith = editado por J. Smith.'],
     {
        'editions_display': [
            '1st ed., edited by J. Smith [translated: editado por J. Smith]'
        ],
        'editions_search': ['1st ed.'],
        'responsibility_search': ['edited by J. Smith', 'editado por J. Smith']
    }),

    # Edition/SOR plus parallel edition/SOR
    (['250 ## $a1st ed. /$bedited by J. Smith = 1a ed. / editado por J. '
              'Smith.'],
     {
        'editions_display': [
            '1st ed., edited by J. Smith [translated: 1a ed., editado por J. '
            'Smith]'
        ],
        'editions_search': ['1st ed.', '1a ed.'],
        'responsibility_search': ['edited by J. Smith', 'editado por J. Smith']
    }),

    # Edition/revision plus parallel (including SORs)
    (['250 ## $a1st ed. /$bedited by J. Smith = 1a ed. / editado por J. '
              'Smith, New rev. / by B. Roberts = Nueva rev. / por B. Roberts.'],
     {
        'editions_display': [
            '1st ed., edited by J. Smith; New rev., by B. Roberts [translated: '
            '1a ed., editado por J. Smith; Nueva rev., por B. Roberts]'
        ],
        'editions_search': [
            '1st ed.; New rev.',
            '1a ed.; Nueva rev.'
        ],
        'responsibility_search': [
            'edited by J. Smith; by B. Roberts', 
            'editado por J. Smith; por B. Roberts'
        ]
    }),

    # Edition/revision plus multiple parallels
    (['250 ## $a1st ed. /$bedited by J. Smith = 1a ed. / editado por J. '
              'Smith = 1e ed. / whatever, New rev. / by B. Roberts = '
              'Nueva rev. / por B. Roberts.'],
     {
        'editions_display': [
            '1st ed., edited by J. Smith; New rev., by B. Roberts [translated: '
            '1a ed., editado por J. Smith; 1e ed., whatever; Nueva rev., '
            'por B. Roberts]'
        ],
        'editions_search': [
            '1st ed.; New rev.',
            '1a ed.; 1e ed.; Nueva rev.'
        ],
        'responsibility_search': [
            'edited by J. Smith; by B. Roberts', 
            'editado por J. Smith; whatever; por B. Roberts'
        ]
    }),

    # 251 Versions
    # Single version
    (['251 ## $aFirst draft.'],
     {
        'editions_display': ['First draft'],
        'editions_search': ['First draft']
    }),

    # Version plus materials specified
    (['251 ## $31988-1989:$aFirst draft.'],
     {
        'editions_display': ['(1988-1989): First draft'],
        'editions_search': ['First draft']
    }),

    # Multiple versions, in multiple $as
    (['251 ## $aFirst draft$aSecond version.'],
     {
        'editions_display': ['First draft; Second version'],
        'editions_search': ['First draft; Second version']
    }),

    # Multiple versions, multiple 251 instances
    (['251 ## $aFirst draft.',
      '251 ## $aSecond version.'],
     {
        'editions_display': ['First draft', 'Second version'],
        'editions_search': ['First draft', 'Second version']
    }),

    # 254 Music Presentation Statements
    # Single statement
    (['254 ## $aFull score.'],
     {
        'editions_display': ['Full score'],
        'type_format_search': ['Full score']
    }),

    # Multiple statements in multiple $as
    (['254 ## $aFull score.$aPartitur.'],
     {
        'editions_display': ['Full score; Partitur'],
        'type_format_search': ['Full score; Partitur']
    }),

    # Multiple statements in multiple 254s
    (['254 ## $aFull score.',
      '254 ## $aPartitur.'],
     {
        'editions_display': ['Full score', 'Partitur'],
        'type_format_search': ['Full score', 'Partitur']
    }),

    # Other
    # 250, 251, 254 all at once
    (['250 ## $a1st ed. =$b1a ed. / edited by J. Smith.',
      '250 ## $aNew rev. =$bNueva rev. / revised by B. Roberts.',
      '251 ## $aFinal draft.',
      '254 ## $aFull score.'],
     {
        'editions_display': [
            '1st ed., edited by J. Smith [translated: 1a ed.]',
            'New rev., revised by B. Roberts [translated: Nueva rev.]',
            'Final draft',
            'Full score'
        ],
        'editions_search': [
            '1st ed.',
            '1a ed.',
            'New rev.',
            'Nueva rev.',
            'Final draft',
        ],
        'responsibility_search': [
            'edited by J. Smith',
            'revised by B. Roberts'
        ],
        'type_format_search': ['Full score']
    }),
], ids=[
    # Edge cases
    'Empty 250, 251, 254 => no editions',
    '250, 251, 254 with no $ab => no editions',

    # 250 Edition Statements
    'Simple edition by itself',
    'Edition and materials specified',
    'Simple edition plus responsibility',
    'Edition plus responsibility and revision',
    'Edition plus responsibility and revision plus responsibility',
    'Edition and parallel edition',
    'Edition/parallel, with one SOR at end',
    'Edition, with SOR and parallel SOR',
    'Edition/SOR plus parallel edition/SOR',
    'Edition/revision plus parallel (including SORs)',
    'Edition/revision plus multiple parallels',

    # 251 Versions
    'Single version',
    'Version plus materials specified',
    'Multiple versions, in multiple $as',
    'Multiple versions, multiple 251 instances',

    # 254 Music Presentation Statements
    'Single statement',
    'Multiple statements in multiple $as',
    'Multiple statements in multiple 254s',

    # Other
    '250, 251, 254 all at once',
])
def test_todscpipeline_geteditions(raw_marcfields, expected,
                                   fieldstrings_to_fields,
                                   bl_sierra_test_record,
                                   todsc_pipeline_class,
                                   bibrecord_to_pymarc,
                                   add_marc_fields,
                                   assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_editions should return data matching the
    expected parameters.
    """
    pipeline = todsc_pipeline_class()
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('250', '251', '254')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, ['editions'])
    assert_bundle_matches_expected(bundle, expected)


@pytest.mark.parametrize('raw_marcfields, expected', [
    # Edge cases
    # No 866s => no library has
    ([], {}),
    # 866 with no $a or $z => no library has
    (['866 ## $cThis is$dincorrect.'], {}),

    # 866s
    # 866 with $a and $z
    (['866 31 $av. 1-4 (1941-1943), v. 6-86 (1945-1987)$zSome issues missing'], 
     {'library_has_display': [
        'v. 1-4 (1941-1943), v. 6-86 (1945-1987); Some issues missing'
      ]
    }),

    # 866 with $a and multiple $zs
    (['866 31 $av. 1-4 (1941-1943), v. 6-86 (1945-1987)$zSome issues missing;'
             '$zAnother note'], 
     {'library_has_display': [
        'v. 1-4 (1941-1943), v. 6-86 (1945-1987); Some issues missing; '
        'Another note'
      ]
    }),

    # 866 with $8, $a, $x, $2, $z -- only $a and $z are included
    (['866 31 $80$av. 1-4 (1941-1943), v. 6-86 (1945-1987)$xinternal note'
             '$zSome issues missing$2usnp'], 
     {'library_has_display': [
        'v. 1-4 (1941-1943), v. 6-86 (1945-1987); Some issues missing'
      ]
    }),

    # 866 with only $a
    (['866 31 $av. 1-4 (1941-1943), v. 6-86 (1945-1987)'], 
     {'library_has_display': [
        'v. 1-4 (1941-1943), v. 6-86 (1945-1987)'
      ]
    }),

    # 866 with only $z
    (['866 31 $zSome issues missing'], 
     {'library_has_display': [
        'Some issues missing'
      ]
    }),

    # Multiple 866s
    (['866 31 $av. 1-4 (1941-1943)',
      '866 31 $av. 6-86 (1945-1987)',
      '866 31 $zSome issues missing'], 
     {'library_has_display': [
        'v. 1-4 (1941-1943)',
        'v. 6-86 (1945-1987)',
        'Some issues missing'
      ]
    }),

], ids=[
    # Edge cases
    'No 866s => no library has',
    '866 with no $a or $z => no library has',

    # 866s
    '866 with $a and $z',
    '866 with $a and multiple $zs',
    '866 with $8, $a, $x, $2, $z -- only $a and $z are included',
    '866 with only $a',
    '866 with only $z',
    'Multiple 866s',
])
def test_todscpipeline_getserialholdings(raw_marcfields, expected,
                                         fieldstrings_to_fields,
                                         bl_sierra_test_record,
                                         todsc_pipeline_class,
                                         bibrecord_to_pymarc,
                                         add_marc_fields,
                                         assert_bundle_matches_expected):
    """
    ToDiscoverPipeline.get_serial_holdings should return data matching
    the expected parameters.
    """
    pipeline = todsc_pipeline_class()
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bib = bl_sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_pymarc(bib)
    bibmarc.remove_fields('866')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, ['serial_holdings'])
    assert_bundle_matches_expected(bundle, expected)


def test_s2mmarcbatch_compileoriginalmarc_vf_order(s2mbatch_class,
                                                   bl_sierra_test_record,
                                                   add_varfields_to_record):
    """
    DiscoverS2MarcBatch `compile_original_marc` method should
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
