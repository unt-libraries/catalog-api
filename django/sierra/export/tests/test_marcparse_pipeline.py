# -*- coding: utf-8 -*-

"""
Tests the blacklight.sierra2marc functions.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import datetime

import pytest
import pytz
import ujson
from six.moves import range, zip

from export import sierramarc as sm
from export.marcparse import pipeline as pl

# FIXTURES AND TEST DATA
pytestmark = pytest.mark.django_db(databases=['sierra'])


@pytest.fixture
def bibrecord_to_marc():
    """
    Pytest fixture for converting a `bib` from the Sierra DB (i.e. a
    base.models.BibRecord instance) to a SierraMarcRecord object.
    """
    def _bibrecord_to_marc(bib):
        return sm.SierraToMarcConverter().compile_original_marc(bib)
    return _bibrecord_to_marc


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
    running a BibDataPipeline process matches an `expected` dict of
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
            print(('{} => {}'.format(k, v)))
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
def subject_sd_test_mappings():
    """
    Pytest fixture. Returns a dict with two elements:
    `sd_patterns` and `sd_term_map`. Use these for subject tests --
    override the `subject_sd_patterns` and `subject_sd_term_map` on the
    pipeline object before calling the `do` method. (This gives us
    stable pattern/term mappings to use for testing, since the live
    ones may change over time.)
    """
    war_words = '(?:war|revolution)'
    sample_pattern_map = [
        [r'annexation to (.+)',
            [('topic', 'Annexation (International law)'), ('region', '{}')],
            'Annexation to the United States'],
        [r'art and(?: the)? {}'.format(war_words),
            [('topic', 'Art and war')],
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
    return {
        'sd_patterns': sample_pattern_map,
        'sd_term_map': sample_term_map
    }


# TESTS


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
    marc_record = sm.SierraMarcRecord(force_utf8=True)
    fields = fieldstrings_to_fields(rawfields)
    marc_record.add_field(*fields)
    grouper = pl.MarcFieldGrouper({
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



def test_bdpipeline_do_creates_compiled_dict():
    """
    The `do` method of BibDataPipeline should return a dict compiled
    from the return value of each of the `get` methods--each key/value
    pair from each return value added to the finished value. If the
    same dict key is returned by multiple methods and the vals are
    lists, the lists are concatenated.
    """
    class DummyPipeline(pl.BibDataPipeline):
        fields = ['dummy1', 'dummy2', 'dummy3', 'dummy4']
        prefix = 'get_'

        def get_dummy1(self):
            return {'d1': 'd1v'}

        def get_dummy2(self):
            return {'d2a': 'd2av', 'd2b': 'd2bv'}

        def get_dummy3(self):
            return {'stuff': ['thing']}

        def get_dummy4(self):
            return {'stuff': ['other thing']}

    dummy_pipeline = DummyPipeline()
    bundle = dummy_pipeline.do(None, None)
    assert bundle == {'d1': 'd1v', 'd2a': 'd2av', 'd2b': 'd2bv',
                      'stuff': ['thing', 'other thing']}


def test_bdpipeline_getid(sierra_test_record):
    """
    BibDataPipeline.get_id should return the bib Record ID
    formatted according to III's specs.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('b6029459')
    val = pipeline.do(bib, None, ['id'])
    assert val == {'id': 'b6029459'}


@pytest.mark.parametrize('in_val, expected', [
    (True, 'true'),
    (False, 'false')
])
def test_bdpipeline_getsuppressed(in_val, expected, sierra_test_record,
                                  setattr_model_instance):
    """
    BibDataPipeline.get_suppressed should return 'false' if the
    record is not suppressed.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('b6029459')
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
def test_bdpipeline_getdateadded(bib_locs, created_date, cat_date, expected,
                                 sierra_test_record,
                                 get_or_make_location_instances,
                                 update_test_bib_inst, setattr_model_instance):
    """
    BibDataPipeline.get_date_added should return the correct date
    in the datetime format Solr requires.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    loc_info = [{'code': code} for code in bib_locs]
    locations = get_or_make_location_instances(loc_info)
    if locations:
        bib = update_test_bib_inst(bib, locations=locations)
    setattr_model_instance(bib, 'cataloging_date_gmt', cat_date)
    setattr_model_instance(bib.record_metadata, 'creation_date_gmt',
                           created_date)
    val = pipeline.do(bib, None, ['date_added'])
    assert val == {'date_added': expected}


def test_bdpipeline_getiteminfo_ids(sierra_test_record, update_test_bib_inst,
                                    assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BibDataPipeline.get_item_info should be a list of JSON objects,
    each one corresponding to an item. The 'i' key for each JSON object
    should match the numeric portion of the III rec num for that item.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=[{}, {'is_suppressed': True}, {}])
    val = pipeline.do(bib, None, ['item_info'])

    links = bib.bibrecorditemrecordlink_set.all().order_by(
        'items_display_order', 'item_record__record_metadata__record_num'
    )
    expected = [{'i': str(link.item_record.record_metadata.record_num)}
                for link in links if not link.item_record.is_suppressed]
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
def test_bdpipeline_getiteminfo_callnum_vol(bib_cn_info, items_info, expected,
                                            sierra_test_record,
                                            update_test_bib_inst,
                                            assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BibDataPipeline.get_item_info should be a list of JSON objects,
    each one corresponding to an item. The 'c' key for each JSON object
    contains the call number, and the 'v' key contains the volume.
    Various parameters test how the item call numbers and volumes are
    generated.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
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
def test_bdpipeline_getiteminfo_bcodes_notes(items_info, expected,
                                             sierra_test_record,
                                             update_test_bib_inst,
                                             assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BibDataPipeline.get_item_info should be a list of JSON objects,
    each one corresponding to an item. The 'b' and 'n' keys for each
    JSON object contain the barcode and public notes, respectively.
    Various parameters test how those are generated.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
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
      {'b': '1'}, ]),
], ids=[
    'fewer than three items => expect <3 items, no more_items',
    'three items => expect 3 items, no more_items',
    'more than three items => expect >3 items, plus more_items',
    'multiple items in bizarre order stay in order'
])
def test_bdpipeline_getiteminfo_num_items(items_info, exp_items,
                                          exp_more_items, sierra_test_record,
                                          update_test_bib_inst,
                                          assert_json_matches_expected):
    """
    BibDataPipeline.get_item_info return value should be a dict with
    keys `items_json`, `more_items_json`, and `has_more_items` that are
    based on the total number of items on the record. The first three
    attached items are in items_json; others are in more_items_json.
    has_more_items is 'true' if more_items_json is not empty.
    Additionally, items should remain in the order they appear on the
    record.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
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


@pytest.mark.parametrize('f856s, items_info, expected_r', [
    ([],
     [({'location_id': 'x'}, {}),
      ({'location_id': 'w3'}, {}),
      ({'location_id': 'w4mau', 'itype_id': 7}, {}),
      ({'location_id': 'xmau', 'itype_id': 7}, {})],
     'catalog'),
    ([],
     [({'location_id': 'czwww'}, {}),
      ({'location_id': 'pwww'}, {}),
      ({'location_id': 'w3', 'item_status_id': 'o'}, {}),
      ({'location_id': 'w3', 'itype_id': 20}, {}),
      ({'location_id': 'w3', 'itype_id': 29}, {}),
      ({'location_id': 'w3', 'itype_id': 69}, {}),
      ({'location_id': 'w3', 'itype_id': 74}, {}),
      ({'location_id': 'w3', 'itype_id': 112}, {}),
      ],
     None),
    ([],
     [({'location_id': 'w4spe'}, {}),
      ({'location_id': 'xspe'}, {}),
      ({'location_id': 'w4mr1'}, {}),
      ({'location_id': 'w4mr2'}, {}),
      ({'location_id': 'w4mr3'}, {}),
      ({'location_id': 'w4mrb'}, {}),
      ({'location_id': 'w4mrx'}, {})
      ], 'aeon'),
    ([],
     [({'location_id': 'w4spc'}, {}),
      ({'location_id': 'xspc'}, {}),
      ], None),
    ([('856', ['u', 'http://example.com', 'y', 'The Resource',
               'z', 'connect to electronic resource'])],
     [({'location_id': 'w4spc'}, {}),
      ({'location_id': 'xspc'}, {}),
      ], None),
    ([('856', ['u', 'http://findingaids.library.unt.edu/?p=collections/'
                    'findingaid&id=897',
               'z', 'Connect to finding aid'])],
     [({'location_id': 'w4spc'}, {}),
      ({'location_id': 'xspc'}, {}),
      ], 'finding_aid'),
    ([('856', ['u', 'http://example.com', 'y', 'The Resource',
               'z', 'connect to electronic resource']),
      ('856', ['u', 'http://findingaids.library.unt.edu/?p=collections/'
                    'findingaid&id=897',
               'z', 'Connect to finding aid'])],
     [({'location_id': 'w4spc'}, {}),
      ({'location_id': 'xspc'}, {}),
      ], 'finding_aid'),
    ([],
     [({'location_id': 'jlf'}, {})],
     'jlf'),
], ids=[
    'items that are requestable through the catalog (Sierra)',
    'items that are not requestable',
    'items that are requestable through Aeon',
    'finding aid items w/o url are not requestable',
    'finding aid items w/o finding aid url are not requestable',
    'finding aid items w/link in 1st 856 are requestable via finding aid',
    'finding aid items w/link in 2nd 856 are requestable via finding aid',
    'items that are at JLF'
])
def test_bdpipeline_getiteminfo_requesting(f856s, items_info, expected_r,
                                           sierra_test_record,
                                           update_test_bib_inst,
                                           bibrecord_to_marc, params_to_fields,
                                           add_marc_fields,
                                           assert_json_matches_expected):
    """
    The `items_json` key of the value returned by
    BibDataPipeline.get_item_info should be a list of JSON objects,
    each one corresponding to an item. The 'r' key for each JSON object
    contains a string describing how end users request the item. (See
    parameters for details.) Note that this hits the highlights but
    isn't exhaustive.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('856', '962')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(f856s))
    val = pipeline.do(bib, bibmarc, ['item_info'])
    exp_items = [{'r': expected_r} for i in range(0, len(items_info))]
    assert_json_matches_expected(val['items_json'], exp_items[0:3],
                                 complete=False)
    if val['more_items_json'] is not None:
        assert_json_matches_expected(val['more_items_json'], exp_items[3:],
                                     complete=False)


@pytest.mark.parametrize('items_info, exp_order', [
    ([(('b', None, ['100']), 1),
      (('b', None, ['101']), 0),
      (('b', None, ['102']), 0),
      (('b', None, ['103']), 3),
      (('b', None, ['104']), 4)],
     ['101', '102', '100', '103', '104']),
    ([(('b', None, ['100']), None),
      (('b', None, ['101']), 0),
      (('b', None, ['102']), 1),
      (('b', None, ['103']), None),
      (('b', None, ['104']), 4)],
     ['101', '102', '104', '100', '103']),
    ([(('b', None, ['100']), None),
      (('b', None, ['101']), None),
      (('b', None, ['102']), None),
      (('b', None, ['103']), None),
      (('b', None, ['104']), 0)],
     ['104', '100', '101', '102', '103']),
    ([(('b', None, ['100']), 1),
      (('b', None, ['101']), 0),
      (('b', None, ['102']), 3),
      (('b', None, ['103']), 2),
      (('b', None, ['104']), 4)],
     ['101', '100', '103', '102', '104']),
])
def test_bdpipeline_sorteditems(items_info, exp_order, sierra_test_record,
                                update_test_bib_inst):
    """
    When the BibDataPipeline.get_item_info method compiles items from
    the bib record, it normally will use the `items_display_order`
    field on the bib->item link to sort the items, to preserve the
    order items would display in Sierra and the III catalog. However,
    we have encountered cases where (for some reason) one or more items
    have a null value (i.e. `None`) for that field. When this happens,
    it's unlikely the sort order will match what's in Sierra. But, in
    order to give it a useful fallback, if the display order is null
    then it will default to float('inf') and use the item record num as
    a secondary sort. This matches what happens in Sierra: items that
    have an `items_display_order` value sort first, in order; items
    without one then display, sorted in record num order.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    items_param = [({}, [tup[0]]) for tup in items_info]
    items_display_order = [tup[1] for tup in items_info]
    bib = update_test_bib_inst(bib, items=items_param)
    for i, link in enumerate(bib.bibrecorditemrecordlink_set.all()):
        link._write_override = True
        link.items_display_order = items_display_order[i]
        link.save()
    pipeline.set_up(bib)
    bc_results = [
        i.record_metadata.varfield_set.get(
            varfield_type_code='b').field_content
        for i in pipeline.sorted_items
    ]
    assert bc_results == exp_order


@pytest.mark.parametrize('bib_locations, bib_cn_info, expected', [
    ([('w', 'Willis Library'), ('czm', 'Chilton Media Library')],
     [('c', '090', ['|aTEST BIB CN'])],
     [{'i': None, 'c': 'TEST BIB CN', 'l': 'w'},
      {'i': None, 'c': 'TEST BIB CN', 'l': 'czm'}]),
    ([],
     [('c', '090', ['|aTEST BIB CN'])],
     [{'i': None, 'c': 'TEST BIB CN', 'l': 'none'}]),
])
def test_bdpipeline_getiteminfo_pseudo_items(bib_locations, bib_cn_info,
                                             expected, sierra_test_record,
                                             get_or_make_location_instances,
                                             update_test_bib_inst,
                                             assert_json_matches_expected):
    """
    When a bib record has no attached items, the `items_json` key of
    the value returned by BibDataPipeline.get_item_info should contain
    "pseudo-item" entries generated based off the bib locations and bib
    call numbers.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
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
       'l': 'The Resource', 't': 'fulltext'}]),
    ([('856', ['u', 'http://example.com" target="_blank"', 'y', 'The Resource',
               'z', 'connect to electronic resource'])],
     [],
     [{'u': 'http://example.com', 'n': 'connect to electronic resource',
       'l': 'The Resource', 't': 'fulltext'}]),
    ([('856', ['u', 'http://example.com', 'u', 'incorrect',
               'z', 'connect to electronic resource', 'z', 'some version'])],
     [],
     [{'u': 'http://example.com',
       'n': 'connect to electronic resource some version',
       't': 'fulltext'}]),
    ([('856', ['u', 'http://example.com'])],
     [],
     [{'u': 'http://example.com', 't': 'link'}]),
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
     [{'u': 'http://example.com', 'n': 'Media Thing', 't': 'media'}]),
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
     [{'t': 'fulltext'}]),
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
def test_bdpipeline_geturlsjson(fparams, items_info, expected,
                                sierra_test_record, bibrecord_to_marc,
                                update_test_bib_inst, params_to_fields,
                                add_marc_fields, assert_json_matches_expected):
    """
    The `urls_json` key of the value returned by
    BibDataPipeline.get_urls_json should be a list of JSON objects,
    each one corresponding to a URL.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bib = update_test_bib_inst(bib, items=items_info)
    bibmarc = bibrecord_to_marc(bib)
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
def test_bdpipeline_getthumbnailurl(fparams, expected_url, sierra_test_record,
                                    bibrecord_to_marc, params_to_fields,
                                    add_marc_fields):
    """
    BibDataPipeline.get_thumbnail_url should return a URL for a local
    thumbnail image, if one exists. (Either Digital Library or a Media
    Library thumbnail.)
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('856', '962')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    val = pipeline.do(bib, bibmarc, ['thumbnail_url'])
    assert val['thumbnail_url'] == expected_url


@pytest.mark.parametrize('ldr_07, f008_06, date1, date2, expected', [
    # General edge cases -- non-dates should not generate entries
    ('m', 's', '', '', []),
    ('m', 's', '    ', '    ', []),
    ('m', 's', '0000', '0000', []),
    ('m', 's', '3952', '', []),
    ('m', 's', '21uu', '', []),

    # Singular dates, non-serials -- coded correctly
    ('m', 's', '1980', '    ', [('1980', None, 1980, 1980, 'publication')]),
    ('m', 's', '201u', '    ', [('201u', None, 2010, 2019, 'publication')]),
    ('m', 's', '20uu', '    ', [('20uu', None, 2000, 2026, 'publication')]),
    ('m', 's', '2uuu', '    ', [('2uuu', None, 2000, 2026, 'publication')]),
    ('m', 's', '1uuu', '    ', [('uuuu', None, -1, -1, 'publication')]),
    ('m', 'e', '1980', '11uu', [('1980', None, 1980, 1980, 'publication')]),
    ('m', 'e', '19uu', '11uu', [('19uu', None, 1900, 1999, 'publication')]),

    # Singular dates, non-serials -- coded incorrectly
    ('m', 's', '1980', '2002', [('1980', None, 1980, 1980, 'publication')]),
    ('m', 's', '9999', '    ', []),
    ('m', 's', '9999', '9999', []),
    ('m', 'c', '1980', '2002', [('1980', None, 1980, 1980, 'publication')]),
    ('m', 'd', '1980', '2002', [('1980', None, 1980, 1980, 'publication')]),
    ('m', 'u', '1980', '2002', [('1980', None, 1980, 1980, 'publication')]),

    # Singular dates, serials (coded incorrectly, by definition)
    ('s', 's', '1980', '    ', [('1980', None, 1980, 1980, 'publication')]),
    ('s', 's', '1980', '9999', [('1980', None, 1980, 1980, 'publication')]),
    ('s', 's', '1980', '1985', [('1980', None, 1980, 1980, 'publication')]),
    ('s', 's', '201u', '    ', [('201u', None, 2010, 2019, 'publication')]),
    ('s', 'e', '1980', '11uu', [('1980', None, 1980, 1980, 'publication')]),

    # Multiple atomic dates, non-serials -- coded correctly
    ('m', 'p', '1985', '1976', [('1985', None, 1985, 1985, 'distribution'),
                                ('1976', None, 1976, 1976, 'creation')]),
    ('m', 'p', 'uuuu', '1976', [('uuuu', None, -1, -1, 'distribution'),
                                ('1976', None, 1976, 1976, 'creation')]),
    ('m', 'p', '1985', 'uuuu', [('1985', None, 1985, 1985, 'distribution'),
                                ('uuuu', None, -1, -1, 'creation')]),
    ('m', 'p', 'uuuu', 'uuuu', [('uuuu', None, -1, -1, 'distribution'),
                                ('uuuu', None, -1, -1, 'creation')]),
    ('m', 'r', '1985', '1976', [('1985', None, 1985, 1985, 'distribution'),
                                ('1976', None, 1976, 1976, 'publication')]),
    ('m', 't', '1976', '1985', [('1976', None, 1976, 1976, 'publication'),
                                ('1985', None, 1985, 1985, 'copyright')]),

    # Multiple atomic dates, non-serials -- coded incorrectly
    ('m', 'p', '    ', '1976', [('1976', None, 1976, 1976, 'creation')]),
    ('m', 'p', '1985', '    ', [('1985', None, 1985, 1985, 'distribution')]),
    ('m', 'p', '9999', '9999', []),
    ('m', 'p', '    ', '    ', []),

    # Multiple atomic dates, serials -- same as non-serials
    ('s', 'p', '1985', '1976', [('1985', None, 1985, 1985, 'distribution'),
                                ('1976', None, 1976, 1976, 'creation')]),
    ('i', 'p', '1985', '1976', [('1985', None, 1985, 1985, 'distribution'),
                                ('1976', None, 1976, 1976, 'creation')]),

    # Date ranges, non-serials -- coded correctly
    ('m', 'i', '1980', '1995', [('1980', '1995', 1980, 1995, 'creation')]),
    ('m', 'k', '1980', '1995', [('1980', '1995', 1980, 1995, 'creation')]),
    ('m', 'm', '1980', '1995', [('1980', '1995', 1980, 1995, 'publication')]),
    ('m', 'q', '1980', '1995', [('1980', '1995', 1980, 1995, 'publication')]),
    ('m', 'q', '19uu', '1915', [('19uu', '1915', 1900, 1915, 'publication')]),
    ('m', 'q', '196u', '196u', [('196u', None, 1960, 1969, 'publication')]),

    # Date ranges, serials -- coded correctly
    ('s', 'c', '1980', '9999', [('1980', '9999', 1980, 2021, 'publication')]),
    ('s', 'c', '198u', '9999', [('198u', '9999', 1980, 2021, 'publication')]),
    ('i', 'c', '198u', '9999', [('198u', '9999', 1980, 2021, 'publication')]),
    ('s', 'd', '1980', '1985', [('1980', '1985', 1980, 1985, 'publication')]),
    ('i', 'd', '1980', '1985', [('1980', '1985', 1980, 1985, 'publication')]),
    ('s', 'd', '19uu', '193u', [('19uu', '193u', 1900, 1939, 'publication')]),
    ('s', 'u', '1980', 'uuuu', [('1980', 'uuuu', 1980, 1980, 'publication')]),
    ('i', 'u', '1980', 'uuuu', [('1980', 'uuuu', 1980, 1980, 'publication')]),
    ('s', 'u', '198u', 'uuuu', [('198u', 'uuuu', 1980, 1989, 'publication')]),
    ('s', 'u', '1uuu', 'uuuu', [('uuuu', None, -1, -1, 'publication')]),

    # Date ranges -- coded incorrectly, or otherwise weird edge cases
    ('s', 'c', '1980', '1985', [('1980', '1985', 1980, 1985, 'publication')]),
    ('s', 'c', '1980', '    ', [('1980', '9999', 1980, 2021, 'publication')]),
    ('s', 'c', '1980', 'uuuu', [('1980', 'uuuu', 1980, 1980, 'publication')]),
    ('s', 'd', '1980', '9999', [('1980', '9999', 1980, 2021, 'publication')]),
    ('s', 'd', '1980', '    ', [('1980', '9999', 1980, 2021, 'publication')]),
    ('s', 'u', '1980', '1985', [('1980', '1985', 1980, 1985, 'publication')]),
    ('s', 'u', '1980', '9999', [('1980', '9999', 1980, 2021, 'publication')]),
    ('s', 'c', '1980', '1979', [('1980', None, 1980, 1980, 'publication')]),
    ('s', 'd', '2023', '2027', [('2023', None, 2023, 2023, 'publication')]),
    ('s', 'd', '2023', '2026', [('2023', '2026', 2023, 2026, 'publication')]),
    ('s', 'd', '9999', '1979', [('1979', None, 1979, 1979, 'publication')]),
    ('s', 'd', '9999', '9999', [('9999', None, 2021, 2021, 'publication')]),
    ('s', 'd', '9999', '    ', [('9999', None, 2021, 2021, 'publication')]),
    ('s', 'd', '9999', '    ', [('9999', None, 2021, 2021, 'publication')]),
])
def test_bdpipeline_interpretcodeddate(ldr_07, f008_06, date1, date2, expected):
    """
    BibDataPipeline.interpret_coded_date should return the expected
    values, given the provided parameters.
    """
    pipeline = pl.BibDataPipeline()
    mr = sm.SierraMarcRecord()
    mr.leader = mr.leader[:7] + ldr_07 + mr.leader[8:]
    pipeline.marc_record = mr
    pipeline.this_year = 2021
    pipeline.year_upper_limit = 2026
    result = pipeline.interpret_coded_date(f008_06, date1, date2)
    print((', '.join([ldr_07, f008_06, date1, date2])))
    print(result)
    print(expected)
    assert len(result) == len(expected)
    for row, expected_row in zip(result, expected):
        assert row[:len(expected_row)] == expected_row


@pytest.mark.parametrize('ldr_07, fparams, exp_boost_year, exp_pub_sort, '
                         'exp_pub_year_display, exp_pub_year_facet, '
                         'exp_pub_dates_search', [
     ('m', [('008', 's2004    '),
            ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004'])],
      2004, '2004', '2004', [2004], ['2004']),
     ('m', [('008', 's2004    '),
            ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004'])],
      2004, '2004', '2004', [2004], ['2004', ]),
     ('m', [('008', 's2004    '),
            ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
            ('260', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
      2004, '2004', '2004', [2004, 2005], ['2004', '2005']),
     ('m', [('008', 's2004    '),
            ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
            ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
      2004, '2004', '2004', [2004, 2005], ['2004', '2005']),
     ('m', [('008', 's2004    '),
            ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2005, c2004'])],
      2004, '2004', '2004', [2004, 2005], ['2004', '2005']),
     ('m', [('008', 's2004    '),
            ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2005'], ' 2'),
            ('264', ['c', 'copyright 2004 by XYZ'], ' 4')],
      2004, '2004', '2004', [2004, 2005], ['2004', '2005']),
     ('m', [('008', 's2004    '),
            ('046', ['a', 's', 'c', '2019']),
            ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
            ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
      2004, '2004', '2004', [2004, 2005, 2019], ['2004', '2005', '2019']),
     ('m', [('008', 's2004    '),
            ('046', ['k', '2019']),
            ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
            ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
      2004, '2004', '2004', [2004, 2005, 2019], ['2004', '2005', '2019']),
     ('m', [('008', 's2004    '),
            ('046', ['a', 's', 'c', '2018', 'k', '2019'])],
      2004, '2004', '2004', [2004, 2018, 2019], ['2004', '2018', '2019']),
     ('m', [('008', 's2004    ')],
      2004, '2004', '2004', [2004], ['2004']),
     ('m', [('008', 's2004    '),
            ('046', ['a', 's', 'k', '05'])],
      2004, '2004', '2004', [2004], ['2004']),
     ('m', [('008', 's2004    '),
            ('046', ['a', 's', 'k', '5'])],
      2004, '2004', '2004', [2004], ['2004']),
     ('m', [('008', 's2004    '),
            ('046', ['a', 's', 'k', '21'])],
      2004, '2004', '2004', [2004], ['2004']),
     ('m', [('046', ['a', 's', 'c', '2019']), ],
      2019, '2019', '2019', [2019], ['2019']),
     ('m', [('046', ['k', '2019']), ],
      2019, '2019', '2019', [2019], ['2019']),
     ('m', [('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']), ],
      2004, '2004', '2004', [2004], ['2004']),
     ('m', [('008', 's2004    '),
            ('046', ['k', '2004']),
            ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004']),
            ('264', ['a', 'Place2 :', 'b', 'Printer,', 'c', '2005'])],
      2004, '2004', '2004', [2004, 2005], ['2004', '2005']),
     ('m', [('008', 's2004    '),
            ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2004, c2003',
                     'e', 'Place2 :', 'f', 'Printer', 'g', '2005'])],
      2004, '2004', '2004', [2003, 2004, 2005], ['2003', '2004', '2005']),
     ('m', [('008', 'b2004    ')], 2004, '2004', '2004', [2004], ['2004']),
     ('s', [('008', 'c20189999')], 2021, '2018', '2018 to present',
      [2018, 2019, 2020, 2021], ['2018', '2019', '2020', '2021']),
     ('s', [('008', 'd20142016')], 2016, '2014', '2014 to 2016',
      [2014, 2015, 2016], ['2014', '2015', '2016']),
     ('s', [('008', 'd20142016'),
            ('264', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '2014-2016'])],
      2016, '2014', '2014 to 2016', [2014, 2015, 2016],
      ['2014', '2015', '2016']),
     ('m', [('008', 'e20041126')], 2004, '2004', '2004', [2004], ['2004']),
     ('m', [('008', 'i20142016')], 2014, '2014', '2014 to 2016',
      [2014, 2015, 2016], ['2014', '2015', '2016']),
     ('m', [('008', 'k20142016')], 2014, '2014', '2014 to 2016',
      [2014, 2015, 2016], ['2014', '2015', '2016']),
     ('m', [('008', 'm20142016')], 2014, '2014', '2014 to 2016',
      [2014, 2015, 2016], ['2014', '2015', '2016']),
     ('m', [('008', 'nuuuuuuuu')], None, '----', 'dates unknown', [], []),
     ('m', [('008', 'p20162004')], 2016, '2016', '2016', [2004, 2016],
      ['2004', '2016']),
     ('m', [('008', 'q20042005')], 2004, '2004', '2004 to 2005', [2004, 2005],
      ['2004', '2005']),
     ('m', [('008', 'r20042016')], 2004, '2004', '2004', [2004, 2016],
      ['2004', '2016']),
     ('m', [('008', 't20042016')], 2004, '2004', '2004', [2004, 2016],
      ['2004', '2016']),
     ('s', [('008', 'u2004uuuu')], 2004, '2004', '2004 to ?', [2004],
      ['2004']),
     ('m', [('008', 's199u    ')], 1995, '199-', '1990s',
      [1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999],
      ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998',
       '1999', '1990s']),
     ('m', [('008', 's10uu    ')], 1050, '10--', '11th century',
      [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011,
       1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023,
       1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035,
       1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047,
       1048, 1049, 1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059,
       1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071,
       1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079, 1080, 1081, 1082, 1083,
       1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1094, 1095,
       1096, 1097, 1098, 1099],
      ['11th century', '1000', '1001', '1002', '1003', '1004', '1005', '1006',
       '1007', '1008', '1009', '1010', '1011', '1012', '1013', '1014', '1015',
       '1016', '1017', '1018', '1019', '1020', '1021', '1022', '1023', '1024',
       '1025', '1026', '1027', '1028', '1029', '1030', '1031', '1032', '1033',
       '1034', '1035', '1036', '1037', '1038', '1039', '1040', '1041', '1042',
       '1043', '1044', '1045', '1046', '1047', '1048', '1049', '1050', '1051',
       '1052', '1053', '1054', '1055', '1056', '1057', '1058', '1059', '1060',
       '1061', '1062', '1063', '1064', '1065', '1066', '1067', '1068', '1069',
       '1070', '1071', '1072', '1073', '1074', '1075', '1076', '1077', '1078',
       '1079', '1080', '1081', '1082', '1083', '1084', '1085', '1086', '1087',
       '1088', '1089', '1090', '1091', '1092', '1093', '1094', '1095', '1096',
       '1097', '1098', '1099']),
     ('m', [('008', 's1uuu    ')], None, '----', 'dates unknown', [], []),
     ('m', [('008', 'q198u1990')], 1985, '198-', '1980s to 1990',
      [1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990],
      ['1980s', '1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987',
       '1988', '1989', '1990']),
     ('m', [('008', 'q198u1985')], 1985, '198-', '1980s (to 1985)',
      [1980, 1981, 1982, 1983, 1984, 1985],
      ['1980s', '1980', '1981', '1982', '1983', '1984', '1985']),
     ('m', [('008', 's2004    '),
            ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '[199-?]'])],
      2004, '2004', '2004',
      [2004, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999],
      ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998',
       '1999', '1990s', '2004']),
     ('m', [('008', 's2004    '),
            ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '[1990s?]'])],
      2004, '2004', '2004',
      [2004, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999],
      ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998',
       '1999', '1990s', '2004']),
     ('m', [('008', 's2004    '),
            ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '1992-1995'])],
      2004, '2004', '2004',
      [2004, 1992, 1993, 1994, 1995], ['1992', '1993', '1994', '1995', '2004']),
     ('m', [('008', 's2014    '),
            ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '201[4]'])],
      2014, '2014', '2014', [2014], ['2014']),
     ('m', [('008', 's0300    '),
            ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '[ca. 300?]'])],
      300, '0300', '300', [300], ['300']),
     ('m', [('008', 's0301    '),
            ('260', ['a', 'Place1 :', 'b', 'Publisher,', 'c', '[ca. 300?]'])],
      301, '0301', '301', [300, 301], ['300', '301']),
     ('m', [('008', 's2014    '),
            ('362', ['a', 'Vol. 1, no. 1 (Apr. 1981)-'], '0 ')],
      2014, '2014', '2014', [2014], ['2014']),
     ('m', [('008', 's2014    '),
            ('362', ['a', 'Began with vol. 4, published in 1947.'], '1 ')],
      2014, '2014', '2014', [2014], ['2014']),
     ('m', [('008', 's2014    '),
            ('362', ['a', 'Published in 1st century.'], '1 ')],
      2014, '2014', '2014', [2014], ['2014']),
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
     'invalid date/year (05) in 046 is ignored',
     'invalid date/year (5) in 046 is ignored',
     'invalid date/year (21) in 046 is ignored',
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
     'unknown date (1uuu) in single-date 008',
     'decade (198u to 1990) in date-range 008',
     'partial decade (198u to 1985) in date-range 008',
     'decade (199-) in 26X but NOT in 008',
     'decade (spelled out) in 26X but NOT in 008',
     'closed range in 26X but NOT in 008',
     'partial date in square brackets is recognizable',
     'three-digit year in 008 and 260c work',
     'three-digit year only 260c works',
     'formatted date in 362 (ignored)',
     'non-formatted date in 362 (ignored)',
     'century (1st) in 362 (ignored)',
 ])
def test_bdpipeline_getpubinfo_dates(ldr_07, fparams, exp_boost_year,
                                     exp_pub_sort, exp_pub_year_display,
                                     exp_pub_year_facet, exp_pub_dates_search,
                                     sierra_test_record, bibrecord_to_marc,
                                     params_to_fields, add_marc_fields,
                                     assert_bundle_matches_expected):
    """
    BibDataPipeline.get_pub_info should return date-string fields
    matching the expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('260', '264', '362')
    if len(fparams) and fparams[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], fparams[0][1], data[15:])
        fparams[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bibmarc.leader = bibmarc.leader[:7] + ldr_07 + bibmarc.leader[8:]
    pipeline.this_year = 2021
    pipeline.year_upper_limit = 2026
    bundle = pipeline.do(bib, bibmarc, ['pub_info'])
    expected = {
        'publication_sort': exp_pub_sort or None,
        'publication_year_display': exp_pub_year_display or None,
        'publication_year_range_facet': exp_pub_year_facet or None,
        'publication_dates_search': exp_pub_dates_search or None
    }
    assert pipeline.year_for_boost == exp_boost_year
    assert_bundle_matches_expected(bundle, expected, bundle_complete=False,
                                   list_order_exact=False)


@pytest.mark.parametrize('ldr_07, fparams, expected', [
    ('m', [('008', 's2004    '),
           ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'])],
     {'publication_display': ['Place : Publisher, 2004']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'], ' 1')],
     {'publication_display': ['Place : Publisher, 2004']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0')],
     {'creation_display': ['Place : Producer, 2004']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Distributor,', 'c', '2004.'], ' 2')],
     {'distribution_display': ['Place : Distributor, 2004']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'manufacture_display': ['Place : Manufacturer, 2004']}),
    ('m', [('008', 's2004    '),
           ('264', ['c', '2004'], ' 4')],
     {'copyright_display': ['2004']}),
    ('m', [('008', 's2004    '),
           ('264', ['c', 'c2004'], ' 4')],
     {'copyright_display': ['©2004']}),
    ('m', [('008', 'b2004    ')],
     {'publication_display': ['2004']}),
    ('s', [('008', 'c20049999')],
     {'publication_display': ['2004 to present']}),
    ('s', [('008', 'd20042012')],
     {'publication_display': ['2004 to 2012']}),
    ('m', [('008', 'e20040101')],
     {'publication_display': ['2004']}),
    ('m', [('008', 'i20042012')],
     {'creation_display': ['Collection created in 2004 to 2012']}),
    ('m', [('008', 'k20042012')],
     {'creation_display': ['Collection created in 2004 to 2012']}),
    ('m', [('008', 'm20042012')],
     {'publication_display': ['2004 to 2012']}),
    ('m', [('008', 'm20049999')],
     {'publication_display': ['2004 to present']}),
    ('m', [('008', 'm2004    ')],
     {'publication_display': ['2004 to present']}),
    ('m', [('008', 'muuuu2012')],
     {'publication_display': ['? to 2012']}),
    ('m', [('008', 'nuuuuuuuu')], {}),
    ('m', [('008', 'p20122004')],
     {'distribution_display': ['Released in 2012'],
      'creation_display': ['Created or produced in 2004']}),
    ('m', [('008', 'q20042012')],
     {'publication_display': ['2004 to 2012']}),
    ('m', [('008', 'r20122004')],
     {'distribution_display': ['Reproduced or reissued in 2012'],
      'publication_display': ['Originally published in 2004']}),
    ('m', [('008', 'ruuuu2004')],
     {'publication_display': ['Originally published in 2004']}),
    ('m', [('008', 'r2012uuuu')],
     {'distribution_display': ['Reproduced or reissued in 2012']}),
    ('m', [('008', 's2004    ')],
     {'publication_display': ['2004']}),
    ('m', [('008', 't20042012')],
     {'publication_display': ['2004'],
      'copyright_display': ['2012']}),
    ('s', [('008', 'u2004uuuu')],
     {'publication_display': ['2004 to ?']}),
    ('m', [('008', 's201u    ')],
     {'publication_display': ['the 2010s']}),
    ('m', [('008', 's20uu    ')],
     {'publication_display': ['the 21st century']}),
    ('m', [('008', 'm200u201u')],
     {'publication_display': ['the 2000s to the 2010s']}),
    ('m', [('008', 'm19uu20uu')],
     {'publication_display': ['the 20th to the 21st century']}),
    ('m', [('008', 'm19uu201u')],
     {'publication_display': ['the 20th century to the 2010s']}),
    ('m', [('008', 'm19uu193u')],
     {'publication_display': ['the 20th century (to the 1930s)']}),
    ('m', [('008', 's2012    '),
           ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2012, c2004.'])],
     {'publication_display': ['Place : Publisher, 2012'],
      'copyright_display': ['©2004']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'Place :', 'b', 'Publisher,',
                    'c', '2004, c2012 by Some Publisher.'])],
     {'publication_display': ['Place : Publisher, 2004'],
      'copyright_display': ['©2012 by Some Publisher']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'Place :', 'b', 'Publisher,',
                    'c', '2012 printing, copyright 2004.'])],
     {'publication_display': ['Place : Publisher, 2012 printing'],
      'copyright_display': ['©2004']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'Place :', 'b', 'Publisher,',
                    'c', '2004-2012.'])],
     {'publication_display': ['Place : Publisher, 2004-2012']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'First Place :', 'b', 'First Publisher;',
                    'a', 'Second Place :', 'b', 'Second Publisher,',
                    'c', '2004.'])],
     {'publication_display': ['First Place : First Publisher; '
                              'Second Place : Second Publisher, 2004']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'First Place;', 'a', 'Second Place :', 'b', 'Publisher,',
                    'c', '2004.'])],
     {'publication_display': ['First Place; Second Place : Publisher, 2004']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
                    'e', '(M Place :', 'f', 'Printer)'])],
     {'publication_display': ['P Place : Publisher, 2004'],
      'manufacture_display': ['M Place : Printer']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
                    'e', '(M Place :', 'f', 'Printer,', 'g', '2005)'])],
     {'publication_display': ['P Place : Publisher, 2004'],
      'manufacture_display': ['M Place : Printer, 2005']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
                    'g', '(2010 printing)'])],
     {'publication_display': ['P Place : Publisher, 2004'],
      'manufacture_display': ['2010 printing']}),
    ('m', [('008', 's2014    '),
           ('362', ['a', 'Vol. 1, no. 1 (Apr. 1981)-'], '0 ')],
     {'publication_display': ['Vol. 1, no. 1 (Apr. 1981)-']}),
    ('m', [('008', 's2014    '),
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
    'from 008, generated, century limited by decade range',
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
def test_bdpipeline_getpubinfo_statements(ldr_07, fparams, expected,
                                          sierra_test_record, bibrecord_to_marc,
                                          params_to_fields, add_marc_fields,
                                          assert_bundle_matches_expected):
    """
    BibDataPipeline.get_pub_info should return display statement fields
    matching the expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('260', '264', '362')
    if len(fparams) and fparams[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], fparams[0][1], data[15:])
        fparams[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bibmarc.leader = bibmarc.leader[:7] + ldr_07 + bibmarc.leader[8:]
    bundle = pipeline.do(bib, bibmarc, ['pub_info'])
    disp_fields = set(['creation_display', 'publication_display',
                       'distribution_display', 'manufacture_display',
                       'copyright_display', 'publication_date_notes'])
    check_bundle = {k: v for k, v in bundle.items() if k in disp_fields}
    assert_bundle_matches_expected(check_bundle, expected)


@pytest.mark.parametrize('ldr_07, fparams, expected', [
    ('m', [('008', 's2004    '),
           ('260', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'])],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Publisher']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', 'P Place :', 'b', 'Publisher,', 'c', '2004',
                    'e', '(M Place :', 'f', 'Printer,', 'g', '2005)'])],
     {'publication_places_search': ['P Place', 'M Place'],
      'publishers_search': ['Publisher', 'Printer']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Producer']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'], ' 1')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Publisher']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Distributor,', 'c', '2004.'], ' 2')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Distributor']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Manufacturer']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Prod Place :', 'b', 'Producer,', 'c', '2004.'], ' 0'),
           ('264', ['a', 'Place :', 'b', 'Publisher,', 'c', '2004.'], ' 1'),
           ('264', ['a', 'Place :', 'b', 'Distributor,', 'c', '2004.'], ' 2'),
           ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'publication_places_search': ['Prod Place', 'Place'],
      'publishers_search': ['Producer', 'Publisher', 'Distributor',
                            'Manufacturer']}),
    ('m', [('008', 's2004    '),
           ('260', ['a', '[S.l. :', 'b', 's.n.]', 'c', '2004.']),
           ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0'),
           ('264', ['a', '[Place of publication not identified] :',
                    'b', 'Publisher,', 'c', '2004.'], ' 1'),
           ('264', ['a', 'Place :', 'b', '[distributor not identified],',
                    'c', '2004.'], ' 2'),
           ('264', ['a', 'Place :', 'b', 'Manufacturer,', 'c', '2004.'], ' 3')],
     {'publication_places_search': ['Place'],
      'publishers_search': ['Producer', 'Publisher', 'Manufacturer']}),
    ('m', [('008', 's2004    '),
           ('260', ['c', '2004.']),
           ('264', ['c', '2004.'], ' 4')], {}),
    ('m', [('008', 's2004    '), ], {}),
    ('m', [('008', 's2004    '),
           ('257', ['a', 'United States ;', 'a', 'Italy']),
           ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0')],
     {'publication_places_search': ['United States', 'Italy', 'Place'],
      'publishers_search': ['Producer']}),
    ('m', [('008', 's2004    '),
           ('257', ['a', 'Place ;', 'a', 'Italy']),
           ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0')],
     {'publication_places_search': ['Italy', 'Place'],
      'publishers_search': ['Producer']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0'),
           ('751', ['0', '(DE-588)4036729-0',
                    '0', 'http://d-nb.info/gnd/4036729-0',
                    'a', 'Luxemburg', 'g', 'Stadt', '4', 'dbp', '2', 'gnd'])],
     {'publication_places_search': ['Place', 'Luxemburg Stadt'],
      'publishers_search': ['Producer']}),
    ('m', [('008', 's2004    '),
           ('264', ['a', 'Place :', 'b', 'Producer,', 'c', '2004.'], ' 0'),
           ('752', ['a', 'United States', 'b', 'California',
                    'c', 'Los Angeles (County)', 'd', 'Los Angeles',
                    'f', 'Little Tokyo.', '2', 'tgn'])],
     {'publication_places_search': [
         'Place',
         'United States California Los Angeles (County) Los Angeles Little '
         'Tokyo.'
      ],
      'publishers_search': ['Producer']}),
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
    'no 260/264 is okay',
    '257: Country of production => publication places',
    '257 is deduplicated against other publication places',
    '751: Geographic Name',
    '752: Hierarchical place name',
])
def test_bdpipeline_getpubinfo_pub_search(ldr_07, fparams, expected,
                                          sierra_test_record, bibrecord_to_marc,
                                          params_to_fields, add_marc_fields,
                                          assert_bundle_matches_expected):
    """
    BibDataPipeline.get_pub_info should return publishers_search and
    publication_places_search fields matching the expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('257', '260', '264', '752')
    if len(fparams) and fparams[0][0] == '008':
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:6], fparams[0][1], data[15:])
        fparams[0] = ('008', data)
        bibmarc.remove_fields('008')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bibmarc.leader = bibmarc.leader[:7] + ldr_07 + bibmarc.leader[8:]
    bundle = pipeline.do(bib, bibmarc, ['pub_info'])
    search_fields = ('publication_places_search', 'publishers_search')
    check_bundle = {k: v for k, v in bundle.items() if k in search_fields}
    assert_bundle_matches_expected(check_bundle, expected,
                                   list_order_exact=False)


@pytest.mark.parametrize('bib_locations, item_locations, sup_item_locations,'
                         'expected', [
     # czm / same bib and item location
     ((('czm', 'Chilton Media Library'),),
      (('czm', 'Chilton Media Library'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None},
     ),

     # czm / bib loc exists, but no items
     ((('czm', 'Chilton Media Library'),),
      tuple(),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None},
     ),

     # czm / all items are suppressed
     ((('czm', 'Chilton Media Library'),),
      tuple(),
      (('lwww', 'UNT ONLINE RESOURCES'),
       ('w3', 'Willis Library-3rd Floor'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None},
     ),

     # czm / unknown bib location and one unknown item
     # location
     ((('blah', 'Blah'),),
      (('blah2', 'Blah2'), ('czm', 'Chilton Media Library'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Media Library'],
       'building_facet': ['Chilton Media Library'],
       'shelf_facet': None}
     ),

     # w3 / one suppressed item, one unsuppressed item,
     # diff locs
     ((('czm', 'Chilton Media Library'),),
      (('w3', 'Willis Library-3rd Floor'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['General Collection'],
       'building_facet': ['Willis Library'],
       'shelf_facet': ['Willis Library-3rd Floor']},
     ),

     # w3 / one suppressed item, one unsuppressed item,
     # same locs
     ((('czm', 'Chilton Media Library'),),
      (('w3', 'Willis Library-3rd Floor'),),
      (('w3', 'Willis Library-3rd Floor'),),
      {'access_facet': ['At the Library'],
       'collection_facet': ['General Collection'],
       'building_facet': ['Willis Library'],
       'shelf_facet': ['Willis Library-3rd Floor']},
     ),

     # all bib and item locations are unknown
     ((('blah', 'Blah'),),
      (('blah2', 'Blah2'), ('blah', 'Blah'),),
      tuple(),
      {'access_facet': None,
       'collection_facet': None,
       'building_facet': None,
       'shelf_facet': None}
     ),

     # r, lwww / online-only item with bib location in
     # different collection
     ((('r', 'Discovery Park Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
      tuple(),
      {'access_facet': ['Online'],
       'collection_facet': ['General Collection'],
       'building_facet': None,
       'shelf_facet': None}
     ),

     # r, lwww / two different bib locations, no items
     ((('r', 'Discovery Park Library'), ('lwww', 'UNT ONLINE RESOURCES')),
      tuple(),
      tuple(),
      {'access_facet': ['At the Library', 'Online'],
       'collection_facet': ['Discovery Park Library', 'General Collection'],
       'building_facet': ['Discovery Park Library'],
       'shelf_facet': None}
     ),

     # fl, flmak / a bib location with multiple collections, no items
     ( (('fl', 'Frisco Landing Library'),
        ('flmak', 'Frisco Landing The Spark')),
       tuple(),
       tuple(),
       {'access_facet': ['At the Library'],
        'collection_facet': ['Frisco Collection', 'The Spark (Makerspace)'],
        'building_facet': ['Frisco Landing Library'],
        'shelf_facet': ['Frisco Landing The Spark'],}
     ),

     # w, lwww / online-only item with bib location in
     # same collection
     ((('w', 'Willis Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'),),
         tuple(),
         {'access_facet': ['Online'],
          'collection_facet': ['General Collection'],
          'building_facet': None,
          'shelf_facet': None}
      ),

     # x, xdoc / Remote Storage, bib loc is x
     ((('x', 'Remote Storage'),),
      (('xdoc', 'Government Documents Remote Storage'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Government Documents'],
       'building_facet': ['Remote Storage'],
       'shelf_facet': None}
     ),

     # sd, xdoc / Remote Storage, bib loc is not x
     ((('sd', 'Sycamore Library Government Documents'),),
      (('xdoc', 'Government Documents Remote Storage'),),
      tuple(),
      {'access_facet': ['At the Library'],
       'collection_facet': ['Government Documents'],
       'building_facet': ['Remote Storage'],
       'shelf_facet': None}
     ),

     # w, lwww, w3 / bib with online and physical
     # locations
     ((('w', 'Willis Library'),),
      (('lwww', 'UNT ONLINE RESOURCES'), ('w3', 'Willis Library-3rd Floor'),),
      tuple(),
      {'access_facet': ['Online', 'At the Library'],
       'collection_facet': ['General Collection'],
       'building_facet': ['Willis Library'],
       'shelf_facet': ['Willis Library-3rd Floor']}
     ),

     # sd, gwww, sdus, rst, xdoc / multiple items at multiple locations
     # NOTE: The library where the Gov Docs collection lives was Eagle
     # Commons Library but was changed to Sycamore Library 8/2021. To
     # avoid confusion and to avoid having to update our test fixtures
     # that still use Eagle Commons, I've changed names of the s*
     # locations, below. Because of the "s Eagle Commons Library" test
     # fixture, the building_facet is still "Eagle Commons Library,"
     # even though the name has changed, in reality.
     # This is fine.
     ((('sd', 'X Government Documents'),),
      (('gwww', 'GOVT ONLINE RESOURCES'),
       ('sdus', 'X US Documents'),
       ('rst', 'Discovery Park Library Storage'),
       ('xdoc', 'Government Documents Remote Storage'),),
      tuple(),
      {'access_facet': ['Online', 'At the Library'],
       'collection_facet': ['Government Documents', 'Discovery Park Library'],
       'building_facet': ['Eagle Commons Library', 'Discovery Park Library',
                          'Remote Storage'],
       'shelf_facet': ['X US Documents',
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
     'fl, flmak / a bib location with multiple collections, no items',
     'w, lwww / online-only item with bib location in same collection',
     'x, xdoc / Remote Storage, bib loc is x',
     'sd, xdoc / Remote Storage, bib loc is not x',
     'w, lwww, w3 / bib with online and physical locations',
     'sd, gwww, sdus, rst, xdoc / multiple items at multiple locations',
 ])
def test_bdpipeline_getaccessinfo(bib_locations, item_locations,
                                  sup_item_locations, expected,
                                  sierra_test_record, update_test_bib_inst,
                                  get_or_make_location_instances,
                                  assert_bundle_matches_expected):
    """
    BibDataPipeline.get_access_info should return the expected access,
    collection, building, and shelf facet values based on the
    configured bib_ and item_locations.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')

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
def test_bdpipeline_getresourcetypeinfo(bcode2, expected, sierra_test_record,
                                        setattr_model_instance,
                                        assert_bundle_matches_expected):
    """
    BibDataPipeline.get_resource_type_info should return the
    expected resource_type and resource_type_facet values based on the
    given bcode2. Note that this doesn't test resource type nor
    category (facet) determination. For that, see base.local_rulesets
    (and associated tests).
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
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

    # Language info just from 377, example 1
    ('', ['377 ##$afre'],
     {'languages': ['French'],
      'language_notes': [
         'Item content: French',
     ]}
    ),

    # Language info just from 377, example 2
    ('', ['377 ##$aeng$afre'],
     {'languages': ['English', 'French'],
      'language_notes': [
         'Item content: English, French',
     ]}
    ),

    # Language info just from 377, example 3
    ('', ['377 ##$aeng$lBostonian'],
     {'languages': ['English', 'Bostonian'],
      'language_notes': [
         'Item content: English, Bostonian',
     ]}
    ),

    # Language info just from titles
    ('', ['130 0#$aBible.$pN.T.$pRomans.$lEnglish.$sRevised standard.',
          '730 02$aBible.$pO.T.$pJudges V.$lGerman$sGrether.'],
     {'languages': ['English', 'German'],
      'language_notes': [
         'Item content: English, German',
     ]}
    ),

    # Language from title ignored if not in MARC language list
    # Background: We end up seeing a lot of garbage in our Language
    # facet when we don't check title languages against a controlled
    # vocabulary.
    ('', ['130 0#$aBible.$pN.T.$pRomans.$lEnglish (1995).$sRevised standard.',
          '730 02$aBible.$pO.T.$pJudges V.$lGerman$sGrether.',
          '730 02$aTest Title.$lEng.'],
     {'languages': ['German'],
      'language_notes': [
         'Item content: German',
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
    ('eng', ['041 0#$deng$eeng$efre',
             '041 0#$geng',
             '041 1#$deng$hrus$eeng$nrus$geng$gfre',
             '130 0#$aBible.$pN.T.$pRomans.$lEnglish.$sRevised standard.',
             '377 ##$lEnglish',
             '377 ##$ager',
             '730 02$aSome title.$lKlingon (Artificial language).'],
     {'languages': ['English', 'French', 'German', 'Russian',
                    'Klingon (Artificial language)'],
      'language_notes': [
         'Item content: English, Klingon (Artificial language), German',
         'Translated from (original): Russian',
         'Librettos: English, French',
         'Librettos translated from (original): Russian',
         'Accompanying materials: English, French'
     ]}
    ),
], ids=[
    # Edge cases
    'No language info at all (no 008s, titles, 041s, or 377s)',
    '008 without valid cps 35-37',

    # Main tests
    'Language info just from 008',
    'Language info just from 041, example 1',
    'Language info just from 041, example 2',
    'Language info just from 041, example 3',
    'Language info just from 041, example 4  -- multiple 041s',
    'Ignore 041 if it uses something other than MARC relator codes',
    'Language info just from 377, example 1',
    'Language info just from 377, example 2',
    'Language info just from 377, example 3',
    'Language info just from titles',
    'Language from title ignored if not in MARC language list',
    'Language info from related titles is not used',
    'If there are 546s, those lang notes override generated ones',
    'Language info from combined sources',
])
def test_bdpipeline_getlanguageinfo(f008_lang, raw_marcfields, expected,
                                    fieldstrings_to_fields, sierra_test_record,
                                    bibrecord_to_marc, add_marc_fields,
                                    assert_bundle_matches_expected):
    """
    BibDataPipeline.get_language_info should return fields matching the
    expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    if f008_lang:
        data = bibmarc.get_fields('008')[0].data
        data = '{}{}{}'.format(data[0:35], f008_lang, data[38:])
        raw_marcfields = [('008 {}'.format(data))] + raw_marcfields
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc.remove_fields('008', '041', '130', '240', '377', '546', '700',
                          '710', '711', '730', '740')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    fields_to_process = ['title_info', 'notes', 'language_info']
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
                                     '(1999)', }]},
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
def test_bdpipeline_getcontributorinfo(fparams, expected, sierra_test_record,
                                       bibrecord_to_marc, params_to_fields,
                                       add_marc_fields,
                                       assert_bundle_matches_expected):
    """
    BibDataPipeline.get_contributor_info should return fields
    matching the expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('100', '110', '111', '700', '710', '711', '800',
                          '810', '811')
    bibmarc = add_marc_fields(bibmarc, params_to_fields(fparams))
    bundle = pipeline.do(bib, bibmarc, ['contributor_info'])
    assert_bundle_matches_expected(bundle, expected)


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
               'n', 'No. 3.'], '0 '), ],
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
                      'Sonatas, piano [by Smith, J.] > Op. 31 > No. 2'}]},
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
                      'Sonatas, piano [by Smith, J.] (Selections) > Op. 31'}]},
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
                      'Sonatas, piano [by Smith, J.] > Op. 31 (Selections)'}]}
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
                      'Protocols, etc., 1951 Mar. 6'}]},
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
                'v': 'poems-complete!Poems (Complete)'}]},
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
                      'Sonatas, piano [by Smith, J.] > Op. 32, C major'}]
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
      ('800', ['a', 'Copeland, Edward.', 't', 'Piano music.'], '1 ')],
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
      ('810', ['a', 'Led Zeppelin', 't', 'Piano music.'], '2 ')],
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
               'e', 'Orchestra.', 't', 'Incidental music.'], '2 ')],
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
      ('830', ['a', 'Piano music.'], ' 0')],
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
               'a', 'Subseries B ;', 'v', 'v. 2', ], '0 ')],
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
    ([('490', ['a', 'Some series (statement).', '1 ']),
      ('490', ['a', 'Piano music of Edward Copeland.', '1 ']),
      ('800', ['a', 'Smith, Joe.', 't', 'Some series.'], '1 '),
      ('800', ['a', 'Copeland, Edward.', 't', 'Piano music.'], '1 ')],
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
      'main_work_title_json': {
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

    # 210: Abbrev title in 210, $a $b $2
    ([('210', ['a', 'Annu. rep.', 'b', '(Chic.)', '2', 'dnlm'], '0#')],
     {'variant_titles_notes': ['Abbreviated title: Annu. rep. (Chic.)'],
      'variant_titles_search': ['Annu. rep. (Chic.)'],
      }),


    # 222: Key title in 222, $a $b $2
    ([('222', ['a', 'Annual report', 'b', '(Chicago)', '2', 'dnlm'], '0#')],
     {'variant_titles_notes': ['ISSN key title: Annual report (Chicago)'],
      'variant_titles_search': ['Annual report (Chicago)'],
      }),

    # 222/245: Duplicate title in 222

    # 242: Parallel title in 242, w/language ($y)
    ([('242', ['a', 'Title in English.', 'n', 'Part 1', 'y', 'eng'], '00')],
     {'variant_titles_notes': [
         'Title translation, English: Title in English > Part 1'
      ],
      'variant_titles_search': ['Title in English > Part 1'],
      }),

    # 242: Parallel title in 242, no language
    ([('242', ['a', 'Title in English.', 'n', 'Part 1'], '00')],
     {'variant_titles_notes': ['Title translation: Title in English > Part 1'],
      'variant_titles_search': ['Title in English > Part 1'],
      }),

    # 242: Parallel title w/responsibility
    ([('242', ['a', 'Title in English /', 'c', 'by Joe Smith.'], '00')],
     {'responsibility_search': ['by Joe Smith'],
      'variant_titles_notes': ['Title translation: Title in English'],
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
      'variant_titles_notes': ['Title translation: Title in English'],
      'variant_titles_search': ['Title in English', 'Title'],
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
      'variant_titles_notes': ['Title translation: Title in English'],
      'variant_titles_search': ['Title in English', 'Title in German'],
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
          'Title translation: Title in Spanish'
      ],
      'variant_titles_search': [
          'Title in English',
          'Title in Spanish',
          'Title'
      ],
      'main_work_title_json': {
          'a': 'smith-joe!Smith, Joe',
          'p': [{'d': 'Title [by Smith, J.]',
                 'v': 'title!Title',
                 's': ' '},
                {'d': '[translated: Title in English; Title in Spanish]'}]
      },
      'included_work_titles_search': [
          'Title',
          'Title in English',
          'Title in Spanish'
      ],
      'title_series_facet': [
          'title!Title',
          'title-in-english!Title in English',
          'title-in-spanish!Title in Spanish'
      ],
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
      ('245', ['a', 'Title in Spanish.', 'p', 'Part One :',
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
          'Title in Spanish > Part One: subtitle'
      ],
      'variant_titles_notes': [
          'Title translation: Title in English > Part One: subtitle'
      ],
      'main_work_title_json': {
          'a': 'smith-joe!Smith, Joe',
          'p': [{'d': 'Title in Spanish [by Smith, J.]',
                 'v': 'title-in-spanish!Title in Spanish',
                 's': ' > '},
                {'d': 'Part One: subtitle',
                 'v': 'title-in-spanish-part-one-subtitle!'
                    'Title in Spanish > Part One: subtitle',
                 's': ' '},
                {'d': '[translated: Title in English > Part One: subtitle]'}]
      },
      'included_work_titles_search': [
          'Title in Spanish > Part One: subtitle',
          'Title in English > Part One: subtitle'
      ],
      'title_series_facet': [
          'title-in-spanish!Title in Spanish',
          'title-in-spanish-part-one-subtitle!'
          'Title in Spanish > Part One: subtitle',
          'title-in-english-part-one-subtitle!'
          'Title in English > Part One: subtitle'
      ],
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
      'variant_titles_notes': ['Title translation: Title in English'],
      'variant_titles_search': ['Title in English', 'Title in German'],
      }),

    # 245/246: Parallel title in 246 (duplicates)
    ([('245', ['a', 'Title in German /',
               'c', 'by German Author = Title in English / by Joe Smith.'],
       '00'),
      ('246', ['a', 'Title in English'], '01'), ],
     {'title_display': 'Title in German [translated: Title in English]',
      'main_title_search': ['Title in German'],
      'title_sort': 'title-in-german',
      'responsibility_display': 'by German Author [translated: by Joe Smith]',
      'responsibility_search': [
          'by German Author',
          'by Joe Smith'
      ],
      'variant_titles_notes': ['Title translation: Title in English'],
      'variant_titles_search': ['Title in English', 'Title in German'],
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
     {'variant_titles_search': ['Some title, Mar. 1924-Nov. 1927']}),

    # 383: Music numbers as variant titles
    ([('383', ['a', 'no. 14,', 'b', 'op. 27, no. 2'], '  '),
      ('383', ['b', 'op. 5', 'e', 'Hummel'], '  '),
      ('383', ['c', 'RV 269', 'c', 'RV 315', 'c', 'RV 293', 'c', 'RV 297',
               'd', 'Ryom', '2', 'mlati'], '  ')],
     {'variant_titles_search': [
         'no. 14, op. 27, no. 2',
         'op. 5 Hummel',
         'RV 269 RV 315 RV 293 RV 297 Ryom'
     ]}),


    # 240/383: Music numbers as variant titles (duplicates)
    ([('245', ['a', 'Piano sonata in C# minor,',
               'n', 'no. 14, op. 27, no. 2,'], '00'),
      ('383', ['a', 'no. 14,', 'b', 'op. 27, no. 2'], '  '),
      ('383', ['b', 'op. 5', 'e', 'Hummel'], '  ')],
     {'title_display': 'Piano sonata in C# minor, no. 14, op. 27, no. 2',
      'main_title_search': ['Piano sonata in C# minor, no. 14, op. 27, no. 2'],
      'title_sort': 'piano-sonata-in-c-minor-no-14-op-27-no-2',
      'variant_titles_search': [
          'Piano sonata in C# minor, no. 14, op. 27, no. 2',
          'op. 5 Hummel',
      ]}),

    # 384: Music key as variant title
    ([('384', ['a', 'C# minor'], '  ')],
     {'variant_titles_search': ['C# minor']
      }),

    # 384: Music key as variant title (duplicates)
    ([('245', ['a', 'Piano sonata in C# minor,',
               'n', 'no. 14, op. 27, no. 2,'], '00'),
      ('384', ['a', 'C# minor'], '  ')],
     {'title_display': 'Piano sonata in C# minor, no. 14, op. 27, no. 2',
      'main_title_search': ['Piano sonata in C# minor, no. 14, op. 27, no. 2'],
      'title_sort': 'piano-sonata-in-c-minor-no-14-op-27-no-2',
      'variant_titles_search': [
          'Piano sonata in C# minor, no. 14, op. 27, no. 2'
      ]}),

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
    '210: Abbrev title in 210, $a $b $2',
    '222: Key title in 222, $a $b $2',
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
    '383: Music numbers as variant titles',
    '245/383: Music numbers as variant titles (duplicates)',
    '384: Music key as variant title',
    '384: Music key as variant title (duplicates)',
])
def test_bdpipeline_gettitleinfo(fparams, expected, sierra_test_record,
                                 bibrecord_to_marc, params_to_fields,
                                 add_marc_fields,
                                 assert_bundle_matches_expected):
    """
    BibDataPipeline.get_title_info should return fields matching the
    expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('100', '110', '111', '130', '210', '222', '240',
                          '242', '243', '245', '246', '247', '383', '384',
                          '490', '700', '710', '711', '730', '740', '800',
                          '810', '811', '830')
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
      ]},
     '8 performers: soprano voice (2); mezzo-soprano voice; tenor '
     'saxophone doubling bass clarinet; trumpet; piano; violin doubling '
     'viola; double bass'),
    ({'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
        [{'solo': [('flute', '1')]}],
        [{'primary': [('orchestra', '1')]}],
      ]},
     '1 performer and 1 ensemble: solo flute; orchestra'),
    ({'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('flute', '1')]},
         {'doubling': [('piccolo', '1'), ('alto flute', '1'),
                       ('bass flute', '1')]}],
      ]},
     '1 performer: flute doubling piccolo, alto flute, and bass '
     'flute'),
    ({'materials_specified': [],
      'total_performers': '3',
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]}, {'doubling': [('flute', '1')]},
         {'alt': [('piccolo', '1')]}],
        [{'primary': [('cello', '1')]}],
        [{'primary': [('piano', '1')]}],
      ]},
     '3 performers: violin doubling flute or piccolo; cello; piano'),
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
      ]},
     '8 performers and 4 ensembles: solo soprano voice (3); solo alto '
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
      ]},
     'Violin or flute doubling viola or alto flute doubling cello or '
     'saxophone doubling double bass'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
         {'alt': [('flute', '1')]},
         {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
      ]},
     'Violin doubling viola, cello, and double bass or flute doubling alto '
     'flute and saxophone'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1', ['Note1', 'Note2'])]},
         {'doubling': [('viola', '1', ['Note3']),
                       ('cello', '2', ['Note4', 'Note5'])]}]
      ]},
     'Violin [Note1 / Note2] doubling viola [Note3] and cello (2) [Note4 / '
     'Note5]'),
    ({'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
        [{'primary': [('violin', '1')]},
         {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
         {'alt': [('flute', '1'), ('clarinet', '1')]},
         {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
      ]},
     'Violin doubling viola, cello, and double bass, flute, or clarinet '
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
      ]},
     'Violin, flute, trumpet, or clarinet doubling viola or alto flute '
     'doubling cello or saxophone doubling double bass'),
    ({'materials_specified': ['Piece One'],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
        [{'solo': [('flute', '1')]}],
        [{'primary': [('orchestra', '1')]}],
      ]},
     '(Piece One) 1 performer and 1 ensemble: solo flute; orchestra')
])
def test_bdpipeline_compileperformancemedium(parsed_pm, expected):
    """
    BibDataPipeline.compile_performance_medium should return a value
    matching `expected`, given the sample `parsed_pm` output from
    parsing a 382 field.
    """
    pipeline = pl.BibDataPipeline()
    assert pipeline.compile_performance_medium(parsed_pm) == expected


def test_bdpipeline_getnotes_3xxinfo(params_to_fields, add_marc_fields,
                                     assert_bundle_matches_expected):
    """
    BibDataPipeline.get_notes should return fields matching the
    expected parameters. For simplicity, this limits to 3XX fields.
    """
    exclude = pl.BibDataPipeline.ignored_marc_fields_by_group_tag['r']
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
        ('r348', ['a', 'vocal score', 'a', 'conductor part', '2', 'code']),
        ('r351', ['3', 'Records', 'c', 'Series;',
                  'a', 'Organized into four subgroups;',
                  'b', 'Arranged by office of origin.']),
        ('r352', ['a', 'Raster :', 'b', 'pixel',
                  'd', '(5,000 x', 'e', '5,000) ;', 'q', 'TIFF.']),
        ('r370', ['4', 'stg', 'i', 'Setting:', 'f', 'Wyoming']),
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
        'arrangement_of_materials': ['(Records) Series; Organized into four '
                                     'subgroups; Arranged by office of '
                                     'origin.'],
        'graphic_representation': ['Raster : pixel (5,000 x 5,000) ; TIFF.'],
        'performance_medium': ['8 performers: soprano voice (2); mezzo-soprano '
                               'voice; tenor saxophone doubling bass clarinet; '
                               'trumpet; piano; violin doubling viola; double '
                               'bass'],
        'publication_dates_search': ['Monthly, 1958-', 'Bimonthly, 1954-1957'],
        'physical_description': ['300 desc 1',
                                 '300 desc 2',
                                 'Setting: Wyoming'],
        'type_format_search': ['300 desc 1',
                               '300 desc 2',
                               '(self-portrait) rice paper; 7" x 9"',
                               'Polyconic; 0.9996; 0; 500,000',
                               'Coordinate pair; meters; 22; 22.',
                               'analog',
                               'Dolby-B encoded',
                               'Cinerama',
                               '24 fps',
                               'VHS',
                               'NTSC',
                               'video file',
                               'DVD video',
                               'region 4',
                               'vocal score', 'conductor part',
                               'Raster : pixel (5,000 x 5,000) ; TIFF.',
                               '8 performers: soprano voice (2); mezzo-soprano '
                               'voice; tenor saxophone doubling bass clarinet; '
                               'trumpet; piano; violin doubling viola; double '
                               'bass'],
        'notes_search': ['(Records) Series; Organized into four subgroups; '
                         'Arranged by office of origin.',
                         'Setting: Wyoming']
    }
    fields = params_to_fields(exc_fields + inc_fields)
    marc = add_marc_fields(sm.SierraMarcRecord(), fields)
    pipeline = pl.BibDataPipeline()
    bundle = pipeline.do(None, marc, ['notes'])
    assert_bundle_matches_expected(bundle, expected)


def test_bdpipeline_getnotes_5xxinfo(params_to_fields, add_marc_fields,
                                     assert_bundle_matches_expected):
    """
    BibDataPipeline.get_notes should return fields matching the
    expected parameters. For simplicity, this limits to 5XX fields.
    """
    exclude = pl.BibDataPipeline.ignored_marc_fields_by_group_tag['n']
    handled = ('592',)
    exc_fields = [(''.join(('n', t)), ['a', 'No']) for t in exclude + handled]
    inc_fields = [
        ('r385', ['m', 'Age group', 'a', 'Children', 'm', 'Language group',
                  'a', 'Spanish Speaking', '2', 'ericd']),
        ('r385', ['3', 'video recording', 'a', 'Parents']),
        ('r386', ['i', 'Performers:', 'm', 'Age group', 'a', 'Children',
                  'a', 'French', '2', 'ericd']),
        ('r386', ['3', 'video recording', 'a', 'Parents']),
        ('r388', ['a', '1781-1791', 'a', '18th century']),
        ('r388', ['a', '1980']),
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
        ('n521', ['3', 'video recording', 'a', '18+.'], '1 '),
        ('n521', ['a', '7-10.'], '1 '),
        ('n521', ['a', '7 & up.'], '2 '),
        ('n521', ['a', 'Vision impaired', 'a', 'fine motor skills impaired',
                  'a', 'audio learner', 'b', 'LENOCA.'], '3 '),
        ('n521', ['a', 'Moderately motivated.'], '4 '),
        ('n521', ['a', 'MPAA rating: R.'], '8 '),
        ('n538', ['a', 'System requirements: IBM PC or compatible; 256K bytes '
                       'of internal memory; DOS 1.1']),
        ('n538', ['a', 'Project methodology for digital version',
                  'i', 'Technical details:',
                  'u', 'http://www.columbia.edu/dlc/linglung/methodology.html']),
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
        ('d658', ['a', 'Health objective 4',
                  'b', 'handicapped impaired education',
                  'd', 'highly correlated', 'c', 'NHPO4-1991', '2', 'ohco']),
        ('y753', ['a', 'IBM PC', 'b', 'Pascal', 'c', 'DOS 1.1'])
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
        'audience': [
            'Children; Spanish Speaking',
            '(video recording) Parents',
            'Clinical students, postgraduate house officers.',
            'Reading grade level: 3.1.',
            '(video recording) Ages: 18+.',
            'Ages: 7-10.',
            'Grades: 7 & up.',
            'Special audience characteristics: Vision impaired; fine motor '
            'skills impaired; audio learner (source: LENOCA)',
            'Motivation/interest level: Moderately motivated.',
            'MPAA rating: R.',
        ],
        'creator_demographics': [
            'Performers: Children; French',
            '(video recording) Parents',
        ],
        'curriculum_objectives': [
            'Health objective 4; handicapped impaired education; highly '
            'correlated; NHPO4-1991',
        ],
        'system_details': [
            'IBM PC; Pascal; DOS 1.1',
            'System requirements: IBM PC or compatible; 256K bytes of internal '
            'memory; DOS 1.1',
            'Project methodology for digital version; Technical details: '
            'http://www.columbia.edu/dlc/linglung/methodology.html'
        ],
        'notes': [
            'General Note.',
            '(plates) condition reviewed; 20040915; mutilated',
            'Cannot determine the relationship to Bowling illustrated, also '
            'published in New York, 1952-58.',
            'Description based on: Vol. 2, no. 2 (Feb. 1984); title from '
            'cover.',
            'Latest issue consulted: 2001.'
        ],
        'responsibility_search': [
            'Educational consultant, Roseanne Gillis.',
            'Hosted by Hugh Downs.',
            'Cast: Colin Blakely, Jane Lapotaire.'
        ],
        'type_format_search': [
            'IBM PC; Pascal; DOS 1.1',
            'System requirements: IBM PC or compatible; 256K bytes of internal '
            'memory; DOS 1.1',
            'Project methodology for digital version; Technical details: '
            'http://www.columbia.edu/dlc/linglung/methodology.html',
            '(Marriage certificate) German; Fraktur.'
        ],
        'notes_search': [
            'Children; Spanish Speaking',
            '(video recording) Parents',
            'Performers: Children; French',
            '(video recording) Parents',
            '1781-1791',
            '18th century',
            '1980',
            'General Note.',
            'Karl Schmidt\'s thesis (doctoral), Munich, 1965.',
            'Ph. D. ― University of North Texas, August, 2012.',
            'Some diss. Ph. D. ― University of North Texas, August, 2012.',
            'Short summary. Long summary.',
            'Short summary. Long summary.',
            'Subject: Two head-and-shoulder portraits ...',
            'Review: This book is great!',
            'Scope and content: Item consists of XYZ.',
            'Abstract: The study examines ...',
            'Content advice: Contains violence [Revealweb organization code]',
            'Content advice: "Not safe for life." [Family Filmgoer]',
            'Clinical students, postgraduate house officers.',
            'Reading grade level: 3.1.',
            '(video recording) Ages: 18+.',
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
            'Latest issue consulted: 2001.',
            'Health objective 4; handicapped impaired education; highly '
            'correlated; NHPO4-1991',
        ],
    }
    fields = params_to_fields(exc_fields + inc_fields)
    marc = add_marc_fields(sm.SierraMarcRecord(), fields)
    pipeline = pl.BibDataPipeline()
    bundle = pipeline.do(None, marc, ['notes'])
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
def test_bdpipeline_getcallnumberinfo(bib_cn_info, items_info, expected,
                                      sierra_test_record, update_test_bib_inst,
                                      assert_bundle_matches_expected):
    """
    The `BibDataPipeline.get_call_number_info` method should return the
    expected values given the provided `bib_cn_info` fields and
    `items_info` parameters.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
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
def test_bdpipeline_getstandardnumberinfo(raw_marcfields, expected,
                                          sierra_test_record, bibrecord_to_marc,
                                          add_marc_fields,
                                          fieldstrings_to_fields,
                                          assert_bundle_matches_expected):
    """
    The `BibDataPipeline.get_standard_number_info` method should return
    the expected values given the provided `marcfields`.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
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
    (['001 on0194068/springer'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068', '194068/springer'],
    }),

    # 001: Non-OCLC vendor number
    (['001 ybp0194068'], {
        'other_control_numbers_display': ['[Unknown Type]: ybp0194068'],
        'all_control_numbers': ['ybp0194068'],
        'control_numbers_search': ['ybp0194068'],
    }),

    # 001: Just OCLC number
    (['001 194068'], {
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
      '035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068'],
    }),

    # 001 and 035: Not-duplicate OCLC numbers
    (['001 123456',
      '035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068', '123456 [Invalid]'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', '123456'],
        'control_numbers_search': ['194068', '123456'],
    }),

    # 001 and 035: Duplicate OCLC numbers, but one has provider suffix
    (['001 194068/springer',
      '035 ## $a(OCoLC)on0194068'], {
        'oclc_numbers_display': ['194068'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068'],
        'control_numbers_search': ['194068', '194068/springer'],
    }),

    # 001 and 035: Duplicate invalid OCLC numbers
    (['001 12345',
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
      '035 ## $z(OCoLC)on12345'], {
        'oclc_numbers_display': ['194068', '12345 [Invalid]'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', '12345'],
        'control_numbers_search': ['12345', '194068'],
    }),

    # 001 and 035: Not-duplicates, 035 is not OCLC
    (['001 194068',
      '035 ## $a(YBP)ybp12345'], {
        'oclc_numbers_display': ['194068'],
        'other_control_numbers_display': ['ybp12345 (source: YBP)'],
        'oclc_numbers': ['194068'],
        'all_control_numbers': ['194068', 'ybp12345'],
        'control_numbers_search': ['ybp12345', '194068'],
    }),

    # 001 and 035: Not-duplicates, 001 is not OCLC
    (['001 ybp12345',
      '035 ## $a(OCoLC)194068'], {
        'oclc_numbers_display': ['194068'],
        'other_control_numbers_display': ['[Unknown Type]: ybp12345'],
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
    '001: Just OCLC number',

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
def test_bdpipeline_getcontrolnumberinfo(raw_marcfields, expected,
                                         sierra_test_record, bibrecord_to_marc,
                                         add_marc_fields,
                                         fieldstrings_to_fields,
                                         assert_bundle_matches_expected):
    """
    The `BibDataPipeline.get_control_number_info` method should return
    the expected values given the provided `marcfields`.
    """
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
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
def test_bdpipeline_getgamesfacetsinfo(raw_marcfields, expected,
                                       sierra_test_record,
                                       get_or_make_location_instances,
                                       update_test_bib_inst, bibrecord_to_marc,
                                       add_marc_fields, fieldstrings_to_fields,
                                       assert_bundle_matches_expected):
    """
    The `BibDataPipeline.get_games_facets_info` method should return
    the expected values given the provided `raw_marcfields`.
    """
    bib = sierra_test_record('bib_no_items')
    czm = [{'code': 'czm', 'name': 'Chilton Media Library'}]
    czm_instance = get_or_make_location_instances(czm)
    bib = update_test_bib_inst(bib, locations=czm_instance)
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('592')
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    pipeline = pl.BibDataPipeline()
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

    # 380, genre term (main term only), LCSH
    (['380 ##$aAudiobooks'], {
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
    (['380 ##$aPlays',
      '600 1#$aChurchill, Winston,$cSir,$d1874-1965$vFiction.',
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
            'p': [{'d': 'Plays',
                   'v': 'plays!Plays'}
                  ]
        }, {
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
            'plays!Plays',
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
            'plays!Plays',
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
            'Plays',
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
            'Plays',
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
            'Plays',
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
    (['380 ##$aHistory$2fast',
      '610 10$aUnited States.$bArmy.',
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
    '380, genre term (main term only), LCSH',
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
def test_bdpipeline_getsubjectsinfo(raw_marcfields, expected,
                                    fieldstrings_to_fields, sierra_test_record,
                                    subject_sd_test_mappings, bibrecord_to_marc,
                                    add_marc_fields,
                                    assert_bundle_matches_expected):
    """
    BibDataPipeline.get_subjects_info should return fields matching the
    expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    pipeline.subject_sd_patterns = subject_sd_test_mappings['sd_patterns']
    pipeline.subject_sd_term_map = subject_sd_test_mappings['sd_term_map']
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('380', '600', '610', '611', '630', '647', '648',
                          '650', '651', '653', '655', '656', '657', '690',
                          '691', '692')
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, ['subjects_info'])
    assert_bundle_matches_expected(bundle, expected, list_order_exact=False)


@pytest.mark.parametrize('this_year, year_for_boost, bib_type, expected', [
    (2020, 2020, '-', 1000),
    (2021, 2020, '-', 1000),
    (2020, 2020, 'd', 1000),
    (2020, 2019, '-', 999),
    (2020, 1920, '-', 900),
    (2020, 1820, '-', 800),
    (2020, 1520, '-', 501),
    (2020, 1420, '-', 501),
    (2020, 2020, 'a', 500),
    (2020, 2020, 'b', 500),
    (2020, 2020, 'r', 500),
    (2020, 2020, 'p', 500),
    (2020, 2020, 'i', 500),
    (2020, 2020, 's', 500),
    (2020, 2020, 't', 500),
    (2020, 2020, 'z', 500),
    (2020, 2020, '0', 500),
    (2020, 2020, '2', 500),
    (2020, 2020, '4', 500),
    (2020, 2019, 'a', 499),
    (2020, 1920, 'a', 400),
    (2020, 1820, 'a', 300),
    (2020, 1520, 'a', 1),
    (2020, 1420, 'a', 1),
    (2020, 9999, '-', 960),
    (2020, 2021, '-', 1001),
    (2020, 2022, '-', 1002),
    (2020, 2023, '-', 1003),
    (2020, 2024, '-', 1004),
    (2020, 2025, '-', 1005),
    (2020, 2026, '-', 960),
    (2020, 1993, '-', 973),
    (2020, None, '-', 960),
])
def test_bdpipeline_getrecordboost(this_year, year_for_boost, bib_type,
                                   expected, sierra_test_record,
                                   bibrecord_to_marc, setattr_model_instance):
    pipeline = pl.BibDataPipeline()
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    pipeline.year_for_boost = year_for_boost
    pipeline.this_year = this_year
    setattr_model_instance(bib, 'bcode1', bib_type)
    bundle = pipeline.do(bib, bibmarc, ['record_boost'], False)
    assert bundle['record_boost'] == expected


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
                 't': 'Other title'}, ]},
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
def test_bdpipeline_getlinkingfields(raw_marcfields, expected,
                                     fieldstrings_to_fields, sierra_test_record,
                                     bibrecord_to_marc, add_marc_fields,
                                     assert_bundle_matches_expected):
    """
    BibDataPipeline.get_linking_fields should return data matching the
    expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    marcfields = fieldstrings_to_fields(raw_marcfields)
    to_do = ['linking_fields']
    to_remove = ['760', '762', '765', '767', '770', '772', '773', '774', '776',
                 '777', '780', '785', '786', '787']
    title_tags = ('130', '240', '242', '243', '245', '246', '247', '490', '700',
                  '710', '711', '730', '740', '800', '810', '811', '830')
    if any([f.tag in title_tags for f in marcfields]):
        to_do.insert(0, 'title_info')
        to_remove.extend(title_tags)

    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields(*to_remove)
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, to_do)
    assert_bundle_matches_expected(bundle, expected)



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
def test_bdpipeline_geteditions(raw_marcfields, expected,
                                fieldstrings_to_fields, sierra_test_record,
                                bibrecord_to_marc, add_marc_fields,
                                assert_bundle_matches_expected):
    """
    BibDataPipeline.get_editions should return data matching the
    expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
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
      ]}),

    # 866 with $a and multiple $zs
    (['866 31 $av. 1-4 (1941-1943), v. 6-86 (1945-1987)$zSome issues missing;'
      '$zAnother note'],
     {'library_has_display': [
         'v. 1-4 (1941-1943), v. 6-86 (1945-1987); Some issues missing; '
         'Another note'
      ]}),

    # 866 with $8, $a, $x, $2, $z -- only $a and $z are included
    (['866 31 $80$av. 1-4 (1941-1943), v. 6-86 (1945-1987)$xinternal note'
      '$zSome issues missing$2usnp'],
     {'library_has_display': [
         'v. 1-4 (1941-1943), v. 6-86 (1945-1987); Some issues missing'
      ]}),

    # 866 with only $a
    (['866 31 $av. 1-4 (1941-1943), v. 6-86 (1945-1987)'],
     {'library_has_display': [
         'v. 1-4 (1941-1943), v. 6-86 (1945-1987)'
      ]}),

    # 866 with only $z
    (['866 31 $zSome issues missing'],
     {'library_has_display': [
         'Some issues missing'
      ]}),

    # Multiple 866s
    (['866 31 $av. 1-4 (1941-1943)',
      '866 31 $av. 6-86 (1945-1987)',
      '866 31 $zSome issues missing'],
     {'library_has_display': [
         'v. 1-4 (1941-1943)',
         'v. 6-86 (1945-1987)',
         'Some issues missing'
      ]})
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
def test_bdpipeline_getserialholdings(raw_marcfields, expected,
                                      fieldstrings_to_fields,
                                      sierra_test_record, bibrecord_to_marc,
                                      add_marc_fields,
                                      assert_bundle_matches_expected):
    """
    BibDataPipeline.get_serial_holdings should return data matching the
    expected parameters.
    """
    pipeline = pl.BibDataPipeline()
    marcfields = fieldstrings_to_fields(raw_marcfields)
    bib = sierra_test_record('bib_no_items')
    bibmarc = bibrecord_to_marc(bib)
    bibmarc.remove_fields('866')
    bibmarc = add_marc_fields(bibmarc, marcfields)
    bundle = pipeline.do(bib, bibmarc, ['serial_holdings'])
    assert_bundle_matches_expected(bundle, expected)
