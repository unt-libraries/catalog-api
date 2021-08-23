"""
Tests custom filters defined in `sierra.base.managers.`
"""

from datetime import datetime

import pytest

from django.utils import timezone as tz

from base import models as m

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    global_model_instance

pytestmark = pytest.mark.django_db

DEFAULT_TZ = tz.get_default_timezone()

class SierraTestRecordMaker(object):
    """
    This is a one-off class used to create test records to load into
    the Sierra (test) database for the tests in this module.

    Usage: This class is intended to be used inside a pytest fixture.
    Instantiate an object by passing in the function/fixture you're
    using as the general `make_model_instance` factory. Typically this
    will be one of `model_instance` (pytest fixture) or
    `global_model_instance` (pytest fixture), depending on the scope of
    the parent fixture. Then, call the `make_all` method and pass in
    a dictionary structure defining the bib/item records you want made.
    
    Note: This class started out as a way to set up a testing
    environment specifically for this module, so it is not exactly
    complete. The model fields that get populated are limited to the
    ones used by these tests. But, having a generalized way to set up
    similar tests would (may?) be helpful, so eventually I may move
    this out into `utils/test_helpers`. For now I don't think that's
    necessary.
    """
    def __init__(self, make_instance):
        self.make_instance = make_instance
        self.reset_vars()

    def reset_vars(self):
        last_bi_link = m.BibRecordItemRecordLink.objects.order_by('-id')[0]
        last_bl = m.BibRecordLocation.objects.order_by('-id')[0]
        last_loc = m.Location.objects.order_by('-id')[0]
        self.next_bib_item_link_id = last_bi_link.id + 1
        self.next_bib_location_id = last_bl.id + 1
        self.next_location_id = last_loc.id + 1

    def make_dt_tz_aware(self, dt):
        return tz.make_aware(dt, DEFAULT_TZ).astimezone(tz.utc)

    def make_record_md_instance(self, iiirnum, vals):
        rtype, rnum = iiirnum[0], iiirnum[1:]
        id_ = int(rnum) + 1000 if rtype == 'i' else int(rnum)
        dts = [self.make_dt_tz_aware(dt) if dt else None for dt in vals[:3]]
        return self.make_instance(
            m.RecordMetadata,
            id=id_,
            record_type_id=rtype,
            record_num=rnum,
            creation_date_gmt=dts[0],
            record_last_updated_gmt=dts[1],
            deletion_date_gmt=dts[2],
        )

    def get_or_make_location_instance(self, lcode):
        try:
            location = m.Location.objects.get(code=lcode)
        except m.Location.DoesNotExist:
            location = self.make_instance(
                m.Location,
                id=self.next_location_id,
                code=lcode
            )
            self.next_location_id += 1
        return location

    def make_item_instance(self, record_md, item_data):
        (location_code,) = item_data or ([],)
        return self.make_instance(
            m.ItemRecord,
            id=record_md.id,
            record_metadata=record_md,
            location=self.get_or_make_location_instance(location_code)
        )

    def make_bib_instance(self, record_md):
        return self.make_instance(
            m.BibRecord,
            id=record_md.id,
            record_metadata=record_md
        )

    def add_locations_to_bib(self, bib, lcodes):
        for lcode in lcodes:
            self.make_instance(
                m.BibRecordLocation,
                id=self.next_bib_location_id,
                bib_record=bib,
                location=self.get_or_make_location_instance(lcode)
            )
            self.next_bib_location_id += 1

    def add_items_to_bib(self, bib, items_bundle):
        for i, item_b in enumerate(items_bundle):
            metadata = item_b['meta']
            iiirnum, item_vals = metadata[0], metadata[1:]
            item_rec_md = self.make_record_md_instance(iiirnum, item_vals)
            item_data = item_b.get('item', [])
            item = self.make_item_instance(item_rec_md, item_data)
            self.make_instance(
                m.BibRecordItemRecordLink,
                id=self.next_bib_item_link_id,
                bib_record=bib,
                item_record=item,
                items_display_order=i
            )
            self.next_bib_item_link_id += 1

    def make_all(self, test_data):
        self.reset_vars()
        for iiirnum, bundles in test_data.items():
            rec_md = self.make_record_md_instance(iiirnum, bundles['meta'])
            if iiirnum[0] == 'b':
                bib = self.make_bib_instance(rec_md)
                bib_data = bundles.get('bib', [])
                (locations,) = bib_data or ([],)
                self.add_locations_to_bib(bib, locations)
                self.add_items_to_bib(bib, bundles.get('items', []))
            elif iiirnum[0] == 'i':
                self.make_item_instance(rec_md, bundles.get('item', []))


@pytest.fixture(scope='module')
def test_env(global_model_instance):
    """
    Pytest fixture that sets up records (bibs, items, etc.) for our
    testing environment. Note that this is a MODULE level fixture, so
    this data is available for all tests in this module. It gets
    deconstructed when the tests finish.
    """
    SierraTestRecordMaker(global_model_instance).make_all({
        'b0': {
            'meta': (
                datetime(2999, 5, 15, 11, 30, 25),
                datetime(2999, 5, 15, 11, 30, 25),
                None
            ),
            'bib': (['_w'],),
            'items': ({
                'meta': (
                    'i0',
                    datetime(2999, 5, 15, 11, 30, 26),
                    datetime(2999, 5, 15, 11, 30, 26),
                    None
                ),
                'item': ('_w3',)
            },),
        },
        'b1': {
            'meta': (
                datetime(2999, 5, 14, 23, 59, 59),
                datetime(2999, 5, 14, 23, 59, 59),
                None
            ),
            'bib': (['_x'],),
            'items': ({
                'meta': (
                    'i1',
                    datetime(2999, 5, 15, 11, 30, 27),
                    datetime(2999, 5, 15, 11, 30, 27),
                    None
                ),
                'item': ('_x',)
            },),
        },
        'b2': {
            'meta': (
                datetime(2999, 5, 1, 11, 30, 25),
                datetime(2999, 5, 15, 0, 0, 0),
                None
            ),
            'bib': (),
            'items': (),
        },
        'b3': {
            'meta': (
                datetime(2999, 5, 1, 11, 30, 26),
                datetime(2999, 5, 15, 0, 0, 1),
                None
            ),
            'bib': (['_w'],),
            'items': ({
                'meta': (
                    'i2',
                    datetime(2999, 5, 15, 11, 30, 27),
                    datetime(2999, 5, 16, 23, 59, 59),
                    None
                ),
                'item': ('_w4m',)
            },),
        },
        'b4': {
            'meta': (
                datetime(2999, 5, 12, 0, 0, 0),
                datetime(2999, 5, 15, 23, 59, 59),
                None
            ),
            'bib': (['_w', '_czm'],),
            'items': ({
                'meta': (
                    'i3',
                    datetime(2999, 5, 15, 11, 30, 28),
                    datetime(2999, 6, 10, 12, 0, 0),
                    None
                ),
                'item': ('_czm',)
            }, {
                'meta': (
                    'i4',
                    datetime(2999, 5, 15, 11, 30, 29),
                    datetime(2999, 5, 30, 12, 0, 0),
                    None
                ),
                'item': ('_w3',)
            },),
        },
        'b5': {
            'meta': (
                datetime(2999, 5, 15, 11, 30, 27),
                datetime(2999, 5, 16, 0, 0, 0),
                None
            ),
            'bib': (['_x'],),
            'items': ({
                'meta': (
                    'i5',
                    datetime(2999, 5, 15, 11, 30, 30),
                    datetime(2999, 6, 15, 12, 59, 59),
                    None
                ),
                'item': ('_xdoc',)
            },),
        },
        'b6': {
            'meta': (
                datetime(2999, 5, 15, 11, 30, 28),
                datetime(2999, 5, 16, 11, 30, 25),
                None
            ),
            'bib': (['_sd'],),
            'items': ({
                'meta': (
                    'i6',
                    datetime(2999, 5, 15, 11, 30, 30),
                    datetime(2999, 5, 15, 11, 30, 30),
                    None
                ),
                'item': ('_czm',)
            },),
        },
        'b100': {
            'meta': (
                datetime(2999, 5, 15, 11, 30, 25),
                datetime(2999, 5, 15, 11, 30, 25),
                datetime(2999, 5, 15),
            ),
        },
        'b101': {
            'meta': (
                datetime(2999, 5, 14, 23, 59, 58),
                datetime(2999, 5, 14, 23, 59, 58),
                datetime(2999, 5, 14),
            ),
        },
        'b102': {
            'meta': (
                datetime(2999, 5, 1, 11, 30, 25),
                datetime(2999, 5, 15, 0, 0, 0),
                datetime(2999, 5, 15),
            ),
        },
        'b103': {
            'meta': (
                datetime(2999, 5, 1, 11, 30, 26),
                None,
                datetime(2999, 5, 15),
            ),
        },
        'b104': {
            'meta': (
                datetime(2999, 5, 12, 0, 0, 0),
                None,
                datetime(2999, 5, 15),
            ),
        },
        'b105': {
            'meta': (
                datetime(2999, 5, 15, 11, 30, 27),
                None,
                datetime(2999, 5, 16),
            ),
        },
        'b106': {
            'meta': (
                datetime(2999, 5, 15, 11, 30, 28),
                datetime(2999, 5, 16, 11, 30, 25),
                datetime(2999, 5, 16),
            ),
        },
        'i100': {
            'meta': (
                datetime(2999, 5, 14, 11, 30, 28),
                datetime(2999, 5, 15, 11, 30, 25),
                datetime(2999, 5, 15),
            ),
        },
        'i101': {
            'meta': (
                datetime(2999, 5, 14, 11, 30, 29),
                None,
                datetime(2999, 5, 15),
            ),
        },
        'i102': {
            'meta': (
                datetime(2999, 5, 14, 11, 30, 30),
                datetime(2999, 5, 16, 12, 0, 0),
                datetime(2999, 5, 16),
            ),
        },
        'i103': {
            'meta': (
                datetime(2999, 5, 14, 11, 30, 31),
                None,
                datetime(2999, 5, 16),
            ),
        },
    })


# TESTS

@pytest.mark.parametrize('model, opts, expected', [
    (
        m.BibRecord,
        [datetime(2999, 5, 13), datetime(2999, 5, 13), False],
        []
    ), (
        m.BibRecord,
        [datetime(2999, 5, 14), datetime(2999, 5, 14), False],
        ['b1', 'b101']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 15), datetime(2999, 5, 15), False],
        ['b0', 'b2', 'b3', 'b4', 'b100', 'b102']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 16), datetime(2999, 5, 16), False],
        ['b5', 'b6', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 15), datetime(2999, 5, 16), False],
        ['b0', 'b2', 'b3', 'b4', 'b5', 'b6', 'b100', 'b102', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 1), datetime(2999, 5, 14), False],
        ['b1', 'b101']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 1), datetime(2999, 5, 31), False],
        ['b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b100', 'b101', 'b102',
         'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 6, 1), datetime(2999, 6, 30), False],
        []
    ), (
        m.BibRecord,
        [datetime(2999, 5, 13), datetime(2999, 5, 13), True],
        []
    ), (
        m.BibRecord,
        [datetime(2999, 5, 14), datetime(2999, 5, 14), True],
        ['b101']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 15), datetime(2999, 5, 15), True],
        ['b100', 'b102', 'b103', 'b104']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 16), datetime(2999, 5, 16), True],
        ['b105', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 15), datetime(2999, 5, 16), True],
        ['b100', 'b102', 'b103', 'b104', 'b105', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 1), datetime(2999, 5, 14), True],
        ['b101']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 1), datetime(2999, 5, 31), True],
        ['b100', 'b101', 'b102', 'b103', 'b104', 'b105', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 6, 1), datetime(2999, 6, 30), True],
        []
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 14), datetime(2999, 5, 14), False],
        []
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 15), datetime(2999, 5, 15), False],
        ['i0', 'i1', 'i6', 'i100']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 16), datetime(2999, 5, 16), False],
        ['i2', 'i102']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 1), datetime(2999, 5, 31), False],
        ['i0', 'i1', 'i2', 'i4', 'i6', 'i100', 'i102']
    ), (
        m.ItemRecord,
        [datetime(2999, 6, 1), datetime(2999, 6, 30), False],
        ['i3', 'i5']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 14), datetime(2999, 5, 14), True],
        []
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 15), datetime(2999, 5, 15), True],
        ['i100', 'i101']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 16), datetime(2999, 5, 16), True],
        ['i102', 'i103']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 1), datetime(2999, 5, 31), True],
        ['i100', 'i101', 'i102', 'i103']
    ), (
        m.ItemRecord,
        [datetime(2999, 6, 1), datetime(2999, 6, 30), True],
        []
    ), (
        m.BibRecord,
        [datetime(2999, 5, 16), datetime(2999, 5, 16), False, [
            'bibrecorditemrecordlink__item_record'
        ]],
        ['b3', 'b5', 'b6', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 6, 1), datetime(2999, 6, 30), False, [
            'bibrecorditemrecordlink__item_record'
        ]],
        ['b4', 'b5']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 15), datetime(2999, 5, 15), False, [
            'bibrecorditemrecordlink__bib_record'
        ]],
        ['i0', 'i1', 'i2', 'i3', 'i4', 'i6', 'i100']
    ),
])
def test_recordmanager_updateddaterange(model, opts, expected, test_env):
    """
    The `updated_date_range` filter should use the provide options
    (`opts`) to filter the queryset by last updated (or deleted) date
    and return the expected result. The `other_updated_rtype_paths`
    option may be used to expand the filter to check last updated date
    of the specified attached records, as well.
    """
    opts_dict = {'date_range_from': opts[0], 'date_range_to': opts[1],
                 'is_deletion': opts[2]}
    if len(opts) == 4:
        opts_dict['other_updated_rtype_paths'] = opts[3]
    results = model.objects.filter_by('updated_date_range', opts_dict)
    record_nums = [r.record_metadata.get_iii_recnum() for r in results]
    assert sorted(record_nums) == sorted(expected)


@pytest.mark.parametrize('model, opts, expected', [
    (
        m.BibRecord,
        ['b1', 'b5'],
        ['b1', 'b2', 'b3', 'b4', 'b5'],
    ), (
        m.BibRecord,
        ['b1', 'b1'],
        ['b1'],
    ), (
        m.BibRecord,
        ['b5', 'b1'],
        [],
    ), (
        m.BibRecord,
        ['b6', 'b101'],
        ['b6', 'b100', 'b101'],
    ), (
        m.ItemRecord,
        ['b6', 'b101'],
        [],
    ), (
        m.ItemRecord,
        ['i5', 'i101'],
        ['i5', 'i6', 'i100', 'i101'],
    )
])
def test_recordmanager_recordrange(model, opts, expected, test_env):
    """
    The `record_range` filter should use the provided options (`opts`)
    to filter the queryset by record number and return the expected
    result.
    """
    opts_dict = {'record_range_from': opts[0], 'record_range_to': opts[1]}
    results = model.objects.filter_by('record_range', opts_dict)
    record_nums = [r.record_metadata.get_iii_recnum() for r in results]
    assert sorted(record_nums) == sorted(expected)


@pytest.mark.parametrize('model, opts, expected', [
    (
        m.BibRecord,
        [datetime(2999, 5, 1, 12, 0, 0), False],
        ['b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b100', 'b101', 'b102',
         'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 16, 10, 0, 0), False],
        ['b6', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 15, 10, 0, 0), True],
        ['b100', 'b102', 'b103', 'b104', 'b105', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 6, 1, 10, 0, 0), False],
        []
    ), (
        m.ItemRecord,
        [datetime(2999, 6, 1, 10, 0, 0), False],
        ['i3', 'i5']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 15, 10, 0, 0), True],
        ['i100', 'i101', 'i102', 'i103']
    ), (
        m.BibRecord,
        [datetime(2999, 5, 16, 10, 0, 0), False, [
            'bibrecorditemrecordlink__item_record'
        ]],
        ['b3', 'b4', 'b5', 'b6', 'b106']
    ), (
        m.BibRecord,
        [datetime(2999, 6, 1, 10, 0, 0), False, [
            'bibrecorditemrecordlink__item_record'
        ]],
        ['b4', 'b5']
    ), (
        m.ItemRecord,
        [datetime(2999, 5, 16, 10, 0, 0), False, [
            'bibrecorditemrecordlink__bib_record'
        ]],
        ['i2', 'i3', 'i4', 'i5', 'i6', 'i102']
    ),
])
def test_recordmanager_lastexport(model, opts, expected, test_env):
    """
    The `last_export` filter should use the provided `latest_time` opt
    to filter the queryset to things updated since a certain time. The
    `other_updated_rtype_paths` option may be used to expand the filter
    to check last updated date of the specified attached records.
    """
    latest_time = tz.make_aware(opts[0], DEFAULT_TZ).astimezone(tz.utc)
    opts_dict = {'latest_time': latest_time, 'is_deletion': opts[1]}
    if len(opts) == 3:
        opts_dict['other_updated_rtype_paths'] = opts[2]
    results = model.objects.filter_by('last_export', opts_dict)
    record_nums = [r.record_metadata.get_iii_recnum() for r in results]
    assert sorted(record_nums) == sorted(expected)


@pytest.mark.parametrize('model', [
    m.BibRecord,
    m.ItemRecord
])
def test_recordmanager_fullexport(model, test_env):
    """
    The `full_export` filter should return ALL model instances, ordered
    by III record number.
    """
    results = model.objects.filter_by('full_export')
    record_nums = [r.record_metadata.get_iii_recnum() for r in results]
    exp_results = model.objects.order_by('record_metadata__record_num')
    expected = [r.record_metadata.get_iii_recnum() for r in exp_results]
    assert record_nums == expected


@pytest.mark.parametrize('model, lcodes, which_loc, expected', [
    (m.BibRecord, ['_w'], 'bib', ['b0', 'b3', 'b4']),
    (m.BibRecord, ['_czm'], 'bib', ['b4']),
    (m.BibRecord, ['_x'], 'bib', ['b1', 'b5']),
    (m.BibRecord, ['_xdoc'], 'bib', []),
    (m.BibRecord, ['_w', '_czm'], 'bib', ['b0', 'b3', 'b4']),
    (m.BibRecord, ['_w', '_x'], 'bib', ['b0', 'b1', 'b3', 'b4', 'b5']),
    (m.BibRecord, ['_w', '_sd', '_sdus', '_w3', '_x', '_xdoc'], 'bib',
     ['b0', 'b1', 'b3', 'b4', 'b5', 'b6']),
    (m.BibRecord, ['_sdus', '_xdoc'], 'bib', []),
    (m.BibRecord, ['_x'], 'item', ['b1']),
    (m.BibRecord, ['_xdoc'], 'item', ['b5']),
    (m.BibRecord, ['_x', '_xdoc'], 'item', ['b1', 'b5']),
    (m.BibRecord, ['_w'], 'item', []),
    (m.BibRecord, ['_w', '_x'], 'item', ['b1']),
    (m.BibRecord, ['_w', '_r'], 'item', []),
    (m.BibRecord, ['_w'], 'both', ['b0', 'b3', 'b4']),
    (m.BibRecord, ['_czm'], 'both', ['b4', 'b6']),
    (m.BibRecord, ['_xdoc'], 'both', ['b5']),
    (m.BibRecord, ['_w', '_w3'], 'both', ['b0', 'b3', 'b4']),
    (m.BibRecord, ['_w', '_w3', '_czm'], 'both', ['b0', 'b3', 'b4', 'b6']),
    (m.BibRecord, ['_s', '_r'], 'both', []),
    (m.ItemRecord, ['_w'], 'bib', ['i0', 'i2', 'i3', 'i4']),
    (m.ItemRecord, ['_czm'], 'bib', ['i3', 'i4']),
    (m.ItemRecord, ['_x'], 'bib', ['i1', 'i5']),
    (m.ItemRecord, ['_xdoc'], 'bib', []),
    (m.ItemRecord, ['_w', '_czm'], 'bib', ['i0', 'i2', 'i3', 'i4']),
    (m.ItemRecord, ['_w', '_x'], 'bib', ['i0', 'i1', 'i2', 'i3', 'i4', 'i5']),
    (m.ItemRecord, ['_w', '_sd', '_sdus', '_w3', '_x', '_xdoc'], 'bib',
     ['i0', 'i1', 'i2', 'i3', 'i4', 'i5', 'i6']),
    (m.ItemRecord, ['_sdus', '_xdoc'], 'bib', []),
    (m.ItemRecord, ['_x'], 'item', ['i1']),
    (m.ItemRecord, ['_xdoc'], 'item', ['i5']),
    (m.ItemRecord, ['_x', '_xdoc'], 'item', ['i1', 'i5']),
    (m.ItemRecord, ['_w'], 'item', []),
    (m.ItemRecord, ['_czm'], 'item', ['i3', 'i6']),
    (m.ItemRecord, ['_w', '_x'], 'item', ['i1']),
    (m.ItemRecord, ['_w', '_r'], 'item', []),
    (m.ItemRecord, ['_w'], 'both',  ['i0', 'i2', 'i3', 'i4']),
    (m.ItemRecord, ['_w3'], 'both', ['i0', 'i4']),
    (m.ItemRecord, ['_czm'], 'both', ['i3', 'i4', 'i6']),
    (m.ItemRecord, ['_xdoc'], 'both', ['i5']),
    (m.ItemRecord, ['_w', '_w3'], 'both', ['i0', 'i2', 'i3', 'i4']),
    (m.ItemRecord, ['_w', '_czm'], 'both', ['i0', 'i2', 'i3', 'i4', 'i6']),
    (m.ItemRecord, ['_sd'], 'both', ['i6']),
    (m.ItemRecord, ['_s', '_r'], 'both', []),
])
def test_recordmanager_location(model, lcodes, which_loc, expected, test_env):
    """
    The `last_export` filter should use the provided location codes
    (`lcodes`) to filter the queryset to things at one or more specific
    locations. The `which_location` option (`which_loc`) defines
    whether to look at only 'item' locations, only 'bib' locations, or
    'both.'
    """
    opts_dict = {'location_code': lcodes, 'which_location': which_loc}
    results = model.objects.filter_by('location', opts_dict)
    record_nums = [r.record_metadata.get_iii_recnum() for r in results]
    assert sorted(record_nums) == sorted(expected)

