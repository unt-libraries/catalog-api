"""
Tests the `export.exporter` (and children) classes.
"""
from __future__ import absolute_import

from datetime import datetime, timedelta

import pytest
import pytz
from six import text_type
from six.moves import range, zip

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    record_sets, new_exporter, redis_obj, basic_solr_assembler,
#    setattr_model_instance, derive_exporter_class,
#    assert_all_exported_records_are_indexed,
#    assert_deleted_records_are_not_indexed,
#    assert_records_are_indexed, assert_records_are_not_indexed

pytestmark = pytest.mark.django_db


@pytest.fixture
def basic_exporter_class(derive_exporter_class):
    def _basic_exporter_class(name):
        return derive_exporter_class(name, 'export.basic_exporters')
    return _basic_exporter_class


@pytest.fixture
def batch_exporter_class(derive_exporter_class):
    def _batch_exporter_class(name):
        return derive_exporter_class(name, 'export.batch_exporters')
    return _batch_exporter_class


# TESTS

@pytest.mark.parametrize('et_code, category', [
    ('BibsToSolr', 'basic'),
    ('EResourcesToSolr', 'basic'),
    ('ItemsToSolr', 'basic'),
    ('ItemStatusesToSolr', 'basic'),
    ('ItypesToSolr', 'basic'),
    ('LocationsToSolr', 'basic'),
    ('ItemsBibsToSolr', 'basic'),
    ('BibsAndAttachedToSolr', 'basic'),
    ('AllMetadataToSolr', 'batch'),
])
def test_exporter_class_versions(et_code, category, new_exporter,
                                 basic_exporter_class, batch_exporter_class):
    """
    For all exporter types / classes that are under test in this test
    module, what we get from the `basic_exporter_class` and
    `batch_exporter_class` fixtures should be derived from the `export`
    app.
    """
    if category == 'basic':
        expclass = basic_exporter_class(et_code)
    else:
        expclass = batch_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'export'
    for child_etcode, child in getattr(exporter, 'children', {}).items():
        assert child.app_name == 'export'


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_set'),
    ('EResourcesToSolr', 'eres_set'),
    ('ItemsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set'),
])
def test_basic_export_get_records_rn_range(et_code, rset_code,
                                           basic_exporter_class,
                                           record_sets, new_exporter):
    """
    For basic exporter classes that get data from Sierra, the
    `get_records` method should return the expected recordset, when
    using the `record_range` filter type.
    """
    qset = record_sets[rset_code].order_by('pk')
    expected_recs = [r for r in qset]

    opts = {}
    start_rnum = expected_recs[0].record_metadata.get_iii_recnum(False)
    end_rnum = expected_recs[-1].record_metadata.get_iii_recnum(False)
    opts = {'record_range_from': start_rnum, 'record_range_to': end_rnum}

    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'record_range', 'waiting', options=opts)
    records = exporter.get_records()
    assert set(records) == set(expected_recs)


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.parametrize('et_code, rstart, rend, bdate, idates, '
                         'bib_exp_in_rset, items_exp_in_rset', [
    ('BibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], True, [False, False]),
    ('BibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),

    # For bib-centric exporters, a bib should be included in the
    # recordset that `get_records` returns if any attached items have
    # been updated within the query range, even if the bib itself has
    # not.
    ('BibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], True, [False, False]),
    ('BibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),
    ('BibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),

    # For exporters that grab attached records, the original recordset
    # that `get_records` returns does not include the attached records.
    # They are fetched during export.
    ('BibsAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),

    # For item-centric exporters, an item should be included in the
    # recordset that `get_records` returns if the parent bib has
    # been updated within the query range, even if the item itself has
    # not.
    ('ItemsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], False, [True, True]),
    ('ItemsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], False, [True, False]),
    ('ItemsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),
    ('ItemsBibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], False, [True, True]),
    ('ItemsBibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsBibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], False, [True, False]),
    ('ItemsBibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsBibsToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),
])
def test_basic_export_get_records_updated_range(et_code, rstart, rend, bdate,
                                                idates, bib_exp_in_rset,
                                                items_exp_in_rset,
                                                sierra_test_record,
                                                setattr_model_instance,
                                                add_items_to_bib,
                                                basic_exporter_class,
                                                new_exporter):
    """
    For basic exporter classes that get data from Sierra, when using
    using the `updated_date_range` filter type, the `get_records`
    method should return a recordset that either has or does not have
    the test bib and items, based on `bib_exp_in_rset` and
    `items_exp_in_rset`.
    """
    bib = sierra_test_record('bib_no_items')
    setattr_model_instance(bib.record_metadata, 'record_last_updated_gmt',
                           datetime(*bdate, tzinfo=pytz.utc))
    item_info = []
    for idate in idates:
        item_info.append({
            'record_metadata': {
                'record_last_updated_gmt': datetime(*idate, tzinfo=pytz.utc)
            }
        })
    bib = add_items_to_bib(bib, item_info)

    expclass = basic_exporter_class(et_code)
    exp = new_exporter(expclass, 'updated_date_range', 'waiting', options={
        'date_range_from': datetime(*rstart, tzinfo=pytz.utc),
        'date_range_to': datetime(*rend, tzinfo=pytz.utc)
    })
    pks = [r.pk for r in exp.get_records()]
    item_links = bib.bibrecorditemrecordlink_set.all()
    assert (bib.pk in pks) == bib_exp_in_rset
    for link, expected in zip(item_links, items_exp_in_rset):
        assert (link.item_record.pk in pks) == expected


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.parametrize('et_code, last_dt, bdate, idates, bib_exp_in_rset, '
                         'items_exp_in_rset', [

    # This should behave like `updated_date_range` tests; see comments
    # on the previous test's parameters for details.
    ('BibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], True, [False, False]),
    ('BibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),
    ('BibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], True, [False, False]),
    ('BibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),
    ('BibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], True, [False, False]),
    ('BibsAndAttachedToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),
    ('ItemsToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], False, [True, True]),
    ('ItemsToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], False, [True, False]),
    ('ItemsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),
    ('ItemsBibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], False, [True, True]),
    ('ItemsBibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsBibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], False, [True, False]),
    ('ItemsBibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], False, [True, True]),
    ('ItemsBibsToSolr', (2020, 8, 14, 1, 15, 0),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False, [False, False]),
])
def test_basic_export_get_records_last_export(et_code, last_dt, bdate, idates,
                                              bib_exp_in_rset,
                                              items_exp_in_rset,
                                              sierra_test_record,
                                              setattr_model_instance,
                                              add_items_to_bib,
                                              basic_exporter_class,
                                              new_exporter):
    """
    For basic exporter classes that get data from Sierra, when using
    using the `last_export` filter type, the `get_records` method
    should return a recordset that either has or does not have the test
    bib and items, based on `bib_exp_in_rset` and `items_exp_in_rset`.
    """
    bib = sierra_test_record('bib_no_items')
    setattr_model_instance(bib.record_metadata, 'record_last_updated_gmt',
                           datetime(*bdate, tzinfo=pytz.utc))
    item_info = []
    for idate in idates:
        item_info.append({
            'record_metadata': {
                'record_last_updated_gmt': datetime(*idate, tzinfo=pytz.utc)
            }
        })
    bib = add_items_to_bib(bib, item_info)

    expclass = basic_exporter_class(et_code)
    last_exp_timestamp = datetime(*last_dt, tzinfo=pytz.utc)
    last_exp = new_exporter(expclass, 'full_export', 'success')
    last_exp.instance.timestamp = last_exp_timestamp
    last_exp.instance.save()
    exp = new_exporter(expclass, 'last_export', 'waiting', options={})
    pks = [r.pk for r in exp.get_records()]
    item_links = bib.bibrecorditemrecordlink_set.all()
    assert (bib.pk in pks) == bib_exp_in_rset
    for link, expected in zip(item_links, items_exp_in_rset):
        assert (link.item_record.pk in pks) == expected


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.parametrize('et_code, test_lcodes, which_loc, bib_lcodes, '
                         'item_lcodes, bib_exp_in_rset, items_exp_in_rset',
[
    ('BibsToSolr', ['w4m'], 'item', ['w'], ['w4m', 'w3'],
     True, [False, False]),
    ('BibsToSolr', ['w4m'], 'bib', ['w'], ['w4m', 'w4m'],
     False, [False, False]),
    ('BibsToSolr', ['x'], 'item', ['x'], ['w4m', 'w4m'],
     False, [False, False]),
    ('BibsToSolr', ['x'], 'bib', ['x'], ['w4m', 'w4m'],
     True, [False, False]),
    ('BibsToSolr', ['w4m', 'x'], 'item', ['r', 's'], ['w4m', 'w4m'],
     True, [False, False]),
    ('BibsToSolr', ['w', 'x'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     True, [False, False]),
    ('BibsToSolr', ['xdoc'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     True, [False, False]),
    ('BibsToSolr', ['r', 's'], 'both', ['w', 'x'], ['xdoc', 'w3'],
     False, [False, False]),
    ('BibsToSolr', ['w', 'x'], 'bib', ['r', 's'], ['w4m', 'xdoc'],
     False, [False, False]),
    ('BibsAndAttachedToSolr', ['w4m'], 'item', ['w'], ['w4m', 'w4m'],
     True, [False, False]),
    ('BibsAndAttachedToSolr', ['w4m'], 'bib', ['w'], ['w4m', 'w4m'],
     False, [False, False]),
    ('BibsAndAttachedToSolr', ['x'], 'item', ['x'], ['w4m', 'w4m'],
     False, [False, False]),
    ('BibsAndAttachedToSolr', ['x'], 'bib', ['x'], ['w4m', 'w4m'],
     True, [False, False]),
    ('BibsAndAttachedToSolr', ['w4m', 'x'], 'item', ['r', 's'], ['w4m', 'w4m'],
     True, [False, False]),
    ('BibsAndAttachedToSolr', ['w', 'x'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     True, [False, False]),
    ('BibsAndAttachedToSolr', ['xdoc'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     True, [False, False]),
    ('BibsAndAttachedToSolr', ['r', 's'], 'both', ['w', 'x'], ['xdoc', 'w3'],
     False, [False, False]),
    ('BibsAndAttachedToSolr', ['w', 'x'], 'bib', ['r', 's'], ['w4m', 'xdoc'],
     False, [False, False]),
    ('ItemsToSolr', ['w4m'], 'item', ['w'], ['w4m', 'w3'],
     False, [True, False]),
    ('ItemsToSolr', ['w4m'], 'bib', ['w'], ['w4m', 'w4m'],
     False, [False, False]),
    ('ItemsToSolr', ['x'], 'item', ['x'], ['w4m', 'w4m'],
     False, [False, False]),
    ('ItemsToSolr', ['x'], 'bib', ['x'], ['w4m', 'w4m'],
     False, [True, True]),
    ('ItemsToSolr', ['w4m', 'x'], 'item', ['r', 's'], ['w4m', 'w4m'],
     False, [True, True]),
    ('ItemsToSolr', ['w', 'x'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     False, [True, True]),
    ('ItemsToSolr', ['xdoc'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     False, [True, False]),
    ('ItemsToSolr', ['x', 'xdoc'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     False, [True, True]),
    ('ItemsToSolr', ['r', 's'], 'both', ['w', 'x'], ['xdoc', 'w3'],
     False, [False, False]),
    ('ItemsToSolr', ['w', 'x'], 'bib', ['r', 's'], ['w4m', 'xdoc'],
     False, [False, False]),
    ('ItemsBibsToSolr', ['w4m'], 'item', ['w'], ['w4m', 'w3'],
     False, [True, False]),
    ('ItemsBibsToSolr', ['w4m'], 'bib', ['w'], ['w4m', 'w4m'],
     False, [False, False]),
    ('ItemsBibsToSolr', ['x'], 'item', ['x'], ['w4m', 'w4m'],
     False, [False, False]),
    ('ItemsBibsToSolr', ['x'], 'bib', ['x'], ['w4m', 'w4m'],
     False, [True, True]),
    ('ItemsBibsToSolr', ['w4m', 'x'], 'item', ['r', 's'], ['w4m', 'w4m'],
     False, [True, True]),
    ('ItemsBibsToSolr', ['w', 'x'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     False, [True, True]),
    ('ItemsBibsToSolr', ['xdoc'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     False, [True, False]),
    ('ItemsBibsToSolr', ['x', 'xdoc'], 'both', ['w', 'x'], ['xdoc', 'w4m'],
     False, [True, True]),
    ('ItemsBibsToSolr', ['r', 's'], 'both', ['w', 'x'], ['xdoc', 'w3'],
     False, [False, False]),
    ('ItemsBibsToSolr', ['w', 'x'], 'bib', ['r', 's'], ['w4m', 'xdoc'],
     False, [False, False]),
])
def test_basic_export_get_records_location(et_code, test_lcodes, which_loc,
                                           bib_lcodes, item_lcodes,
                                           bib_exp_in_rset, items_exp_in_rset,
                                           sierra_test_record,
                                           get_or_make_location_instances,
                                           add_items_to_bib,
                                           add_locations_to_bib,
                                           basic_exporter_class, new_exporter):
    """
    For basic exporter classes that get data from Sierra, when using
    using the `location` filter type and the provided options, the
    `get_records` method should return a recordset that either has or
    does not have the test bib and items, based on `bib_exp_in_rset`
    and `items_exp_in_rset`.
    """
    bib = sierra_test_record('bib_no_items')
    blocs = get_or_make_location_instances([{'code': lc} for lc in bib_lcodes])
    ilocs = get_or_make_location_instances([{'code': lc} for lc in item_lcodes])
    item_info = [{'attrs': {'location_id': lc}} for lc in item_lcodes]
    bib = add_locations_to_bib(add_items_to_bib(bib, item_info), blocs, True)    

    expclass = basic_exporter_class(et_code)
    exp = new_exporter(expclass, 'location', 'waiting', options={
        'which_location': which_loc,
        'location_code': test_lcodes
    })
    pks = [r.pk for r in exp.get_records()]
    item_links = bib.bibrecorditemrecordlink_set.all()
    assert (bib.pk in pks) == bib_exp_in_rset
    for link, expected in zip(item_links, items_exp_in_rset):
        assert (link.item_record.pk in pks) == expected


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.basic
@pytest.mark.parametrize('et_code', [
    'BibsToSolr', 
    'BibsToSolr', 
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr',
    'ItemsBibsToSolr',
    'BibsAndAttachedToSolr',
    'BibsAndAttachedToSolr',
    'ItemsBibsToSolr',
])
def test_basic_export_get_records_full_export(et_code, basic_exporter_class,
                                              new_exporter):
    """
    For exporter classes that get data from Sierra, the `get_records`
    method with export type `full_export` should return a recordset
    with all available records for that exporter.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting', options={})
    expected_qset = exporter.model.objects.all()
    expected_recs = [r for r in expected_qset]
    records = exporter.get_records()
    assert set(records) == set(expected_recs)


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.batch
def test_allmdtosolr_export_get_records(batch_exporter_class, record_sets,
                                        new_exporter):
    """
    The `AllMetadataToSolr` `get_records` method should return a dict
    of all applicable record sets.
    """
    expected_rsets = {
        'LocationsToSolr': record_sets['location_set'],
        'ItypesToSolr': record_sets['itype_set'],
        'ItemStatusesToSolr': record_sets['istatus_set'],
    }
    expclass = batch_exporter_class('AllMetadataToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    rsets = exporter.get_records()

    assert len(list(expected_rsets.keys())) == len(list(rsets.keys()))
    for name, records in rsets.items():
        assert set(records) == set(expected_rsets[name])


@pytest.mark.deletions
@pytest.mark.get_records
@pytest.mark.basic
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_del_set'),
    ('EResourcesToSolr', 'eres_del_set'),
    ('ItemsToSolr', 'item_del_set'),
    ('ItemStatusesToSolr', None),
    ('ItypesToSolr', None),
    ('LocationsToSolr', None),
    ('ItemsBibsToSolr', 'item_del_set'),
    ('BibsAndAttachedToSolr', 'bib_del_set'),
])
def test_basic_export_get_deletions(et_code, rset_code, basic_exporter_class,
                                    record_sets, new_exporter):
    """
    For basic Exporter classes that get data from Sierra, the
    `get_deletions` method should return a record set containing the
    expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    records = exporter.get_deletions()
    if rset_code is None:
        assert records is None
    else:
        assert set(records) == set(record_sets[rset_code])


@pytest.mark.deletions
@pytest.mark.get_records
@pytest.mark.batch
def test_allmdtosolr_export_get_deletions(batch_exporter_class, record_sets,
                                          new_exporter):
    """
    The `AllMetadataToSolr` `get_deletions` method should return None.
    """
    expclass = batch_exporter_class('AllMetadataToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    rsets = exporter.get_deletions()
    assert rsets is None


@pytest.mark.exports
@pytest.mark.do_export
@pytest.mark.basic
@pytest.mark.parametrize('et_code, rset_code, rtype, do_reindex', [
    ('BibsToSolr', 'bib_set', 'bib', False),
    ('EResourcesToSolr', 'eres_set', 'eresource', False),
    ('ItemsToSolr', 'item_set', 'item', False),
    ('ItemStatusesToSolr', 'istatus_set', 'itemstatus', True),
    ('ItypesToSolr', 'itype_set', 'itype', True),
    ('LocationsToSolr', 'location_set', 'location', True),
])
def test_basic_tosolr_export_records(et_code, rset_code, rtype, do_reindex,
                                     basic_exporter_class, record_sets,
                                     new_exporter, solr_conns, solr_search,
                                     basic_solr_assembler, do_commit,
                                     assert_records_are_indexed,
                                     assert_records_are_not_indexed):
    """
    For basic ToSolrExporter classes, the `export_records` method
    should load the expected records into the expected Solr index. This
    uses the `solr_assemble_specific_record_data` fixture to help
    preload some data into Solr. We want to make sure that exporters
    where indexes are supposed to be fully refreshed with each run
    delete the old data and only load the data in the recordset, while
    other exporters add records to the existing recordset.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    records = record_sets[rset_code]
    index = [ind for ind in exporter.indexes.values()][0]
    assert len(exporter.indexes.values()) == 1

    id_field = index.reserved_fields['haystack_id']

    # Do some setup to put some meaningful data into the index first.
    # We want some records that overlap with the incoming record set
    # and some that don't.
    num_existing = records.count() / 2
    overlap_recs = records[0:num_existing]
    only_new = records[num_existing:]
    old_rec_pks = [text_type(pk) for pk in range(99991, 99995)]
    only_old_rec_data = [(pk, {}) for pk in old_rec_pks]

    overlap_rec_data = []
    for r in overlap_recs:
        overlap_rec_data.append((index.get_qualified_id(r), {}))

    data = only_old_rec_data + overlap_rec_data
    basic_solr_assembler.load_static_test_data(rtype, data, id_field=id_field)

    # Check the setup to make sure existing records are indexed and new
    # records are not.
    conn = solr_conns[getattr(index, 'using', 'default')]
    results = solr_search(conn, '*')
    only_old = [r for r in results if r[id_field] in old_rec_pks]
    assert len(only_old) == len(old_rec_pks)
    assert_records_are_indexed(index, overlap_recs, results=results)
    assert_records_are_not_indexed(index, only_new, results=results)

    exporter.export_records(records)
    do_commit(exporter)

    conn = solr_conns[getattr(index, 'using', 'default')]
    results = solr_search(conn, '*')
    only_old = [r for r in results if r[id_field] in old_rec_pks]
    assert len(only_old) == 0 if do_reindex else len(old_rec_pks)
    assert_records_are_indexed(index, overlap_recs, results=results)
    if exporter.is_active:
        assert_records_are_indexed(index, only_new, results=results)
    else:
        assert_records_are_not_indexed(index, only_new, results=results)


@pytest.mark.exports
@pytest.mark.do_export
@pytest.mark.batch
def test_allmdtosolr_export_records(batch_exporter_class, record_sets,
                                    new_exporter, do_commit,
                                    assert_all_exported_records_are_indexed):
    """
    The `AllMetadataToSolr` `export_records` method should load the
    expected records into the expected Solr index. This is just a
    simple check to make sure all child exporters processed the
    appropriate recordset; the children are tested more extensively
    elsewhere.
    """
    records = {
        'LocationsToSolr': record_sets['location_set'],
        'ItypesToSolr': record_sets['itype_set'],
        'ItemStatusesToSolr': record_sets['istatus_set'],
    }
    expclass = batch_exporter_class('AllMetadataToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    do_commit(exporter)
    for key, child in exporter.children.items():
        assert_all_exported_records_are_indexed(child, records[key])


@pytest.mark.deletions
@pytest.mark.do_export
@pytest.mark.basic
@pytest.mark.parametrize('et_code, rset_code, rtype', [
    ('BibsToSolr', 'bib_del_set', 'bib'),
    ('EResourcesToSolr', 'eres_del_set', 'eresource'),
    ('ItemsToSolr', 'item_del_set', 'item'),
])
def test_basic_tosolr_delete_records(et_code, rset_code, rtype,
                                     basic_exporter_class, record_sets,
                                     new_exporter, basic_solr_assembler,
                                     assert_records_are_indexed, do_commit,
                                     assert_deleted_records_are_not_indexed):
    """
    For basic ToSolrExporter classes that have loaded data into Solr,
    the `delete_records` method should delete records from the
    appropriate index or indexes.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    index = [ind for ind in exporter.indexes.values()][0]
    assert len(exporter.indexes.values()) == 1

    id_field = index.reserved_fields['haystack_id']
    
    data = [(index.get_qualified_id(r), {}) for r in records]
    basic_solr_assembler.load_static_test_data(rtype, data, id_field=id_field)
    assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    do_commit(exporter)
    assert_deleted_records_are_not_indexed(exporter, records)


@pytest.mark.exceptions
def test_tosolr_index_update_errors(basic_exporter_class, record_sets,
                                    new_exporter, setattr_model_instance,
                                    assert_records_are_indexed, do_commit,
                                    assert_records_are_not_indexed):
    """
    When updating indexes via a ToSolrExporter, if one record causes an
    error during preparation (e.g. via the haystack SearchIndex obj),
    the export process should: 1) skip that record, and 2) log the
    error as a warning on the exporter. Other records in the same batch
    should still be indexed.
    """
    records = record_sets['item_set']
    expclass = basic_exporter_class('ItemsToSolr')
    invalid_loc_code = '_____'
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    def prepare_location_code(obj):
        code = obj.location_id
        if code == invalid_loc_code:
            raise Exception('Code not valid')
        return code

    exporter.indexes['Items'].prepare_location_code = prepare_location_code
    setattr_model_instance(records[0], 'location_id', invalid_loc_code)
    exporter.export_records(records)
    do_commit(exporter)

    assert_records_are_not_indexed(exporter.indexes['Items'], [records[0]])
    assert_records_are_indexed(exporter.indexes['Items'], records[1:])
    assert exporter.is_active
    assert len(exporter.indexes['Items'].last_batch_errors) == 1


@pytest.mark.exports
@pytest.mark.do_export
@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set')
])
def test_attached_solr_export_records(et_code, rset_code, basic_exporter_class,
                                      record_sets, new_exporter, do_commit,
                                      assert_all_exported_records_are_indexed):
    """
    For AttachedRecordExporter classes that load data into Solr, the
    `export_records` method should load the expected records into the
    expected Solr indexes. This is just a simple check to make sure all
    child exporters processed the appropriate recordsets; the children
    are tested more extensively elsewhere.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    do_commit(exporter)
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
@pytest.mark.do_export
@pytest.mark.parametrize('et_code, rset_code, rtype', [
    ('ItemsBibsToSolr', 'item_del_set', 'item'),
    ('BibsAndAttachedToSolr', 'bib_del_set', 'bib'),
])
def test_attached_solr_delete_records(et_code, rset_code, rtype,
                                      basic_exporter_class, record_sets,
                                      new_exporter, basic_solr_assembler,
                                      assert_records_are_indexed, do_commit,
                                      assert_deleted_records_are_not_indexed):
    """
    For Exporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    index = [ind for ind in exporter.main_child.indexes.values()][0]
    assert len(exporter.main_child.indexes.values()) == 1

    id_field = index.reserved_fields['haystack_id']
    
    data = [(index.get_qualified_id(r), {}) for r in records]
    basic_solr_assembler.load_static_test_data(rtype, data, id_field=id_field)
    assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    do_commit(exporter)
    assert_deleted_records_are_not_indexed(exporter, records)


@pytest.mark.parametrize('et_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr'])
def test_max_chunk_settings_overrides(et_code, settings, basic_exporter_class,
                                      new_exporter):
    """
    Using EXPORTER_MAX_RC_CONFIG and EXPORTER_MAX_DC_CONFIG settings
    should override values set on the class when an exporter is
    instantiated.
    """
    expclass = basic_exporter_class(et_code)
    test_et_code = expclass.__name__
    new_rc_val, new_dc_val = 77777, 88888
    settings.EXPORTER_MAX_RC_CONFIG[test_et_code] = new_rc_val
    settings.EXPORTER_MAX_DC_CONFIG[test_et_code] = new_dc_val

    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.max_rec_chunk == new_rc_val
    assert exporter.max_del_chunk == new_dc_val
    assert new_rc_val != expclass.max_rec_chunk
    assert new_dc_val != expclass.max_del_chunk


@pytest.mark.parametrize('et_code', [
    'BibsToSolr',
    'EResourcesToSolr',
    'ItemsToSolr',
    'ItemStatusesToSolr',
    'ItypesToSolr',
    'LocationsToSolr'])
def test_max_chunk_settings_defaults(et_code, settings, basic_exporter_class,
                                     new_exporter):
    """
    If NOT using EXPORTER_MAX_RC_CONFIG and EXPORTER_MAX_DC_CONFIG
    settings, the `max_rec_chunk` and `max_del_chunk` values for a
    given job should come from the exporter class.
    """
    settings.EXPORTER_MAX_RC_CONFIG = {}
    settings.EXPORTER_MAX_DC_CONFIG = {}
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.max_rec_chunk == expclass.max_rec_chunk
    assert exporter.max_del_chunk == expclass.max_del_chunk


@pytest.mark.return_vals
@pytest.mark.parametrize('vals_list, expected', [
    ([{'ids': ['b1', 'b2']},
      {'ids': ['b4', 'b5']},
      {'ids': ['b3']}
      ], {'ids': ['b1', 'b2', 'b4', 'b5', 'b3']}),

    ([{'names': [{'first': 'Bob', 'last': 'Jones'}]},
      {'names': [{'first': 'Sarah', 'last': 'Kim'}]},
      {'names': [{'first': 'Sally', 'last': 'Smith'}]}
      ], {'names': [{'first': 'Bob', 'last': 'Jones'},
                    {'first': 'Sarah', 'last': 'Kim'},
          {'first': 'Sally', 'last': 'Smith'}]}),

    ([{'grades': {'Bob Jones': ['A', 'B'],
                  'Sarah Kim': ['B', 'A', 'C']}},
      {'grades': {'Bob Jones': ['A']}},
      {'grades': {'Sally Smith': ['A', 'A', 'B']}}
      ], {'grades': {'Bob Jones': ['A', 'B', 'A'],
                     'Sarah Kim': ['B', 'A', 'C'],
                     'Sally Smith': ['A', 'A', 'B']}}),

    ([{'list1': ['a', 'b'], 'list2': [1, 2]},
      {'list1': ['c', 'd'], 'list2': [3, 4]},
      {'list1': ['e', 'f'], 'list2': [5, 6]},
      ], {'list1': ['a', 'b', 'c', 'd', 'e', 'f'],
          'list2': [1, 2, 3, 4, 5, 6]}),

    ([{'list1': ['a', 'b'], 'list2': [1, 2]},
      {'list1': ['c', 'd']},
      {'list1': ['e', 'f'], 'list2': [5, 6]},
      ], {'list1': ['a', 'b', 'c', 'd', 'e', 'f'],
          'list2': [1, 2, 5, 6]}),

    ([{'list1': [], 'list2': [1, 2]},
      {'list1': ['c', 'd'], 'list2': [3, 4]},
      {'list1': ['e', 'f'], 'list2': [5, 6]},
      ], {'list1': ['c', 'd', 'e', 'f'],
          'list2': [1, 2, 3, 4, 5, 6]}),

    ([{'list1': ['a', 'b']},
      {'list2': [1, 2]},
      ], {'list1': ['a', 'b'],
          'list2': [1, 2]}),

    ([{'list1': [1, 2, 3]},
      None,
      {'list1': [4, 5, 6]}
      ], {'list1': [1, 2, 3, 4, 5, 6]}),

    ([None, None, None], None),
], ids=[
    'one key, arrays of values',
    'one key, arrays of dicts',
    'one key, nested dicts',
    'two keys, normal',
    'two keys, one is absent',
    'two keys, one is a blank value',
    'two keys, mutually exclusive',
    'one vals_list is None',
    'all vals_lists are None',
])
def test_exporter_default_compile_vals(vals_list, expected, new_exporter,
                                       derive_exporter_class):
    """
    The default `Exporter.compile_vals` method should take a list of
    dicts and return a single dict that represents a merger of all
    dicts in the list.
    """
    expclass = derive_exporter_class('Exporter', 'export.exporter')
    exp = new_exporter(expclass, 'full_export', 'waiting')
    assert exp.compile_vals(vals_list) == expected


@pytest.mark.compound
@pytest.mark.return_vals
@pytest.mark.do_export
@pytest.mark.parametrize('classname, method, children_and_retvals, expected', [
    ('AttachedRecordExporter', 'export_records',
        [('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
         ('C3', None)],
        {'C1': {'colors': ['red', 'green']},
         'C2': {'sounds': ['woosh']},
         'C3': None}
     ),
    ('AttachedRecordExporter', 'delete_records',
        [('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
         ('C3', None)],
        {'C1': {'colors': ['red', 'green']}}
     ),
    ('BatchExporter', 'export_records',
        [('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
         ('C3', None)],
        {'C1': {'colors': ['red', 'green']},
         'C2': {'sounds': ['woosh']},
         'C3': None}
     ),
    ('BatchExporter', 'delete_records',
        [('C1', {'colors': ['red', 'green']}), ('C2', {'sounds': ['woosh']}),
         ('C3', None)],
        {'C1': {'colors': ['red', 'green']},
         'C2': {'sounds': ['woosh']},
         'C3': None}
     ),
], ids=[
    'AttachedRE export_records: all children run and return their vals',
    'AttachedRE delete_records: only main child runs and returns vals',
    'BatchE export_records: all children run and return their vals',
    'BatchE delete_records: all children run and return their vals',
])
def test_compound_ops_and_return_vals(classname, method, children_and_retvals,
                                      expected, derive_compound_exporter_class,
                                      derive_child_exporter_class, new_exporter,
                                      mocker):
    """
    The `export_records` and `delete_records` methods for
    AttachedRecordExporter and BatchExporter should return a dict where
    each key contains the return vals for each child that ran.
    """
    child_classes = []
    for name, retvals in children_and_retvals:
        child = derive_child_exporter_class(newname=name)
        mocker.patch.object(child, method)
        getattr(child, method).return_value = retvals
        child_classes.append(child)

    expclass = derive_compound_exporter_class(classname, 'export.exporter',
                                              children=child_classes)
    exp = new_exporter(expclass, 'full_export', 'waiting')
    return_vals = getattr(exp, method)([])
    assert return_vals == expected
    for name, child in exp.children.items():
        if name in list(expected.keys()):
            getattr(child, method).assert_called_with([])
        else:
            getattr(child, method).assert_not_called()


@pytest.mark.compound
@pytest.mark.return_vals
@pytest.mark.parametrize('classname, children, vals_list, expected', [
    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}, 'C2': {'sounds': ['pop']}},
         {'C1': {'colors': ['tan']}, 'C2': {'sounds': ['squee', 'pop']}}],
        {'C1': {'colors': ['red', 'blue', 'tan']},
         'C2': {'sounds': ['pop', 'squee', 'pop']}}),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
         {'C1': {'colors': ['tan']}, 'C2': {'colors': ['pink', 'brown']}}],
        {'C1': {'colors': ['red', 'blue', 'tan']},
         'C2': {'colors': ['yellow', 'pink', 'brown']}}),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
         {'C1': {'sounds': ['pop', 'bang']}, 'C2': {'colors': ['red']}}],
        {'C1': {'colors': ['red', 'blue'], 'sounds': ['pop', 'bang']},
         'C2': {'colors': ['yellow', 'red']}}),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': None, 'C2': {'colors': ['yellow']}},
         {'C1': None, 'C2': {'colors': ['pink', 'brown']}}],
        {'C1': None, 'C2': {'colors': ['yellow', 'pink', 'brown']}}),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red']}, 'C2': {'colors': ['yellow']}},
         {'C1': None, 'C2': {'colors': ['pink', 'brown']}}],
        {'C1': {'colors': ['red']},
         'C2': {'colors': ['yellow', 'pink', 'brown']}}),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': None, 'C2': None}, {'C1': None, 'C2': None}],
        {'C1': None, 'C2': None}),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red']}},
         {'C1': {'colors': ['tan']}, 'C2': {'colors': ['pink', 'brown']}}],
        {'C1': {'colors': ['red', 'tan']},
         'C2': {'colors': ['pink', 'brown']}}),

    ('AttachedRecordExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red']}}, {'C1': {'colors': ['tan']}}],
        {'C1': {'colors': ['red', 'tan']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}},
         {'C1': {'colors': ['tan', 'red']}}],
        {'C1': {'colors': ['red', 'blue', 'tan', 'red']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}},
         {'C1': {'sounds': ['pop']}}],
        {'C1': {'colors': ['red', 'blue'], 'sounds': ['pop']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': None}, {'C1': {'colors': ['red']}}],
        {'C1': {'colors': ['red']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': None}, {'C1': None}],
        {'C1': None}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}}, {'C2': {'sounds': ['pop']}}],
        {'C1': {'colors': ['red', 'blue']}, 'C2': {'sounds': ['pop']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': None}, {'C2': {'sounds': ['pop']}}],
        {'C1': None, 'C2': {'sounds': ['pop']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': None}, {'C2': None}],
        {'C1': None, 'C2': None}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}, 'C2': {'sounds': ['pop']}},
         {'C1': {'colors': ['tan']}, 'C2': {'sounds': ['squee', 'pop']}}],
        {'C1': {'colors': ['red', 'blue', 'tan']},
         'C2': {'sounds': ['pop', 'squee', 'pop']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
         {'C1': {'colors': ['tan']}, 'C2': {'colors': ['pink', 'brown']}}],
        {'C1': {'colors': ['red', 'blue', 'tan']},
         'C2': {'colors': ['yellow', 'pink', 'brown']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red', 'blue']}, 'C2': {'colors': ['yellow']}},
         {'C1': {'sounds': ['pop', 'bang']}, 'C2': {'colors': ['red']}}],
        {'C1': {'colors': ['red', 'blue'], 'sounds': ['pop', 'bang']},
         'C2': {'colors': ['yellow', 'red']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': None, 'C2': {'colors': ['yellow']}},
         {'C1': None, 'C2': {'colors': ['pink', 'brown']}}],
        {'C1': None, 'C2': {'colors': ['yellow', 'pink', 'brown']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': {'colors': ['red']}, 'C2': {'colors': ['yellow']}},
         {'C1': None, 'C2': {'colors': ['pink', 'brown']}}],
        {'C1': {'colors': ['red']},
         'C2': {'colors': ['yellow', 'pink', 'brown']}}),

    ('BatchExporter', ('C1', 'C2'),
        [{'C1': None, 'C2': None}, {'C1': None, 'C2': None}],
        {'C1': None, 'C2': None}),

], ids=[
    'AttachedRE: children have different keys',
    'AttachedRE: children have the same keys',
    'AttachedRE: same child has different keys',
    'AttachedRE: one child has return value None',
    'AttachedRE: one child returns None once',
    'AttachedRE: both children return None',
    'AttachedRE: one child missing an entry (did not run, once)',
    'AttachedRE: one child has no entries (did not run, at all)',
    'BatchE: only one child ran; same keys',
    'BatchE: only one child ran; different keys',
    'BatchE: only one child ran; one chunk has no ret vals',
    'BatchE: only one child ran; no ret vals',
    'BatchE: different children ran',
    'BatchE: different children ran, one has no ret vals',
    'BatchE: different children ran, neither has ret vals',
    'BatchE: both children ran; children have different keys',
    'BatchE: both children ran; children have the same keys',
    'BatchE: both children ran; same child has different keys',
    'BatchE: both children ran; one child has return value None',
    'BatchE: both children ran; one child returns None once',
    'BatchE: both children ran; both children return None',
])
def test_compound_compile_vals(classname, children, vals_list, expected,
                               derive_compound_exporter_class,
                               derive_child_exporter_class, new_exporter):
    """
    The `compile_vals` method for AttachedRecordExporter and
    BatchExporter should take a list of vals dicts and return a merged
    vals dict, assuming that each key/value pair represents the return
    values for each child exporter that ran during an export or delete
    operation. Keys are the names of each child.
    """
    child_classes = [derive_child_exporter_class(newname=n) for n in children]
    expclass = derive_compound_exporter_class(classname, 'export.exporter',
                                              children=child_classes)
    exp = new_exporter(expclass, 'full_export', 'waiting')
    assert exp.compile_vals(vals_list) == expected


@pytest.mark.compound
@pytest.mark.callback
@pytest.mark.parametrize('classname, children, vals', [
    ('AttachedRecordExporter', ('C1', 'C2'),
        {'C1': {'colors': ['red']}, 'C2': {'sounds': 'pop'}}),
    ('AttachedRecordExporter', ('C1', 'C2'),
        {'C1': {'colors': ['red']}, 'C2': None}),
    ('AttachedRecordExporter', ('C1', 'C2'),
        {'C1': {'colors': ['red']}}),
    ('AttachedRecordExporter', ('C1', 'C2'), None),
    ('BatchExporter', ('C1', 'C2'),
        {'C1': {'colors': ['red']}, 'C2': {'sounds': 'pop'}}),
    ('BatchExporter', ('C1', 'C2'),
        {'C1': {'colors': ['red']}, 'C2': None}),
    ('BatchExporter', ('C1', 'C2'),
        {'C1': {'colors': ['red']}}),
    ('BatchExporter', ('C1', 'C2'), None),
], ids=[
    'AttachedRE: both children have vals',
    'AttachedRE: one child has vals, the other has None',
    'AttachedRE: one child has vals, the other is missing',
    'AttachedRE: vals is None',
    'BatchE: both children have vals',
    'BatchE: one child has vals, the other has None',
    'BatchE: one child has vals, the other is missing',
    'BatchE: vals is None',
])
def test_compound_final_callback(classname, children, vals,
                                 derive_compound_exporter_class,
                                 derive_child_exporter_class, new_exporter,
                                 mocker):
    """
    The `final_callback` method for the given Compound exporter type
    (AttachedRecordExporter or BatchExporter) should run the
    `final_callback` method on each child, passing the appropriate
    portion of `vals` to each.
    """
    child_classes = []
    for name in children:
        child = derive_child_exporter_class(newname=name)
        mocker.patch.object(child, 'final_callback')
        child_classes.append(child)
    expclass = derive_compound_exporter_class(classname, 'export.exporter',
                                              children=child_classes)
    exp = new_exporter(expclass, 'full_export', 'waiting')
    exp.final_callback(vals=vals)
    for name, child in exp.children.items():
        expected_vals = None if vals is None else vals.get(name, None)
        child.final_callback.assert_called_with(vals=expected_vals,
                                                status='success')


@pytest.mark.return_vals
def test_ertosolr_export_returns_h_lists(basic_exporter_class, record_sets,
                                         new_exporter):
    """
    The EResourcesToSolr exporter `export_records` method should return
    a dict with an `h_lists` key, which has a dict mapping eresources
    (record nums) to holdings/checkins (record nums).
    """
    records = record_sets['eres_set']
    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    vals = exporter.export_records(records)
    assert 'h_lists' in vals
    for rec in records:
        er_recnum = rec.record_metadata.get_iii_recnum(False)
        assert er_recnum in vals['h_lists']
        exp_holdings = [h.record_metadata.get_iii_recnum(False)
                        for h in rec.holding_records.all()]
        assert vals['h_lists'][er_recnum] == exp_holdings


@pytest.mark.return_vals
def test_ertosolr_delete_returns_deletions(basic_exporter_class, record_sets,
                                           new_exporter):
    """
    The EResourcesToSolr exporter `delete_records` method should return
    a dict with a `deletions` key, which has the list of eresources
    (record nums) that were deleted in the batch.
    """
    records = record_sets['eres_del_set']
    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    vals = exporter.delete_records(records)
    assert 'deletions' in vals
    exp_deletions = [r.get_iii_recnum(False) for r in records]
    assert vals['deletions'] == exp_deletions


@pytest.mark.callback
def test_ertosolr_export_callback_commits_to_redis(basic_exporter_class,
                                                   new_exporter, redis_obj):
    """
    The EResourcesToSolr exporter `final_callback` method should commit
    updated holdings lists for new and updated eresources to Redis, and
    it should add them to or otherwise update the reverse holdings list
    appropriately.
    """
    existing = {
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'eresource_holdings_list:e3': ['c4', 'c5'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2', 'c4': 'e3', 'c5': 'e3'
        }
    }

    for key, val in existing.items():
        redis_obj(key).set(val)

    vals = {'h_lists': {'e1': ['c6', 'c4'], 'e3': ['c5', 'c7']}}
    expected = {
        'eresource_holdings_list:e1': ['c6', 'c4'],
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'eresource_holdings_list:e3': ['c5', 'c7'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2', 'c4': 'e1', 'c5': 'e3',
            'c6': 'e1', 'c7': 'e3'
        }
    }

    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.final_callback(vals=vals, status='success')

    for key in redis_obj.conn.keys():
        assert key in expected
        assert redis_obj(key).get() == expected[key]


@pytest.mark.callback
def test_ertosolr_delete_callback_commits_to_redis(basic_exporter_class,
                                                   new_exporter, redis_obj):
    """
    The EResourcesToSolr exporter `final_callback` method should remove
    deleted eresources' holdings lists in Redis, and it should remove
    the applicable holdings records from the reverse holdings list.
    """
    existing = {
        'eresource_holdings_list:e1': ['c6', 'c4'],
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'eresource_holdings_list:e3': ['c5', 'c7'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2', 'c4': 'e1', 'c5': 'e3',
            'c6': 'e1', 'c7': 'e3'
        }
    }

    for key, val in existing.items():
        redis_obj(key).set(val)

    vals = {'deletions': ['e1', 'e3']}
    expected = {
        'eresource_holdings_list:e2': ['c1', 'c2', 'c3'],
        'reverse_holdings_list:0': {
            'c1': 'e2', 'c2': 'e2', 'c3': 'e2'
        }
    }

    expclass = basic_exporter_class('EResourcesToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.final_callback(vals=vals, status='success')

    for key in redis_obj.conn.keys():
        assert key in expected
        assert redis_obj(key).get() == expected[key]
