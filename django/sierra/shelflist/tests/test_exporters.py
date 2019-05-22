"""
Tests shelflist-app classes derived from `export.exporter.Exporter`.
"""

import pytest
import importlib
import random


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, new_exporter,
#    record_sets, export_records, delete_records,
#    derive_exporter_class, assert_all_exported_records_are_indexed,
#    assert_deleted_records_are_not_indexed

pytestmark = pytest.mark.django_db


@pytest.fixture
def exporter_class(derive_exporter_class):
    def _exporter_class(name):
        return derive_exporter_class(name, 'shelflist.exporters')
    return _exporter_class


# TESTS

def test_main_itemstosolr_version(new_exporter, exporter_class):
    """
    Make sure that the main ItemsToSolr job we're testing is from the
    shelflist app, not the export app.
    """
    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'shelflist'


@pytest.mark.parametrize('et_code', [
    'ItemsBibsToSolr',
    'BibsAndAttachedToSolr'
])
def test_child_itemstosolr_versions(et_code, new_exporter, exporter_class):
    """
    Compound exporters from the main `export` app should use the
    ItemsToSolr from the `shelflist` app.
    """
    expclass = exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'export'
    for child_etcode, child in exporter.children.items():
        if child_etcode == 'ItemsToSolr':
            assert child.app_name == 'shelflist'
        else:
            assert child.app_name == 'export'


@pytest.mark.exports
def test_itemstosolr_get_records(exporter_class, record_sets, new_exporter):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_records()
    assert len(db_records) > 0
    assert all([rec in db_records for rec in record_sets['item_set']])


@pytest.mark.deletions
def test_itemstosolr_get_deletions(exporter_class, record_sets, new_exporter):
    """
    For Exporter classes that get data from Sierra, the `get_deletions`
    method should return a record set containing the expected records.
    """
    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_deletions()
    assert all([rec in db_records for rec in record_sets['item_del_set']])


@pytest.mark.exports
def test_itemstosolr_records_to_solr(exporter_class, record_sets, new_exporter,
                                     assert_all_exported_records_are_indexed):
    """
    The shelflist app ItemsToSolr `export_records` method should load
    the expected records into the expected Solr index.
    """
    records = record_sets['item_set']
    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    exporter.commit_indexes()
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
def test_itemstosolr_delete_records(exporter_class, record_sets, new_exporter,
                                    solr_assemble_specific_record_data,
                                    assert_records_are_indexed,
                                    assert_deleted_records_are_not_indexed):
    """
    The shelflist app ItemsToSolr `delete_records` method should delete
    records from the appropriate index or indexes.
    """
    records = record_sets['item_del_set']
    data = ({'id': r.id, 'record_number': r.get_iii_recnum()} for r in records)
    assembler = solr_assemble_specific_record_data(data, ('item',))
    
    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    for index in exporter.indexes.values():
        assert_records_are_indexed(index, records)
    exporter.delete_records(records)
    exporter.commit_indexes()
    assert_deleted_records_are_not_indexed(exporter, records)


@pytest.mark.shelflist
@pytest.mark.exports
@pytest.mark.return_vals
def test_itemstosolr_export_returns_lcodes(exporter_class,
                                           sierra_full_object_set,
                                           new_exporter,
                                           setattr_model_instance):
    """
    The shelflist app ItemsToSolr `export_records` method should return
    a vals structure containing a `seen_lcodes` list, or list of unique
    locations that appear in the record set.
    """
    lcode_opts = ['czm', 'r', 'sd', 'lwww']
    expected_lcodes = set()
    records = sierra_full_object_set('ItemRecord').order_by('pk')[0:20]
    for rec in records:
        lcode = random.choice(lcode_opts)
        expected_lcodes.add(lcode)
        setattr_model_instance(rec, 'location_id', lcode)

    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    vals = exporter.export_records(records)
    assert vals['seen_lcodes'] == expected_lcodes 


@pytest.mark.shelflist
@pytest.mark.deletions
@pytest.mark.return_vals
def test_itemstosolr_delete_returns_lcodes(exporter_class,
                                           sierra_full_object_set,
                                           new_exporter,
                                           solr_assemble_specific_record_data):
    """
    The shelflist app ItemsToSolr `delete_records` method should return
    a vals structure containing a `seen_lcodes` list, or list of unique
    locations that appear(ed) in the record set.
    """
    # Set up existing data to be deleted.
    records = sierra_full_object_set('RecordMetadata')
    records = records.filter(record_type_id='i').order_by('pk')[0:20]

    data, lcode_opts, expected_lcodes = [], ['czm', 'r', 'sd', 'lwww'], set()
    for rec in records:
        lcode = random.choice(lcode_opts)
        expected_lcodes.add(lcode)
        data.append({'id': rec.id, 'record_number': rec.get_iii_recnum(),
                     'location_code': lcode})
    assembler = solr_assemble_specific_record_data(data, ('item',))

    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    vals = exporter.delete_records(records)
    assert vals['seen_lcodes'] == expected_lcodes


@pytest.mark.shelflist
@pytest.mark.return_vals
def test_itemstosolr_compile_vals(exporter_class, new_exporter):
    """
    The shelflist app ItemsToSolr `compile_vals` method should return a
    vals structure that joins `seen_lcodes` sets from each result in
    the provided results.
    """
    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    results = [{'seen_lcodes': set(['czm', 'r', 'w4m'])},
               {'seen_lcodes': set(['czm', 'w4m', 'sd', 'xdoc', 'lwww'])}]
    expected = {'seen_lcodes': set(['czm', 'r', 'w4m', 'sd', 'xdoc', 'lwww'])}
    assert exporter.compile_vals(results) == expected


@pytest.mark.shelflist
@pytest.mark.callback
def test_itemstosolr_shelflist_manifests(exporter_class, new_exporter,
                                         solr_assemble_specific_record_data,
                                         redis_obj):
    """
    The shelflist app ItemsToSolr `final_callback` method should build
    or rebuild the shelflist manifest for each location provided in the
    vals['seen_lcode'] set. Each shelflist manifest should be saved in
    Redis.
    """
    expclass = exporter_class('ItemsToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    # Define some completely fake item data that makes it easy to do
    # sorting without getting caught up in details about sorting
    # call numbers. For this test we don't care about call number
    # sorting. The tuples in the below dict represent the Solr fields
    # `id`, `call_number_sort`, `volume_sort`, and `copy_number`.
    # When manifests are created, items should be sorted in that order.
    items = {
        '1_1_1': (1, '1', '1', 1),
        '1_1_2': (2, '1', '1', 2),
        '1_2_1': (3, '1', '2', 1),
        '1_2_2': (4, '1', '2', 2),
        '2_1_1': (5, '2', '1', 1),
        '2_1_2': (6, '2', '1', 2),
        '2_1_3': (7, '2', '1', 3),
        '3_1_1': (8, '3', '1', 1),
        '3_1_2': (9, '3', '1', 2),
        '3_1_3': (10, '3', '1', 3),
        '3_2_1': (11, '3', '2', 1),
        '3_2_2': (12, '3', '2', 2),
        '4_1_1': (13, '4', '1', 1),
        '4_2_1': (14, '4', '2', 1),
    }

    # Add some pre-existing shelflistitem-manifest data to Redis.
    existing_shelflists = {
        'w3': [items['1_1_1'], items['1_2_1'], items['1_2_2']],
        'w4m': [items['3_1_2'], items['3_2_1'], items['3_2_2']],
        'x': [items['4_1_1'], items['4_2_1']]
    }
    for lcode, item_data in existing_shelflists.items():
        key = '{}:{}'.format(expclass.redis_shelflist_prefix, lcode)
        redis_obj(key).set([i[0] for i in item_data])

    # Now simulate data that's been loaded into Solr that's updated one
    # or more existing shelflists.
    updated_shelflists = {
        'w2': [items['1_1_1'], items['1_1_2'], items['2_1_1'], items['2_1_2']],
        'w3': [items['1_2_1'], items['1_2_2'], items['2_1_3'], items['3_2_2']],
        'w4m': [items['3_1_1'], items['3_1_2'], items['3_2_1']]
    }
    solr_data = []
    for lcode, slist in updated_shelflists.items():
        for pk, cn, vol, copy in slist:
            solr_data.append({'id': pk, 'location_code': lcode,
                             'call_number_sort': cn, 'volume_sort': vol,
                             'copy_number': copy, 'call_number_type': 'lc'})
    assembler = solr_assemble_specific_record_data(solr_data, ('item',))

    # Run `final_callback`, passing the appropriate location codes to
    # update shelflistitem manifests for via vals['seen_lcodes'].
    vals = {'seen_lcodes': set(updated_shelflists.keys())}
    exporter.final_callback(vals=vals, status='success')

    # Now check the results. Updated shelflists should be stored in
    # Redis; any existing ones that weren't in `updated_shelflists`
    # should still be in Redis.
    for key in redis_obj.conn.keys():
        lcode = key.split(':')[1]
        if lcode in updated_shelflists:
            expected_shelflist = [i[0] for i in updated_shelflists[lcode]]
        else:
            expected_shelflist = [i[0] for i in existing_shelflists[lcode]]
        assert redis_obj(key).get() == expected_shelflist
