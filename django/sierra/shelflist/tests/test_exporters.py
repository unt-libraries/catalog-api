"""
Tests shelflist-app classes derived from `export.exporter.Exporter`.
"""

import pytest

from shelflist.exporters import ItemsToSolr


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    new_exporter, get_records, export_records, delete_records,
#    solr_conn, solr_search, configure_export_type_classpaths

pytestmark = pytest.mark.django_db


@pytest.fixture(scope='module')
def do_export_type_cfg(configure_export_type_classpaths):
    mapping = {'ItemsToSolr': 'shelflist.exporters.ItemsToSolr'}
    configure_export_type_classpaths(mapping)


@pytest.fixture
def solr_exporter_test_params(sierra_records_by_recnum_range,
                              sierra_full_object_set):
    bib_set = sierra_records_by_recnum_range('b4371446')
    item_set = sierra_records_by_recnum_range('i4264281')

    return {
        'ItemsToSolr': {
            'export_type': 'ItemsToSolr',
            'record_set': item_set
        },
        'ItemsBibsToSolr': {
            'export_type': 'ItemsBibsToSolr',
            'record_set': item_set
        },
        'BibsAndAttachedToSolr': {
            'export_type': 'BibsAndAttachedToSolr',
            'record_set': bib_set
        },
    }


# TESTS

@pytest.mark.parametrize('test_id', [
    'ItemsToSolr',
    'ItemsBibsToSolr',
    'BibsAndAttachedToSolr'])
def test_itemstosolr_version(test_id, solr_exporter_test_params, new_exporter,
                             do_export_type_cfg):
    """
    Make sure that the ItemsToSolr job is correctly overridden by the
    shelflist.ItemsToSolr job.
    """
    test_params = solr_exporter_test_params[test_id]
    export_type = test_params['export_type']
    exporter = new_exporter(export_type, 'full_export', 'waiting')

    children = getattr(exporter, 'children', {}).values()
    exp_types = { export_type: type(exporter) }
    for child in children:
        exp_types[type(child).__name__] = type(child)
        for basetype in type(child).__bases__:
            exp_types[basetype.__name__] = basetype

    assert 'ItemsToSolr' in exp_types
    assert exp_types['ItemsToSolr'] == ItemsToSolr


@pytest.mark.parametrize('test_id', [
    'ItemsToSolr',
    'ItemsBibsToSolr',
    'BibsAndAttachedToSolr'])
def test_export_get_records(test_id, solr_exporter_test_params,
                            new_exporter, get_records, do_export_type_cfg):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    test_params = solr_exporter_test_params[test_id]
    export_type = test_params['export_type']
    exporter = new_exporter(export_type, 'full_export', 'waiting')
    db_records = get_records(exporter)
    expected_records = test_params['record_set']
    assert len(db_records) > 0
    assert all([rec in db_records for rec in expected_records])


@pytest.mark.parametrize('test_id', [
    'ItemsToSolr',
    'ItemsBibsToSolr',
    'BibsAndAttachedToSolr'])
def test_exports_to_solr(test_id, solr_exporter_test_params, new_exporter,
                         export_records, delete_records, solr_conns,
                         solr_search, assert_records_are_indexed,
                         do_export_type_cfg):
    """
    For Exporter classes that load data into Solr, the `export_records`
    method should load the expected records into the expected Solr
    index. If the Exporter is configured to delete records, then the
    `delete_records` method should remove the expected records from the
    applicable index(es). 
    """
    # Get test parameters from the `solr_exporter_test_params` fixture.
    test_params = solr_exporter_test_params[test_id]
    export_type_name = test_params['export_type']
    input_record_set = test_params['record_set']

    # Run the test: create the parent test_exporter and run the
    # `export_records` job.
    test_exporter = new_exporter(export_type_name, 'full_export', 'waiting')
    export_records(test_exporter, input_record_set)

    # Check results in all indexes for the parent test_exporter.
    test_indexes = getattr(test_exporter, 'indexes', {}).values()
    for index in test_indexes:
        assert_records_are_indexed(index, input_record_set)

    # Check results in all child indexes (if any).
    if hasattr(test_exporter, 'children'):
        child_rsets = test_exporter.generate_record_sets(input_record_set)
        for child_name, child in test_exporter.children.items():
            child_indexes = getattr(child, 'indexes', {}).values()
            for index in child_indexes:
                assert_records_are_indexed(index, child_rsets[child_name])

    # Try deleting the records.
    del_exporter = new_exporter(export_type_name, 'full_export', 'waiting')
    delete_records(del_exporter, input_record_set)
    for index in test_indexes:
        conn = solr_conns[getattr(index, 'using', 'default')]
        assert len(solr_search(conn, '*')) == 0
