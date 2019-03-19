"""
Tests shelflist-app classes derived from `export.exporter.Exporter`.
"""

import pytest
import importlib


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, new_exporter, get_records,
#    export_records, delete_records, export_type,
#    configure_export_types, assert_all_exported_records_are_indexed,
#    assert_deleted_records_are_not_indexed

pytestmark = pytest.mark.django_db


@pytest.fixture(scope='module')
def shelflist_export_types(configure_export_types):
    return configure_export_types(['ItemsToSolr'], 'shelflist.exporters')


@pytest.fixture(scope='module')
def main_export_types(configure_export_types):
    etypes = ['ItemsBibsToSolr', 'BibsAndAttachedToSolr']
    return configure_export_types(etypes)


@pytest.fixture
def record_sets(sierra_records_by_recnum_range):
    return {
        'bib_set': sierra_records_by_recnum_range('b4371446'),
        'item_set': sierra_records_by_recnum_range('i4264281'),
    }


# TESTS

@pytest.mark.parametrize('et_code', [
    'ItemsToSolr',
    'ItemsBibsToSolr',
    'BibsAndAttachedToSolr'
])
def test_itemstosolr_version(et_code, new_exporter, shelflist_export_types,
                             main_export_types):
    """
    Make sure that the ItemsToSolr job is correctly overridden by the
    shelflist.ItemsToSolr job.
    """
    exporter = new_exporter(et_code, 'full_export', 'waiting')
    children = getattr(exporter, 'children', {}).values()
    exp_types = { et_code: type(exporter) }
    for child in children:
        exp_types[type(child).__name__] = type(child)
        for basetype in type(child).__bases__:
            exp_types[basetype.__name__] = basetype

    shelflist_exporters = importlib.import_module('shelflist.exporters')

    assert 'ItemsToSolr' in exp_types
    assert exp_types['ItemsToSolr'] == shelflist_exporters.ItemsToSolr


@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsToSolr', 'item_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set')
])
def test_export_get_records(et_code, rset_code, shelflist_export_types,
                            main_export_types, record_sets,
                            new_exporter, get_records):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    exporter = new_exporter(et_code, 'full_export', 'waiting')
    db_records = get_records(exporter)
    assert len(db_records) > 0
    assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsToSolr', 'item_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set')
])
def test_export_records_to_solr(et_code, rset_code, shelflist_export_types,
                                main_export_types, record_sets, new_exporter,
                                export_records,
                                assert_all_exported_records_are_indexed):
    """
    For Exporter classes that load data into Solr, the `export_records`
    method should load the expected records into the expected Solr
    index.
    """
    input_record_set = record_sets[rset_code]
    test_exporter = new_exporter(et_code, 'full_export', 'waiting')
    export_records(test_exporter, input_record_set)

    assert_all_exported_records_are_indexed(test_exporter, input_record_set)


@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsToSolr', 'item_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set')
])
def test_export_delete_records(et_code, rset_code, shelflist_export_types,
                               main_export_types, record_sets, new_exporter,
                               export_records, delete_records,
                               assert_deleted_records_are_not_indexed):
    """
    For Exporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
    """
    input_record_set = record_sets[rset_code]
    test_exporter = new_exporter(et_code, 'full_export', 'waiting')
    export_records(test_exporter, input_record_set)
    delete_records(test_exporter, input_record_set)

    assert_deleted_records_are_not_indexed(test_exporter, input_record_set)
