"""
Tests shelflist-app classes derived from `export.exporter.Exporter`.
"""

import pytest
import importlib


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, new_exporter, get_records,
#    export_records, delete_records, derive_exporter_class,
#    assert_all_exported_records_are_indexed,
#    assert_deleted_records_are_not_indexed

pytestmark = pytest.mark.django_db


@pytest.fixture
def exporter_class(derive_exporter_class):
    def _exporter_class(name):
        return derive_exporter_class(name, 'shelflist.exporters')
    return _exporter_class


@pytest.fixture
def record_sets(sierra_records_by_recnum_range):
    return {
        'bib_set': sierra_records_by_recnum_range('b4371446'),
        'item_set': sierra_records_by_recnum_range('i4264281'),
    }


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
    For the compound exporter jobs we're testing in this test module,
    the main exporters themselves should be from the `export` app, but
    any ItemsToSolr children should be from the `shelflist` app.
    """
    expclass = exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'export'
    for child_etcode, child in exporter.children.items():
        if child_etcode == 'ItemsToSolr':
            assert child.app_name == 'shelflist'
        else:
            assert child.app_name == 'export'


@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsToSolr', 'item_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set')
])
def test_export_get_records(et_code, rset_code, exporter_class, record_sets,
                            new_exporter, get_records):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    expclass = exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = get_records(exporter)
    assert len(db_records) > 0
    assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsToSolr', 'item_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set')
])
def test_export_records_to_solr(et_code, rset_code, exporter_class,
                                record_sets, new_exporter, export_records,
                                assert_all_exported_records_are_indexed):
    """
    For Exporter classes that load data into Solr, the `export_records`
    method should load the expected records into the expected Solr
    index.
    """
    input_record_set = record_sets[rset_code]
    expclass = exporter_class(et_code)
    test_exporter = new_exporter(expclass, 'full_export', 'waiting')
    export_records(test_exporter, input_record_set)

    assert_all_exported_records_are_indexed(test_exporter, input_record_set)


@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsToSolr', 'item_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set')
])
def test_export_delete_records(et_code, rset_code, exporter_class, record_sets,
                               new_exporter, export_records, delete_records,
                               assert_deleted_records_are_not_indexed):
    """
    For Exporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
    """
    input_record_set = record_sets[rset_code]
    expclass = exporter_class(et_code)
    test_exporter = new_exporter(expclass, 'full_export', 'waiting')
    export_records(test_exporter, input_record_set)
    delete_records(test_exporter, input_record_set)

    assert_deleted_records_are_not_indexed(test_exporter, input_record_set)
