"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    new_exporter, get_records, export_records, delete_records,
#    export_type, configure_export_types,
#    assert_all_exported_records_are_indexed,
#    assert_deleted_records_are_not_indexed

pytestmark = pytest.mark.django_db


@pytest.fixture(scope='module')
def basic_export_types(configure_export_types):
    basic_types = [
        'BibsToSolr', 'ItemsToSolr', 'EResourcesToSolr', 'ItemStatusesToSolr',
        'ItypesToSolr', 'LocationsToSolr', 'ItemsBibsToSolr',
        'BibsAndAttachedToSolr'
    ]
    return configure_export_types(basic_types, 'export.basic_exporters')


@pytest.fixture
def record_sets(sierra_records_by_recnum_range, sierra_full_object_set):
    return {
        'bib_set': sierra_records_by_recnum_range('b4371446'),
        'er_bib_set': sierra_records_by_recnum_range('b5784429'),
        'eres_set': sierra_records_by_recnum_range('e1001249'),
        'item_set': sierra_records_by_recnum_range('i4264281'),
        'itype_set': sierra_full_object_set('ItypeProperty'),
        'istatus_set': sierra_full_object_set('ItemStatusProperty'),
        'location_set': sierra_full_object_set('Location')
    }


# TESTS

@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_set'),
    ('EResourcesToSolr', 'eres_set'),
    ('ItemsToSolr', 'item_set'),
    ('ItemStatusesToSolr', 'istatus_set'),
    ('ItypesToSolr', 'itype_set'),
    ('LocationsToSolr', 'location_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set')
])
def test_export_get_records(et_code, rset_code, basic_export_types,
                            record_sets, new_exporter, get_records):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    exporter = new_exporter(et_code, 'full_export', 'waiting')
    db_records = get_records(exporter)
    assert len(db_records) > 0
    assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_set'),
    ('EResourcesToSolr', 'eres_set'),
    ('ItemsToSolr', 'item_set'),
    ('ItemStatusesToSolr', 'istatus_set'),
    ('ItypesToSolr', 'itype_set'),
    ('LocationsToSolr', 'location_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set')
])
def test_exports_to_solr(et_code, rset_code, basic_export_types, record_sets,
                         new_exporter, export_records,
                         assert_all_exported_records_are_indexed):
    """
    For Exporter classes that load data into Solr, the `export_records`
    method should load the expected records into the expected Solr
    index. If the Exporter is configured to delete records, then the
    `delete_records` method should remove the expected records from the
    applicable index(es). 
    """
    input_record_set = record_sets[rset_code]
    test_exporter = new_exporter(et_code, 'full_export', 'waiting')
    export_records(test_exporter, input_record_set)

    assert_all_exported_records_are_indexed(test_exporter, input_record_set)


@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_set'),
    ('EResourcesToSolr', 'eres_set'),
    ('ItemsToSolr', 'item_set'),
    ('ItemStatusesToSolr', 'istatus_set'),
    ('ItypesToSolr', 'itype_set'),
    ('LocationsToSolr', 'location_set'),
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set')
])
def test_export_delete_records(et_code, rset_code, basic_export_types,
                               record_sets, new_exporter, export_records,
                               delete_records,
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
