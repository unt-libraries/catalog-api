"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    new_exporter, get_records, export_records, delete_records,
#    setattr_model_instance, derive_exporter_class
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
def record_sets(sierra_records_by_recnum_range, sierra_full_object_set):
    return {
        'bib_set': sierra_records_by_recnum_range('b4371446'),
        'er_bib_set': sierra_records_by_recnum_range('b5784429'),
        'eres_set': sierra_records_by_recnum_range('e1001249'),
        'item_set': sierra_records_by_recnum_range('i4264281', 'i4278316'),
        'itype_set': sierra_full_object_set('ItypeProperty'),
        'istatus_set': sierra_full_object_set('ItemStatusProperty'),
        'location_set': sierra_full_object_set('Location')
    }


# TESTS

@pytest.mark.parametrize('et_code', [
    ('BibsToSolr'),
    ('EResourcesToSolr'),
    ('ItemsToSolr'),
    ('ItemStatusesToSolr'),
    ('ItypesToSolr'),
    ('LocationsToSolr'),
    ('ItemsBibsToSolr'),
    ('BibsAndAttachedToSolr')
])
def test_exporter_class_versions(et_code, new_exporter, basic_exporter_class):
    """
    For all exporter types / classes that are under test in this test
    module, what we get from the `basic_exporter_class` fixture should
    be derived from the `export` app.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'export'
    for child_etcode, child in getattr(exporter, 'children', {}).items():
        assert child.app_name == 'export'


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
def test_export_get_records(et_code, rset_code, basic_exporter_class,
                            record_sets, new_exporter, get_records):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
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
def test_exports_to_solr(et_code, rset_code, basic_exporter_class, record_sets,
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
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    export_records(exporter, input_record_set)

    assert_all_exported_records_are_indexed(exporter, input_record_set)


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
def test_export_delete_records(et_code, rset_code, basic_exporter_class,
                               record_sets, new_exporter, export_records,
                               delete_records,
                               assert_deleted_records_are_not_indexed):
    """
    For Exporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
    """
    input_record_set = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    export_records(exporter, input_record_set)
    delete_records(exporter, input_record_set)

    assert_deleted_records_are_not_indexed(exporter, input_record_set)


def test_tosolrexporter_index_update_errors(basic_exporter_class, record_sets,
                                            new_exporter, export_records,
                                            setattr_model_instance,
                                            assert_records_are_indexed,
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
    export_records(exporter, records)

    assert_records_are_not_indexed(exporter.indexes['Items'],
                                   [records[0]])
    assert_records_are_indexed(exporter.indexes['Items'], records[1:])
    assert len(exporter.indexes['Items'].last_batch_errors) == 1

