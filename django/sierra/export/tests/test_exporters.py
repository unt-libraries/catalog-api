"""
Tests classes derived from `export.exporter.Exporter`.
"""

import pytest

# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/base/tests/conftest.py:
#    sierra_records_by_recnum_range, sierra_full_object_set,
#    record_sets, new_exporter, process_records,
#    solr_assemble_specific_record_data,
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


@pytest.mark.exports
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
                            record_sets, new_exporter):
    """
    For Exporter classes that get data from Sierra, the `get_records`
    method should return a record set containing the expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_records()
    print exporter.prefetch_related
    assert len(db_records) > 0
    assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.deletions
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
def test_export_get_deletions(et_code, rset_code, basic_exporter_class,
                              record_sets, new_exporter):
    """
    For Exporter classes that get data from Sierra, the `get_deletions`
    method should return a record set containing the expected records.
    """
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    db_records = exporter.get_deletions()
    if rset_code is None:
        assert db_records == None
    else:
        assert all([rec in db_records for rec in record_sets[rset_code]])


@pytest.mark.exports
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToSolr', 'bib_set'),
    ('EResourcesToSolr', 'eres_set'),
    ('ItemsToSolr', 'item_set'),
    ('ItemStatusesToSolr', 'istatus_set'),
    ('ItypesToSolr', 'itype_set'),
    ('LocationsToSolr', 'location_set'),
])
def test_tosolr_export_records(et_code, rset_code, basic_exporter_class,
                               record_sets, new_exporter,
                               assert_all_exported_records_are_indexed):
    """
    For ToSolrExporter classes, the `export_records` method should load
    the expected records into the expected Solr index.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    exporter.commit_indexes()
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
@pytest.mark.parametrize('et_code, rset_code, rectypes', [
    ('BibsToSolr', 'bib_del_set', ('bib', 'marc')),
    ('EResourcesToSolr', 'eres_del_set', ('eresource',)),
    ('ItemsToSolr', 'item_del_set', ('item',)),
])
def test_tosolr_delete_records(et_code, rset_code, rectypes,
                               basic_exporter_class, record_sets, new_exporter,
                               solr_assemble_specific_record_data,
                               assert_records_are_indexed,
                               assert_deleted_records_are_not_indexed):
    """
    For ToSolrExporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
    """
    records = record_sets[rset_code]
    data = ({'id': r.id, 'record_number': r.get_iii_recnum()} for r in records)
    assembler = solr_assemble_specific_record_data(data, rectypes)
    
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    for index in exporter.indexes.values():
        assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    exporter.commit_indexes()
    assert_deleted_records_are_not_indexed(exporter, records)


def test_tosolr_index_update_errors(basic_exporter_class, record_sets,
                                    new_exporter, process_records,
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
    exporter.export_records(records)
    exporter.commit_indexes()

    assert_records_are_not_indexed(exporter.indexes['Items'], [records[0]])
    assert_records_are_indexed(exporter.indexes['Items'], records[1:])
    assert len(exporter.indexes['Items'].last_batch_errors) == 1


@pytest.mark.exports
@pytest.mark.parametrize('et_code, rset_code', [
    ('ItemsBibsToSolr', 'item_set'),
    ('BibsAndAttachedToSolr', 'bib_set'),
    ('BibsAndAttachedToSolr', 'er_bib_set')
])
def test_attached_solr_export_records(et_code, rset_code, basic_exporter_class,
                                      record_sets, new_exporter,
                                      assert_all_exported_records_are_indexed):
    """
    For AttachedRecordExporter classes that load data into Solr, the
    `export_records` method should load the expected records into the
    expected Solr indexes.
    """
    records = record_sets[rset_code]
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    for child in exporter.children.values():
        child.commit_indexes()
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
@pytest.mark.parametrize('et_code, rset_code, rectypes', [
    ('ItemsBibsToSolr', 'item_del_set', ('item',)),
    ('BibsAndAttachedToSolr', 'bib_del_set', ('bib', 'marc')),
])
def test_attached_solr_delete_records(et_code, rset_code, rectypes,
                                      basic_exporter_class, record_sets,
                                      new_exporter,
                                      solr_assemble_specific_record_data,
                                      assert_records_are_indexed,
                                      assert_deleted_records_are_not_indexed):
    """
    For Exporter classes that have loaded data into Solr, the
    `delete_records` method should delete records from the appropriate
    index or indexes.
    """
    records = record_sets[rset_code]
    data = ({'id': r.id, 'record_number': r.get_iii_recnum()} for r in records)
    assembler = solr_assemble_specific_record_data(data, rectypes)
    
    expclass = basic_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    for index in exporter.main_child.indexes.values():
        assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    for child in exporter.children.values():
        child.commit_indexes()
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
