"""
Tests the `blacklight.exporters_alpha_solrmarc` classes.
"""

import pytest


# FIXTURES AND TEST DATA
# Fixtures used in the below tests can be found in
# django/sierra/conftest.py:
#    derive_exporter_class, record_sets, new_exporter, solr_conns,
#    solr_search, assert_records_are_indexed,
#    assert_records_are_not_indexed,
#    assert_all_exported_records_are_indexed,
#    assert_deleted_records_are_not_indexed, basic_solr_assembler
#
# django/sierra/blacklight/tests/conftest.py:
#    bl_solr_assembler

pytestmark = pytest.mark.django_db


@pytest.fixture
def asm_exporter_class(derive_exporter_class):
    def _asm_exporter_class(name):
        modpath = 'blacklight.exporters_alpha_solrmarc'
        return derive_exporter_class(name, modpath)
    return _asm_exporter_class


@pytest.fixture
def do_commit():
    """
    Pytest fixture. Ensures all indexes for all [grand]children run
    their `commit_indexes` method to commit changes to Solr.
    """
    def _do_commit(exporter):
        if hasattr(exporter, 'commit_indexes'):
            exporter.commit_indexes()
        for child in getattr(exporter, 'children', {}).values():
            _do_commit(child)
    return _do_commit


# TESTS

@pytest.mark.parametrize('et_code', [
    'BibsToAlphaSolrmarc',
    'BibsToAlphaSmAndAttachedToSolr'
])
def test_exporter_class_versions(et_code, new_exporter, asm_exporter_class):
    """
    For all exporter types / classes that are under test in this test
    module, what we get from the `asm_exporter_class` fixture should be
    derived from the `blacklight` app.
    """
    expclass = asm_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    assert exporter.app_name == 'blacklight'


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToAlphaSolrmarc', 'bib_set'),
    ('BibsToAlphaSmAndAttachedToSolr', 'bib_set')
])
def test_asm_export_get_records(et_code, rset_code, asm_exporter_class,
                                record_sets, new_exporter):
    """
    The `get_records` method for alpha-solrmarc exporters should return
    the expected recordset.
    """
    qset = record_sets[rset_code].order_by('pk')
    expected_recs = [r for r in qset]

    opts = {}
    start_rnum = expected_recs[0].record_metadata.get_iii_recnum(False)
    end_rnum = expected_recs[-1].record_metadata.get_iii_recnum(False)
    opts = {'record_range_from': start_rnum, 'record_range_to': end_rnum}

    expclass = asm_exporter_class(et_code)
    exporter = new_exporter(expclass, 'record_range', 'waiting', options=opts)
    records = exporter.get_records()

    assert set(records) == set(expected_recs)


@pytest.mark.deletions
@pytest.mark.get_records
@pytest.mark.parametrize('et_code, rset_code', [
    ('BibsToAlphaSolrmarc', 'bib_del_set'),
    ('BibsToAlphaSmAndAttachedToSolr', 'bib_del_set')
])
def test_asm_export_get_deletions(et_code, rset_code, asm_exporter_class,
                                  record_sets, new_exporter):
    """
    The `get_deletions` method for alpha-solrmarc exporters should
    return a record set containing the expected records.
    """
    expclass = asm_exporter_class(et_code)
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    records = exporter.get_deletions()
    assert set(records) == set(record_sets[rset_code])


@pytest.mark.exports
@pytest.mark.do_export
def test_bibstoasm_export_records(asm_exporter_class, record_sets,
                                  new_exporter, solr_conns, solr_search,
                                  bl_solr_assembler,
                                  assert_records_are_indexed,
                                  assert_records_are_not_indexed):
    """
    The BibsToAlphaSolrmarc `export_records` method should load the
    expected records into the expected Solr index. This uses the
    `solr_assemble_specific_record_data` fixture to help preload some
    data into Solr. This exporter should add records to the existing
    recordset.
    """
    expclass = asm_exporter_class('BibsToAlphaSolrmarc')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    records = record_sets['bib_set']
    
    # Do some setup to put some meaningful data into the index first.
    # We want some records that overlap with the incoming record set
    # and some that don't.
    num_existing = records.count() / 2
    overlap_recs = records[0:num_existing]
    only_new_recs = records[num_existing:]
    old_rec_pks = [unicode(pk) for pk in range(99991,99995)]
    only_old_rec_data = [(pk, {}) for pk in old_rec_pks]

    overlap_rec_data = []
    for r in overlap_recs:
        overlap_rec_data.append((r.record_metadata.get_iii_recnum(False), {}))

    data = only_old_rec_data + overlap_rec_data
    bl_solr_assembler.load_static_test_data('alphasolrmarc', data,
                                            id_field='id')

    # Check the setup to make sure existing records are indexed and new
    # records are not.
    for index in exporter.indexes.values():
        conn = solr_conns[getattr(index, 'using', 'default')]
        results = solr_search(conn, '*')
        only_old_recs = [r for r in results if r['id'] in old_rec_pks]
        assert len(only_old_recs) == len(old_rec_pks)
        assert_records_are_indexed(index, overlap_recs, results=results)
        assert_records_are_not_indexed(index, only_new_recs, results=results)

    exporter.export_records(records)
    exporter.commit_indexes()

    for i, index in enumerate(exporter.indexes.values()):
        conn = solr_conns[getattr(index, 'using', 'default')]
        results = solr_search(conn, '*')
        only_old_recs = [r for r in results if r['id'] in old_rec_pks]
        assert len(only_old_recs) == len(old_rec_pks)
        assert_records_are_indexed(index, overlap_recs, results=results)
        assert_records_are_indexed(index, only_new_recs, results=results)


@pytest.mark.deletions
@pytest.mark.do_export
def test_bibstoasm_delete_records(asm_exporter_class, record_sets,
                                  new_exporter, bl_solr_assembler,
                                  assert_records_are_indexed,
                                  assert_deleted_records_are_not_indexed):
    """
    The BibsToAlphaSolrmarc  `delete_records` method should delete
    records from the appropriate index or indexes.
    """
    records = record_sets['bib_del_set']
    data = [(r.get_iii_recnum(False), {}) for r in records]
    bl_solr_assembler.load_static_test_data('alphasolrmarc', data,
                                            id_field='id')
    
    expclass = asm_exporter_class('BibsToAlphaSolrmarc')
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    for index in exporter.indexes.values():
        assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    exporter.commit_indexes()
    assert_deleted_records_are_not_indexed(exporter, records)


@pytest.mark.exports
@pytest.mark.do_export
def test_attachedtoasm_export_records(asm_exporter_class, do_commit,
                                      record_sets, new_exporter,
                                      assert_all_exported_records_are_indexed):
    """
    The BibsToAlphaSmAndAttachedToSolr `export_records` method should
    load the expected records into the expected Solr indexes. This is
    just a simple check to make sure all child exporters processed the
    appropriate recordsets; the children are tested more extensively
    elsewhere.
    """
    records = record_sets['bib_set']
    expclass = asm_exporter_class('BibsToAlphaSmAndAttachedToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(records)
    do_commit(exporter)
    assert_all_exported_records_are_indexed(exporter, records)


@pytest.mark.deletions
@pytest.mark.do_export
def test_attachedtoasm_delete_records(asm_exporter_class, do_commit,
                                      record_sets, new_exporter,
                                      basic_solr_assembler,
                                      bl_solr_assembler,
                                      assert_records_are_indexed,
                                      assert_deleted_records_are_not_indexed):
    """
    The BibsToAlphaSmAndAttachedToSolr `delete_records` method should
    delete records from the appropriate indexes.
    """
    records = record_sets['bib_del_set']
    ams_data = [(r.get_iii_recnum(False), {}) for r in records]
    bib_data = [(r.id, {'record_number': r.get_iii_recnum()}) for r in records]
    bl_solr_assembler.load_static_test_data('alphasolrmarc', ams_data)
    basic_solr_assembler.load_static_test_data('bib', bib_data)
    basic_solr_assembler.load_static_test_data('marc', bib_data)
    
    expclass = asm_exporter_class('BibsToAlphaSmAndAttachedToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    children_w_indexes = [
        exporter.children['BibsToAlphaSolrmarc'],
        exporter.children['BibsAndAttachedToSolr'].main_child
    ]

    for child in children_w_indexes:
        for index in child.indexes.values():
            assert_records_are_indexed(index, records)

    exporter.delete_records(records)
    do_commit(exporter)
    for child in exporter.children.values():
        assert_deleted_records_are_not_indexed(child, records)
