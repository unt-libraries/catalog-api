"""
Tests the `blacklight.exporters_alpha_solrmarc` classes.
"""

import pytest
from datetime import datetime
import pytz
import random

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
    'BibsToAlphaSmAndAttachedToSolr',
    # 'BuildAlphaSolrmarcSuggest'
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
def test_asm_export_get_records_rn_range(et_code, rset_code, asm_exporter_class,
                                         record_sets, new_exporter):
    """
    The `get_records` method for alpha-solrmarc exporters should return
    the expected recordset, when using the `record_range` filter type.
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


@pytest.mark.exports
@pytest.mark.get_records
@pytest.mark.parametrize('et_code, rstart, rend, bdate, idates, expected', [
    ('BibsToAlphaSolrmarc', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], True),
    ('BibsToAlphaSolrmarc', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], True),
    ('BibsToAlphaSolrmarc', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], True),
    ('BibsToAlphaSolrmarc', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], True),
    ('BibsToAlphaSolrmarc', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False),
    ('BibsToAlphaSmAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2019, 9, 1), (2020, 8, 13)], True),
    ('BibsToAlphaSmAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2020, 8, 15), [(2020, 8, 15), (2020, 8, 15)], True),
    ('BibsToAlphaSmAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 13)], True),
    ('BibsToAlphaSmAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 15), (2020, 8, 15)], True),
    ('BibsToAlphaSmAndAttachedToSolr', (2020, 8, 14), (2020, 8, 16),
     (2019, 9, 1), [(2020, 8, 13), (2019, 8, 15)], False),
])
def test_asm_export_get_records_updated_range(et_code, rstart, rend, bdate,
                                              idates, expected,
                                              bl_sierra_test_record,
                                              setattr_model_instance,
                                              add_items_to_bib,
                                              asm_exporter_class, new_exporter):
    """
    The `get_records` method for alpha-solrmarc exporters should return
    the expected recordset, when using the `updated_date_range` filter
    type.
    """
    bib = bl_sierra_test_record('bib_no_items')
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

    expclass = asm_exporter_class(et_code)
    exp = new_exporter(expclass, 'updated_date_range', 'waiting', options={
        'date_range_from': datetime(*rstart, tzinfo=pytz.utc),
        'date_range_to': datetime(*rend, tzinfo=pytz.utc)
    })
    assert (bib.pk in [r.pk for r in exp.get_records()]) == expected
    


@pytest.mark.exports
@pytest.mark.get_records
def disabled_test_buildsuggest_get_records(asm_exporter_class, new_exporter,
                                           bl_solr_assembler):
    """
    THIS TEST IS CURRENTLY DISABLED
    The `get_records` method for the `BuildAlphaSolrmarcSuggest`
    exporter should return the expected recordset. Currently the
    `full_export` and `last_export` filter types are the only ones that
    are supported, so both are tested.
    """
    expclass = asm_exporter_class('BuildAlphaSolrmarcSuggest')

    gen_a = bl_solr_assembler.gen_factory.type('date', mx=(2015, 1, 1, 0, 0))
    recs_a = bl_solr_assembler.make('alphasolrmarc', 10,
                                    timestamp_of_last_solr_update=gen_a)
    bl_solr_assembler.save('alphasolrmarc')
    full_exp = new_exporter(expclass, 'full_export', 'success', {})
    full_exp.instance.timestamp = datetime(2015, 1, 1, 12, 0, tzinfo=pytz.utc)
    full_exp.instance.save()
    full_recs = full_exp.get_records()
    assert set([r['id'] for r in full_recs]) == set([r['id'] for r in recs_a])
    
    gen_b = bl_solr_assembler.gen_factory.type('date', mn=(2015, 1, 2, 0, 0))
    recs_b = bl_solr_assembler.make('alphasolrmarc', 10,
                                    timestamp_of_last_solr_update=gen_b)
    bl_solr_assembler.save('alphasolrmarc')
    last_exp = new_exporter(expclass, 'last_export', 'waiting', options={})
    last_recs = last_exp.get_records()
    assert set([r['id'] for r in last_recs]) == set([r['id'] for r in recs_b])


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


@pytest.mark.deletions
@pytest.mark.get_records
def disabled_test_buildsuggest_get_deletions(asm_exporter_class, new_exporter,
                                             bl_solr_assembler):
    """
    THIS TEST IS CURRENTLY DISABLED
    The `get_deletions` method for the `BuildAlphaSolrmarcSuggest`
    exporter should return None, no matter what. (Deletions aren't
    handled this way for this exporter.)
    """
    expclass = asm_exporter_class('BuildAlphaSolrmarcSuggest')
    exporter = new_exporter(expclass, 'full_export', 'success', {})
    bl_solr_assembler.make('alphasolrmarc', 10)
    bl_solr_assembler.save('alphasolrmarc')
    assert exporter.get_deletions() is None


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
    recordset. Additionally, for any existing records that are updated,
    a stub of the old version of the record should be created and
    suppressed.
    """
    expclass = asm_exporter_class('BibsToAlphaSolrmarc')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    records = record_sets['bib_set']
    
    # Do some setup to put some meaningful data into the index first.
    # We want some records that overlap with the incoming record set
    # and some that don't.
    num_existing = records.count() / 2
    overlap_recs = records[0:num_existing]
    overlap_rec_pks = [r.pk for r in overlap_recs]
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
    index = exporter.indexes['Bibs']
    conn = solr_conns[getattr(index, 'using', 'default')]
    results = solr_search(conn, '*')
    only_old_recs = [r for r in results if r['id'] in old_rec_pks]
    assert len(only_old_recs) == len(old_rec_pks)
    assert_records_are_indexed(index, overlap_recs, results=results)
    assert_records_are_not_indexed(index, only_new_recs, results=results)

    suprecs = ['{}_{}'.format(r['id'], r['_version_']) for r in results
               if r['id'] in overlap_rec_pks]

    exporter.export_records(records)
    exporter.commit_indexes()

    conn = solr_conns[getattr(index, 'using', 'default')]
    results = solr_search(conn, '*')
    rdict = {r['id']: r for r in results}
    only_old_recs = [r for r in results if r['id'] in old_rec_pks]
    assert len(only_old_recs) == len(old_rec_pks)
    assert_records_are_indexed(index, overlap_recs, results=results)
    assert_records_are_indexed(index, only_new_recs, results=results)
    for supkey in suprecs:
        assert rdict[supkey]['suppressed']
        assert len(rdict[supkey]) == 2


@pytest.mark.exports
@pytest.mark.do_export
def disabled_test_buildsuggest_export_records(asm_exporter_class,
                                              new_exporter, solr_conns,
                                              solr_search, bl_solr_assembler):
    """
    THIS TEST IS CURRENTLY DISABLED
    The `BuildAlphaSolrmarcSuggest` exporter should use the "suggest"
    index `builder_class` object to construct a set of suggest records
    to load into Solr. For headings where all records that have that
    heading are suppressed, the heading should not be included in the
    index.
    """
    bad_creator = 'Smith, First'
    good_creators = ['Jones, Second', 'Thompson, Third', 'Donovan, Fourth']
    creator_choices = [bad_creator] + good_creators
    creator_gen = bl_solr_assembler.gen_factory.choice(creator_choices)

    def suppressed_gen(record):
        if record['creator'] == bad_creator:
            return True
        return random.choice([False, False, True])

    recs = bl_solr_assembler.make('alphasolrmarc', 50, creator=creator_gen,
                                  suppressed=suppressed_gen)
    bl_solr_assembler.save('alphasolrmarc')

    expclass = asm_exporter_class('BuildAlphaSolrmarcSuggest')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    exporter.export_records(exporter.get_records())
    exporter.commit_indexes()

    index = exporter.indexes['suggest']
    builder = index.builder_class()
    srecs = builder.extract_suggest_recs(recs)

    there = [r for r in srecs if r['record_count'] > 0]
    not_there = [r for r in srecs if r['record_count'] == 0]

    conn = solr_conns[getattr(index, 'using', 'default')]
    results = solr_search(conn, '*')
    rdict = {r['id']: r for r in results}
    authors = {r['heading_display']: r for r in results
               if r['heading_type'] == 'author'}

    assert len(there) == len(rdict.keys())

    for srec in there:
        assert srec['id'] in rdict
        assert srec['heading'] == rdict[srec['id']]['heading']

    for srec in not_there:
        assert srec['id'] not in rdict

    assert bad_creator not in authors

    for creator in good_creators:
        assert creator in authors
        assert authors[creator]['record_count'] > 1


@pytest.mark.deletions
@pytest.mark.do_export
def test_bibstoasm_delete_records(asm_exporter_class, record_sets,
                                  new_exporter, bl_solr_assembler, solr_conns,
                                  solr_search, assert_records_are_indexed,
                                  assert_deleted_records_are_not_indexed,
                                  get_records_from_index):
    """
    The BibsToAlphaSolrmarc  `delete_records` method should, for each
    record to be deleted: 1) create a stub copy of the record and
    suppress it, and 2) delete it from the appropriate index(es).
    """
    records = record_sets['bib_del_set']
    data = [(r.get_iii_recnum(False), {'suppressed': False}) for r in records]
    bl_solr_assembler.load_static_test_data('alphasolrmarc', data,
                                            id_field='id')
    
    expclass = asm_exporter_class('BibsToAlphaSolrmarc')
    exporter = new_exporter(expclass, 'full_export', 'waiting')
    index = exporter.indexes['Bibs']

    found = get_records_from_index(index, records)
    assert_records_are_indexed(index, records, found.values())
    # suprecs = ['{}_{}'.format(r['id'], r['_version_']) for r in found.values()]

    exporter.delete_records(records)
    exporter.commit_indexes()
    assert_deleted_records_are_not_indexed(exporter, records)

    # conn = solr_conns[getattr(index, 'using', 'default')]
    # results = solr_search(conn, '*')
    # rdict = {r['id']: r for r in results}
    # for supkey in suprecs:
        # assert rdict[supkey]['suppressed']


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
                                      record_sets, new_exporter, solr_conns,
                                      solr_search, basic_solr_assembler,
                                      bl_solr_assembler,
                                      assert_records_are_indexed,
                                      assert_deleted_records_are_not_indexed,
                                      get_records_from_index):
    """
    The BibsToAlphaSmAndAttachedToSolr `delete_records` method should
    delete records from the appropriate indexes. For the
    BibsToAlphaSolrmarc child exporter, it should also create a stub
    copy of each record and suppress it.
    """
    records = record_sets['bib_del_set']
    ams_data = [(r.get_iii_recnum(False), {}) for r in records]
    bib_data = [(r.id, {'record_number': r.get_iii_recnum()}) for r in records]
    bl_solr_assembler.load_static_test_data('alphasolrmarc', ams_data)
    basic_solr_assembler.load_static_test_data('bib', bib_data)
    # basic_solr_assembler.load_static_test_data('marc', bib_data)
    
    expclass = asm_exporter_class('BibsToAlphaSmAndAttachedToSolr')
    exporter = new_exporter(expclass, 'full_export', 'waiting')

    asm_exporter = exporter.children['BibsToAlphaSolrmarc']
    bib_exporter = exporter.children['BibsAndAttachedToSolr']

    for index in bib_exporter.main_child.indexes.values():
        assert_records_are_indexed(index, records)

    asm_index = asm_exporter.indexes['Bibs']
    found = get_records_from_index(asm_index, records)
    assert_records_are_indexed(asm_index, records, found.values())
    # suprecs = ['{}_{}'.format(r['id'], r['_version_']) for r in found.values()]

    exporter.delete_records(records)
    do_commit(exporter)
    for child in exporter.children.values():
        assert_deleted_records_are_not_indexed(child, records)

    # conn = solr_conns[getattr(asm_index, 'using', 'default')]
    # results = solr_search(conn, '*')
    # rdict = {r['id']: r for r in results}
    # for supkey in suprecs:
        # assert rdict[supkey]['suppressed']
