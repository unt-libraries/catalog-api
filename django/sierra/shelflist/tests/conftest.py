"""
Contains all shared pytest fixtures and hooks for shelflist app.
"""

from __future__ import absolute_import

import pytest

from django.conf import settings

from . import solr_test_profiles as tp


@pytest.fixture(scope='module')
def shelflist_api_solr_profile_definitions(global_solr_conn,
                                           api_solr_profile_definitions):
    """
    Module-scoped pytest fixture. Returns the definition for the
    `shelflistitem` profile for generating Solr test data.
    """
    pdefs = api_solr_profile_definitions.copy()
    pdefs['shelflistitem'] = {
        'conn': global_solr_conn(
            settings.REST_VIEWS_HAYSTACK_CONNECTIONS['ShelflistItems']
        ),
        'user_fields': tp.SHELFLISTITEM_FIELDS,
        'field_gens': tp.SHELFLISTITEM_GENS
    }
    return pdefs


@pytest.fixture(scope='module')
def shelflist_export_solr_profile_definitions(global_solr_conn,
                                              export_solr_profile_definitions):
    """
    Module-scoped pytest fixture. Returns the definition for the
    `shelflistitem` profile for generating Solr test data.
    """
    pdefs = export_solr_profile_definitions.copy()
    pdefs['shelflistitem'] = {
        'conn': global_solr_conn(
            settings.EXPORTER_HAYSTACK_CONNECTIONS['ItemsToSolr']
        ),
        'user_fields': tp.SHELFLISTITEM_FIELDS,
        'field_gens': tp.SHELFLISTITEM_GENS
    }
    return pdefs


@pytest.fixture(scope='module')
def global_shelflist_api_solr_assembler(global_solr_data_assembler,
                                        shelflist_api_solr_profile_definitions):
    """
    Module-scoped pytest fixture. Returns a Solr test data assembler
    for the `shelflistitem` profile.
    """
    assembler = global_solr_data_assembler
    return assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS, tp.GENS,
                     shelflist_api_solr_profile_definitions)


@pytest.fixture(scope='function')
def shelflist_api_solr_assembler(solr_data_assembler,
                                 shelflist_api_solr_profile_definitions):
    """
    Function-scoped pytest fixture. Returns a Solr test data assembler
    for the `shelflistitem` profile.
    """
    return solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                               tp.GENS, shelflist_api_solr_profile_definitions)


@pytest.fixture(scope='module')
def global_shelflist_export_solr_assembler(global_solr_data_assembler,
                                           shelflist_export_solr_profile_definitions):
    """
    Module-scoped pytest fixture. Returns a Solr test data assembler
    for the `shelflistitem` profile.
    """
    assembler = global_solr_data_assembler
    return assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS, tp.GENS,
                     shelflist_export_solr_profile_definitions)


@pytest.fixture(scope='function')
def shelflist_export_solr_assembler(solr_data_assembler,
                                    shelflist_export_solr_profile_definitions):
    """
    Function-scoped pytest fixture. Returns a Solr test data assembler
    for the `shelflistitem` profile.
    """
    return solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                               tp.GENS,
                               shelflist_export_solr_profile_definitions)


@pytest.fixture(scope='module')
def shelflist_solr_env(global_shelflist_api_solr_assembler):
    """
    Pytest fixture that generates and populates Solr with some random
    background test data for shelflist API tests. Fixture is module-
    scoped, so test data is regenerated each time the test module runs,
    NOT between tests.
    """
    assembler = global_shelflist_api_solr_assembler
    gens = assembler.gen_factory
    loc_recs = assembler.make('location', 10)
    itype_recs = assembler.make('itype', 10)
    status_recs = assembler.make('itemstatus', 10)
    shelflistitem_recs = assembler.make('shelflistitem', 200,
                                        location_code=gens.choice(
                                            [r['code'] for r in loc_recs]),
                                        item_type_code=gens.choice(
                                            [r['code'] for r in itype_recs]),
                                        status_code=gens.choice(
                                            [r['code'] for r in status_recs]),
                                        )
    item_conn = assembler.profiles['item'].conn
    sl_item_conn = assembler.profiles['shelflistitem'].conn
    if item_conn.url != sl_item_conn.url:
        test_data = ((rec['id'], rec) for rec in shelflistitem_recs)
        item_recs = assembler.load_static_test_data(
            'item', test_data, id_field='id',
            context=assembler.records.get('item')
        )
    assembler.save_all()
    return assembler


@pytest.fixture(scope='function')
def assemble_shelflist_test_records(assemble_test_records, shelflist_solr_env,
                                    shelflist_api_solr_assembler):
    """
    Pytest fixture. Returns a helper function that assembles & loads a
    set of test records (for one test) into the existing module-level
    shelflist_solr_env test-data environment.

    The only required arg is `test_data`, a set of static partial
    records for this test. Default profile used is 'shelflistitem', but
    you may override that via the `profile` kwarg. The default id field
    (for ensuring uniqueness in the test data) is 'id', but you may
    override that via the `id_field` kwarg. Returns a tuple of default
    shelflist_solr_env records and the new test records that were
    loaded from the provided test data. len(env_recs) + len(test_recs)
    should equal the total number of Solr records for that profile.
    """
    def _assemble_shelflist_test_records(test_data, profile='shelflistitem',
                                         id_field='id'):
        if profile == 'shelflistitem':
            item_conn = shelflist_solr_env.profiles['item'].conn
            sl_item_conn = shelflist_solr_env.profiles['shelflistitem'].conn
            if item_conn.url != sl_item_conn.url:
                assemble_test_records('item', id_field, test_data,
                                      env=shelflist_solr_env,
                                      assembler=shelflist_api_solr_assembler)
            
        return assemble_test_records(profile, id_field, test_data,
                                     env=shelflist_solr_env,
                                     assembler=shelflist_api_solr_assembler)
    return _assemble_shelflist_test_records
