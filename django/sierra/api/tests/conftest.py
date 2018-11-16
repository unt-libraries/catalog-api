"""
Contains pytest fixtures shared amongst test files for the `api` app.
"""

import pytest

from rest_framework import test as drftest

from . import solr_test_profiles as tp


# External fixtures used below can be found in
# django/sierra/conftest.py:
#    global_solr_conn
#    global_solr_data_factory
#    solr_data_factory


@pytest.fixture(scope='module')
def profile_definitions(global_solr_conn):
    """
    Pytest fixture that returns definitions for Solr profiles, for
    generating test data via the *_solr_data_factory fixtures.
    """
    hs_conn = global_solr_conn('haystack')
    bib_conn = global_solr_conn('bibdata')
    return {
        'location': {
            'conn': hs_conn,
            'user_fields': tp.CODE_FIELDS,
            'field_gens': tp.LOCATION_GENS
        },
        'itype': {
            'conn': hs_conn,
            'user_fields': tp.CODE_FIELDS,
            'field_gens': tp.ITYPE_GENS
        },
        'itemstatus': {
            'conn': hs_conn,
            'user_fields': tp.CODE_FIELDS,
            'field_gens': tp.ITEMSTATUS_GENS
        },
        'item': {
            'conn': hs_conn,
            'user_fields': tp.ITEM_FIELDS,
            'field_gens': tp.ITEM_GENS
        },
        'eresource': {
            'conn': hs_conn,
            'user_fields': tp.ERES_FIELDS,
            'field_gens': tp.ERES_GENS
        },
        'bib': {
            'conn': bib_conn,
            'user_fields': tp.BIB_FIELDS,
            'field_gens': tp.BIB_GENS
        }
    }


@pytest.fixture(scope='function')
def api_data_factory(solr_data_factory, profile_definitions):
    """
    Function-scoped pytest fixture that returns a Solr test data
    factory. Records created via this fixture within a test function
    are deleted when the test function finishes. (The type of factory
    object returned is `SolrTestDataFactoryMaker.SolrTestDataFactory`,
    from the root-level conftest file.)
    """
    return solr_data_factory(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                             tp.GENS, profile_definitions)


@pytest.fixture(scope='module')
def global_api_data_factory(global_solr_data_factory, profile_definitions):
    """
    Module-scoped pytest fixture that returns a Solr test data
    factory. Records created via this fixture persist while all tests
    in the module run. (The type of factory object returned is
    `SolrTestDataFactoryMaker.SolrTestDataFactory`, from the root-level
    conftest file.)
    """
    return global_solr_data_factory(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                                    tp.GENS, profile_definitions)


@pytest.fixture(scope='module')
def api_solr_env(global_api_data_factory):
    """
    Pytest fixture that generates and populates Solr with some random
    background test data for API integration tests. Fixture is module-
    scoped, so test data is regenerated each time the test module runs
    and is NOT regenerated between tests.
    """
    factory = global_api_data_factory
    gens = factory.gen_factory
    loc_recs = factory.make('location', 10)
    itype_recs = factory.make('itype', 10)
    status_recs = factory.make('itemstatus', 10)
    bib_recs = factory.make('bib', 100)
    item_recs = factory.make('item', 200,
        location_code=gens.choice([r['code'] for r in loc_recs]),
        item_type_code=gens.choice([r['code'] for r in itype_recs]),
        status_code=gens.choice([r['code'] for r in status_recs]),
        parent_bib_id=gens(tp.choose_and_link_to_parent_bib(bib_recs))
    )
    eres_recs = factory.make('eresource', 25)
    factory.save_all()
    return factory


@pytest.fixture
def api_client():
    """
    Pytest fixture that returns a new rest_framework.test.APIClient
    object.
    """
    return drftest.APIClient()

