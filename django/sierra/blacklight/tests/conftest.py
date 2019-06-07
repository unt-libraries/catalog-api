"""
Contains pytest fixtures shared by Catalog API blacklight app
"""

import pytest

from . import solr_test_profiles as tp


@pytest.fixture(scope='module')
def bl_solr_profile_definitions(global_solr_conn, solr_profile_definitions):
    """
    Module-scoped pytest fixture. Returns profile definitions for
    generating `blacklight` app Solr test data.
    """
    pdefs = solr_profile_definitions.copy()
    pdefs['alphasolrmarc'] = {
        'conn': global_solr_conn('alpha-solrmarc'),
        'user_fields': tp.ALPHASOLRMARC_FIELDS,
        'field_gens': tp.ALPHASOLRMARC_GENS
    }
    return pdefs


@pytest.fixture(scope='module')
def global_bl_solr_assembler(global_solr_data_assembler,
                             bl_solr_profile_definitions):
    """
    Module-scoped pytest fixture. Returns a Solr test data assembler
    for the blacklight profiles.
    """
    assembler = global_solr_data_assembler
    return assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS, tp.GENS,
                     bl_solr_profile_definitions)


@pytest.fixture(scope='function')
def bl_solr_assembler(solr_data_assembler, bl_solr_profile_definitions):
    """
    Function-scoped pytest fixture. Returns a Solr test data assembler
    for the blacklight profiles.
    """
    return solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                               tp.GENS, bl_solr_profile_definitions)


@pytest.fixture(scope='module')
def bl_solr_env(global_bl_solr_assembler):
    """
    Pytest fixture that generates and populates Solr with some random
    background test data for blacklight tests. Fixture is module-
    scoped, so test data is regenerated each time the test module runs,
    NOT between tests.
    """
    assembler = global_bl_solr_assembler
    alphasolrmarc_recs = assembler.make('alphasolrmarc', 200)
    assembler.save_all()
    return assembler


@pytest.fixture(scope='function')
def assemble_bl_test_records(assemble_test_records, bl_solr_env,
                             bl_solr_assembler):
    """
    Pytest fixture. Returns a helper function that assembles & loads a
    set of test records (for one test) into the existing module-level
    bl_solr_env test-data environment.

    Required args include: `test_data`, a set of static partial
    records for this test; and `profile`, the profile to use. The
    default id field (for ensuring uniqueness in the test data) is
    'id', but you may override that via the `id_field` kwarg. Returns a
    tuple of default bl_solr_env records and the new test
    records that were loaded from the provided test data.
    len(env_recs) + len(test_recs) should equal the total number of
    Solr records for that profile.
    """
    def _assemble_bl_test_records(test_data, profile, id_field='id'):
        return assemble_test_records(profile, id_field, test_data,
                                     env=bl_solr_env,
                                     assembler=bl_solr_assembler)
    return _assemble_bl_test_records
