"""
Contains all shared pytest fixtures and hooks for shelflist app.
"""

import pytest

from .helpers import solr_test_profiles as tp


@pytest.fixture(scope='function')
def shelflist_solr_assembler(solr_data_assembler, solr_profile_definitions):
    """
    Pytest fixture. Returns a Solr test data assembler that defines a
    `shelflistitem` profile.
    """
    profile_def = {
        'shelflistitem': {
            'conn': solr_profile_definitions['item']['conn'],
            'user_fields': tp.SHELFLISTITEM_FIELDS,
            'field_gens': tp.SHELFLISTITEM_GENS
        }
    }
    return solr_data_assembler(tp.SOLR_TYPES, tp.GLOBAL_UNIQUE_FIELDS,
                               tp.GENS, profile_def)


@pytest.fixture(scope='function')
def solr_assemble_shelflist_record_data(solr_assemble_specific_record_data,
                                        shelflist_solr_assembler):
    """
    Pytest fixture. Assembles specific record data to load into Solr
    for the shelflistitem profile.
    """
    def _solr_assemble_shelflist_record_data(rdicts):
        return solr_assemble_specific_record_data(rdicts, ('shelflistitem',),
                                                  shelflist_solr_assembler)
    return _solr_assemble_shelflist_record_data

