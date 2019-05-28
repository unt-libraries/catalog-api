"""
Contains all shared pytest fixtures and hooks for shelflist app.
"""

import pytest

from . import solr_test_profiles as tp


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
