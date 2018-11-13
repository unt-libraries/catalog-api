"""
Contains integration tests for the `api` app.
"""

import pytest

from . import solr_test_profiles as tp

# FIXTURES AND TEST DATA
# External fixtures used below can be found in
# django/sierra/conftest.py:
#
# django/sierra/api/tests/conftest.py:
#     api_solr_env
#     api_client


# TESTS


@pytest.mark.parametrize('url, err_text', [
    ('/api/v1/items/?dueDate[gt]=2018', 'datetime was formatted incorrectly')
    ('/api/v1/items/?recordNumber[invalid]=i10000100', 'not a valid operator'),
    ('/api/v1/items/?recordNumber[in]=i10000100', 'require an array'),
    ('/api/v1/items/?recordNumber[range]=i10000100', 'require an array'),
    ('/api/v1/items/?nonExistent=0', 'not a valid field for filtering'),
    ('/api/v1/items/?orderBy=nonExistent', 'not a valid field for ordering'),

])
def test_request_error_badquery(url, err_text, api_solr_env, api_client):
    """
    Requesting from the given URL should result in a 400 error response
    (due to a bad query), which contains the given error text.
    """
    response = api_client.get(url)
    assert response.status_code == 400
    assert 'Query filter criteria' in response.data['detail']
    assert err_text in response.data['detail']
