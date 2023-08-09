"""
Contains tests for utils.solr.

Note that this does not test the whole module; I'm adding tests as I
make updates and changes. (Anything not here is already thoroughly
tested via other tests, since utils.solr is pretty fundamental.)
"""

from __future__ import absolute_import

import time

import pytest
from django.core.exceptions import ImproperlyConfigured

from utils import solr


# FIXTURES AND TEST DATA

# TESTS

def test_commit_replication(solr_conn, settings):
    """
    The 'commit' function should trigger Solr replication when the
    corresponding HAYSTACK_CONNECTIONS settings are set.
    """
    test_records = [{'id': '1'}, {'id': '2'}, {'id': '3'}, {'id': '4'}]
    exp_ids = set(['1', '2', '3', '4'])
    core = 'discover-01'
    leader = f'{core}|update'
    leader_conn = solr_conn(leader)
    follower = f'{core}|search'
    follower_url = settings.HAYSTACK_CONNECTIONS[follower]['URL']
    follower_conn = solr_conn(follower)
    settings.HAYSTACK_CONNECTIONS[leader]['MANUAL_REPLICATION'] = True
    settings.HAYSTACK_CONNECTIONS[leader]['FOLLOWER_URLS'] = [follower_url]
    leader_conn.add(test_records, commit=False)
    solr.commit(leader_conn, leader, specify_leader_url=True)
    # Need to wait a few seconds to give the replication time to finish
    time.sleep(3)
    assert set([r['id'] for r in leader_conn.search(q='*:*')]) == exp_ids
    assert set([r['id'] for r in follower_conn.search(q='*:*')]) == exp_ids


def test_commit_no_replication(solr_conn, settings):
    """
    The 'commit' function should NOT trigger Solr replication when the
    corresponding HAYSTACK_CONNECTIONS MANUAL_REPLICATION setting is
    False.
    """
    test_records = [{'id': '1'}, {'id': '2'}, {'id': '3'}, {'id': '4'}]
    exp_ids = set(['1', '2', '3', '4'])
    core = 'discover-01'
    leader = f'{core}|update'
    leader_conn = solr_conn(leader)
    follower = f'{core}|search'
    follower_url = settings.HAYSTACK_CONNECTIONS[follower]['URL']
    follower_conn = solr_conn(follower)
    settings.HAYSTACK_CONNECTIONS[leader]['MANUAL_REPLICATION'] = False
    settings.HAYSTACK_CONNECTIONS[leader]['FOLLOWER_URLS'] = [follower_url]
    leader_conn.add(test_records, commit=False)
    solr.commit(leader_conn, leader, specify_leader_url=True)
    # Need to wait a few seconds to give theoretical replication time
    # to finish
    time.sleep(3)
    assert set([r['id'] for r in leader_conn.search(q='*:*')]) == exp_ids
    assert set([r['id'] for r in follower_conn.search(q='*:*')]) == set()


def test_commit_error_if_wrong_replication_handler(solr_conn, settings):
    """
    The 'commit' function should raise an error if the configured
    replication handler does not exist.
    """
    test_records = [{'id': '1'}, {'id': '2'}, {'id': '3'}, {'id': '4'}]
    core = 'discover-01'
    leader = f'{core}|update'
    leader_conn = solr_conn(leader)
    follower = f'{core}|search'
    follower_url = settings.HAYSTACK_CONNECTIONS[follower]['URL']
    follower_conn = solr_conn(follower)
    settings.HAYSTACK_CONNECTIONS[leader]['MANUAL_REPLICATION'] = True
    settings.HAYSTACK_CONNECTIONS[leader]['FOLLOWER_URLS'] = [follower_url]
    settings.HAYSTACK_CONNECTIONS[leader]['REPLICATION_HANDLER'] = 'incorrect'
    leader_conn.add(test_records, commit=False)
    with pytest.raises(ImproperlyConfigured):
        solr.commit(leader_conn, leader, specify_leader_url=True)


def test_commit_error_if_no_leader_url(solr_conn, settings):
    """
    The 'commit' function should raise an error if replication is
    misconfigured -- specifically, if the follower does not specify a
    leader URL and the 'leaderUrl' parameter is not supplied.
    """
    test_records = [{'id': '1'}, {'id': '2'}, {'id': '3'}, {'id': '4'}]
    core = 'discover-01'
    leader = f'{core}|update'
    leader_conn = solr_conn(leader)
    follower = f'{core}|search'
    follower_url = settings.HAYSTACK_CONNECTIONS[follower]['URL']
    follower_conn = solr_conn(follower)
    settings.HAYSTACK_CONNECTIONS[leader]['MANUAL_REPLICATION'] = True
    settings.HAYSTACK_CONNECTIONS[leader]['FOLLOWER_URLS'] = [follower_url]
    leader_conn.add(test_records, commit=False)
    with pytest.raises(ImproperlyConfigured):
        solr.commit(leader_conn, leader, specify_leader_url=False)
