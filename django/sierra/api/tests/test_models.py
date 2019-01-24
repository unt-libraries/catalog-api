"""
Contains tests for the `api.users` module.

This tests creating users and setting permissions. The APIUsers API
resource (including authentication) is tested via `test_api.py`.
"""

import pytest


# FIXTURES AND TEST DATA
# ---------------------------------------------------------------------
# External fixtures used below can be found in
# django/sierra/conftest.py:
#
# django/sierra/api/tests/conftest.py:
#   apiuser_with_custom_defaults



# TESTS
# ---------------------------------------------------------------------


def test_apiuser_setpermissions(apiuser_with_custom_defaults):
    """
    The `set_permissions` method should set the provided boolean
    value on the given list of permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    u = test_cls()
    u.set_permissions(['first', 'third'], True)
    assert u.permissions_dict == {'first': True, 'second': False,
                                  'third': True}

def test_apiuser_setpermissions_missing_perm(apiuser_with_custom_defaults):
    """
    The `set_permissions` method should ignore any permissions in the
    permissions list that aren't registered as default permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    u = test_cls()
    u.set_permissions(['first', 'third', 'fourth'], True)
    assert u.permissions_dict == {'first': True, 'second': False,
                                  'third': True}

def test_apiuser_setallpermissions(apiuser_with_custom_defaults):
    """
    The `set_all_permissions` method should set the provided boolean
    value on ALL available permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    u = test_cls()
    u.set_all_permissions(True)
    assert u.permissions_dict == {'first': True, 'second': True,
                                  'third': True}
