"""
Contains tests for the `api.users` module.

This tests creating users and setting permissions. The APIUsers API
resource (including authentication) is tested via `test_api.py`.
"""

import pytest

import ujson

from api.models import APIUserException


# FIXTURES AND TEST DATA
# ---------------------------------------------------------------------
# External fixtures used below can be found in
# django/sierra/conftest.py:
#   model_init_instance
#
# django/sierra/api/tests/conftest.py:
#   apiuser_with_custom_defaults



# TESTS
# ---------------------------------------------------------------------

@pytest.mark.django_db
def test_apiuser_save_errors_with_no_secret(apiuser_with_custom_defaults,
                                            model_init_instance):
    """
    Attempting to save an APIUser that has no secret should raise
    an APIUserException.
    """
    test_cls = apiuser_with_custom_defaults()
    u = model_init_instance(test_cls)
    assert u.secret == ''
    with pytest.raises(APIUserException):
        u.save()


@pytest.mark.django_db
def test_apiuser_save_errors_with_no_user(apiuser_with_custom_defaults,
                                          model_init_instance):
    """
    Attempting to save an APIUser that has no related user should raise
    an APIUserException.
    """
    test_cls = apiuser_with_custom_defaults()
    u = model_init_instance(test_cls, secret_text='secret')
    with pytest.raises(APIUserException):
        u.save()


@pytest.mark.django_db
def test_apiuser_setpermissionstovalue(apiuser_with_custom_defaults,
                                       model_init_instance):
    """
    The `set_permissions_to_value` method should set the provided boolean
    value on the given list of permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    u = model_init_instance(test_cls)
    u.set_permissions_to_value(['first', 'third'], True)
    assert ujson.decode(u.permissions) == {'first': True, 'second': False,
                                           'third': True}


@pytest.mark.django_db
def test_apiuser_setpermissionstovalue_error(apiuser_with_custom_defaults,
                                             model_init_instance):
    """
    The `set_permissions_to_value` method should raise an
    APIUserException if it encounters any permissions in the
    permissions list that aren't registered as default permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    u = model_init_instance(test_cls)
    with pytest.raises(APIUserException):
        u.set_permissions_to_value(['first', 'third', 'fourth'], True)


@pytest.mark.django_db
def test_apiuser_setallpermissionstovalue(apiuser_with_custom_defaults,
                                          model_init_instance):
    """
    The `set_all_permissions_to_value` method should set the provided boolean
    value on ALL available permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    u = model_init_instance(test_cls)
    u.set_all_permissions_to_value(True)
    assert ujson.decode(u.permissions) == {'first': True, 'second': True,
                                           'third': True}
