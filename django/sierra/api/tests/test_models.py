"""
Contains tests for the `api.users` module.

This tests creating users and setting permissions. The APIUsers API
resource (including authentication) is tested via `test_api.py`.
"""

import pytest

import ujson

from django.contrib.auth.models import User
from django.contrib.auth import authenticate

from api.models import APIUserException, UserExists, remove_null_kwargs


pytestmark = pytest.mark.django_db

# FIXTURES AND TEST DATA
# ---------------------------------------------------------------------
# External fixtures used below can be found in
# django/sierra/conftest.py:
#   
#
# django/sierra/api/tests/conftest.py:
#   apiuser_with_custom_defaults



# TESTS
# ---------------------------------------------------------------------

def test_removenullkwargs_removes_kwargs_having_None_value():
    """
    The utility function `remove_null_kwargs` should accept any kwargs
    and return a dict where any items with a None value are removed.
    """
    test_kwargs = remove_null_kwargs(a=None, b=1, c=None, d=1, e=None)
    assert test_kwargs == {'b': 1, 'd': 1}


def test_apiuser_save_requires_secret(apiuser_with_custom_defaults):
    """
    Attempting to save an APIUser that has no secret should raise
    an APIUserException.
    """
    test_cls = apiuser_with_custom_defaults()
    u = test_cls()
    assert u.secret == ''
    with pytest.raises(APIUserException):
        u.save()


def test_apiuser_save_requires_user(apiuser_with_custom_defaults):
    """
    Attempting to save an APIUser that has no related user should raise
    an APIUserException.
    """
    test_cls = apiuser_with_custom_defaults()
    u = test_cls(secret_text='secret')
    with pytest.raises(APIUserException):
        u.save()


@pytest.mark.parametrize('password, email, first_name, last_name', [
    ('password', 'email', 'first_name', 'last_name'),
    ('password', None, None, None),
    ('password', '', '', ''),
    ('password', None, 'first_name', None),
    ('password', '', 'first_name', ''),
])
def test_apiuser_creation_creates_new_user(apiuser_with_custom_defaults,
                                           password, email, first_name,
                                           last_name):
    """
    When creating a new APIUser (via the manager `create_user` method),
    if there is no existing User obj with the provided username, the
    User should be created using the provided credentials/details.
    """
    username, secret = 'test_user', 'secret'
    kwargs = remove_null_kwargs(password=password, email=email,
                                first_name=first_name, last_name=last_name)
    test_cls = apiuser_with_custom_defaults()
    user_preexists = bool(len(User.objects.filter(username=username)))
    apiuser = test_cls.objects.create_user(username, secret, **kwargs)

    assert not user_preexists
    assert apiuser.user == User.objects.get(username=username)
    assert apiuser == test_cls.objects.get(user__username=username)
    assert apiuser.user.username == username
    assert authenticate(username=username, password=password)
    assert apiuser.user.email == (email or '')
    assert apiuser.user.first_name == (first_name or '')
    assert apiuser.user.last_name == (last_name or '')


def test_apiuser_creation_requires_password(apiuser_with_custom_defaults):
    """
    When creating a new APIUser (via the manager `create_user` method),
    if there is no existing User obj with the provided username, but no
    `password` parameter is provided to use when creating a new User,
    an error should be raised.
    """
    username, secret = 'test_user', 'secret'
    test_cls = apiuser_with_custom_defaults()
    user_preexists = bool(len(User.objects.filter(username=username)))

    assert not user_preexists
    with pytest.raises(APIUserException):
        u = test_cls.objects.create_user(username, secret)


def test_apiuser_creation_requires_secret_text(apiuser_with_custom_defaults):
    """
    When creating a new APIUser (via the manager `create_user` method),
    the `secret_text` arg is required not to be None. If the caller
    forces None to be passed, then saving the object raises an error.
    """
    username, secret, password = 'test_user', None, 'password'
    test_cls = apiuser_with_custom_defaults()
    with pytest.raises(APIUserException):
        u = test_cls.objects.create_user(username, secret, password=password)


def test_user_state_on_apiuser_creation_error(apiuser_with_custom_defaults):
    """
    During APIUser creation, if a Django User object gets created and
    saved, and then saving the APIUser fails, then the creation of the
    Django User should be rolled back.
    """
    username = 'test_user'
    test_cls = apiuser_with_custom_defaults()

    user_preexists = bool(len(User.objects.filter(username=username)))
    try:
        u = test_cls.objects.create_user(username, None, password='pw')
    except APIUserException:
        pass
    user_exists_after = bool(len(User.objects.filter(username=username)))
    assert not user_preexists
    assert not user_exists_after


@pytest.mark.parametrize('password, email, first_name, last_name', [
    (None, None, None, None),
    ('password', 'email', 'first', 'last'),
    ('password', None, None, None),
    (None, None, 'first', None),
])
def test_creating_apiuser_for_existing_user(apiuser_with_custom_defaults,
                                            password, email, first_name,
                                            last_name):
    """
    When creating a new APIUser (via the manager `create_user` method),
    if there is already an existing User obj with the provided username
    but no related APIUser, a new APIUser should be created and related
    to that User (provided the User details don't conflict with any
    of the supplied credentials/details). If `None` is passed, or the
    kwarg is not supplied, it is ignored.
    """
    username, secret, u_password = 'test_user', 'secret', 'password'
    u_email, u_first_name, u_last_name = 'email', 'first', 'last'
    kwargs = remove_null_kwargs(password=password, email=email,
                                first_name=first_name, last_name=last_name)
    user_preexists = bool(len(User.objects.filter(username=username)))
    user = User.objects.create_user(username=username, password=u_password,
                                    email=u_email, first_name=u_first_name,
                                    last_name=u_last_name)
    test_cls = apiuser_with_custom_defaults()
    apiuser = test_cls.objects.create_user(username, secret, **kwargs)

    assert not user_preexists
    assert user == User.objects.get(username=username)
    assert apiuser == test_cls.objects.get(user__username=username)
    assert apiuser.user == user


@pytest.mark.parametrize('password, email, first_name, last_name', [
    ('wrong', 'email', 'first', 'last'),
    ('password', 'wrong', 'first', 'last'),
    ('password', 'email', 'wrong', 'last'),
    ('password', 'email', 'first', 'wrong'),
    ('password', '', 'first', 'wrong'),
])
def test_creating_apiuser_for_wrong_user(apiuser_with_custom_defaults,
                                         password, email, first_name,
                                         last_name):
    """
    When creating a new APIUser (via the manager `create_user` method),
    if there is already an existing User obj with the provided username
    but no related APIUser, that User should be checked against any
    additional provided credentials/details before the APIUser is
    created, and, if there is a conflict, an error should be raised.
    """
    username, secret, u_password = 'test_user', 'secret', 'password'
    u_email, u_first_name, u_last_name = 'email', 'first', 'last'
    kwargs = remove_null_kwargs(password=password, email=email,
                                first_name=first_name, last_name=last_name)
    user = User.objects.create_user(username=username, password=u_password,
                                    email=u_email, first_name=u_first_name,
                                    last_name=u_last_name)
    test_cls = apiuser_with_custom_defaults()
    with pytest.raises(APIUserException):
        apiuser = test_cls.objects.create_user(username, secret, **kwargs)


def test_creating_existing_apiuser(apiuser_with_custom_defaults):
    """
    When creating a new APIUser (via the manager `create_user` method),
    if there is already an existing User obj with the provided username
    that already has a related APIUser, an error should be raised.
    """
    username, secret = 'test_user', 'secret'
    kwargs = {'password': 'password', 'email': 'email', 'first_name': 'first',
              'last_name': 'last'}
    test_cls = apiuser_with_custom_defaults()
    test_cls.objects.create_user(username, secret, **kwargs)

    assert User.objects.get(username=username)
    assert test_cls.objects.get(user__username=username)
    with pytest.raises(APIUserException):
        apiuser = test_cls.objects.create_user(username, 'whatever')


def test_apiuser_creation_encodes_secret(apiuser_with_custom_defaults):
    """
    When creating a new APIUser via the model's `__init__` method, the
    APIUser's `secret` attribute should be generated by running the
    provided `secret_text` through the model's `encode_secret` method.
    """
    secret = 'secret'
    test_cls = apiuser_with_custom_defaults()
    apiuser = test_cls(secret_text=secret)

    assert apiuser.secret == test_cls.encode_secret(secret)


@pytest.mark.parametrize('permissions_dict', [
    None,
    {},
    {'first': True, 'third': True},
    {'first': True, 'second': True, 'third': True},
])
def test_apiuser_creation_makes_permissions(apiuser_with_custom_defaults,
                                            permissions_dict):
    """
    When creating a new APIUser via the model's `__init__` method, the
    APIUser's permissions should be derived correctly, based on the
    supplied `permissions_dict`. The `permissions` attribute should be
    a JSON-encoded string.
    """
    secret = 'secret'
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls(secret_text=secret, permissions_dict=permissions_dict)
    exp_permissions = custom_defaults.copy()
    exp_permissions.update(permissions_dict or {})

    assert ujson.decode(apiuser.permissions) == exp_permissions


@pytest.mark.parametrize('perm_str, perm_dict', [
    ({'first': True}, {'first': True}),
    ({'first': True}, {'first': False}),
    ({'first': True}, {'second': True}),
    ({'first': True, 'third': True}, {'second': True}),
])
def test_apiuser_creation_permission_conflicts(apiuser_with_custom_defaults,
                                               perm_str, perm_dict):
    """
    When creating a new APIUser via the model's `__init__` method, and
    permissions are provided via BOTH a `permissions` kwarg AND a
    `permissions_dict` kwarg, they should be combined, with the dict
    taking precedence when there are conflicts. (This is an edge case;
    under normal circumstances, APIUsers are created via the manager's
    `create_user` method, which uses the `permissions_dict` kwarg by
    itself.)
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls(permissions=ujson.encode(perm_str),
                       permissions_dict=perm_dict)
    exp_permissions = custom_defaults.copy()
    exp_permissions.update(perm_str)
    exp_permissions.update(perm_dict)
    assert ujson.decode(apiuser.permissions) == exp_permissions


@pytest.mark.parametrize('perm_dict', [
    {'fourth': True},
    {'first': True, 'fourth': True},
    {'first': 'a wild string appears!'},
    {'first': 0}
])
def test_apiuser_creation_permission_errors(apiuser_with_custom_defaults,
                                            perm_dict):
    """
    When creating a new APIUser via the model's `__init__` method, an
    error should be raised if it encounters any permissions in the
    permissions list that aren't registered as default permissions OR
    if it enounters any non-boolean permission values. These errors
    should occur whether the permissions are set via `permissions` or
    `permissions_dict` kwargs.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    with pytest.raises(APIUserException):
        apiuser = test_cls(permissions_dict=perm_dict)
    with pytest.raises(APIUserException):
        apiuser = test_cls(permissions=ujson.encode(perm_dict))


@pytest.mark.parametrize('secret_text, permissions_dict, pw, email, fn, ln', [
    ('new_s', {'first': True}, 'new_password', 'new_e', 'new_f', 'new_l'),
    (None, None, None, None, None, None),
    ('new_s', None, None, None, None, None),
    ('secret', None, 'password', 'email', 'first', 'last'),
    ('secret', None, 'password', 'new_e', 'first', 'last'),
    (None, None, 'new_password', None, None, None),
    (None, None, None, None, 'new_f', None),
    ('', None, '', '', '', ''),
])
def test_apiuser_updateandsave_works_correctly(apiuser_with_custom_defaults,
                                               secret_text, permissions_dict,
                                               pw, email, fn, ln):
    """
    The APIUser `update_and_save` method should update the APIUser
    and/or associated user based on the provided details. Missing
    details are not updated and left alone.
    """
    start_secret = 'secret'
    start_kwargs = {'username': 'test_user', 'password': 'password',
                    'email': 'email', 'first_name': 'first',
                    'last_name': 'last'}
    new_kwargs = remove_null_kwargs(secret_text=secret_text, password=pw,
                                    permissions_dict=permissions_dict,
                                    email=email, first_name=fn, last_name=ln)
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    user = User.objects.create_user(**start_kwargs)
    apiuser = test_cls(secret_text=start_secret, user=user)
    apiuser.save()
    apiuser.update_and_save(**new_kwargs)

    cmp_apiuser = test_cls.objects.get(user__username=start_kwargs['username'])
    cmp_user = User.objects.get(username=start_kwargs['username'])
    exp_permissions = custom_defaults.copy()
    exp_permissions.update(permissions_dict or {})
    exp_secret = (secret_text if secret_text is not None else start_secret)
    exp_pw = (pw if pw is not None else start_kwargs['password'])
    exp_email = (email if email is not None else start_kwargs['email'])
    exp_first_name = (fn if fn is not None else start_kwargs['first_name'])
    exp_last_name = (ln if ln is not None else start_kwargs['last_name'])

    assert cmp_apiuser == apiuser
    assert cmp_user == apiuser.user
    assert apiuser.secret == test_cls.encode_secret(exp_secret)
    assert ujson.decode(apiuser.permissions) == exp_permissions
    assert authenticate(username=start_kwargs['username'], password=exp_pw)
    assert apiuser.user.email == exp_email
    assert apiuser.user.first_name == exp_first_name
    assert apiuser.user.last_name == exp_last_name


def test_user_state_on_apiuser_updatesave_error(apiuser_with_custom_defaults):
    """
    When updating an APIUser (via the `update_and_save` method), if the
    User object is updated and saved but the APIUser save then fails,
    then the changes made to the User object should get rolled back.
    """
    username, secret, password = 'username', 'secret', 'password'
    start_email, new_email = 'blank', 'email'
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    user = User.objects.create_user(username=username, password=password,
                                    email=start_email)
    apiuser = test_cls(secret_text=secret, user=user)
    apiuser.save()

    user_email_before = User.objects.get(username=username).email
    apiuser.secret = None
    with pytest.raises(APIUserException):
        apiuser.update_and_save(email=new_email)
    user_email_after = User.objects.get(username=username).email

    assert user_email_before == start_email
    assert user_email_after == start_email


@pytest.mark.parametrize('new_perms', [
    {},
    {'first': True},
    {'first': False},
    {'second': True},
    {'first': True, 'third': True},
    {'first': True, 'second': True, 'third': True},
    {'first': False, 'second': False, 'third': True},
    {'first': False, 'second': False, 'third': False},
])
def test_apiuser_updatepermissions(apiuser_with_custom_defaults, new_perms):
    """
    The APIUser `update_permissions` method should set the object's
    `permissions` attribute based on the provided `permissions_dict`
    arg. It should return a dict containing _all_ of the APIUser's
    permissions (including the ones that were updated).
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls()
    result = apiuser.update_permissions(new_perms)
    expected = custom_defaults.copy()
    expected.update(new_perms)
    assert ujson.decode(apiuser.permissions) == expected
    assert result == expected


@pytest.mark.parametrize('new_perms', [
    {'fourth': True},
    {'first': True, 'fourth': True},
    {'first': 'a wild string appears!'},
    {'first': 0}
])
def test_apiuser_updatepermissions_errors(apiuser_with_custom_defaults,
                                          new_perms):
    """
    The APIUser `update_permissions` method should raise an error if
    it encounters any permissions in the permissions list that aren't
    registered as default permissions OR if it enounters any non-
    boolean permission values.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls()
    with pytest.raises(APIUserException):
        apiuser.update_permissions(new_perms)


def test_apiuser_setpermissionstovalue(apiuser_with_custom_defaults):
    """
    The APIUser `set_permissions_to_value` method should set the
    provided boolean value on the given list of permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls()
    apiuser.set_permissions_to_value(['first', 'third'], True)
    assert ujson.decode(apiuser.permissions) == {'first': True, 
                                                 'second': False,
                                                 'third': True}


@pytest.mark.parametrize('new_perms, value', [
    (['fourth'], True),
    (['first, fourth'], True),
    (['first'], 'a wild string appears!'),
    (['first'], 0)
])
def test_apiuser_setpermissionstovalue_errors(apiuser_with_custom_defaults,
                                              new_perms, value):
    """
    The APIUser `set_permissions_to_value` method should raise an error
    if any permissions in the permissions list aren't registered as
    default permissions OR if the permission value is not a boolean.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls()
    with pytest.raises(APIUserException):
        apiuser.set_permissions_to_value(new_perms, value)


def test_apiuser_setallpermissionstovalue(apiuser_with_custom_defaults):
    """
    The APIUser `set_all_permissions_to_value` method should set the
    provided boolean value on ALL available permissions.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls()
    apiuser.set_all_permissions_to_value(True)
    assert ujson.decode(apiuser.permissions) == {'first': True, 'second': True,
                                                 'third': True}


def test_apiuser_setallpermissionstovalue_errors(apiuser_with_custom_defaults):
    """
    The APIUser `set_all_permissions_to_value` method should raise an
    error if the permission value is not a boolean.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls()
    with pytest.raises(APIUserException):
        apiuser.set_all_permissions_to_value('not a boolean')
    