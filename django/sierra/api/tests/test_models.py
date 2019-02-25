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
#   apiuser_with_custom_defaults
#


def calculate_expected_apiuser_details(test_cls, new, start=None):
    """
    Calculate and return the expected values for an APIUser instance.

    This is a utility function used in a few of the tests. It
    determines the expected final attributes for an APIUser for tests
    that test create/update operations. `test_cls` is the APIUser test
    class that test uses. `new` is a dict containing data attributes
    for the new/updated APIUser. And `start` is a dict containing data
    attributes for the existing user that `new` is updating, if you're
    testing an update operation. Expected values are returned as a
    dict.
    """
    exp, start = {}, (start or {})
    exp['permissions_dict'] = test_cls.permission_defaults.copy()
    exp['permissions_dict'].update(start.get('permissions_dict', None) or {})
    exp['permissions_dict'].update(new.get('permissions_dict', None) or {})
    exp['permissions'] = ujson.encode(exp['permissions_dict'])

    key_fields = ('username', 'secret_text', 'password', 'email', 'first_name',
                  'last_name')
    for k in key_fields:
        new_val = new.get(k, None)
        default = start.get(k, None) or ''
        exp[k] = new_val if new_val is not None else default

    exp['secret'] = test_cls.encode_secret(exp['secret_text'])
    return exp


def assert_apiuser_matches_expected_data(apiuser, expected):
    """
    Assert that an `apiuser` obj matches `expected` data.
    """
    assert apiuser.secret == expected['secret']
    assert apiuser.permissions == expected['permissions']
    assert authenticate(username=expected['username'],
                        password=expected['password'])
    assert apiuser.user.email == expected['email']
    assert apiuser.user.first_name == expected['first_name']
    assert apiuser.user.last_name == expected['last_name']


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


def test_apiuser_save_requires_username(apiuser_with_custom_defaults):
    """
    Attempting to save an APIUser that has a related user with no
    username should raise an APIUserException.
    """
    test_cls = apiuser_with_custom_defaults()
    u = User(username='', password='pw')
    api_u = test_cls(secret_text='secret', user=u)
    with pytest.raises(APIUserException):
        api_u.save()


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


@pytest.mark.parametrize('username, secret, password', [
    (None, 'se1', 'pw1'),
    ('', 'se1', 'pw1'),
    ('un1', None, 'pw1'),
    ('un1', 'se1', None),
])
def test_apiuser_creation_required_field_errors(apiuser_with_custom_defaults,
                                                username, secret, password):
    """
    When creating a new APIUser (via the manager `create_user` method),
    not providing a required field should raise an error. Note that the
    `password` field is only required if you are creating an APIUser
    where a User doesn't already exist and has to be created. Also,
    blank usernames are NOT allowed, but blank secrets/passwords are.
    """
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
], ids=[
    'all fields updated',
    'no data provided; nothing updated',
    'only new secret_text value',
    'provided data is the same as the existing data; nothing updated',
    'some provided data is the same and some is new',
    'only new password value',
    'only new first_name value',
    'provided data is all blank strings'
])
def test_apiuser_updateandsave_works_correctly(apiuser_with_custom_defaults,
                                               secret_text, permissions_dict,
                                               pw, email, fn, ln):
    """
    The APIUser `update_and_save` method should update the APIUser
    and/or associated user based on the provided details. Missing
    details are not updated and left alone.
    """
    # Initialization.
    start_secret = 'secret'
    start_udata = {'username': 'test_user', 'password': 'password',
                   'email': 'email', 'first_name': 'first',
                   'last_name': 'last'}
    new_udata = remove_null_kwargs(secret_text=secret_text, password=pw,
                                   permissions_dict=permissions_dict,
                                   email=email, first_name=fn, last_name=ln)

    # Setup: Create our test APIUser class.
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)

    # Setup: Create existing User and APIUser instances.
    user = User.objects.create_user(**start_udata)
    apiuser = test_cls(secret_text=start_secret, user=user)
    apiuser.save()

    # Test: Call `update_and_save` with new user data.
    apiuser.update_and_save(**new_udata)

    # Post-test: Formulate expected results
    exp_apiuser = test_cls.objects.get(user__username=start_udata['username'])
    exp_user = User.objects.get(username=start_udata['username'])
    start_apiuser_data = start_udata.copy()
    start_apiuser_data['secret_text'] = start_secret
    exp = calculate_expected_apiuser_details(test_cls, new_udata,
                                             start_apiuser_data)

    # Results: Are the User and APIUser in the DB a match for what was
    # updated?
    assert exp_apiuser == apiuser
    assert exp_user == apiuser.user

    # Results: Does this APIUser have the correct (updated) values?
    assert apiuser.secret == exp['secret']
    assert apiuser.permissions == exp['permissions']
    assert authenticate(username=start_udata['username'],
                        password=exp['password'])
    assert apiuser.user.email == exp['email']
    assert apiuser.user.first_name == exp['first_name']
    assert apiuser.user.last_name == exp['last_name']


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
    apiuser.user.username = ''
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


@pytest.mark.parametrize('fields, start_vals, new_vals, err_vals', [
    (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', {'first': True})],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', {'first': True}),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', {'third': True})],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', None, {'first': True})]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [],
        [('un1', None, 'em1', 'fn1', 'ln1', 'se1', {'first': True})]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [],
        [(None, 'pw1', 'em1', 'fn1', 'ln1', 'se1', {'first': True})]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [],
        [('', 'pw1', 'em1', 'fn1', 'ln1', 'se1', {'first': True})]
    ), (
        ('username', 'password', 'email', 'fname', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', {'first': True})]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict', 'undefined_field'),
        [],
        [],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', {'first': True}, '')]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [],
        [],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', {'fourth': True})]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [('un3', 'pw3', 'em3', 'fn3', 'ln3', 'se3', {'first': True})],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [('un3', 'pw3', 'em3', 'fn3', 'ln3', 'se3', {'first': True}),
         ('un4', 'pw4', 'em4', 'fn4', 'ln4', 'se4', {'third': True})],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [('un3', 'pw3', 'em3', 'fn3', 'ln3', 'se3', {'first': True}),
         ('un4', 'pw4', 'em4', 'fn4', 'ln4', 'se4', {'third': True})],
        [('un5', None, 'em3', 'fn3', 'ln3', 'se3', {'first': True}),
         ('un6', 'pw4', 'em4', 'fn4', 'ln4', None, {'third': True})]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [],
        [('un1', None, None, None, None, None, {'first': 'ERROR'}),
         ('un2', None, None, None, None, None, {'fourth': False})]
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [('un1', 'npw', 'nem', 'nfn', 'nln', 'nse', {'second': True})],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [('un1', None, None, None, None, None, None)],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None)],
        [('un2', None, 'nem', None, None, None, None)],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None),
         ('un3', 'pw3', 'em3', 'fn3', 'ln3', 'se3', None)],
        [('un1', 'npw', '', None, None, None, None),
         ('un3', None, '', None, None, None, {'first': True})],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None),
         ('un3', 'pw3', 'em3', 'fn3', 'ln3', 'se3', None)],
        [('un1', 'npw', '', None, None, None, None),
         ('un3', None, '', None, None, None, {'first': True}),
         ('un4', 'pw4', 'em4', 'fn4', 'ln4', 'se4', {'third': True}),
         ('un5', 'pw5', 'em5', 'fn5', 'ln5', 'se5', {'second': True})],
        []
    ), (
        ('username', 'password', 'email', 'first_name', 'last_name',
         'secret_text', 'permissions_dict'),
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1', None),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2', None),
         ('un3', 'pw3', 'em3', 'fn3', 'ln3', 'se3', None)],
        [('un1', 'npw', '', None, None, None, None),
         ('un4', 'pw4', 'em4', 'fn4', 'ln4', 'se4', {'third': True}),
         ('un5', 'pw5', 'em5', 'fn5', 'ln5', 'se5', {'second': True})],
        [('un3', None, '', None, None, None, {'first': 0}),
         ('un6', None, '', None, None, None, None),
         ('un7', 'pw7', 'em7', 'fn7,' 'ln7', None, None)]
    ),
], ids=[
    'none exist, create one; no errors',
    'none exist, create some; no errors',
    'none exist, create none; one creation error (missing secret)',
    'none exist, create none; one creation error (missing password)',
    'none exist, create none; one creation error (missing username, None)',
    'none exist, create none; one creation error (missing username, empty)',
    'none exist, create none; one creation error (field misnamed)',
    'none exist, create none; one creation error (unknown field)',
    'none exist, create none; one creation error (unknown permission)',
    'some exist, create one; no errors',
    'some exist, create some; no errors',
    'some exist, create some; some creation errors',
    'some exist; create none; some update errors',
    'some exist, update one (all new field data); no errors',
    'some exist, update one (no data provided, so nothing updated); no errors',
    'some exist, update one (no new data, so nothing updated); no errors',
    'some exist, update one (one new value); no errors',
    'some exist, update some; no errors',
    'some exist, update some, create some; no errors',
    'some exist, update one, create some; creation and update errors',
])
def test_apiusermgr_batchimport_works(apiuser_with_custom_defaults, fields,
                                      start_vals, new_vals, err_vals):
    """
    The APIUser manager's `batch_import_users` method should: create
    new Users/APIUsers for ones that don't already exist; update
    Users/APIUsers for ones that do exist; and return lists of the
    APIUser objects that were created and updated.

    If a create/update operation produces errors (e.g. if a password
    or secret aren't provided for a new user, if a username isn't
    provided, etc.): it should log the error and go on to the next
    user. The user that triggered the error should not be created or
    modified. The list of errors and user records that caused them is
    returned.
    """
    # Initialization
    start_udata = [{fields[i]: v for i, v in enumerate(u)} for u in start_vals]
    new_udata = [{fields[i]: v for i, v in enumerate(u)} for u in new_vals]
    err_udata = [{fields[i]: v for i, v in enumerate(u)} for u in err_vals]
    start_unames = [u['username'] for u in start_udata]
    new_unames = [u['username'] for u in new_udata]
    err_unames = [u['username'] for u in err_udata]

    # Setup: Create our test APIUser class.
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    apiuser = test_cls()

    # Setup: Create the users that already exist.
    for start in start_udata:
        kwargs = start.copy()
        args = [kwargs.pop('username'), kwargs.pop('secret_text')]
        test_cls.objects.create_user(*args, **kwargs)

    # Test: Batch import new users.
    the_batch = new_udata + err_udata
    (created, updated, errors) = test_cls.objects.batch_import_users(the_batch)

    # Results: Were the expected users created and/or updated?
    exp_created = list(set(new_unames) - set(start_unames))
    exp_updated = list(set(new_unames) & set(start_unames))
    assert set(exp_created) == set([u.user.username for u in created])
    assert set(exp_updated) == set([u.user.username for u in updated])

    # Results: Does the new user data match expectations?
    for new in new_udata:
        apiuser = test_cls.objects.get(user__username=new['username'])
        start = [u for u in start_udata if u['username'] == new['username']]
        start = start[0] if len(start) else {}
        exp = calculate_expected_apiuser_details(test_cls, new, start)
        assert_apiuser_matches_expected_data(apiuser, exp)

    # Results: Were the correct number of errors raised?
    assert len(errors) == len(err_udata)

    # Results: Confirm that anything that raised an error is in its
    #          initial state.
    err_dicts = [e[1] for e in errors]
    for err in err_udata:
        assert err in err_dicts
        if err['username']:
            if err['username'] in start_unames:
                apiuser = test_cls.objects.get(user__username=err['username'])
                start = [u for u in start_udata
                         if u['username'] == err['username']]
                exp = calculate_expected_apiuser_details(test_cls, start[0])
                assert_apiuser_matches_expected_data(apiuser, exp)
            else:
                with pytest.raises(test_cls.DoesNotExist):
                    test_cls.objects.get(user__username=err['username'])
                with pytest.raises(User.DoesNotExist):
                    User.objects.get(username=err['username'])


@pytest.mark.parametrize('table, expected', [
    (
        [['username', 'secret_text', 'password', 'email', 'first_name',
         'last_name', 'first', 'second', 'third'],
         ['un1', 'se1', 'pw1', 'em1', 'fn1', 'ln1', 'true', 'true', 'true']],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1',
          {'first': True, 'second': True, 'third': True}),]
    ),
    (
        [['username', 'secret_text', 'password', 'email', 'first_name',
         'last_name', 'first', 'second', 'third'],
         ['un1', 'se1', 'pw1', 'em1', 'fn1', 'ln1', 'true', 'true', 'true'],
         ['un2', 'se2', 'pw2', 'em2', 'fn2', 'ln2', 'true', 'true', 'true'],
         ['un3', 'se3', 'pw3', 'em3', 'fn3', 'ln3', 'true', 'true', 'true']],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1',
          {'first': True, 'second': True, 'third': True}),
         ('un2', 'pw2', 'em2', 'fn2', 'ln2', 'se2',
          {'first': True, 'second': True, 'third': True}),
         ('un3', 'pw3', 'em3', 'fn3', 'ln3', 'se3',
          {'first': True, 'second': True, 'third': True}),]
    ),
    (
        [['second', 'last_name', 'secret_text', 'email', 'username', 'first',
          'password', 'third', 'first_name'],
         ['true', 'ln1', 'se1', 'em1', 'un1', 'true', 'pw1', 'true', 'fn1']],
        [('un1', 'pw1', 'em1', 'fn1', 'ln1', 'se1',
          {'first': True, 'second': True, 'third': True}),]
    ),
    (
        [['username'], ['un1'], ['un2'], ['un3']],
        [('un1', None, None, None, None, None, None),
         ('un2', None, None, None, None, None, None),
         ('un3', None, None, None, None, None, None),]
    ),
    (
        [['username', 'second'],
         ['un1', 'true']],
        [('un1', None, None, None, None, None, {'second': True}),]
    ),
], ids=[
    'one row, all fields entered',
    'multiple rows, all fields entered',
    'field order does not matter',
    'allowed to have fields missing',
    'allowed to have only some permissions'
])
def test_apiusermgr_tabletobatch_works(apiuser_with_custom_defaults, table,
                                       expected):
    """
    Running a list of rows through the APIUserManager's
    `table_to_batch` method should return a list of data dicts suitable
    for passing in as a batch to the `batch_import_users` method.
    """
    # Set up "expected" values.
    exp_fields = ('username', 'password', 'email', 'first_name', 'last_name',
                  'secret_text', 'permissions_dict')
    exp_batch = [{exp_fields[i]: v for i, v in enumerate(u) if v is not None}
                  for u in expected]

    # Set up test class.
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)

    batch = test_cls.objects.table_to_batch(table)
    assert batch == exp_batch


@pytest.mark.parametrize('bool_str, expected', [
    ('0', False),
    ('000000', False),
    ('f', False),
    ('F', False),
    ('false', False),
    ('False', False),
    ('FALSE', False),
    ('1', True),
    ('01', True),
    ('t', True),
    ('T', True),
    ('True', True),
    ('TRUE', True),
    ('', False)
])
def test_apiusermgr_tabletobatch_permission_vals(apiuser_with_custom_defaults,
                                                 bool_str, expected):
    """
    The `permission_*` fields used in a CSV file run through the
    APIUserManager's `table_to_batch` method should be interpreted
    correctly: '0' or anything beginning with 'f' or 'F' is False; a
    blank string is None; everything else is True.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_cls = apiuser_with_custom_defaults(custom_defaults)
    table = [['username', 'first'], ['un1', bool_str]]
    batch = test_cls.objects.table_to_batch(table)
    assert batch[0]['permissions_dict']['first'] == expected
