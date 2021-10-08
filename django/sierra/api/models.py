"""
Contains models for api app.
"""
from __future__ import absolute_import

import hashlib
import importlib
import re

import ujson
from django.conf import settings
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.db import models, transaction, IntegrityError


class APIUserException(Exception):
    pass


class UserExists(APIUserException):
    pass


def get_permission_defaults_from_apps():
    """
    Return all valid permissions and their default values as a dict.

    Gathers permissions from apps that are configured in
    settings.API_PERMISSIONS. Apps that want to contribute permissions
    to the APIUser model must have a `permissions` module that supplies
    a DEFAULTS dict.
    """
    permission_defaults = {}
    for app_name in settings.API_PERMISSIONS:
        perm = importlib.import_module('{}.permissions'.format(app_name))
        permission_defaults.update(perm.DEFAULTS)
    return permission_defaults


def remove_null_kwargs(**kwargs):
    """
    Return a kwargs dict having items with a None value removed.
    """
    return {k: v for k, v in kwargs.items() if v is not None}


class APIUserManager(models.Manager):

    @staticmethod
    def _set_new_user_password(user, pw):
        un = user.username
        if pw is None:
            msg = ('APIUser for {} not created: Django user not found and not '
                   'created. (You need to provide a password!)'.format(un))
            raise APIUserException(msg)
        user.set_password(pw)
        user.save()

    @staticmethod
    def _existing_password_is_okay(user, pw):
        un = user.username
        return (pw is None) or (bool(authenticate(username=un, password=pw)))

    @staticmethod
    def _apiuser_already_exists(user):
        try:
            api_user = user.apiuser
        except ObjectDoesNotExist:
            return False
        return True

    @transaction.atomic
    def create_user(self, username, secret_text, permissions_dict=None,
                    password=None, email=None, first_name=None,
                    last_name=None):
        """
        Create, save, and return a new APIUser object.

        If no Django user with the given username exists, it is created
        along with the APIUser, using the provided password, email,
        first_name, and last_name. (In this case at least a password
        must be provided. The other fields are optional.)

        If a Django user with the given username already exists but has
        no related APIUser, the APIUser is created and related to the
        existing user. In this case, if a password, email, first_name,
        or last_name are provided, then those are also matched when
        fetching the user. Any parameters you provide that are
        different than the ones in the database cause an error.

        If a Django user with the given username AND an associated
        APIUser already exist, a `UserExists` error is raised.
        """
        kwargs = remove_null_kwargs(email=email, first_name=first_name,
                                    last_name=last_name)
        user_model = self.model._meta.get_field('user').related_model
        try:
            user, created = user_model.objects.get_or_create(username=username,
                                                             **kwargs)
            if created:
                self._set_new_user_password(user, password)
            else:
                if not self._existing_password_is_okay(user, password):
                    raise IntegrityError(1062, 'Attempted to create'
                                               'duplicate user')
                elif self._apiuser_already_exists(user):
                    msg = ('Could not create APIUser for Django user {}. '
                           'APIUser already exists.'.format(username))
                    raise UserExists(msg)

        except IntegrityError as xxx_todo_changeme:
            (ie_num, detail) = xxx_todo_changeme.args
            if ie_num == 1062:
                detail = ('Existing Django user found, but it may not be '
                          'the correct one. Its details do not match the ones '
                          'supplied.')
            msg = ('Could not create APIUser for Django user {}. {}'
                   ''.format(username, detail))
            raise APIUserException(msg)

        api_user = self.model(user=user, secret_text=secret_text,
                              permissions_dict=permissions_dict)
        api_user.save()
        return api_user

    def batch_import_users(self, user_records):
        """
        Create and/or update a list of APIUsers in one batch operation.

        APIUsers are created (if they do not already exist) or updated
        (if they already exist) based on the provided `user_records`
        arg. Returns a tuple: (created, updated, errors), where:
            `errors` is a list of (exception, record) tuples,
            `created` is a list of APIUser objs that were created, and
            `updated` is a list of APIUser objs that were updated.

        The `user_records` arg should be a list of dictionaries, where
        each dict contains a `username` plus optional `secret_text`,
        `permissions_dict`, `password`, `email`, `first_name`, and
        `last_name` elements. A username is of course required, and a
        secret and password are required if the APIUser is being
        created; otherwise, elements that are either set to None or not
        included at all are not set or changed.

        E.g., if you wanted to update secrets for a list of existing
        APIUsers, you could provide ONLY the `username` and
        `secret_text` for each.

        A dict with one or more elements NOT in the ones listed above
        (and not a valid permission key) is an error: the element may
        be a misnamed field or an extra field.
        """
        created, updated, errors = [], [], []
        kwarg_names = ('permissions_dict', 'email', 'first_name', 'last_name',
                       'password')
        valid_fields = set(('secret_text', 'username') + kwarg_names)
        for i, record in enumerate(user_records):
            secret_text = record.get('secret_text', None)
            kwargs = {k: record.get(k, None) for k in kwarg_names}
            username = record.get('username', None)
            unknown_fields = tuple(set(record.keys()) - valid_fields)
            try:
                if unknown_fields:
                    msg = 'Unknown fields in record: {}'.format(unknown_fields)
                    raise APIUserException
                try:
                    au = self.get(user__username=username)
                except ObjectDoesNotExist:
                    au = self.create_user(username, secret_text, **kwargs)
                    created.append(au)
                else:
                    au.update_and_save(secret_text, **kwargs)
                    updated.append(au)
            except APIUserException as e:
                errors.append((i + 1, record, e))
        return (created, updated, errors)

    def table_to_batch(self, table):
        """
        Convert a list of data rows to an APIUser import batch.

        Pass an iterable object containing rows of tabular APIUser data
        fields and get a list of dictionaries (a "batch") suitable for
        passing into the `batch_import_users` method.

        The first row of the iterable object should contain column
        names. Columns should be named so that they can be converted
        directly to dict keys; permissions should use the permission
        key as the column name and a string ('true' or 'false') as the
        value.
        """
        def _str_to_bool(string):
            return False if re.match(r'([Ff]|0+$|$)', string) else True

        permission_columns = list(self.model.permission_defaults.keys())
        rows = (r for r in table)
        colnames = next(rows)
        user_records = []
        for row in rows:
            rec = {col: row[i] for i, col in enumerate(colnames)}
            perm_dict = {c: _str_to_bool(rec.pop(c))
                         for c in permission_columns if c in rec}
            if perm_dict:
                rec['permissions_dict'] = perm_dict
            user_records.append(rec)
        return user_records


class APIUser(models.Model):
    """
    Provides fields/features for secrets and permissions.
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    secret = models.CharField(max_length=128)
    permissions = models.TextField(default='{}')
    permission_defaults = get_permission_defaults_from_apps()
    objects = APIUserManager()

    def __init__(self, *args, **kwargs):
        """
        When an APIUser object is initialized, the `secret` and
        `permissions` fields may be set via a `secret_text` and
        `permissions_dict` kwarg, respectively.

        `secret_text` is the user-readable text of the secret, which is
        encoded to produce the `secret` field value.

        `permissions_dict` is a Python dict w/permissions to override
        the defaults.
        """
        pdict = ujson.decode(kwargs.pop('permissions', '{}')) or {}
        pdict.update(kwargs.pop('permissions_dict', {}) or {})
        secret_text = kwargs.pop('secret_text', None)
        super(APIUser, self).__init__(*args, **kwargs)
        self.update_permissions(pdict)
        if not self.secret and secret_text is not None:
            self.secret = self.encode_secret(secret_text)

    def save(self, *args, **kwargs):
        """
        An APIUser MUST have a `secret` and a `user` relation, and the
        user must have a non-blank username, before the APIUser obj is
        saved. Otherwise, an APIUserException is raised.
        """
        msg = ''
        if not self.secret:
            msg = 'APIUser obj cannot be saved without a `secret`.'
        else:
            try:
                self.user
            except User.DoesNotExist:
                msg = 'APIUser obj cannot be saved without a related user.'
            else:
                if not self.user.username:
                    msg = ('APIUser obj cannot be save if the related user '
                           'has a blank `username`.')
        if msg:
            raise APIUserException(msg)
        super(APIUser, self).save(*args, **kwargs)

    @transaction.atomic
    def update_and_save(self, secret_text=None, permissions_dict=None,
                        password=None, email=None, first_name=None,
                        last_name=None):
        """
        Update AND SAVE an existing APIUser with any or all new values.
        For any of `email`, `password`, `first_name`, and `last_name`,
        the related User object is updated with the appropriate
        value(s).
        """
        kwargs = remove_null_kwargs(password=password, email=email,
                                    first_name=first_name, last_name=last_name)
        for field, value in kwargs.items():
            if field == 'password':
                self.user.set_password(value)
            else:
                setattr(self.user, field, value)
        if secret_text is not None:
            self.secret = self.encode_secret(secret_text)
        if permissions_dict:
            self.update_permissions(permissions_dict)
        self.user.save()
        self.save()
        return self

    def update_permissions(self, permissions_dict):
        """
        Update certain permissions' values via a `permissions_dict`.

        The passed `permissions_dict` is a dictionary with key-value
        pairs that set particular permissions (keys) to specific bool
        values.

        Names for the permissions that are set MUST exist in
        cls.permission_defaults. An APIUserException is raised if an
        unexpected permission is encountered.

        Returns a dictionary of all user permissions after the update.
        """
        permissions = type(self).permission_defaults.copy()
        permissions.update(ujson.decode(self.permissions))
        for pname, pvalue in permissions_dict.items():
            if not isinstance(pvalue, bool):
                msg = ('Permission values must be set to a boolean True or '
                       'False. "{}" is not valid.').format(pvalue)
                raise APIUserException(msg)
            if pname in permissions:
                permissions[pname] = pvalue
            else:
                msg = 'Permission `{}` is not valid.'.format(pname)
                raise APIUserException(msg)
        self.permissions = ujson.encode(permissions)
        return permissions

    def set_permissions_to_value(self, permissions, value):
        """
        Set certain permissions to the given `value` for this APIUser.

        `permissions` is a list of permissions to set to the supplied
        boolean value.

        Returns a dictionary of all user permissions after the update.
        """
        return self.update_permissions({pname: value for pname in permissions})

    def set_all_permissions_to_value(self, value):
        """
        Set ALL permissions for this APIUser to the given value.

        Returns a dictionary of all user permissions after the update.
        """
        permissions = list(self.permission_defaults.keys())
        return self.set_permissions_to_value(permissions, value)

    @staticmethod
    def encode_secret(secret, hash_type='sha256'):
        """
        Make and return a `secret` string for this APIUser.

        `hash_type` should be a string representing the hashing
        algorithm to use: md5, sha1, sha224, sha256, sha384, sha512.
        """
        valid_hashes = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
        if hash_type not in valid_hashes:
            raise APIUserException('Provided hash_type argument must be one '
                                   'of: {}'.format(', '.join(valid_hashes)))

        hasher = getattr(hashlib, hash_type)(secret.encode())
        return hasher.hexdigest()

    class Meta:
        app_label = 'api'
