"""
Contains models for api app.
"""
import hashlib
import importlib

import ujson

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.contrib.auth.models import User

from api.managers import APIUserManager


def get_permission_defaults_from_apps():
    """
    Utility function that gathers permissions and default values from
    apps that are configured in settings.API_PERMISSIONS.
    """
    permission_defaults = {}
    for app_name in settings.API_PERMISSIONS:
        perm = importlib.import_module('{}.permissions'.format(app_name))
        permission_defaults.update(perm.DEFAULTS)
    return permission_defaults


class APIUserException(Exception):
    pass


class APIUser(models.Model):
    """
    Provides fields/features for secrets and permissions.
    """
    user = models.OneToOneField(User)
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
        permissions_dict = kwargs.pop('permissions_dict', {})
        secret_text = kwargs.pop('secret_text', None)
        super(APIUser, self).__init__(*args, **kwargs)
        self.update_permissions(permissions_dict)
        if not self.secret and secret_text is not None:
            self.secret = self.encode_secret(secret_text)

    def save(self, *args, **kwargs):
        """
        An APIUser MUST have a `secret` and a `user` relation before
        the object is saved; otherwise, an APIUserException is raised.
        """
        if not self.secret:
            msg = 'APIUser obj cannot be saved without a `secret`.'
            raise APIUserException(msg)
        try:
            self.user
        except User.DoesNotExist:
            msg = 'APIUser obj cannot be saved without a related user.'
            raise APIUserException(msg)
        super(APIUser, self).save(*args, **kwargs)

    def update_and_save(secret_text=None, permissions_dict=None, email=None,
                        password=None, first_name=None, last_name=None):
        """
        Update AND SAVE an existing APIUser with any or all new values.
        For any of `email`, `password`, `first_name`, and `last_name`,
        the related User object is updated with the appropriate
        value(s).
        """
        user_kwargs = {'password': password, 'email': email,
                       'first_name': first_name, 'last_name': last_name}
        if any([v is not None for v in user_kwargs.values()]):
            for field, value in user_kwargs.items():
                if value is not None:
                    setattr(self.user, field, value)
            self.user.save()
        if secret_text:
            self.secret = self.encode_secret(secret_text)
        if permissions_dict:
            self.update_permissions(permissions_dict)
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
        permissions = self.permission_defaults.keys()
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

        hasher = getattr(hashlib, hash_type)(secret)
        return hasher.hexdigest()

    class Meta:
        app_label = 'api'
