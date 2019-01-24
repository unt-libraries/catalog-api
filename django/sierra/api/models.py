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

    def __init__(self, *args, **kwargs):
        super(APIUser, self).__init__(*args, **kwargs)
        my_perms = ujson.decode(self.permissions)
        all_perms = type(self).permission_defaults.copy()
        all_perms.update(my_perms)
        self.permissions_dict = all_perms

    def save(self, *args, **kwargs):
        self.permissions = ujson.encode(self.permissions_dict)
        super(APIUser, self).save(*args, **kwargs)        

    def set_permissions(self, permissions, value):
        """
        Set certain permissions to the given `value` for this APIUser.
        
        `permissions` is a list of permissions to set to the supplied
        boolean value.
        """
        for perm in permissions:
            if perm in self.permissions_dict:
                self.permissions_dict[perm] = value

    def set_all_permissions(self, value):
        """
        Set ALL permissions for this APIUser to the given value.
        """
        self.set_permissions(self.permissions_dict.keys(), value)

    def make_secret(self, secret, hash_type='sha256'):
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
