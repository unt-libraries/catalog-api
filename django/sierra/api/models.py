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


ALL_PERMISSIONS = {}
for app_name in settings.API_PERMISSIONS:
    app = importlib.import_module(app_name)
    ALL_PERMISSIONS.update(app.permissions)


class APIUserException(Exception):
    pass


class APIUser(models.Model):
    """
    API User model that provides a 'secret' field.
    """
    user = models.OneToOneField(User)
    secret = models.CharField(max_length=128)
    permissions = models.TextField(default=ujson.encode(ALL_PERMISSIONS))

    def set_secret(self, secret, hash_type='sha256'):
        valid_hashes = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
        if hash_type not in valid_hashes:
            raise APIUserException('Provided hash_type argument must be one '
                                   'of: {}'.format(', '.join(valid_hashes)))

        hasher = getattr(hashlib, hash_type)(secret)
        secret = hasher.hexdigest()
        self.secret = secret
        self.save()
        return secret

    def set_permissions(self, permissions=None, default=False):
        set_perms = {} if permissions is None else permissions
        my_perms = ujson.decode(self.permissions)
        new_perms = {}
        for perm in ALL_PERMISSIONS:
            if type(set_perms) == bool:
                new_perms[perm] = set_perms
            else:
                new_perms[perm] = set_perms.get(perm, my_perms.get(perm, 
                                        ALL_PERMISSIONS.get(perm, default)))
        self.permissions = ujson.encode(new_perms)
        self.save()
        return permissions

    class Meta:
        app_label = 'api'
